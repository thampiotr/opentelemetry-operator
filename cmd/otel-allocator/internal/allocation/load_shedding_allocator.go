// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package allocation

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/target"
)

// LoadSheddingStrategyConfig configures the size window and recomputation interval.
type LoadSheddingStrategyConfig struct {
	Interval         time.Duration
	Window           int
	DefaultSize      float64
	ProbeInterval    time.Duration
	ProbeTimeout     time.Duration
	ProbeConcurrency int
	ProbeMaxBytes    int64
}

func newLoadSheddingStrategy(config StrategyConfig) (allocationStrategy, error) {
	c := config.LoadShedding
	if c.Interval == 0 {
		c.Interval = 5 * time.Second
	}
	if c.Window == 0 {
		c.Window = 6
	}
	if c.DefaultSize == 0 {
		c.DefaultSize = 1000
	}
	if c.Interval < time.Millisecond || c.Window < 1 || c.Window > 120 || !validSize(c.DefaultSize) {
		return nil, fmt.Errorf("load-shedding requires interval >= 1ms, window between 1 and 120, and default_size in (0, 1e12]")
	}
	if c.ProbeInterval == 0 {
		c.ProbeInterval = 30 * time.Second
	}
	if c.ProbeTimeout == 0 {
		c.ProbeTimeout = 5 * time.Second
	}
	if c.ProbeConcurrency == 0 {
		c.ProbeConcurrency = 4
	}
	if c.ProbeMaxBytes == 0 {
		c.ProbeMaxBytes = 64 << 20
	}
	if c.ProbeInterval < time.Millisecond || c.ProbeTimeout < time.Millisecond || c.ProbeConcurrency < 1 || c.ProbeConcurrency > 64 || c.ProbeMaxBytes < 1 || c.ProbeMaxBytes > 1<<30 {
		return nil, fmt.Errorf("load-shedding requires probe durations >= 1ms, concurrency between 1 and 64, and max bytes between 1 and 1GiB")
	}
	return &loadSheddingStrategy{config: c}, nil
}

// Runner is implemented by allocators which need a background recomputation loop.
type Runner interface {
	Run(context.Context) error
}

type loadSheddingAllocator struct {
	strategy   *loadSheddingStrategy
	log        logr.Logger
	probeJobs  map[string]*probeJob
	probes     map[target.ItemHash]*probeState
	probeWake  chan struct{}
	mu         sync.Mutex
	targets    map[target.ItemHash]*target.Item
	collectors map[string]*Collector
	estimates  map[target.ItemHash]float64
	window     sizeWindow

	// Readers see immutable snapshots. Discovery and planning do not hold this lock.
	publishedMu         sync.RWMutex
	published           allocationSnapshot
	targetsPerCollector metric.Int64Gauge
	timeToAssign        metric.Float64Histogram
	unresolved          metric.Int64Gauge
}

type allocationSnapshot struct {
	targets    map[target.ItemHash]*target.Item
	collectors map[string]*Collector
	jobs       map[string]map[string][]*target.Item
}

var (
	_ Allocator            = (*loadSheddingAllocator)(nil)
	_ ScrapeConfigReceiver = (*loadSheddingAllocator)(nil)
	_ Runner               = (*loadSheddingAllocator)(nil)
)

func newLoadSheddingAllocator(log logr.Logger, strategy *loadSheddingStrategy) (Allocator, error) {
	meter := otel.GetMeterProvider().Meter("targetallocator")
	targets, err := meter.Int64Gauge("opentelemetry_allocator_targets_per_collector")
	if err != nil {
		return nil, err
	}
	duration, err := meter.Float64Histogram("opentelemetry_allocator_time_to_allocate")
	if err != nil {
		return nil, err
	}
	unresolved, err := meter.Int64Gauge("opentelemetry_allocator_overloaded_collectors", metric.WithDescription("Collectors still above the load-shedding capacity after allocation."))
	if err != nil {
		return nil, err
	}
	log.Info("Starting load-shedding allocation", "interval", strategy.config.Interval, "window", strategy.config.Window, "defaultSize", strategy.config.DefaultSize)
	return &loadSheddingAllocator{
		strategy:            strategy,
		log:                 log,
		probes:              make(map[target.ItemHash]*probeState),
		probeWake:           make(chan struct{}, 1),
		targets:             make(map[target.ItemHash]*target.Item),
		collectors:          make(map[string]*Collector),
		estimates:           make(map[target.ItemHash]float64),
		window:              sizeWindow{length: strategy.config.Window},
		published:           makeAllocationSnapshot(nil, nil, nil),
		targetsPerCollector: targets,
		timeToAssign:        duration,
		unresolved:          unresolved,
	}, nil
}

func (a *loadSheddingAllocator) SetTargets(items []*target.Item) {
	a.mu.Lock()
	defer a.mu.Unlock()
	next := make(map[target.ItemHash]*target.Item, len(items))
	for _, item := range items {
		itemCopy := *item
		next[item.Hash()] = &itemCopy
		if a.probes[item.Hash()] == nil {
			a.probes[item.Hash()] = &probeState{}
		}
	}
	a.targets = next
	for id := range a.probes {
		if _, exists := next[id]; !exists {
			delete(a.probes, id)
		}
	}
	a.wakeProbes()
	for id := range a.estimates {
		if _, exists := next[id]; !exists {
			delete(a.estimates, id)
		}
	}
}

func (a *loadSheddingAllocator) SetCollectors(collectors map[string]*Collector) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.collectors = make(map[string]*Collector, len(collectors))
	for name, collector := range collectors {
		a.collectors[name] = NewCollector(name, collector.NodeName)
	}
}

func validSize(size float64) bool { return size > 0 && size <= 1e12 }

func (a *loadSheddingAllocator) Run(ctx context.Context) error {
	var workers sync.WaitGroup
	for range a.strategy.config.ProbeConcurrency {
		workers.Go(func() { a.runProber(ctx) })
	}
	defer func() {
		workers.Wait()
		a.mu.Lock()
		defer a.mu.Unlock()
		closeProbeJobs(a.probeJobs)
	}()

	interval := a.strategy.config.Interval
	for {
		// Align cycles approximately; this does not imply identical discovery across replicas.
		timer := time.NewTimer(time.Until(time.Now().Truncate(interval).Add(interval)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
			a.cycle()
		}
	}
}

func (a *loadSheddingAllocator) cycle() {
	begin := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	raw := make(map[target.ItemHash]float64, len(a.targets))
	for id := range a.targets {
		raw[id] = a.strategy.config.DefaultSize
		if size, ok := a.estimates[id]; ok {
			raw[id] = size
		}
	}
	sizes, total := a.window.observe(raw)
	plan := a.strategy.plan(a.targets, a.collectors, sizes, total)
	next := makeAllocationSnapshot(a.targets, a.collectors, plan.owners)
	a.publish(next)
	a.unresolved.Record(context.Background(), int64(plan.unresolved))
	a.timeToAssign.Record(context.Background(), time.Since(begin).Seconds(), metric.WithAttributes(attribute.String("strategy", loadSheddingStrategyName), attribute.String("method", "cycle")))
}

func makeAllocationSnapshot(items map[target.ItemHash]*target.Item, collectors map[string]*Collector, owners map[target.ItemHash]string) allocationSnapshot {
	s := allocationSnapshot{
		targets:    make(map[target.ItemHash]*target.Item, len(items)),
		collectors: make(map[string]*Collector, len(collectors)),
		jobs:       make(map[string]map[string][]*target.Item, len(collectors)),
	}
	for name, collector := range collectors {
		s.collectors[name] = NewCollector(name, collector.NodeName)
		s.jobs[name] = map[string][]*target.Item{}
	}
	for id, item := range items {
		itemCopy := *item
		itemCopy.CollectorName = owners[id]
		s.targets[id] = &itemCopy
		if collector := s.collectors[itemCopy.CollectorName]; collector != nil {
			collector.NumTargets++
			collector.TargetsPerJob[itemCopy.JobName]++
			s.jobs[itemCopy.CollectorName][itemCopy.JobName] = append(s.jobs[itemCopy.CollectorName][itemCopy.JobName], &itemCopy)
		}
	}
	return s
}

func (a *loadSheddingAllocator) publish(next allocationSnapshot) {
	a.publishedMu.Lock()
	previous := a.published
	a.published = next
	a.publishedMu.Unlock()
	for name := range previous.collectors {
		if _, exists := next.collectors[name]; !exists {
			a.recordCount(name, 0)
		}
	}
	for name, collector := range next.collectors {
		a.recordCount(name, collector.NumTargets)
	}
}

func (a *loadSheddingAllocator) recordCount(name string, count int) {
	a.targetsPerCollector.Record(context.Background(), int64(count), metric.WithAttributes(attribute.String("collector_name", name), attribute.String("strategy", loadSheddingStrategyName)))
}

func (a *loadSheddingAllocator) TargetItems() map[target.ItemHash]*target.Item {
	a.publishedMu.RLock()
	defer a.publishedMu.RUnlock()
	return maps.Clone(a.published.targets)
}

func (a *loadSheddingAllocator) Collectors() map[string]*Collector {
	a.publishedMu.RLock()
	defer a.publishedMu.RUnlock()
	return maps.Clone(a.published.collectors)
}

func (a *loadSheddingAllocator) GetTargetsForCollectorAndJob(collector, job string) []*target.Item {
	a.publishedMu.RLock()
	defer a.publishedMu.RUnlock()
	return append([]*target.Item(nil), a.published.jobs[collector][job]...)
}
