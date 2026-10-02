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

// SizeAwareStrategyConfig configures sizing and allocation cycles.
type SizeAwareStrategyConfig struct {
	Interval         time.Duration
	Window           int
	DefaultSize      float64
	ProbeInterval    time.Duration
	ProbeTimeout     time.Duration
	ProbeConcurrency int
	ProbeMaxBytes    int64
}

func sizeAwareConfig(name string, c SizeAwareStrategyConfig) (SizeAwareStrategyConfig, error) {
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
		return c, fmt.Errorf("%s requires interval >= 1ms, window between 1 and 120, and default_size in (0, 1e12]", name)
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
		return c, fmt.Errorf("%s requires probe durations >= 1ms, concurrency between 1 and 64, and max bytes between 1 and 1GiB", name)
	}
	return c, nil
}

// Runner is implemented by allocators which need a background recomputation loop.
type Runner interface {
	Run(context.Context) error
}

type sizeAwareAllocator struct {
	strategy   sizeAwareStrategy
	config     SizeAwareStrategyConfig
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
	sizePerCollector    metric.Float64Gauge
	moves               metric.Int64Counter
	timeToAssign        metric.Float64Histogram
	unresolved          metric.Int64Gauge
}

type allocationSnapshot struct {
	sizes      map[target.ItemHash]float64
	updated    time.Time
	targets    map[target.ItemHash]*target.Item
	collectors map[string]*Collector
	jobs       map[string]map[string][]*target.Item
}

var (
	_ Allocator            = (*sizeAwareAllocator)(nil)
	_ ScrapeConfigReceiver = (*sizeAwareAllocator)(nil)
	_ Runner               = (*sizeAwareAllocator)(nil)
)

func newSizeAwareAllocator(log logr.Logger, strategy sizeAwareStrategy) (Allocator, error) {
	meter := otel.GetMeterProvider().Meter("targetallocator")
	targets, err := meter.Int64Gauge("opentelemetry_allocator_targets_per_collector")
	if err != nil {
		return nil, err
	}
	size, err := meter.Float64Gauge("opentelemetry_allocator_estimated_series_per_collector")
	if err != nil {
		return nil, err
	}
	moves, err := meter.Int64Counter("opentelemetry_allocator_target_moves", metric.WithDescription("Owner changes for retained, assigned targets; excludes initial placement and removals."))
	if err != nil {
		return nil, err
	}
	duration, err := meter.Float64Histogram("opentelemetry_allocator_time_to_allocate")
	if err != nil {
		return nil, err
	}
	unresolved, err := meter.Int64Gauge("opentelemetry_allocator_overloaded_collectors", metric.WithDescription("Collectors still above capacity after allocation."))
	if err != nil {
		return nil, err
	}
	log.Info("Starting size-aware allocation", "strategy", strategy.GetName(), "interval", strategy.configuration().Interval, "window", strategy.configuration().Window, "defaultSize", strategy.configuration().DefaultSize)
	return &sizeAwareAllocator{
		strategy:            strategy,
		config:              strategy.configuration(),
		log:                 log,
		probes:              make(map[target.ItemHash]*probeState),
		probeWake:           make(chan struct{}, 1),
		targets:             make(map[target.ItemHash]*target.Item),
		collectors:          make(map[string]*Collector),
		estimates:           make(map[target.ItemHash]float64),
		window:              sizeWindow{length: strategy.configuration().Window},
		published:           makeAllocationSnapshot(nil, nil, nil),
		targetsPerCollector: targets,
		sizePerCollector:    size,
		moves:               moves,
		timeToAssign:        duration,
		unresolved:          unresolved,
	}, nil
}

func (a *sizeAwareAllocator) SetTargets(items []*target.Item) {
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

func (a *sizeAwareAllocator) SetCollectors(collectors map[string]*Collector) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.collectors = make(map[string]*Collector, len(collectors))
	for name, collector := range collectors {
		a.collectors[name] = NewCollector(name, collector.NodeName)
	}
}

func validSize(size float64) bool { return size > 0 && size <= 1e12 }

func (a *sizeAwareAllocator) Run(ctx context.Context) error {
	var workers sync.WaitGroup
	for range a.config.ProbeConcurrency {
		workers.Go(func() { a.runProber(ctx) })
	}
	defer func() {
		workers.Wait()
		a.mu.Lock()
		defer a.mu.Unlock()
		closeProbeJobs(a.probeJobs)
	}()

	interval := a.config.Interval
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

func (a *sizeAwareAllocator) cycle() {
	begin := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	raw := make(map[target.ItemHash]float64, len(a.targets))
	for id := range a.targets {
		raw[id] = a.config.DefaultSize
		if size, ok := a.estimates[id]; ok {
			raw[id] = size
		}
	}
	sizes, total := a.window.observe(raw)
	plan := a.strategy.plan(a.targets, a.collectors, sizes, total)
	next := makeAllocationSnapshot(a.targets, a.collectors, plan.owners)
	next.sizes, next.updated = sizes, time.Now()
	a.publish(next)
	a.unresolved.Record(context.Background(), int64(plan.unresolved))
	a.timeToAssign.Record(context.Background(), time.Since(begin).Seconds(), metric.WithAttributes(attribute.String("strategy", a.strategy.GetName()), attribute.String("method", "cycle")))
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

func (a *sizeAwareAllocator) publish(next allocationSnapshot) {
	a.publishedMu.Lock()
	previous := a.published
	a.published = next
	a.publishedMu.Unlock()
	a.moves.Add(context.Background(), assignmentMoves(previous, next), metric.WithAttributes(attribute.String("strategy", a.strategy.GetName())))
	loads := make(map[string]float64, len(next.collectors))
	for id, item := range next.targets {
		loads[item.CollectorName] += next.sizes[id]
	}
	for name := range previous.collectors {
		if _, exists := next.collectors[name]; !exists {
			loads[name] = 0
		}
	}
	for name := range next.collectors {
		if _, exists := loads[name]; !exists {
			loads[name] = 0
		}
	}
	for name, size := range loads {
		if name != "" {
			a.sizePerCollector.Record(context.Background(), size, metric.WithAttributes(attribute.String("collector_name", name), attribute.String("strategy", a.strategy.GetName())))
		}
	}
	for name := range previous.collectors {
		if _, exists := next.collectors[name]; !exists {
			a.recordCount(name, 0)
		}
	}
	for name, collector := range next.collectors {
		a.recordCount(name, collector.NumTargets)
	}
}

func (a *sizeAwareAllocator) recordCount(name string, count int) {
	a.targetsPerCollector.Record(context.Background(), int64(count), metric.WithAttributes(attribute.String("collector_name", name), attribute.String("strategy", a.strategy.GetName())))
}

func (a *sizeAwareAllocator) TargetItems() map[target.ItemHash]*target.Item {
	a.publishedMu.RLock()
	defer a.publishedMu.RUnlock()
	return maps.Clone(a.published.targets)
}

func (a *sizeAwareAllocator) Collectors() map[string]*Collector {
	a.publishedMu.RLock()
	defer a.publishedMu.RUnlock()
	return maps.Clone(a.published.collectors)
}

func (a *sizeAwareAllocator) GetTargetsForCollectorAndJob(collector, job string) []*target.Item {
	a.publishedMu.RLock()
	defer a.publishedMu.RUnlock()
	return append([]*target.Item(nil), a.published.jobs[collector][job]...)
}

// sizeAwareStrategy plans a complete assignment under the allocator's state lock.
type sizeAwareStrategy interface {
	allocationStrategy
	configuration() SizeAwareStrategyConfig
	plan(map[target.ItemHash]*target.Item, map[string]*Collector, map[target.ItemHash]float64, float64) allocationPlan
}

type allocationPlan struct {
	owners     map[target.ItemHash]string
	loads      map[string]float64
	unresolved int
}

type weightedTarget struct {
	id   target.ItemHash
	key  string
	size float64
}

func assignmentMoves(previous, next allocationSnapshot) int64 {
	var moves int64
	for id, item := range next.targets {
		old, exists := previous.targets[id]
		if exists && old.CollectorName != "" && item.CollectorName != "" && old.CollectorName != item.CollectorName {
			moves++
		}
	}
	return moves
}
