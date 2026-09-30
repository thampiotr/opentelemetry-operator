// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package allocation

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/target"
)

func TestLoadSheddingConfig(t *testing.T) {
	a := testSheddingAllocator(t)
	require.Equal(t, LoadSheddingStrategyConfig{Interval: 5 * time.Second, Window: 6, DefaultSize: 1000}, a.strategy.config)
	require.Contains(t, GetRegisteredAllocatorNames(), loadSheddingStrategyName)
	for _, config := range []LoadSheddingStrategyConfig{
		{Interval: -1}, {Window: -1}, {Window: 121}, {DefaultSize: -1}, {DefaultSize: math.NaN()}, {DefaultSize: math.Inf(1)},
	} {
		_, err := New(loadSheddingStrategyName, logr.Discard(), WithStrategyConfig(StrategyConfig{LoadShedding: config}))
		require.Error(t, err)
	}
	_, err := New(perNodeStrategyName, logr.Discard(), WithStrategyConfig(StrategyConfig{PerNode: PerNodeStrategyConfig{FallbackStrategy: &FallbackStrategyConfig{Name: loadSheddingStrategyName}}}))
	require.ErrorContains(t, err, "cannot be used as a per-target fallback")
}

func TestLoadSheddingPublication(t *testing.T) {
	a := testSheddingAllocator(t)
	first := sheddingItem("job-a", "host:80")
	second := sheddingItem("job-b", "host:80")
	a.SetTargets([]*target.Item{first, second})
	a.SetCollectors(map[string]*Collector{"old": NewCollector("old", "")})
	require.Empty(t, a.TargetItems())
	a.cycle()
	old := a.TargetItems()
	require.Len(t, old, 2)
	require.Equal(t, "old", old[first.Hash()].CollectorName)
	require.Equal(t, 2, a.Collectors()["old"].NumTargets)
	require.Len(t, a.GetTargetsForCollectorAndJob("old", "job-a"), 1)
	require.Empty(t, first.CollectorName, "must not mutate discovery inputs")
	require.Equal(t, float64(1000), a.window.cycles[0][first.Hash()])

	// Coalesce intermediate membership and target lists, using only the latest at the boundary.
	a.SetCollectors(map[string]*Collector{"intermediate": NewCollector("intermediate", "")})
	a.SetCollectors(map[string]*Collector{"new": NewCollector("new", "")})
	a.SetTargets([]*target.Item{second})
	a.cycle()
	require.Equal(t, "old", old[first.Hash()].CollectorName, "published pointers must stay immutable")
	require.Len(t, a.TargetItems(), 1)
	require.Equal(t, "new", a.TargetItems()[second.Hash()].CollectorName)
	require.Empty(t, a.GetTargetsForCollectorAndJob("old", "job-b"))
	require.Equal(t, 1, a.Collectors()["new"].TargetsPerJob["job-b"])
	a.SetCollectors(nil)
	a.cycle()
	require.Empty(t, a.TargetItems()[second.Hash()].CollectorName)
	require.Empty(t, a.Collectors())
}

func TestLoadSheddingReports(t *testing.T) {
	a := testSheddingAllocator(t)
	item := sheddingItem("job", "host:80")
	id := item.Hash()
	a.SetTargets([]*target.Item{item})
	n, err := a.ReportSizes(map[target.ItemHash]float64{id: 20, 0: 30})
	require.NoError(t, err)
	require.Equal(t, 1, n)
	a.cycle()
	require.Equal(t, float64(20), a.window.cycles[0][id])
	for _, invalid := range []float64{0, -1, math.NaN(), math.Inf(1), 1e13} {
		_, err = a.ReportSizes(map[target.ItemHash]float64{id: 30, 0: invalid})
		require.Error(t, err)
		require.Equal(t, float64(20), a.estimates[id], "reject invalid batches atomically")
	}
	a.SetTargets(nil)
	require.Empty(t, a.estimates)
	for range 12 {
		a.cycle()
	}
	require.Len(t, a.window.cycles, 6)
	require.Len(t, a.window.totals, 6)
	for _, cycle := range a.window.cycles {
		require.Empty(t, cycle)
	}
}

func TestLoadSheddingConcurrentReaders(t *testing.T) {
	a := testSheddingAllocator(t)
	item := sheddingItem("job", "host:80")
	a.SetCollectors(map[string]*Collector{"node": NewCollector("node", "")})
	a.SetTargets([]*target.Item{item})
	a.cycle()
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 100 {
			for _, item := range a.TargetItems() {
				_ = item.CollectorName
			}
			for _, collector := range a.Collectors() {
				_ = collector.NumTargets
				_ = collector.TargetsPerJob["job"]
			}
			for _, item := range a.GetTargetsForCollectorAndJob("node", "job") {
				_ = item.CollectorName
			}
		}
	})
	wg.Go(func() {
		for range 100 {
			a.SetTargets([]*target.Item{item})
			_, _ = a.ReportSizes(map[target.ItemHash]float64{item.Hash(): 10})
		}
	})
	for range 100 {
		a.cycle()
	}
	wg.Wait()
}

func TestLoadSheddingRun(t *testing.T) {
	a := testSheddingAllocator(t)
	a.strategy.config.Interval = 10 * time.Millisecond
	a.SetTargets([]*target.Item{sheddingItem("job", "host:80")})
	a.SetCollectors(map[string]*Collector{"node": NewCollector("node", "")})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	require.Eventually(t, func() bool { return len(a.GetTargetsForCollectorAndJob("node", "job")) == 1 }, time.Second, 10*time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("allocator did not stop")
	}
}

func testSheddingAllocator(t *testing.T) *loadSheddingAllocator {
	t.Helper()
	a, err := New(loadSheddingStrategyName, logr.Discard())
	require.NoError(t, err)
	return a.(*loadSheddingAllocator)
}

func sheddingItem(job, address string) *target.Item {
	ls := labels.FromStrings("__address__", address)
	return target.NewItem(job, address, ls, "", target.HashLabels(ls, job))
}
