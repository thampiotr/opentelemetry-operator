// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package allocation

import (
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/target"
)

func TestStickyPublication(t *testing.T) {
	allocator, err := New(stickyStrategyName, logr.Discard(), WithStrategyConfig(StrategyConfig{Sticky: SizeAwareStrategyConfig{DefaultSize: 10}}))
	require.NoError(t, err)
	a := allocator.(*sizeAwareAllocator)
	require.Equal(t, 6, a.config.Window)
	require.Contains(t, GetRegisteredAllocatorNames(), stickyStrategyName)
	items := []*target.Item{sheddingItem("job", "one:80"), sheddingItem("job", "two:80"), sheddingItem("job", "three:80")}
	a.SetTargets(items)
	a.SetCollectors(map[string]*Collector{"a": NewCollector("a", ""), "b": NewCollector("b", "")})
	a.cycle()
	first := a.Inspect()
	require.Equal(t, stickyStrategyName, first.Strategy)
	require.Len(t, first.Targets, 3)
	for id, item := range first.Targets {
		require.Contains(t, first.Collectors, item.CollectorName)
		require.Equal(t, 10.0, first.Sizes[id])
	}
	// Losing an owner reassigns its targets in the next publication, without
	// waiting for the overload delay or a successful size probe.
	a.SetCollectors(map[string]*Collector{"b": NewCollector("b", "")})
	a.cycle()
	for _, item := range a.TargetItems() {
		require.Equal(t, "b", item.CollectorName)
	}
	require.Len(t, a.GetTargetsForCollectorAndJob("b", "job"), 3)
	a.SetTargets(items[:1])
	a.cycle()
	require.Len(t, a.strategy.(*stickyStrategy).owners, 1)
	require.Len(t, a.GetTargetsForCollectorAndJob("b", "job"), 1)
	require.Len(t, first.Targets, 3)
	a.SetCollectors(nil)
	a.cycle()
	require.Empty(t, a.strategy.(*stickyStrategy).owners)
}

func TestStickyConfiguration(t *testing.T) {
	_, err := New(stickyStrategyName, logr.Discard(), WithStrategyConfig(StrategyConfig{Sticky: SizeAwareStrategyConfig{Window: -1}}))
	require.ErrorContains(t, err, "sticky requires")
	_, err = New(perNodeStrategyName, logr.Discard(), WithStrategyConfig(StrategyConfig{PerNode: PerNodeStrategyConfig{FallbackStrategy: &FallbackStrategyConfig{Name: stickyStrategyName}}}))
	require.ErrorContains(t, err, "cannot be used as a per-target fallback")
}

func TestAssignmentMoves(t *testing.T) {
	previous := allocationSnapshot{targets: map[target.ItemHash]*target.Item{
		1: {CollectorName: "a"}, 2: {CollectorName: "b"}, 3: {CollectorName: "a"}, 4: {CollectorName: ""}, 5: {CollectorName: "a"},
	}}
	next := allocationSnapshot{targets: map[target.ItemHash]*target.Item{
		1: {CollectorName: "b"}, 2: {CollectorName: "b"}, 4: {CollectorName: "a"}, 5: {CollectorName: ""}, 6: {CollectorName: "a"},
	}}
	require.Equal(t, int64(1), assignmentMoves(previous, next))
	require.Zero(t, assignmentMoves(allocationSnapshot{}, next))
	require.Zero(t, assignmentMoves(previous, allocationSnapshot{}))
}
