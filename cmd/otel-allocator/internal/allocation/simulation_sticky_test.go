// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package allocation

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/target"
)

// Assignment history is unbounded; only measurements and overload detection use K.
type simSticky struct{ strategy *stickyStrategy }

func newSimSticky() *simSticky {
	return &simSticky{strategy: &stickyStrategy{owners: map[target.ItemHash]string{}, overloaded: map[string]int{}}}
}

func (s *simSticky) allocate(t *testing.T, history []simSnapshot, k int) simAllocation {
	t.Helper()
	a := windowMeasurements(t, history, k)
	latest := history[len(history)-1]
	items := map[target.ItemHash]*target.Item{}
	sizes := map[target.ItemHash]float64{}
	collectors := map[string]*Collector{}
	for _, name := range latest.collectors {
		collectors[name] = NewCollector(name, "")
	}
	for _, item := range latest.targets {
		id := target.ItemHash((hasher{}).Sum64([]byte(item.address)))
		items[id] = &target.Item{TargetURL: item.address}
		sizes[id] = float64(a.sizes[item.address])
	}
	s.strategy.config.Window = k
	p := s.strategy.plan(items, collectors, sizes, float64(a.total))
	for id, owner := range p.owners {
		a.owners[items[id].TargetURL] = owner
	}
	for name, load := range p.loads {
		a.loads[name] = int(load)
	}
	if len(collectors) > 0 {
		a.capacity = 1.2 * float64(a.total) / float64(len(collectors))
	}
	return a
}

func TestSimulatedAllocation_Sticky(t *testing.T) {
	t.Run("deterministic_initial_placement", func(t *testing.T) {
		snapshot := simSnapshot{simTargets(60), []string{"a", "b", "c"}}
		a := newSimSticky().allocate(t, []simSnapshot{snapshot}, simWindow)
		slices.Reverse(snapshot.targets)
		slices.Reverse(snapshot.collectors)
		require.Equal(t, a, newSimSticky().allocate(t, []simSnapshot{snapshot}, simWindow))
		require.Equal(t, map[string]int{"a": 200, "b": 200, "c": 200}, a.loads)
	})
	t.Run("unrelated_churn_preserves_established_targets", func(t *testing.T) {
		s := newSimSticky()
		base := simSnapshot{simTargets(60), []string{"a", "b", "c"}}
		original := s.allocate(t, []simSnapshot{base}, simWindow).owners
		history := []simSnapshot{base}
		for cycle := range 60 {
			current := cloneSimulation(base)
			if cycle%2 == 0 {
				current.targets = append(current.targets, simulatedTarget{"temporary", 10})
			}
			history = append(history, current)
			a := s.allocate(t, history, simWindow)
			require.Zero(t, countMoves(original, a.owners))
		}
	})
	t.Run("departure_moves_only_orphans_immediately", func(t *testing.T) {
		s := newSimSticky()
		snapshot := simSnapshot{simTargets(60), []string{"a", "b", "c"}}
		before := s.allocate(t, []simSnapshot{snapshot}, simWindow).owners
		snapshot.collectors = []string{"a", "b"}
		after := s.allocate(t, []simSnapshot{snapshot}, simWindow).owners
		for target, owner := range before {
			require.Contains(t, snapshot.collectors, after[target])
			if owner != "c" {
				require.Equal(t, owner, after[target])
			}
		}
	})
	t.Run("join_waits_for_sustained_overload_then_settles", func(t *testing.T) {
		s := newSimSticky()
		snapshot := simSnapshot{simTargets(60), []string{"a", "b"}}
		history := []simSnapshot{snapshot}
		previous := s.allocate(t, history, simWindow).owners
		snapshot.collectors = []string{"a", "b", "c"}
		for cycle := range 30 {
			history = append(history, snapshot)
			a := s.allocate(t, history, simWindow)
			if cycle < simWindow-1 || cycle >= simWindow {
				require.Zero(t, countMoves(previous, a.owners))
			}
			if cycle >= simWindow-1 {
				for _, load := range a.loads {
					require.LessOrEqual(t, float64(load), a.capacity)
				}
			}
			previous = a.owners
		}
	})
	t.Run("empty_membership", func(t *testing.T) {
		s := newSimSticky()
		require.Empty(t, s.allocate(t, []simSnapshot{{simTargets(3), nil}}, simWindow).owners)
	})
}

func simulateSticky(t *testing.T, targets []simulatedTarget, collectors ...string) map[string]string {
	t.Helper()
	return newSimSticky().allocate(t, []simSnapshot{{targets, collectors}}, simWindow).owners
}
