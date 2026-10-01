// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package allocation

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// Assignment history is unbounded; only measurements and overload detection use K.
type simSticky struct {
	owners     map[string]string
	overloaded map[string]int
}

func newSimSticky() *simSticky {
	return &simSticky{owners: map[string]string{}, overloaded: map[string]int{}}
}

func (s *simSticky) allocate(t *testing.T, history []simSnapshot, k int) simAllocation {
	t.Helper()
	a := windowMeasurements(t, history, k)
	latest := history[len(history)-1]
	names := slices.Clone(latest.collectors)
	slices.Sort(names)
	for _, name := range names {
		a.loads[name] = 0
	}
	if len(names) == 0 {
		s.owners = a.owners
		clear(s.overloaded)
		return a
	}
	a.capacity = 1.2 * float64(a.total) / float64(len(names))
	targets := slices.Clone(latest.targets)
	slices.SortFunc(targets, func(x, y simulatedTarget) int {
		if a.sizes[x.address] != a.sizes[y.address] {
			return a.sizes[y.address] - a.sizes[x.address]
		}
		return hashOrder(x.address, y.address)
	})
	var pending []simulatedTarget
	for _, target := range targets {
		owner, exists := s.owners[target.address]
		if _, alive := a.loads[owner]; exists && alive {
			a.owners[target.address] = owner
			a.loads[owner] += a.sizes[target.address]
		} else {
			pending = append(pending, target)
		}
	}
	for _, target := range pending {
		owner := stickyLeastLoaded(names, a.loads)
		a.owners[target.address] = owner
		a.loads[owner] += a.sizes[target.address]
	}
	for name := range s.overloaded {
		if _, alive := a.loads[name]; !alive {
			delete(s.overloaded, name)
		}
	}
	// Count consecutive overloaded cycles before disturbing established targets.
	for _, name := range names {
		if float64(a.loads[name]) > a.capacity {
			s.overloaded[name] = min(k, s.overloaded[name]+1)
		} else {
			s.overloaded[name] = 0
		}
	}
	donors := slices.Clone(names)
	slices.SortFunc(donors, func(x, y string) int {
		if a.loads[x] != a.loads[y] {
			return a.loads[y] - a.loads[x]
		}
		return hashOrder(x, y)
	})
	for _, donor := range donors {
		if s.overloaded[donor] < k {
			continue
		}
		for _, target := range targets {
			if float64(a.loads[donor]) <= a.capacity {
				break
			}
			if a.owners[target.address] != donor {
				continue
			}
			receiver := stickyLeastLoaded(names, a.loads)
			size := a.sizes[target.address]
			// Each move reduces the pair's peak; receivers stay below 110% of ideal.
			if receiver == donor || float64(a.loads[receiver]+size) > a.capacity*1.1/1.2 || a.loads[receiver]+size >= a.loads[donor] {
				continue
			}
			a.owners[target.address] = receiver
			a.loads[donor] -= size
			a.loads[receiver] += size
		}
	}
	s.owners = a.owners
	return a
}

func stickyLeastLoaded(names []string, loads map[string]int) string {
	owner := names[0]
	for _, name := range names[1:] {
		if loads[name] < loads[owner] {
			owner = name
		}
	}
	return owner
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
