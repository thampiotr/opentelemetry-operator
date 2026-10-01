// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package allocation

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/target"
)

// Common simulation tests.

func TestSimulatedAllocation_Common(t *testing.T) {
	for _, strategy := range []struct {
		name   string
		assign func(*testing.T, []simulatedTarget, ...string) map[string]string
	}{
		{"ConsistentHashing", simulateHashing},
		{"CapacityAware", simulateCapacityAware},
		{"LoadShedding", simulateLoadShedding},
		{"Sticky", simulateSticky},
	} {
		t.Run(strategy.name, func(t *testing.T) {
			t.Run("valid_assignments", func(t *testing.T) {
				for _, tc := range []struct {
					name       string
					targets    []simulatedTarget
					collectors []string
				}{
					{"empty_targets", nil, []string{"alloy-0"}},
					{"one_collector", simTargets(27), []string{"alloy-0"}},
					{"multiple_collectors", simTargets(1000), []string{"alloy-0", "alloy-1", "alloy-2"}},
				} {
					t.Run(tc.name, func(t *testing.T) {
						owners := strategy.assign(t, tc.targets, tc.collectors...)
						require.Len(t, owners, len(tc.targets))
						for _, item := range tc.targets {
							require.Contains(t, owners, item.address)
							require.Contains(t, tc.collectors, owners[item.address])
						}
					})
				}
			})
			t.Run("balance", func(t *testing.T) {
				testBalance(t, strategy.assign)
			})
		})
	}
}

const simWindow = 6

type simSnapshot struct {
	targets    []simulatedTarget
	collectors []string
}

type simAllocation struct {
	owners     map[string]string
	sizes      map[string]int
	loads      map[string]int
	total      int
	capacity   float64
	fallbacks  int
	unresolved int
}

func windowSizes(history []simSnapshot, cycle, k int) map[string]int {
	sizes := map[string]int{}
	for _, item := range history[cycle].targets {
		sizes[item.address] = item.series
	}
	for _, snapshot := range history[max(0, cycle-k+1) : cycle+1] {
		for _, item := range snapshot.targets {
			if size, current := sizes[item.address]; current {
				sizes[item.address] = max(size, item.series)
			}
		}
	}
	return sizes
}

type simulatedTarget struct {
	address string
	series  int
}

func testBalance(t *testing.T, assign func(*testing.T, []simulatedTarget, ...string) map[string]string) {
	for _, skewed := range []bool{false, true} {
		t.Run(fmt.Sprintf("skewed=%t", skewed), func(t *testing.T) {
			targets := simTargets(27)
			if skewed {
				for i := range 3 {
					targets[i].series = 800
				}
			}
			owners := assign(t, targets, "alloy-0", "alloy-1", "alloy-2")
			loads := map[string]int{"alloy-0": 0, "alloy-1": 0, "alloy-2": 0}
			total, largest, maximum := 0, 0, 0
			for _, item := range targets {
				loads[owners[item.address]] += item.series
				total += item.series
				largest = max(largest, item.series)
			}
			for _, load := range loads {
				maximum = max(maximum, load)
			}
			mean := float64(total) / float64(len(loads))
			// Balance thresholds belong in strategy-specific tests.
			t.Logf("loads=%v max/mean=%.3f max-load lower bound=%.0f", loads, float64(maximum)/mean, max(mean, float64(largest)))
		})
	}
}

func simTargets(count int) []simulatedTarget {
	result := make([]simulatedTarget, count)
	for i := range result {
		result[i] = simulatedTarget{address: fmt.Sprintf("target-%04d:8080", i), series: 10}
	}
	return result
}

func countMoves(before, after map[string]string) int {
	moved := 0
	for address, owner := range before {
		if next, exists := after[address]; exists && next != owner {
			moved++
		}
	}
	return moved
}

func TestSimulatedAllocation_Scenarios(t *testing.T) {
	runSimulatedScenarios(t, scenarioSnapshot)
}

func TestSimulatedAllocation_Sensitivity(t *testing.T) {
	for _, variant := range []struct {
		name                                   string
		targets, collectors, whales, variation float64
	}{
		{"baseline", 1, 1, 1, 1},
		{"targets_half", .5, 1, 1, 1},
		{"targets_double", 2, 1, 1, 1},
		{"collectors_half", 1, .5, 1, 1},
		{"collectors_double", 1, 2, 1, 1},
		{"whales_half", 1, 1, .5, 1},
		{"whales_double", 1, 1, 2, 1},
		{"variation_half", 1, 1, 1, .5},
		{"variation_double", 1, 1, 1, 2},
	} {
		t.Run(variant.name, func(t *testing.T) {
			runSimulatedScenarios(t, func(scenario string, cycle int) simSnapshot {
				return scaledScenarioSnapshot(scenario, cycle, variant.targets, variant.collectors, variant.whales, variant.variation)
			})
		})
	}
}

func runSimulatedScenarios(t *testing.T, snapshotAt func(string, int) simSnapshot) {
	const cycles = 360 // Thirty simulated minutes at five seconds per cycle.
	for _, scenario := range []string{"targets", "sizes", "collectors", "combined"} {
		t.Run(scenario, func(t *testing.T) {
			for _, algorithm := range []string{"ConsistentHashing", "CapacityAware", "LoadShedding", "Sticky"} {
				t.Run(algorithm, func(t *testing.T) {
					var history []simSnapshot
					sticky := newSimSticky()
					var previous map[string]string
					priorOwner := map[string]string{}
					moves, opportunities, reversals, forced := 0, 0, 0, 0
					seriesMoveSum, seriesMoveWorst := 0.0, 0.0
					moveSum, moveWorst, eventSum, backgroundSum := 0.0, 0.0, 0.0, 0.0
					events, background := 0, 0
					ratioSum, ratioWorst, lowerBoundWorst := 0.0, 0.0, 0.0
					for cycle := range cycles {
						snapshot := snapshotAt(scenario, cycle)
						history = append(history, snapshot)
						history = history[max(0, len(history)-(2*simWindow-1)):]
						var owners map[string]string
						if algorithm == "ConsistentHashing" {
							owners = simulateHashing(t, snapshot.targets, snapshot.collectors...)
						} else if algorithm == "Sticky" {
							owners = sticky.allocate(t, history, simWindow).owners
						} else if algorithm == "LoadShedding" {
							owners = allocateLoadShedding(t, history, simWindow).owners
						} else {
							owners = allocateCapacityAware(t, history, simWindow).owners
						}
						require.Len(t, owners, len(snapshot.targets))
						loads := make(map[string]int, len(snapshot.collectors))
						for _, name := range snapshot.collectors {
							loads[name] = 0
						}
						cycleMoves, survivors, largest := 0, 0, 0
						movedSeries, survivingSeries := 0, 0
						for _, item := range snapshot.targets {
							owner, assigned := owners[item.address]
							_, valid := loads[owner]
							if !assigned || !valid {
								t.Fatalf("invalid assignment for %s: %q", item.address, owner)
							}
							loads[owner] += item.series
							largest = max(largest, item.series)
							if old, existed := previous[item.address]; existed {
								survivors++
								survivingSeries += item.series
								if old != owner {
									cycleMoves++
									movedSeries += item.series
									if _, stillPresent := loads[old]; !stillPresent {
										forced++
									}
									if priorOwner[item.address] == owner {
										reversals++
									}
									priorOwner[item.address] = old
								}
							}
						}
						for address := range priorOwner {
							if _, exists := owners[address]; !exists {
								delete(priorOwner, address)
							}
						}
						if cycle > 0 {
							require.Positive(t, survivors)
							seriesPercent := 100 * float64(movedSeries) / float64(survivingSeries)
							seriesMoveSum += seriesPercent
							seriesMoveWorst = max(seriesMoveWorst, seriesPercent)
							percent := 100 * float64(cycleMoves) / float64(survivors)
							moves += cycleMoves
							opportunities += survivors
							moveSum += percent
							moveWorst = max(moveWorst, percent)
							if cycle%30 == 0 {
								events++
								eventSum += percent
							} else {
								background++
								backgroundSum += percent
							}
						}
						// With constant sizes, changes must settle before the next major event.
						if cycle%30 == 29 && (scenario == "targets" || scenario == "collectors") {
							require.Zero(t, cycleMoves)
						}
						previous = owners
						total, maximum := 0, 0
						for _, load := range loads {
							total += load
							maximum = max(maximum, load)
						}
						mean := float64(total) / float64(len(loads))
						ratioSum += float64(maximum) / mean
						ratioWorst = max(ratioWorst, float64(maximum)/mean)
						lowerBoundWorst = max(lowerBoundWorst, max(1, float64(largest)/mean))
					}
					t.Logf("SUMMARY moves=%d opportunities=%d movement_pct=%.3f avg_cycle_pct=%.3f worst_cycle_pct=%.3f major_event_pct=%.3f other_cycle_pct=%.3f forced=%d reversals=%d mean_ratio=%.3f worst_ratio=%.3f worst_indivisible_lower_bound=%.3f avg_series_move_pct=%.3f worst_series_move_pct=%.3f", moves, opportunities, 100*float64(moves)/float64(opportunities), moveSum/(cycles-1), moveWorst, eventSum/float64(events), backgroundSum/float64(background), forced, reversals, ratioSum/cycles, ratioWorst, lowerBoundWorst, seriesMoveSum/(cycles-1), seriesMoveWorst)
				})
			}
		})
	}
}

// scenarioSnapshot generates reproducible synthetic workloads, not production traces.
// Major changes occur every 30 cycles; sizes also have deterministic +/-5% noise.
func scenarioSnapshot(scenario string, cycle int) simSnapshot {
	return scaledScenarioSnapshot(scenario, cycle, 1, 1, 1, 1)
}

func scaledScenarioSnapshot(scenario string, cycle int, targetScale, collectorScale, whaleScale, variationScale float64) simSnapshot {
	phase := cycle / 30
	count, offset, nodes := 1500, 0, 6
	if scenario == "targets" || scenario == "combined" {
		count = []int{1200, 1500, 1100, 1800, 1400, 2000, 1600, 1250, 1900, 1300, 1700, 1200}[phase]
		offset = phase * 37
	}
	if scenario == "collectors" || scenario == "combined" {
		nodes = []int{3, 4, 6, 8, 10, 7, 5, 3, 6, 10, 4, 3}[phase]
	}
	count = max(1, int(float64(count)*targetScale))
	offset = int(float64(offset) * targetScale)
	nodes = max(2, int(float64(nodes)*collectorScale+.5))
	s := simSnapshot{targets: make([]simulatedTarget, count), collectors: make([]string, nodes)}
	for i := range s.collectors {
		s.collectors[i] = fmt.Sprintf("alloy-%d", i)
	}
	for i := range s.targets {
		address := fmt.Sprintf("target-%04d:8080", i+offset)
		h := (hasher{}).Sum64([]byte(address))
		size := 0
		switch bucket := h % 1000; {
		case bucket < 750:
			size = 50 + int((h/1000)%451)
		case bucket < 950:
			size = 1000 + int((h/1000)%4001)
		case bucket < 995:
			size = 10000 + int((h/1000)%20001)
		default:
			size = int(float64(100000+int((h/1000)%300001)) * whaleScale)
		}
		if scenario == "sizes" || scenario == "combined" {
			if h%7 == 0 {
				percent := []int{100, 200, 50, 150, 75, 250, 100, 40, 180, 60, 130, 100}[phase]
				percent = max(10, 100+int(float64(percent-100)*variationScale))
				size = max(1, size*percent/100)
			}
			noise := int((hasher{}).Sum64([]byte(fmt.Sprintf("%s/%d", address, cycle)))%11) - 5
			size = max(1, size*(100+int(float64(noise)*variationScale))/100)
		}
		s.targets[i] = simulatedTarget{address, size}
	}
	return s
}

// windowMeasurements uses two rolling maxima with a 2*k-1 cycle horizon.
func windowMeasurements(t *testing.T, history []simSnapshot, k int) simAllocation {
	t.Helper()
	require.Positive(t, k)
	require.NotEmpty(t, history)
	history = history[max(0, len(history)-(2*k-1)):]
	a := simAllocation{owners: map[string]string{}, loads: map[string]int{}}
	for cycle := max(0, len(history)-k); cycle < len(history); cycle++ {
		sizes := windowSizes(history, cycle, k)
		total := 0
		for _, size := range sizes {
			total += size
		}
		a.total = max(a.total, total)
		a.sizes = sizes
	}
	return a
}

// Consistent hashing.

func TestSimulatedAllocation_ConsistentHashing(t *testing.T) {
	t.Run("stable_inputs", testHashingStableInputs)
	t.Run("target_changes", testHashingTargetChanges)
	t.Run("membership_changes", testHashingMembershipChanges)
}

func simulateHashing(t *testing.T, targets []simulatedTarget, names ...string) map[string]string {
	t.Helper()
	collectors := make(map[string]*Collector, len(names))
	for _, name := range names {
		collectors[name] = NewCollector(name, "")
	}
	strategy := newConsistentHashingStrategy()
	strategy.SetCollectors(collectors)
	owners := make(map[string]string, len(targets))
	for _, item := range targets {
		collector, err := strategy.GetCollectorForTarget(collectors, &target.Item{TargetURL: item.address})
		require.NoError(t, err)
		require.NotNil(t, collector)
		if _, duplicate := owners[item.address]; duplicate {
			t.Fatalf("duplicate target: %s", item.address)
		}
		owners[item.address] = collector.Name
	}
	return owners
}

func testHashingStableInputs(t *testing.T) {
	targets := simTargets(1000)
	before := simulateHashing(t, targets, "alloy-0", "alloy-1", "alloy-2")
	for round := range 10 {
		after := simulateHashing(t, targets, "alloy-0", "alloy-1", "alloy-2")
		require.Equal(t, before, after, "round %d", round)
	}
	slices.Reverse(targets)
	require.Equal(t, before, simulateHashing(t, targets, "alloy-2", "alloy-0", "alloy-1"))
	for i := range targets {
		targets[i].series = (i + 1) * 100
	}
	// Size changes currently have no effect on placement.
	require.Equal(t, before, simulateHashing(t, targets, "alloy-0", "alloy-1", "alloy-2"))
}

func testHashingTargetChanges(t *testing.T) {
	targets := simTargets(1000)
	before := simulateHashing(t, targets[:900], "alloy-0", "alloy-1", "alloy-2")
	added := simulateHashing(t, targets, "alloy-0", "alloy-1", "alloy-2")
	require.Zero(t, countMoves(before, added))
	removed := simulateHashing(t, targets[100:], "alloy-0", "alloy-1", "alloy-2")
	require.Zero(t, countMoves(added, removed))
}

func testHashingMembershipChanges(t *testing.T) {
	targets := simTargets(1000)
	initial := []string{"alloy-0", "alloy-1", "alloy-2"}
	before := simulateHashing(t, targets, initial...)
	for _, tc := range []struct {
		name       string
		collectors []string
	}{
		{"scale_up", []string{"alloy-0", "alloy-1", "alloy-2", "alloy-3"}},
		{"scale_down", []string{"alloy-0", "alloy-1"}},
		{"same_count_replacement", []string{"alloy-0", "alloy-1", "alloy-3"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			after := simulateHashing(t, targets, tc.collectors...)
			moved := countMoves(before, after)
			require.Greater(t, moved, 0)
			require.Less(t, moved, len(targets))
			require.Equal(t, after, simulateHashing(t, targets, tc.collectors...))
			require.Equal(t, before, simulateHashing(t, targets, initial...))
			t.Logf("targets=%d moved=%d (%.1f%%); unchanged membership then causes zero moves", len(targets), moved, 100*float64(moved)/float64(len(targets)))
		})
	}
}

// Capacity-aware hashing.

func TestSimulatedAllocation_CapacityAware(t *testing.T) {
	t.Run("one_cycle", func(t *testing.T) {
		targets := simTargets(27)
		for i := range 3 {
			targets[i].series = 800
		}
		s := simSnapshot{targets, []string{"alloy-0", "alloy-1", "alloy-2"}}
		a := allocateCapacityAware(t, []simSnapshot{s}, simWindow)
		require.Len(t, a.owners, 27)
		require.Equal(t, 2640, a.total)
		require.InDelta(t, 1056, a.capacity, 0.001)
		require.Zero(t, a.fallbacks)
		for _, load := range a.loads {
			require.LessOrEqual(t, float64(load), a.capacity)
		}
		require.Equal(t, a, allocateCapacityAware(t, []simSnapshot{s}, simWindow))
		slices.Reverse(s.targets)
		slices.Reverse(s.collectors)
		require.Equal(t, a, allocateCapacityAware(t, []simSnapshot{s}, simWindow))
	})
	t.Run("two_windows_expire", func(t *testing.T) {
		var history []simSnapshot
		for cycle := range 12 {
			size := 10
			if cycle == 0 {
				size = 100
			}
			history = append(history, simSnapshot{[]simulatedTarget{{"target", size}}, []string{"alloy-0"}})
			a := allocateCapacityAware(t, history, simWindow)
			expectedSize, expectedTotal := 10, 10
			if cycle < 6 {
				expectedSize = 100
			}
			if cycle < 11 {
				expectedTotal = 100
			}
			require.Equal(t, expectedSize, a.sizes["target"], "cycle %d", cycle)
			require.Equal(t, expectedTotal, a.total, "cycle %d", cycle)
		}
		// Different older histories produce the same result once both windows expire.
		require.Equal(t, allocateCapacityAware(t, history, simWindow), allocateCapacityAware(t, history[1:], simWindow))
	})
	t.Run("growth_and_current_membership", func(t *testing.T) {
		history := []simSnapshot{
			{[]simulatedTarget{{"gone", 100}, {"kept", 10}}, []string{"old"}},
			{[]simulatedTarget{{"kept", 200}, {"new", 10}}, []string{"new-owner"}},
		}
		a := allocateCapacityAware(t, history, simWindow)
		require.Equal(t, map[string]string{"kept": "new-owner", "new": "new-owner"}, a.owners)
		require.Equal(t, 200, a.sizes["kept"])
		require.Equal(t, 210, a.total)
		history = append(history, simSnapshot{history[1].targets, nil})
		require.Empty(t, allocateCapacityAware(t, history, simWindow).owners)
	})
	t.Run("stable_window", func(t *testing.T) {
		s := simSnapshot{simTargets(27), []string{"alloy-0", "alloy-1", "alloy-2"}}
		history := []simSnapshot{s}
		before := allocateCapacityAware(t, history, simWindow)
		for range 12 {
			history = append(history, s)
			require.Equal(t, before, allocateCapacityAware(t, history, simWindow))
		}
	})
	t.Run("oversized_target", func(t *testing.T) {
		a := allocateCapacityAware(t, []simSnapshot{{[]simulatedTarget{{"whale", 1000}, {"small", 10}}, []string{"c", "b", "a"}}}, simWindow)
		require.Equal(t, 1, a.fallbacks)
		require.Equal(t, "a", a.owners["whale"])
		require.NotEqual(t, "a", a.owners["small"])
	})
}

func simulateCapacityAware(t *testing.T, targets []simulatedTarget, collectors ...string) map[string]string {
	t.Helper()
	return allocateCapacityAware(t, []simSnapshot{{targets, collectors}}, simWindow).owners
}

// allocateCapacityAware uses two rolling maxima, so its input horizon is 2*k-1 cycles.
// History is oldest first; membership always comes from the latest snapshot.
func allocateCapacityAware(t *testing.T, history []simSnapshot, k int) simAllocation {
	t.Helper()
	a := windowMeasurements(t, history, k)
	latest := history[len(history)-1]
	names := slices.Clone(latest.collectors)
	slices.Sort(names)
	if len(names) == 0 {
		return a
	}
	ideal := float64(a.total) / float64(len(names))
	a.capacity = 1.2 * ideal
	collectors := map[string]*Collector{}
	for _, name := range names {
		collectors[name] = NewCollector(name, "")
		a.loads[name] = 0
	}
	strategy := newConsistentHashingStrategy()
	strategy.SetCollectors(collectors)
	targets := slices.Clone(latest.targets)
	hash := hasher{}
	slices.SortFunc(targets, func(aTarget, bTarget simulatedTarget) int {
		aTier, bTier := sizeTier(a.sizes[aTarget.address], ideal), sizeTier(a.sizes[bTarget.address], ideal)
		if aTier != bTier {
			return aTier - bTier
		}
		aHash, bHash := hash.Sum64([]byte(aTarget.address)), hash.Sum64([]byte(bTarget.address))
		if aHash < bHash {
			return -1
		}
		if aHash > bHash {
			return 1
		}
		return strings.Compare(aTarget.address, bTarget.address)
	})
	for _, item := range targets {
		candidates, err := strategy.consistentHasher.GetClosestN([]byte(item.address), len(names))
		require.NoError(t, err)
		owner := ""
		for _, candidate := range candidates {
			name := candidate.String()
			if float64(a.loads[name]+a.sizes[item.address]) <= a.capacity {
				owner = name
				break
			}
		}
		if owner == "" {
			owner = names[0]
			for _, name := range names[1:] {
				if a.loads[name] < a.loads[owner] {
					owner = name
				}
			}
			a.fallbacks++
		}
		a.owners[item.address] = owner
		a.loads[owner] += a.sizes[item.address]
	}
	return a
}

func sizeTier(size int, ideal float64) int {
	for tier, divisor := range []float64{2, 8, 32, 128} {
		if float64(size) >= ideal/divisor {
			return tier
		}
	}
	return 4
}

// Load shedding.

func TestSimulatedAllocation_LoadShedding(t *testing.T) {
	t.Run("demo_whales", testDemoWhales)
	t.Run("small_moves_block_feasible_whale_move", func(t *testing.T) {
		snapshot := sheddingFixture(t, [][]int{{1220}, {1, 1, 1, 1}, {500}})
		// Put a small target before the first whale in stable hash order.
		donor := snapshot.targets[1:5]
		slices.SortFunc(donor, func(a, b simulatedTarget) int { return hashOrder(a.address, b.address) })
		for i, size := range []int{120, 800, 290, 800} {
			donor[i].series = size
		}
		a := allocateLoadShedding(t, []simSnapshot{snapshot}, simWindow)
		require.Equal(t, map[string]int{"alloy-0": 1220, "alloy-1": 1600, "alloy-2": 910}, a.loads)
		require.Equal(t, 1, a.unresolved)
		// Moving the first whale instead of the small targets would satisfy every bound.
		ideal := float64(a.total) / 3
		require.GreaterOrEqual(t, float64(2010-800), .8*ideal)
		require.LessOrEqual(t, float64(500+800), 1.1*ideal)
	})
	t.Run("local_corrections", func(t *testing.T) {
		s := sheddingFixture(t, [][]int{{20, 20, 20, 20, 20, 20, 20, 20, 20}, {20, 20, 20}, {20, 20, 20}})
		home := simulateHashing(t, s.targets, s.collectors...)
		a := allocateLoadShedding(t, []simSnapshot{s}, simWindow)
		require.Equal(t, 3, countMoves(home, a.owners))
		require.Equal(t, 120, a.loads["alloy-0"])
		require.Zero(t, a.unresolved)
		for _, item := range s.targets {
			if home[item.address] != "alloy-0" {
				require.Equal(t, home[item.address], a.owners[item.address])
			}
		}
		for _, name := range []string{"alloy-1", "alloy-2"} {
			require.LessOrEqual(t, a.loads[name], 110)
		}
		slices.Reverse(s.targets)
		slices.Reverse(s.collectors)
		require.Equal(t, a, allocateLoadShedding(t, []simSnapshot{s}, simWindow))
		history := []simSnapshot{s}
		for range 12 {
			history = append(history, s)
			require.Equal(t, a, allocateLoadShedding(t, history, simWindow))
		}
	})
	t.Run("overloaded_nodes_do_not_adopt", func(t *testing.T) {
		s := sheddingFixture(t, [][]int{{60, 60, 60}, {50, 50, 50}, {30}})
		home := simulateHashing(t, s.targets, s.collectors...)
		a := allocateLoadShedding(t, []simSnapshot{s}, simWindow)
		require.Positive(t, countMoves(home, a.owners))
		for address, owner := range a.owners {
			if owner != home[address] {
				require.Equal(t, "alloy-2", owner)
			}
		}
		require.LessOrEqual(t, a.loads["alloy-2"], 132)
	})
	t.Run("no_room_keeps_home", func(t *testing.T) {
		s := sheddingFixture(t, [][]int{{60, 60, 60}, {60}, {60}})
		a := allocateLoadShedding(t, []simSnapshot{s}, simWindow)
		require.Equal(t, simulateHashing(t, s.targets, s.collectors...), a.owners)
		require.Equal(t, 1, a.unresolved)
		require.Equal(t, 180, a.loads["alloy-0"])
	})
	t.Run("do_not_empty_donor", func(t *testing.T) {
		s := sheddingFixture(t, [][]int{{1000, 10}, {10}, {10}})
		a := allocateLoadShedding(t, []simSnapshot{s}, simWindow)
		for _, item := range s.targets {
			if item.series == 1000 {
				require.Equal(t, "alloy-0", a.owners[item.address])
			}
		}
		require.Equal(t, 1, a.unresolved)
	})
	t.Run("windows_and_membership", func(t *testing.T) {
		var history []simSnapshot
		for cycle := range 12 {
			size := 10
			if cycle == 0 {
				size = 100
			}
			history = append(history, simSnapshot{[]simulatedTarget{{"kept", size}}, []string{"new-owner"}})
			a := allocateLoadShedding(t, history, simWindow)
			wantSize, wantTotal := 10, 10
			if cycle < 6 {
				wantSize = 100
			}
			if cycle < 11 {
				wantTotal = 100
			}
			require.Equal(t, wantSize, a.sizes["kept"])
			require.Equal(t, wantTotal, a.total)
		}
		require.Equal(t, allocateLoadShedding(t, history, simWindow), allocateLoadShedding(t, history[1:], simWindow))
		history = append(history, simSnapshot{[]simulatedTarget{{"replacement", 20}}, []string{"replacement-owner"}})
		require.Equal(t, map[string]string{"replacement": "replacement-owner"}, allocateLoadShedding(t, history, simWindow).owners)
		history = append(history, simSnapshot{history[len(history)-1].targets, nil})
		require.Empty(t, allocateLoadShedding(t, history, simWindow).owners)
	})
}

func testDemoWhales(t *testing.T) {
	data, err := os.ReadFile("testdata/load_shedding_demo.json")
	require.NoError(t, err)
	var fixture struct {
		Collectors []string
		Targets    []struct {
			Address string
			Series  int
			Owner   string
		}
	}
	require.NoError(t, json.Unmarshal(data, &fixture))
	snapshot := simSnapshot{collectors: fixture.Collectors}
	expected := map[string]string{}
	for _, item := range fixture.Targets {
		snapshot.targets = append(snapshot.targets, simulatedTarget{item.Address, item.Series})
		expected[item.Address] = item.Owner
	}
	home := simulateHashing(t, snapshot.targets, snapshot.collectors...)
	actual := allocateLoadShedding(t, []simSnapshot{snapshot}, simWindow)
	require.Equal(t, expected, actual.owners)
	homeLoads := map[string]int{}
	for _, item := range snapshot.targets {
		homeLoads[home[item.address]] += item.series
	}
	t.Logf("home=%v final=%v moves=%d unresolved=%d", homeLoads, actual.loads, countMoves(home, actual.owners), actual.unresolved)
	for _, item := range snapshot.targets {
		if item.series == 800 {
			t.Logf("whale %s home=%s final=%s", item.address, home[item.address], actual.owners[item.address])
		}
	}
	// Follow the donor's stable order to explain which earlier moves block the whales.
	ordered := slices.Clone(snapshot.targets)
	slices.SortFunc(ordered, func(a, b simulatedTarget) int { return hashOrder(a.address, b.address) })
	movedSize := 0
	donor, receiver := snapshot.collectors[1], snapshot.collectors[2]
	ideal := float64(actual.total) / float64(len(snapshot.collectors))
	for _, item := range ordered {
		if home[item.address] != donor {
			continue
		}
		if item.series == 800 {
			t.Logf("whale=%s earlier_shed=%d donor_before=%d receiver_before=%d donor_after=%d receiver_after=%d floor=%.2f adoption_cap=%.2f", item.address, movedSize, homeLoads[donor]-movedSize, homeLoads[receiver]+movedSize, homeLoads[donor]-movedSize-800, homeLoads[receiver]+movedSize+800, .8*ideal, 1.1*ideal)
		}
		if actual.owners[item.address] != donor {
			movedSize += item.series
		}
	}
	require.Equal(t, map[string]int{snapshot.collectors[0]: 1220, donor: 1600, receiver: 910}, actual.loads)
	require.Equal(t, 20, countMoves(home, actual.owners))
	require.Equal(t, 1, actual.unresolved)
	// A single move from the home placement satisfies both admission thresholds.
	require.GreaterOrEqual(t, float64(homeLoads[donor]-800), .8*ideal)
	require.LessOrEqual(t, float64(homeLoads[receiver]+800), 1.1*ideal)
	for _, load := range []int{homeLoads[snapshot.collectors[0]], homeLoads[donor] - 800, homeLoads[receiver] + 800} {
		require.LessOrEqual(t, float64(load), 1.2*ideal)
	}

	history := []simSnapshot{snapshot}
	for range 12 {
		history = append(history, snapshot)
		require.Equal(t, actual, allocateLoadShedding(t, history, simWindow))
	}
}

func simulateLoadShedding(t *testing.T, targets []simulatedTarget, collectors ...string) map[string]string {
	t.Helper()
	return allocateLoadShedding(t, []simSnapshot{{targets, collectors}}, simWindow).owners
}

func allocateLoadShedding(t *testing.T, history []simSnapshot, k int) simAllocation {
	t.Helper()
	require.NotEmpty(t, history)
	require.Positive(t, k)
	window := sizeWindow{length: k}
	var sizes map[target.ItemHash]float64
	var total float64
	var items map[target.ItemHash]*target.Item
	for _, snapshot := range history[max(0, len(history)-(2*k-1)):] {
		raw := make(map[target.ItemHash]float64, len(snapshot.targets))
		items = make(map[target.ItemHash]*target.Item, len(snapshot.targets))
		for _, item := range snapshot.targets {
			id := target.ItemHash((hasher{}).Sum64([]byte(item.address)))
			items[id] = target.NewItem("simulation", item.address, labels.EmptyLabels(), "", id)
			raw[id] = float64(item.series)
		}
		sizes, total = window.observe(raw)
	}
	collectors := map[string]*Collector{}
	for _, name := range history[len(history)-1].collectors {
		collectors[name] = NewCollector(name, "")
	}
	plan := (&loadSheddingStrategy{}).plan(items, collectors, sizes, total)
	result := simAllocation{owners: map[string]string{}, sizes: map[string]int{}, loads: map[string]int{}, total: int(total), unresolved: plan.unresolved}
	if len(collectors) > 0 {
		result.capacity = 1.2 * total / float64(len(collectors))
	}
	for id, item := range items {
		result.sizes[item.TargetURL] = int(sizes[id])
		if owner, ok := plan.owners[id]; ok {
			result.owners[item.TargetURL] = owner
		}
	}
	for name, load := range plan.loads {
		result.loads[name] = int(load)
	}
	return result
}

// sheddingFixture finds target identities with the requested home-node sizes.
func sheddingFixture(t *testing.T, sizes [][]int) simSnapshot {
	t.Helper()
	s := simSnapshot{collectors: []string{"alloy-0", "alloy-1", "alloy-2"}}
	collectors := map[string]*Collector{}
	for _, name := range s.collectors {
		collectors[name] = NewCollector(name, "")
	}
	strategy := newConsistentHashingStrategy()
	strategy.SetCollectors(collectors)
	for node, weights := range sizes {
		found := 0
		for i := 0; found < len(weights) && i < 10000; i++ {
			address := fmt.Sprintf("fixture-%d-%d:8080", node, i)
			if strategy.consistentHasher.LocateKey([]byte(address)).String() == s.collectors[node] {
				s.targets = append(s.targets, simulatedTarget{address, weights[found]})
				found++
			}
		}
		require.Equal(t, len(weights), found)
	}
	return s
}
