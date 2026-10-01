// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package allocation

import (
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// These small workloads have an exact optimum, so indivisible targets cannot
// conceal an avoidable placement failure behind a max/mean average.
func TestSimulatedAllocation_Adversarial(t *testing.T) {
	for _, tc := range adversarialScenarios(t) {
		t.Run(tc.name, func(t *testing.T) {
			for _, algorithm := range []string{"ConsistentHashing", "CapacityAware", "LoadShedding", "Sticky"} {
				t.Run(algorithm, func(t *testing.T) {
					sticky := newSimSticky()
					history := []simSnapshot{}
					previous := map[string]string{}
					ratioSum, worstRatio, gapSum, worstGap, moveSum, worstMoves, homeSum := 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
					avoidable, opportunities, moves := 0, 0, 0
					optima := map[string]int{}
					for cycle := range 90 {
						snapshot := tc.snapshot(cycle)
						history = append(history, snapshot)
						var owners map[string]string
						switch algorithm {
						case "ConsistentHashing":
							owners = simulateHashing(t, snapshot.targets, snapshot.collectors...)
						case "CapacityAware":
							owners = allocateCapacityAware(t, history, simWindow).owners
						case "Sticky":
							owners = sticky.allocate(t, history, simWindow).owners
						case "LoadShedding":
							owners = allocateLoadShedding(t, history, simWindow).owners
						}
						loads := map[string]int{}
						total := 0
						for _, name := range snapshot.collectors {
							loads[name] = 0
						}
						require.Len(t, owners, len(snapshot.targets))
						for _, item := range snapshot.targets {
							require.Contains(t, loads, owners[item.address])
							loads[owners[item.address]] += item.series
							total += item.series
						}
						peak := 0
						for _, load := range loads {
							peak = max(peak, load)
						}
						weights := make([]int, len(snapshot.targets))
						for i, item := range snapshot.targets {
							weights[i] = item.series
						}
						slices.Sort(weights)
						key := fmt.Sprint(len(snapshot.collectors), weights)
						optimum, exists := optima[key]
						if !exists {
							optimum = optimalPeak(weights, len(snapshot.collectors))
							optima[key] = optimum
						}
						require.GreaterOrEqual(t, peak, optimum)
						mean := float64(total) / float64(len(snapshot.collectors))
						ratio := float64(peak) / mean
						gap := 100 * (float64(peak)/float64(optimum) - 1)
						ratioSum += ratio
						worstRatio = max(worstRatio, ratio)
						gapSum += gap
						worstGap = max(worstGap, gap)
						if ratio > 1.2+1e-9 && float64(optimum)/mean <= 1.2+1e-9 {
							avoidable++
						}
						home := simulateHashing(t, snapshot.targets, snapshot.collectors...)
						homeSum += 100 * float64(countMoves(home, owners)) / float64(len(snapshot.targets))
						if cycle > 0 {
							survivors := 0
							for address := range owners {
								if _, ok := previous[address]; ok {
									survivors++
								}
							}
							moved := countMoves(previous, owners)
							percent := 100 * float64(moved) / float64(survivors)
							moves += moved
							opportunities += survivors
							moveSum += percent
							worstMoves = max(worstMoves, percent)
							if algorithm != "Sticky" && cycle%15 >= 2*simWindow-1 {
								require.Zero(t, moved, "unchanged input must settle after both windows expire")
							}
						}
						previous = owners
					}
					t.Logf("ADVERSARIAL scenario=%s algorithm=%s ratio=%.3f/%.3f excess_over_optimum_pct=%.1f/%.1f moves_pct=%.2f/%.2f weighted_moves_pct=%.2f away_from_home_pct=%.1f avoidable_overload_cycles=%d/90", tc.name, algorithm, ratioSum/90, worstRatio, gapSum/90, worstGap, moveSum/89, worstMoves, 100*float64(moves)/float64(opportunities), homeSum/90, avoidable)
					// Preserve each strategy's current worst-case quality ceiling;
					// improvements are allowed, regressions must be reviewed.
					index := slices.Index([]string{"ConsistentHashing", "CapacityAware", "LoadShedding"}, algorithm)
					if index >= 0 {
						require.LessOrEqual(t, worstGap, adversarialGapLimits[tc.name][index]+0.1, "placement regressed against the exact optimum")
					}

				})
			}
		})
	}
}

// Maximum percent excess over the optimum: hashing, capacity-aware, load-shedding.
var adversarialGapLimits = map[string][3]float64{
	"small_first":          {54.6, 0, 23.1},
	"large_first":          {54.6, 0, 0},
	"recipient_room":       {65, 0, 8.2},
	"donor_floor":          {22.1, 0, 22.1},
	"two_donors":           {6.3, 0, 0},
	"indivisible_whale":    {0.9, 0, 0},
	"headroom_oscillation": {54.6, 0, 23.1},
	"whales_grow_shrink":   {54.6, 0, 23.1},
	"small_target_churn":   {54.6, 0, 23.1},
	"collector_join_leave": {54.6, 0, 31.1},
}

type adversarialScenario struct {
	name     string
	snapshot func(int) simSnapshot
}

func adversarialScenarios(t *testing.T) []adversarialScenario {
	t.Helper()
	smallFirst := orderedSheddingFixture(t, [][]int{{1220}, {120, 800, 290, 800}, {500}})
	largeFirst := orderedSheddingFixture(t, [][]int{{1220}, {800, 120, 290, 800}, {500}})
	static := func(name string, s simSnapshot) adversarialScenario {
		return adversarialScenario{name, func(int) simSnapshot { return s }}
	}
	scenarios := []adversarialScenario{
		static("small_first", smallFirst), static("large_first", largeFirst),
		static("recipient_room", orderedSheddingFixture(t, [][]int{{1220}, {120, 800, 290, 800}, {400}})),
		static("donor_floor", orderedSheddingFixture(t, [][]int{{800, 420}, {800, 800}, {400, 510}})),
		static("two_donors", orderedSheddingFixture(t, [][]int{{100, 800, 800}, {100, 800, 800}, {400, 400}})),
		static("indivisible_whale", orderedSheddingFixture(t, [][]int{{2400, 20}, {400, 20}, {400, 20}})),
	}
	scenarios = append(scenarios, adversarialScenario{"headroom_oscillation", func(cycle int) simSnapshot {
		s := cloneSimulation(smallFirst)
		s.targets[1].series = []int{60, 120, 60, 120, 60, 120}[cycle/15]
		return s
	}}, adversarialScenario{"whales_grow_shrink", func(cycle int) simSnapshot {
		s := cloneSimulation(smallFirst)
		for i := range s.targets {
			if s.targets[i].series == 800 {
				s.targets[i].series = []int{400, 800, 400, 800, 400, 800}[cycle/15]
			}
		}
		return s
	}}, adversarialScenario{"small_target_churn", func(cycle int) simSnapshot {
		s := cloneSimulation(smallFirst)
		if cycle/15%2 == 1 {
			s.targets = append(s.targets[:1], s.targets[2:]...)
		}
		return s
	}}, adversarialScenario{"collector_join_leave", func(cycle int) simSnapshot {
		s := cloneSimulation(smallFirst)
		if cycle/15%2 == 1 {
			s.collectors = append(s.collectors, "alloy-3")
		}
		return s
	}})
	return scenarios
}

func cloneSimulation(s simSnapshot) simSnapshot {
	return simSnapshot{slices.Clone(s.targets), slices.Clone(s.collectors)}
}

// orderedSheddingFixture places weights in stable hash order within each home.
func orderedSheddingFixture(t *testing.T, weights [][]int) simSnapshot {
	t.Helper()
	s := sheddingFixture(t, weights)
	offset := 0
	for _, group := range weights {
		items := s.targets[offset : offset+len(group)]
		slices.SortFunc(items, func(a, b simulatedTarget) int { return hashOrder(a.address, b.address) })
		for i, size := range group {
			items[i].series = size
		}
		offset += len(group)
	}
	return s
}

// optimalPeak enumerates whole-target placements. It is only for tiny fixtures;
// equal-load branches are symmetric and cannot improve the optimum twice.
func optimalPeak(weights []int, n int) int {
	weights = slices.Clone(weights)
	slices.SortFunc(weights, func(a, b int) int { return b - a })
	loads := make([]int, n)
	best := math.MaxInt
	var visit func(int, int)
	visit = func(index, peak int) {
		if peak >= best {
			return
		}
		if index == len(weights) {
			best = peak
			return
		}
		seen := map[int]bool{}
		for i, load := range loads {
			if seen[load] {
				continue
			}
			seen[load] = true
			loads[i] += weights[index]
			visit(index+1, max(peak, loads[i]))
			loads[i] = load
		}
	}
	visit(0, 0)
	return best
}

func TestSimulatedAllocation_OptimalPeak(t *testing.T) {
	for _, tc := range []struct {
		weights []int
		n, want int
	}{
		{[]int{800, 800, 800, 120, 290, 420, 500}, 3, 1300},
		{[]int{6, 6, 6, 6, 6}, 3, 12},
		{[]int{2400, 20, 400, 20, 400, 20}, 3, 2400},
		{[]int{10, 20}, 1, 30},
	} {
		require.Equal(t, tc.want, optimalPeak(tc.weights, tc.n))
	}
}
