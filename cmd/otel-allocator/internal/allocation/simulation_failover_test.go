// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package allocation

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type failoverReplica struct {
	history []simSnapshot
	sticky  *simSticky
	owners  map[string]string
}

func (r *failoverReplica) step(t *testing.T, algorithm string, snapshot simSnapshot) {
	t.Helper()
	r.history = append(r.history, snapshot)
	r.history = r.history[max(0, len(r.history)-(2*simWindow-1)):]
	switch algorithm {
	case "ConsistentHashing":
		r.owners = simulateHashing(t, snapshot.targets, snapshot.collectors...)
	case "CapacityAware":
		r.owners = allocateCapacityAware(t, r.history, simWindow).owners
	case "LoadShedding":
		r.owners = allocateLoadShedding(t, r.history, simWindow).owners
	case "Sticky":
		if r.sticky == nil {
			r.sticky = newSimSticky()
		}
		r.owners = r.sticky.allocate(t, r.history, simWindow).owners
	default:
		t.Fatalf("unknown algorithm %s", algorithm)
	}
	require.Len(t, r.owners, len(snapshot.targets))
	for _, target := range snapshot.targets {
		require.Contains(t, snapshot.collectors, r.owners[target.address])
	}
}

func TestSimulatedAllocation_Failover(t *testing.T) {
	const trials = 8
	for _, mode := range []string{"identical", "lagged", "missed_updates", "late_start"} {
		t.Run(mode, func(t *testing.T) {
			for _, algorithm := range []string{"ConsistentHashing", "CapacityAware", "LoadShedding", "Sticky"} {
				t.Run(algorithm, func(t *testing.T) {
					moveSum, moveWorst, seriesSum, seriesWorst, settledSum, settledWorst := 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
					missingSum, invalidSum := 0.0, 0.0
					for trial := range trials {
						leader, standby := &failoverReplica{}, &failoverReplica{}
						lag := 1 + trial%3
						snapshotAt := func(cycle int) simSnapshot { return scenarioSnapshot("combined", (trial*31+1+max(0, cycle))%360) }
						for cycle := range 90 {
							leader.step(t, algorithm, snapshotAt(cycle))
							if mode == "late_start" && cycle < 60 {
								continue
							}
							observed := cycle
							if mode == "lagged" || (mode == "missed_updates" && (trial*31+1+cycle)%30 < lag) {
								observed = max(0, cycle-lag)
							}
							standby.step(t, algorithm, snapshotAt(observed))
						}
						latest := snapshotAt(89)
						// A stale discovery snapshot can omit new targets or reference departed owners.
						missing, invalid := 0, 0
						live := map[string]bool{}
						for _, name := range latest.collectors {
							live[name] = true
						}
						for _, target := range latest.targets {
							owner, exists := standby.owners[target.address]
							if !exists {
								missing++
							} else if !live[owner] {
								invalid++
							}
						}
						missingSum += 100 * float64(missing) / float64(len(latest.targets))
						invalidSum += 100 * float64(invalid) / float64(len(latest.targets))
						// Refresh discovery on takeover. Every current target must have a live owner;
						// measurement history and sticky state remain the standby's own.
						leader.step(t, algorithm, latest)
						standby.step(t, algorithm, latest)
						moved, series := failoverDifference(latest, leader.owners, standby.owners)
						moveSum += moved
						moveWorst = max(moveWorst, moved)
						seriesSum += series
						seriesWorst = max(seriesWorst, series)
						if mode == "identical" {
							require.Zero(t, moved)
						}
						// Hold inputs fixed long enough for both measurement windows and the
						// sticky overload delay to expire. Compare with a no-failover leader.
						for range 20 {
							leader.step(t, algorithm, latest)
							standby.step(t, algorithm, latest)
						}
						settled, _ := failoverDifference(latest, leader.owners, standby.owners)
						settledSum += settled
						settledWorst = max(settledWorst, settled)
						if algorithm != "Sticky" || mode == "identical" {
							require.Zero(t, settled)
						}
						t.Logf("TRIAL mode=%s algorithm=%s trial=%d lag=%d targets=%d collectors=%d takeover_pct=%.3f series_pct=%.3f settled_pct=%.3f missing=%d invalid=%d", mode, algorithm, trial, lag, len(latest.targets), len(latest.collectors), moved, series, settled, missing, invalid)
					}
					t.Logf("FAILOVER mode=%s algorithm=%s trials=%d avg_pct=%.3f worst_pct=%.3f avg_series_pct=%.3f worst_series_pct=%.3f settled_avg_pct=%.3f settled_worst_pct=%.3f stale_missing_avg_pct=%.3f stale_invalid_avg_pct=%.3f", mode, algorithm, trials, moveSum/trials, moveWorst, seriesSum/trials, seriesWorst, settledSum/trials, settledWorst, missingSum/trials, invalidSum/trials)
				})
			}
		})
	}
}

func failoverDifference(snapshot simSnapshot, before, after map[string]string) (float64, float64) {
	moves, movedSeries, total := 0, 0, 0
	for _, target := range snapshot.targets {
		total += target.series
		if before[target.address] != after[target.address] {
			moves++
			movedSeries += target.series
		}
	}
	return 100 * float64(moves) / float64(len(snapshot.targets)), 100 * float64(movedSeries) / float64(total)
}
