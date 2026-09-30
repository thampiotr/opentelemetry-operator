// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package allocation

import (
	"maps"
	"slices"

	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/target"
)

// sizeWindow applies a K-cycle maximum to target sizes, then to their sum.
// Together the two windows forget a sample after 2*K-1 cycles.
type sizeWindow struct {
	cycles []map[target.ItemHash]float64
	totals []float64
	length int
}

func (w *sizeWindow) observe(raw map[target.ItemHash]float64) (map[target.ItemHash]float64, float64) {
	w.cycles = append(w.cycles, maps.Clone(raw))
	if len(w.cycles) > w.length {
		w.cycles = slices.Delete(w.cycles, 0, 1)
	}
	sizes := maps.Clone(raw)
	for id := range sizes {
		for _, cycle := range w.cycles {
			sizes[id] = max(sizes[id], cycle[id])
		}
	}
	var total float64
	for _, id := range slices.Sorted(maps.Keys(sizes)) {
		total += sizes[id]
	}
	w.totals = append(w.totals, total)
	if len(w.totals) > w.length {
		w.totals = slices.Delete(w.totals, 0, 1)
	}
	return sizes, slices.Max(w.totals)
}
