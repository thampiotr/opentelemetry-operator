// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package allocation

import (
	"maps"
	"time"

	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/target"
)

// Inspection is a consistent copy of the current allocation for diagnostic readers.
type Inspection struct {
	Strategy   string
	Updated    time.Time
	Targets    map[target.ItemHash]*target.Item
	Collectors map[string]*Collector
	Sizes      map[target.ItemHash]float64
}

// Inspect returns the published assignments and the accounting sizes used to create them.
func (a *loadSheddingAllocator) Inspect() Inspection {
	a.publishedMu.RLock()
	defer a.publishedMu.RUnlock()
	return Inspection{loadSheddingStrategyName, a.published.updated, maps.Clone(a.published.targets), maps.Clone(a.published.collectors), maps.Clone(a.published.sizes)}
}

func (a *allocator) Inspect() Inspection {
	a.m.RLock()
	defer a.m.RUnlock()
	items := make(map[target.ItemHash]*target.Item, len(a.targetItems))
	for id, item := range a.targetItems {
		copy := *item
		items[id] = &copy
	}
	return Inspection{Strategy: a.strategy.GetName(), Targets: items, Collectors: maps.Clone(a.collectors)}
}
