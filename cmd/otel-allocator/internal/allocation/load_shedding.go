// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package allocation

import (
	"cmp"
	"maps"
	"slices"

	"github.com/buraksezer/consistent"

	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/target"
)

const loadSheddingStrategyName = "load-shedding"

type loadSheddingStrategy struct {
	config SizeAwareStrategyConfig
}

func (*loadSheddingStrategy) GetName() string { return loadSheddingStrategyName }

// plan starts from hashing homes on every cycle; previous assignments are not inputs.
func (s *loadSheddingStrategy) plan(items map[target.ItemHash]*target.Item, collectors map[string]*Collector, sizes map[target.ItemHash]float64, total float64) allocationPlan {
	p := allocationPlan{
		owners: make(map[target.ItemHash]string, len(items)),
		loads:  make(map[string]float64, len(collectors)),
	}
	if len(collectors) == 0 {
		return p
	}
	hashing := newConsistentHashingStrategy()
	hashing.SetCollectors(collectors)
	names := slices.SortedFunc(maps.Keys(collectors), hashOrder)
	homes := p.assignHomes(items, sizes, hashing.consistentHasher, names)
	ideal := total / float64(len(names))
	overloaded := make(map[string]bool, len(names))
	for _, name := range names {
		overloaded[name] = p.loads[name] > 1.2*ideal
	}
	for _, name := range names {
		if overloaded[name] {
			p.shed(name, homes[name], overloaded, hashing.consistentHasher, ideal)
		}
	}
	return p
}

func (p *allocationPlan) assignHomes(items map[target.ItemHash]*target.Item, sizes map[target.ItemHash]float64, ring *consistent.Consistent, names []string) map[string][]weightedTarget {
	homes := make(map[string][]weightedTarget, len(names))
	for _, name := range names {
		p.loads[name] = 0
	}
	for id, item := range items {
		home := ring.LocateKey([]byte(item.TargetURL)).String()
		p.owners[id] = home
		homes[home] = append(homes[home], weightedTarget{id, item.TargetURL, sizes[id]})
	}
	// Sum in stable order too: floating-point addition must not depend on map iteration.
	for _, name := range names {
		slices.SortFunc(homes[name], func(a, b weightedTarget) int {
			if order := hashOrder(a.key, b.key); order != 0 {
				return order
			}
			return cmp.Compare(a.id, b.id)
		})
		for _, item := range homes[name] {
			p.loads[name] += item.size
		}
	}
	return homes
}

func (p *allocationPlan) shed(donor string, items []weightedTarget, overloaded map[string]bool, ring *consistent.Consistent, ideal float64) {
	for _, item := range items {
		if p.loads[donor] <= 1.2*ideal {
			break
		}
		if p.loads[donor]-item.size < 0.8*ideal {
			continue
		}
		candidates, err := ring.GetClosestN([]byte(item.key), len(overloaded))
		if err != nil {
			// Keep the home assignment if the ring has no candidate.
			continue
		}
		for _, candidate := range candidates {
			name := candidate.String()
			if overloaded[name] || p.loads[name]+item.size > 1.1*ideal {
				continue
			}
			p.owners[item.id] = name
			p.loads[donor] -= item.size
			p.loads[name] += item.size
			break
		}
	}
	if p.loads[donor] > 1.2*ideal {
		p.unresolved++
	}
}

func hashOrder(a, b string) int {
	if order := cmp.Compare((hasher{}).Sum64([]byte(a)), (hasher{}).Sum64([]byte(b))); order != 0 {
		return order
	}
	return cmp.Compare(a, b)
}

func newLoadSheddingStrategy(config StrategyConfig) (allocationStrategy, error) {
	c, err := sizeAwareConfig(loadSheddingStrategyName, config.LoadShedding)
	if err != nil {
		return nil, err
	}
	return &loadSheddingStrategy{config: c}, nil
}

func (s *loadSheddingStrategy) configuration() SizeAwareStrategyConfig { return s.config }
