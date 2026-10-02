// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package allocation

import (
	"cmp"
	"maps"
	"slices"

	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/target"
)

const stickyStrategyName = "sticky"

type stickyStrategy struct {
	config     SizeAwareStrategyConfig
	owners     map[target.ItemHash]string
	overloaded map[string]int
}

func newStickyStrategy(config StrategyConfig) (allocationStrategy, error) {
	c, err := sizeAwareConfig(stickyStrategyName, config.Sticky)
	if err != nil {
		return nil, err
	}
	return &stickyStrategy{config: c, owners: map[target.ItemHash]string{}, overloaded: map[string]int{}}, nil
}

func (*stickyStrategy) GetName() string                          { return stickyStrategyName }
func (s *stickyStrategy) configuration() SizeAwareStrategyConfig { return s.config }

func (s *stickyStrategy) plan(items map[target.ItemHash]*target.Item, collectors map[string]*Collector, sizes map[target.ItemHash]float64, total float64) allocationPlan {
	p := allocationPlan{owners: map[target.ItemHash]string{}, loads: map[string]float64{}}
	names := slices.Sorted(maps.Keys(collectors))
	if len(names) == 0 {
		s.owners = p.owners
		clear(s.overloaded)
		return p
	}
	for _, name := range names {
		p.loads[name] = 0
	}
	targets := stickyTargets(items, sizes)
	s.place(&p, targets, names)
	ideal := total / float64(len(names))
	s.trackOverload(p.loads, 1.2*ideal)
	s.rebalance(&p, targets, names, ideal)
	for _, load := range p.loads {
		if load > 1.2*ideal {
			p.unresolved++
		}
	}
	s.owners = p.owners
	return p
}

func stickyTargets(items map[target.ItemHash]*target.Item, sizes map[target.ItemHash]float64) []weightedTarget {
	targets := make([]weightedTarget, 0, len(items))
	for id, item := range items {
		targets = append(targets, weightedTarget{id, item.TargetURL, sizes[id]})
	}
	slices.SortFunc(targets, func(a, b weightedTarget) int {
		if order := cmp.Compare(b.size, a.size); order != 0 {
			return order
		}
		if order := hashOrder(a.key, b.key); order != 0 {
			return order
		}
		return cmp.Compare(a.id, b.id)
	})
	return targets
}

func (s *stickyStrategy) place(p *allocationPlan, targets []weightedTarget, names []string) {
	var pending []weightedTarget
	for _, item := range targets {
		owner, exists := s.owners[item.id]
		if _, alive := p.loads[owner]; exists && alive {
			p.owners[item.id] = owner
			p.loads[owner] += item.size
		} else {
			pending = append(pending, item)
		}
	}
	for _, item := range pending {
		owner := leastLoaded(names, p.loads)
		p.owners[item.id] = owner
		p.loads[owner] += item.size
	}
}

func (s *stickyStrategy) trackOverload(loads map[string]float64, capacity float64) {
	for name := range s.overloaded {
		if _, alive := loads[name]; !alive {
			delete(s.overloaded, name)
		}
	}
	for name, load := range loads {
		if load > capacity {
			s.overloaded[name] = min(s.config.Window, s.overloaded[name]+1)
		} else {
			s.overloaded[name] = 0
		}
	}
}

func (s *stickyStrategy) rebalance(p *allocationPlan, targets []weightedTarget, names []string, ideal float64) {
	donors := slices.Clone(names)
	slices.SortFunc(donors, func(a, b string) int {
		if order := cmp.Compare(p.loads[b], p.loads[a]); order != 0 {
			return order
		}
		return hashOrder(a, b)
	})
	for _, donor := range donors {
		if s.overloaded[donor] < s.config.Window {
			continue
		}
		for _, item := range targets {
			if p.loads[donor] <= 1.2*ideal {
				break
			}
			if p.owners[item.id] != donor {
				continue
			}
			receiver := leastLoaded(names, p.loads)
			next := p.loads[receiver] + item.size
			if receiver == donor || next > 1.1*ideal || next >= p.loads[donor] {
				continue
			}
			p.owners[item.id] = receiver
			p.loads[donor] -= item.size
			p.loads[receiver] = next
		}
	}
}

func leastLoaded(names []string, loads map[string]float64) string {
	owner := names[0]
	for _, name := range names[1:] {
		if loads[name] < loads[owner] {
			owner = name
		}
	}
	return owner
}
