// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package allocation

import (
	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/target"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestInspectionUsesPublishedSizes(t *testing.T) {
	a := testSheddingAllocator(t)
	item := sheddingItem("job", "host:80")
	a.SetTargets([]*target.Item{item})
	a.SetCollectors(map[string]*Collector{"a": NewCollector("a", ""), "empty": NewCollector("empty", "")})
	a.estimates[item.Hash()] = 2000
	a.cycle()
	first := a.Inspect()
	require.Equal(t, float64(2000), first.Sizes[item.Hash()])
	require.Len(t, first.Collectors, 2)
	require.False(t, first.Updated.IsZero())
	a.estimates[item.Hash()] = 3000
	require.Equal(t, float64(2000), a.Inspect().Sizes[item.Hash()], "unpublished estimates must not leak")
	a.cycle()
	require.Equal(t, float64(3000), a.Inspect().Sizes[item.Hash()])
	require.Equal(t, float64(2000), first.Sizes[item.Hash()])
	delete(first.Sizes, item.Hash())
	require.Len(t, a.Inspect().Sizes, 1)
}
