// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Note: These utilities are used by other packages, which is why they're defined in a non-test file.

package allocation

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/go-logr/logr"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/target"
)

func colIndex(index, numCols int) int {
	if numCols == 0 {
		return -1
	}
	return index % numCols
}

func MakeNNewTargets(n, numCollectors, startingIndex int) []*target.Item {
	toReturn := []*target.Item{}
	for i := startingIndex; i < n+startingIndex; i++ {
		collector := fmt.Sprintf("collector-%d", colIndex(i, numCollectors))
		label := labels.New(
			labels.Label{Name: "i", Value: strconv.Itoa(i)},
			labels.Label{Name: "total", Value: strconv.Itoa(n + startingIndex)},
		)
		jobName := fmt.Sprintf("test-job-%d", i)
		newTarget := target.NewItem(jobName, fmt.Sprintf("test-url-%d", i), label, collector, target.HashLabels(label, jobName))
		toReturn = append(toReturn, newTarget)
	}
	return toReturn
}

func MakeNCollectors(n, startingIndex int) map[string]*Collector {
	toReturn := map[string]*Collector{}
	for i := startingIndex; i < n+startingIndex; i++ {
		collector := fmt.Sprintf("collector-%d", i)
		toReturn[collector] = &Collector{
			Name:       collector,
			NumTargets: 0,
			NodeName:   fmt.Sprintf("node-%d", i),
		}
	}
	return toReturn
}

func MakeNNewTargetsWithEmptyCollectors(n, startingIndex int) []*target.Item {
	toReturn := []*target.Item{}
	for i := startingIndex; i < n+startingIndex; i++ {
		label := labels.New(
			labels.Label{Name: "i", Value: strconv.Itoa(i)},
			labels.Label{Name: "total", Value: strconv.Itoa(n + startingIndex)},
			labels.Label{Name: "__meta_kubernetes_pod_node_name", Value: "node-0"},
		)
		jobName := fmt.Sprintf("test-job-%d", i)
		newTarget := target.NewItem(jobName, fmt.Sprintf("test-url-%d", i), label, "", target.HashLabels(label, jobName))
		toReturn = append(toReturn, newTarget)
	}
	return toReturn
}

// MakeNTargetsForJob creates n targets all belonging to the same job.
func MakeNTargetsForJob(n int, jobName string, startingIndex int) []*target.Item {
	toReturn := []*target.Item{}
	for i := startingIndex; i < n+startingIndex; i++ {
		label := labels.New(
			labels.Label{Name: "i", Value: strconv.Itoa(i)},
			labels.Label{Name: "total", Value: strconv.Itoa(n + startingIndex)},
		)
		newTarget := target.NewItem(jobName, fmt.Sprintf("test-url-%d", i), label, "", target.HashLabels(label, jobName))
		toReturn = append(toReturn, newTarget)
	}
	return toReturn
}

func RunForAllStrategies(t *testing.T, f func(t *testing.T, allocator Allocator)) {
	allocatorNames := GetRegisteredAllocatorNames()
	logger := logf.Log.WithName("unit-tests")
	for _, allocatorName := range allocatorNames {
		t.Run(allocatorName, func(t *testing.T) {
			allocator, err := NewTestAllocator(allocatorName, logger)
			require.NoError(t, err)
			f(t, allocator)
		})
	}
}

// NewTestAllocator advances periodic allocators after each update, without real timers.
// It is for tests and benchmarks which expect synchronous SetTargets/SetCollectors.
func NewTestAllocator(name string, log logr.Logger) (Allocator, error) {
	a, err := New(name, log)
	if periodic, ok := a.(*sizeAwareAllocator); ok {
		return synchronousAllocator{periodic}, err
	}
	return a, err
}

type synchronousAllocator struct{ *sizeAwareAllocator }

func (a synchronousAllocator) SetTargets(items []*target.Item) {
	a.sizeAwareAllocator.SetTargets(items)
	a.cycle()
}

func (a synchronousAllocator) SetCollectors(collectors map[string]*Collector) {
	a.sizeAwareAllocator.SetCollectors(collectors)
	a.cycle()
}
