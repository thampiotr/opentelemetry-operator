// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/go-logr/logr"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/allocation"
	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/target"
)

func TestAllocationExplorer(t *testing.T) {
	a, err := allocation.New("consistent-hashing", logr.Discard())
	require.NoError(t, err)
	a.SetCollectors(map[string]*allocation.Collector{"a": allocation.NewCollector("a", ""), "b": allocation.NewCollector("b", "")})
	items := make([]*target.Item, 60)
	for i := range items {
		address := fmt.Sprintf("host-%d:80", i)
		ls := labels.FromStrings("__address__", address, "service", "checkout-api", "note", "<script>alert(1)</script>")
		items[i] = target.NewItem("job", address, ls, "", target.HashLabels(ls, "job"))
	}
	a.SetTargets(items)
	s, err := NewServer(logr.Discard(), a, "")
	require.NoError(t, err)
	get := func(path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		s.server.Handler.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		return w
	}
	w := get("/debug/allocation/data")
	require.Equal(t, 200, w.Code)
	var summary map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &summary))
	require.NotContains(t, summary, "targets")
	require.JSONEq(t, "60", string(summary["count"]))
	var first struct {
		Targets []allocationTarget
		Total   int
	}
	w = get("/debug/allocation/search?q=CHECKOUT%20service")
	require.Equal(t, 200, w.Code)
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &first))
	require.Equal(t, 60, first.Total)
	require.Len(t, first.Targets, 25)
	require.Nil(t, first.Targets[0].Size)
	require.NotContains(t, w.Body.String(), "<script>")
	require.Equal(t, "<script>alert(1)</script>", first.Targets[0].Labels["note"])
	var next struct {
		Targets []allocationTarget
		Total   int
	}
	require.NoError(t, json.Unmarshal(get("/debug/allocation/search?offset=25").Body.Bytes(), &next))
	require.Len(t, next.Targets, 25)
	seen := map[string]bool{}
	for _, row := range first.Targets {
		seen[row.ID] = true
	}
	for _, row := range next.Targets {
		require.False(t, seen[row.ID])
	}
	require.NoError(t, json.Unmarshal(get("/debug/allocation/search?offset=50").Body.Bytes(), &next))
	require.Len(t, next.Targets, 10)
	require.NoError(t, json.Unmarshal(get("/debug/allocation/search?q=absent").Body.Bytes(), &next))
	require.Empty(t, next.Targets)
	require.NoError(t, json.Unmarshal(get("/debug/allocation/search?owner=a").Body.Bytes(), &next))
	for _, row := range next.Targets {
		require.Equal(t, "a", row.Owner)
	}
	require.Equal(t, 400, get("/debug/allocation/search?offset=-1").Code)
	require.Equal(t, 400, get("/debug/allocation/search?offset=bad").Code)
	w = get("/debug/allocation")
	require.Equal(t, 200, w.Code)
	require.Contains(t, w.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'")
}
