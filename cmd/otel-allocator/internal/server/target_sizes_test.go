// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/allocation"
	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/target"
)

func TestTargetSizes(t *testing.T) {
	a, err := allocation.New("load-shedding", logr.Discard(), allocation.WithStrategyConfig(allocation.StrategyConfig{LoadShedding: allocation.LoadSheddingStrategyConfig{Interval: 10 * time.Millisecond}}))
	require.NoError(t, err)
	a.SetTargets([]*target.Item{baseTargetItem})
	a.SetCollectors(map[string]*allocation.Collector{"alloy": allocation.NewCollector("alloy", "")})
	go func() { _ = a.(allocation.Runner).Run(t.Context()) }()
	require.Eventually(t, func() bool { return len(a.TargetItems()) == 1 }, time.Second, 10*time.Millisecond)
	s, err := NewServer(logr.Discard(), a, "")
	require.NoError(t, err)
	w := httptest.NewRecorder()
	s.server.Handler.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/target-sizes", http.NoBody))
	require.Equal(t, http.StatusOK, w.Code)
	var targets []sizeTarget
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &targets))
	require.Len(t, targets, 1)
	require.Equal(t, baseTargetItem.Hash().String(), targets[0].ID)
	require.Equal(t, baseTargetItem.JobName, targets[0].Job)

	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"report", fmt.Sprintf(`{"%s":2000}`, targets[0].ID), http.StatusAccepted},
		{"unknown", `{"0":123}`, http.StatusAccepted},
		{"empty", `{}`, http.StatusAccepted},
		{"invalid-id", `{"invalid":123}`, http.StatusBadRequest},
		{"negative", fmt.Sprintf(`{"%s":-1}`, targets[0].ID), http.StatusBadRequest},
		{"malformed", `{`, http.StatusBadRequest},
		{"null", `null`, http.StatusBadRequest},
		{"extra-object", `{} {}`, http.StatusBadRequest},
		{"too-large", `{"` + strings.Repeat("0", 1<<20) + `":1}`, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			s.server.Handler.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/target-sizes", strings.NewReader(tc.body)))
			require.Equal(t, tc.status, w.Code, w.Body.String())
			if tc.name == "report" {
				require.JSONEq(t, `{"accepted":1,"ignored":0}`, w.Body.String())
			}
			if tc.name == "unknown" {
				require.JSONEq(t, `{"accepted":0,"ignored":1}`, w.Body.String())
			}
		})
	}

	// The same allocator continues to serve standard HTTP SD for Alloy.
	w = httptest.NewRecorder()
	s.server.Handler.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/jobs/test-job/targets?collector_id=alloy", http.NoBody))
	require.Equal(t, http.StatusOK, w.Code)
	var groups []targetJSON
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &groups))
	require.Len(t, groups, 1)
	require.Equal(t, []string{baseTargetItem.TargetURL}, groups[0].TargetURL)
}

func TestTargetSizesDisabledForOtherStrategies(t *testing.T) {
	a, err := allocation.New("consistent-hashing", logr.Discard())
	require.NoError(t, err)
	s, err := NewServer(logr.Discard(), a, "")
	require.NoError(t, err)
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		w := httptest.NewRecorder()
		s.server.Handler.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), method, "/target-sizes", http.NoBody))
		require.Equal(t, http.StatusNotFound, w.Code)
	}
}
