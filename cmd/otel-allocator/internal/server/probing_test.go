// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	promconfig "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/allocation"
	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/target"
)

func TestServerConfiguresProbes(t *testing.T) {
	probed := make(chan struct{}, 1)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/custom" {
			http.NotFound(w, r)
			return
		}
		select {
		case probed <- struct{}{}:
		default:
		}
		_, _ = io.WriteString(w, "x 1\n")
	}))
	defer endpoint.Close()
	a, err := allocation.New("load-shedding", logr.Discard(), allocation.WithStrategyConfig(allocation.StrategyConfig{LoadShedding: allocation.LoadSheddingStrategyConfig{Interval: 10 * time.Millisecond}}))
	require.NoError(t, err)
	s, err := NewServer(logr.Discard(), a, "")
	require.NoError(t, err)
	cfg := promconfig.DefaultScrapeConfig
	cfg.JobName, cfg.MetricsPath = "job", "/custom"
	require.NoError(t, s.UpdateScrapeConfigResponse(map[string]*promconfig.ScrapeConfig{"job": &cfg}))
	address := strings.TrimPrefix(endpoint.URL, "http://")
	ls := labels.FromStrings("__address__", address)
	a.SetTargets([]*target.Item{target.NewItem("job", address, ls, "", target.HashLabels(ls, "job"))})
	a.SetCollectors(map[string]*allocation.Collector{"alloy": allocation.NewCollector("alloy", "")})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.(allocation.Runner).Run(ctx) }()
	select {
	case <-probed:
	case <-time.After(time.Second):
		t.Fatal("scrape config did not reach prober")
	}
	require.Eventually(t, func() bool { return len(a.GetTargetsForCollectorAndJob("alloy", "job")) == 1 }, time.Second, 10*time.Millisecond)
	w := httptest.NewRecorder()
	s.server.Handler.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/jobs/job/targets?collector_id=alloy", http.NoBody))
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), address)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("allocator did not stop")
	}
}

func TestTargetSizesEndpointRemoved(t *testing.T) {
	for _, strategy := range []string{"load-shedding", "consistent-hashing"} {
		a, err := allocation.New(strategy, logr.Discard())
		require.NoError(t, err)
		s, err := NewServer(logr.Discard(), a, "")
		require.NoError(t, err)
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			w := httptest.NewRecorder()
			s.server.Handler.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), method, "/target-sizes", http.NoBody))
			require.Equal(t, http.StatusNotFound, w.Code)
		}
	}
}
