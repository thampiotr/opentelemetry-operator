// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package allocation

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	commonconfig "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	promconfig "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/target"
)

func TestCountSampleLines(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		want       float64
	}{
		{"empty", "", 0},
		{"comments", "# HELP x help\n# TYPE x counter\n\n \t\r\n# EOF\n", 0},
		{"samples", "x{a=\"escaped\\nvalue\"} 1\ny 2 1234\n z 3 # {trace_id=\"1\"} 4\r\nlast 4", 4},
		{"long-line", "x{a=\"" + strings.Repeat("v", 100000) + "\"} 1\ny 2\n", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			size, err := countSampleLines(strings.NewReader(tc.text), int64(len(tc.text)))
			require.NoError(t, err)
			require.Equal(t, tc.want, size)
		})
	}
	_, err := countSampleLines(strings.NewReader("x 1\n"), 3)
	require.ErrorContains(t, err, "exceeds")
	_, err = countSampleLines(io.MultiReader(strings.NewReader("x 1\n"), iotest.ErrReader(io.ErrUnexpectedEOF)), 100)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestProbeRequest(t *testing.T) {
	var received atomic.Bool
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "user" || password != "secret" || r.URL.Path != "/custom" || r.URL.Query().Encode() != "x=override&x=second" {
			http.Error(w, "wrong request", http.StatusBadRequest)
			return
		}
		received.Store(true)
		w.Header().Set("Content-Type", "application/openmetrics-text;version=1.0.0")
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		_, _ = io.WriteString(gz, "# TYPE x counter\nx 1\ny 2\n# EOF\n")
		_ = gz.Close()
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	cfg := probeConfig()
	cfg.Scheme = "https"
	cfg.HTTPClientConfig.TLSConfig.InsecureSkipVerify = true
	cfg.HTTPClientConfig.BasicAuth = &commonconfig.BasicAuth{Username: "user", Password: "secret"}
	cfg.Params = url.Values{"x": []string{"first", "second"}}
	address, path := relabel.DefaultRelabelConfig, relabel.DefaultRelabelConfig
	address.TargetLabel = model.AddressLabel
	address.Replacement = u.Host
	path.SourceLabels = model.LabelNames{"__meta_path"}
	path.TargetLabel = model.MetricsPathLabel
	require.NoError(t, address.Validate(model.UTF8Validation))
	require.NoError(t, path.Validate(model.UTF8Validation))
	cfg.RelabelConfigs = []*relabel.Config{&address, &path}
	ls := labels.FromStrings("__address__", "original:80", "__meta_path", "/custom", "__param_x", "override")
	item := target.NewItem("job", "original:80", ls, "", target.HashLabels(ls, "job"))
	a := testSheddingAllocator(t)
	a.SetScrapeConfigs(map[string]*promconfig.ScrapeConfig{"job": cfg})
	a.SetTargets([]*target.Item{item})
	work := a.takeProbe(time.Now())
	require.NotNil(t, work)
	size, err := a.measureTarget(t.Context(), work)
	require.NoError(t, err)
	require.Equal(t, float64(2), size)
	a.strategy.config.ProbeMaxBytes = 4
	_, err = a.measureTarget(t.Context(), work)
	require.ErrorContains(t, err, "exceeds", "limit applies after gzip decompression")
	require.True(t, received.Load())
	require.Equal(t, []string{"first", "second"}, cfg.Params["x"], "URL building must not mutate shared job config")
	closeProbeJobs(a.probeJobs)
}

func TestProbeFailures(t *testing.T) {
	for _, tc := range []struct {
		name, contentType, body string
		status                  int
		stall                   bool
	}{
		{"status", "text/plain", "unavailable", 503, false},
		{"binary", "application/vnd.google.protobuf", "binary", 200, false},
		{"html", "text/html", "<html>error</html>", 200, false},
		{"too-large", "text/plain", strings.Repeat("x 1\n", 100), 200, false},
		{"timeout", "text/plain", "", 200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.stall {
					<-r.Context().Done()
					return
				}
				w.Header().Set("Content-Type", tc.contentType)
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()
			a := testSheddingAllocator(t)
			a.strategy.config.ProbeTimeout = 30 * time.Millisecond
			a.strategy.config.ProbeMaxBytes = 100
			a.SetScrapeConfigs(map[string]*promconfig.ScrapeConfig{"job": probeConfig()})
			item := sheddingItem("job", strings.TrimPrefix(srv.URL, "http://"))
			a.SetTargets([]*target.Item{item})
			a.estimates[item.Hash()] = 123
			work := a.takeProbe(time.Now())
			size, err := a.measureTarget(t.Context(), work)
			require.Error(t, err)
			a.finishProbe(work, size, err)
			require.Equal(t, float64(123), a.estimates[item.Hash()])
			require.Nil(t, a.takeProbe(time.Now()), "failed targets must wait before retrying")
			closeProbeJobs(a.probeJobs)
		})
	}
}

func TestProbeDiscardsObsoleteResults(t *testing.T) {
	a := testSheddingAllocator(t)
	cfg := map[string]*promconfig.ScrapeConfig{"job": probeConfig()}
	a.SetScrapeConfigs(cfg)
	item := sheddingItem("job", "host:80")
	a.SetTargets([]*target.Item{item})
	old := a.takeProbe(time.Now())
	a.SetTargets(nil)
	a.SetTargets([]*target.Item{item})
	a.finishProbe(old, 200, nil)
	require.Empty(t, a.estimates)
	old = a.takeProbe(time.Now())
	a.SetScrapeConfigs(cfg)
	a.finishProbe(old, 300, nil)
	require.Empty(t, a.estimates)
	current := a.takeProbe(time.Now())
	require.NotNil(t, current)
	a.finishProbe(current, 0, nil)
	require.Equal(t, float64(1), a.estimates[item.Hash()], "empty targets retain a minimum accounting weight")
	current = a.takeProbe(time.Now().Add(time.Minute))
	require.NotNil(t, current, "successful targets must be probed again")
	a.finishProbe(current, 0, errors.New("unavailable"))
	require.Equal(t, float64(1), a.estimates[item.Hash()])
	closeProbeJobs(a.probeJobs)
}

func TestAsyncProbesDoNotDelayAllocation(t *testing.T) {
	started, release := make(chan struct{}, 8), make(chan struct{})
	var active, peak atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		started <- struct{}{}
		select {
		case <-release:
			_, _ = io.WriteString(w, "x 1\n")
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	a := testSheddingAllocator(t)
	a.strategy.config.Interval = 10 * time.Millisecond
	a.strategy.config.ProbeConcurrency = 2
	a.SetScrapeConfigs(map[string]*promconfig.ScrapeConfig{"job": probeConfig()})
	items := MakeNTargetsForJob(4, "job", 0)
	for _, item := range items {
		item.TargetURL = strings.TrimPrefix(srv.URL, "http://")
	}
	a.SetTargets(items)
	a.SetCollectors(map[string]*Collector{"node": NewCollector("node", "")})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("probes did not start")
		}
	}
	require.Eventually(t, func() bool { return len(a.GetTargetsForCollectorAndJob("node", "job")) == 4 }, time.Second, 10*time.Millisecond)
	a.mu.Lock()
	estimates := len(a.estimates)
	a.mu.Unlock()
	require.Zero(t, estimates, "targets must be assigned while probes are still blocked")
	close(release)
	require.Eventually(t, func() bool { a.mu.Lock(); defer a.mu.Unlock(); return len(a.estimates) == 4 }, time.Second, 10*time.Millisecond)
	require.Equal(t, int32(2), peak.Load())
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("probers did not stop")
	}
}

func probeConfig() *promconfig.ScrapeConfig {
	cfg := promconfig.DefaultScrapeConfig
	cfg.JobName = "job"
	return &cfg
}

func TestProbeClientFailureDoesNotBlockAllocation(t *testing.T) {
	a := testSheddingAllocator(t)
	cfg := probeConfig()
	cfg.HTTPClientConfig.TLSConfig.CAFile = filepath.Join(t.TempDir(), "missing.pem")
	a.SetScrapeConfigs(map[string]*promconfig.ScrapeConfig{"job": cfg})
	require.Empty(t, a.probeJobs)
	a.SetTargets([]*target.Item{sheddingItem("job", "host:80")})
	a.SetCollectors(map[string]*Collector{"node": NewCollector("node", "")})
	a.cycle()
	require.Len(t, a.GetTargetsForCollectorAndJob("node", "job"), 1)
	require.Nil(t, a.takeProbe(time.Now()))
}
