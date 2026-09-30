// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package allocation

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	commonconfig "github.com/prometheus/common/config"
	promconfig "github.com/prometheus/prometheus/config"

	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/target"
)

// ScrapeConfigReceiver receives the same job configurations served to collectors.
type ScrapeConfigReceiver interface {
	SetScrapeConfigs(map[string]*promconfig.ScrapeConfig)
}

type probeJob struct {
	client *http.Client
	config *promconfig.ScrapeConfig
}

type probeState struct {
	next    time.Time
	running bool
}

type sizeProbe struct {
	item  *target.Item
	job   *probeJob
	state *probeState
}

func (a *loadSheddingAllocator) SetScrapeConfigs(configs map[string]*promconfig.ScrapeConfig) {
	jobs := make(map[string]*probeJob, len(configs))
	for name, cfg := range configs {
		client, err := commonconfig.NewClientFromConfig(cfg.HTTPClientConfig, "target_allocator_size_probe")
		if err != nil {
			a.log.Error(err, "Unable to create target size probe client", "job", name)
			continue
		}
		jobs[name] = &probeJob{client: client, config: cfg}
	}
	a.mu.Lock()
	previous := a.probeJobs
	a.probeJobs = jobs
	for _, state := range a.probes {
		state.next = time.Time{}
	}
	a.mu.Unlock()
	closeProbeJobs(previous)
	a.wakeProbes()
}

func closeProbeJobs(jobs map[string]*probeJob) {
	for _, job := range jobs {
		job.client.CloseIdleConnections()
	}
}

func (a *loadSheddingAllocator) wakeProbes() {
	select {
	case a.probeWake <- struct{}{}:
	default:
	}
}

func (a *loadSheddingAllocator) runProber(ctx context.Context) {
	ticker := time.NewTicker(min(time.Second, a.strategy.config.ProbeInterval))
	defer ticker.Stop()
	for ctx.Err() == nil {
		if work := a.takeProbe(time.Now()); work != nil {
			size, err := a.measureTarget(ctx, work)
			a.finishProbe(work, size, err)
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-a.probeWake:
		}
	}
}

// takeProbe selects from current discovery, without queuing snapshots behind slow requests.
func (a *loadSheddingAllocator) takeProbe(now time.Time) *sizeProbe {
	a.mu.Lock()
	defer a.mu.Unlock()
	var work *sizeProbe
	for id, state := range a.probes {
		item := a.targets[id]
		job := a.probeJobs[item.JobName]
		if state.running || state.next.After(now) || job == nil {
			continue
		}
		if work == nil || state.next.Before(work.state.next) || (state.next.Equal(work.state.next) && id < work.item.Hash()) {
			work = &sizeProbe{item: item, job: job, state: state}
		}
	}
	if work != nil {
		work.state.running = true
	}
	return work
}

func (a *loadSheddingAllocator) finishProbe(work *sizeProbe, size float64, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.probes[work.item.Hash()] != work.state {
		return
	}
	work.state.running = false
	if a.probeJobs[work.item.JobName] != work.job {
		work.job.client.CloseIdleConnections()
		return
	}
	work.state.next = time.Now().Add(a.strategy.config.ProbeInterval)
	if err == nil {
		a.estimates[work.item.Hash()] = max(1, size)
	} else {
		a.log.V(1).Info("Target size probe failed; retaining estimate", "target", work.item.Hash().String(), "job", work.item.JobName)
	}
}

func (a *loadSheddingAllocator) measureTarget(ctx context.Context, work *sizeProbe) (float64, error) {
	u, err := work.item.ScrapeURL(work.job.config)
	if err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, a.strategy.config.ProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("Accept", "text/plain;version=0.0.4,application/openmetrics-text;version=1.0.0;q=0.9")
	resp, err := work.job.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return countProbeResponse(resp, a.strategy.config.ProbeMaxBytes)
}

func countProbeResponse(resp *http.Response, limit int64) (float64, error) {
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("probe status %d", resp.StatusCode)
	}
	if contentType := resp.Header.Get("Content-Type"); contentType != "" {
		mediaType, _, err := mime.ParseMediaType(contentType)
		if err != nil || (mediaType != "text/plain" && mediaType != "application/openmetrics-text") {
			return 0, fmt.Errorf("probe requires a text exposition")
		}
	}
	var body io.Reader = resp.Body
	switch strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding"))) {
	case "", "identity":
	case "gzip":
		decoded, err := gzip.NewReader(resp.Body)
		if err != nil {
			return 0, err
		}
		defer decoded.Close()
		body = decoded
	default:
		return 0, fmt.Errorf("unsupported probe content encoding")
	}
	return countSampleLines(body, limit)
}

// countSampleLines scans bounded chunks, without allocating metric names, labels or values.
// A text exposition sample occupies one non-empty, non-comment line.
func countSampleLines(r io.Reader, limit int64) (float64, error) {
	r = io.LimitReader(r, limit+1)
	buf := make([]byte, 32*1024)
	var read int64
	var samples float64
	lineStarted := false
	for {
		n, err := r.Read(buf)
		read += int64(n)
		if read > limit {
			return 0, fmt.Errorf("probe body exceeds %d bytes", limit)
		}
		for _, b := range buf[:n] {
			if b == '\n' {
				lineStarted = false
				continue
			}
			if lineStarted || b == ' ' || b == '\t' || b == '\r' {
				continue
			}
			lineStarted = true
			if b != '#' {
				samples++
			}
		}
		if err == io.EOF {
			return samples, nil
		}
		if err != nil {
			return 0, err
		}
	}
}
