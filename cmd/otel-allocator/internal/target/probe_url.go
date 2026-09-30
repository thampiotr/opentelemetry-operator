// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package target

import (
	"fmt"
	"net/url"
	"slices"
	"strings"

	"github.com/prometheus/common/model"
	promconfig "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
)

// ScrapeURL resolves a discovered target using the job's defaults and relabeling.
// Item.Labels contains discovery labels, before target relabeling.
func (t *Item) ScrapeURL(cfg *promconfig.ScrapeConfig) (*url.URL, error) {
	b := labels.NewBuilder(t.Labels)
	for _, seed := range scrapeConfigSeeds(cfg) {
		if b.Get(seed.Name) == "" {
			b.Set(seed.Name, seed.Value)
		}
	}
	if b.Get(model.AddressLabel) == "" {
		b.Set(model.AddressLabel, t.TargetURL)
	}
	if b.Get(model.SchemeLabel) == "" {
		b.Set(model.SchemeLabel, "http")
	}
	if b.Get(model.MetricsPathLabel) == "" {
		b.Set(model.MetricsPathLabel, "/metrics")
	}
	// Match the allocator's no-sharding input to target relabeling.
	b.Set(disableShardingLabelName, "true")
	if !relabel.ProcessBuilder(b, cfg.RelabelConfigs...) {
		return nil, fmt.Errorf("target dropped by relabeling")
	}
	u := &url.URL{Scheme: b.Get(model.SchemeLabel), Host: b.Get(model.AddressLabel), Path: b.Get(model.MetricsPathLabel)}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("invalid probe scheme or address")
	}
	params := make(url.Values, len(cfg.Params))
	for name, values := range cfg.Params {
		params[name] = slices.Clone(values)
	}
	b.Range(func(l labels.Label) {
		if !strings.HasPrefix(l.Name, model.ParamLabelPrefix) {
			return
		}
		name := strings.TrimPrefix(l.Name, model.ParamLabelPrefix)
		if len(params[name]) == 0 {
			params[name] = []string{l.Value}
		} else {
			params[name][0] = l.Value
		}
	})
	u.RawQuery = params.Encode()
	return u, nil
}
