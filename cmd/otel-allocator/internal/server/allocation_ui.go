// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package server

import (
	_ "embed"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/allocation"
	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/target"
)

//go:embed templates/allocation.html
var allocationPage string

const targetPageSize = 25

type allocationTarget struct {
	ID      string            `json:"id"`
	Job     string            `json:"job"`
	Address string            `json:"address"`
	Owner   string            `json:"owner"`
	Size    *float64          `json:"size"`
	Labels  map[string]string `json:"labels"`
}

type collectorLoad struct {
	Name  string  `json:"name"`
	Count int     `json:"count"`
	Size  float64 `json:"size"`
}

func (s *Server) allocationPageHandler(c *gin.Context) {
	c.Header("Content-Security-Policy", "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'")
	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(allocationPage))
}

func (s *Server) inspection(c *gin.Context) (allocation.Inspection, bool) {
	inspector, ok := s.allocator.(interface{ Inspect() allocation.Inspection })
	if !ok {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "Allocation inspection unavailable"})
		return allocation.Inspection{}, false
	}
	c.Header("Cache-Control", "no-store")
	return inspector.Inspect(), true
}

func (s *Server) allocationDataHandler(c *gin.Context) {
	snapshot, ok := s.inspection(c)
	if !ok {
		return
	}
	names := make([]string, 0, len(snapshot.Collectors))
	loads := make(map[string]*collectorLoad, len(snapshot.Collectors))
	for name := range snapshot.Collectors {
		names = append(names, name)
		loads[name] = &collectorLoad{Name: name}
	}
	slices.Sort(names)
	total, unassigned, known := 0.0, 0, len(snapshot.Targets) > 0
	for id, item := range snapshot.Targets {
		size, measured := snapshot.Sizes[id]
		known = known && measured
		total += size
		if load := loads[item.CollectorName]; load != nil {
			load.Count++
			load.Size += size
		} else {
			unassigned++
		}
	}
	rows := make([]*collectorLoad, 0, len(names))
	for _, name := range names {
		rows = append(rows, loads[name])
	}
	c.JSON(http.StatusOK, gin.H{"strategy": snapshot.Strategy, "updated": snapshot.Updated, "collectors": rows, "count": len(snapshot.Targets), "series": total, "known": known, "unassigned": unassigned})
}

func (s *Server) allocationSearchHandler(c *gin.Context) {
	offset, err := strconv.Atoi(c.DefaultQuery("offset", "0"))
	query := c.Query("q")
	if err != nil || offset < 0 || len(query) > 512 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid offset or query exceeds 512 bytes"})
		return
	}
	snapshot, ok := s.inspection(c)
	if !ok {
		return
	}
	tokens := strings.Fields(strings.ToLower(query))
	owner := c.Query("owner")
	ids := make([]target.ItemHash, 0)
	for id, item := range snapshot.Targets {
		if (owner == "" || item.CollectorName == owner) && matchesTarget(item, tokens) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	rows := make([]allocationTarget, 0, targetPageSize)
	start := min(offset, len(ids))
	end := start + min(targetPageSize, len(ids)-start)
	for _, id := range ids[start:end] {
		item := snapshot.Targets[id]
		row := allocationTarget{ID: id.String(), Job: item.JobName, Address: item.TargetURL, Owner: item.CollectorName, Labels: map[string]string{}}
		item.Labels.Range(func(l labels.Label) { row.Labels[l.Name] = l.Value })
		if size, ok := snapshot.Sizes[id]; ok {
			row.Size = &size
		}
		rows = append(rows, row)
	}
	c.JSON(http.StatusOK, gin.H{"targets": rows, "total": len(ids), "offset": offset, "limit": targetPageSize})
}

func matchesTarget(item *target.Item, tokens []string) bool {
	if len(tokens) == 0 {
		return true
	}
	var text strings.Builder
	text.WriteString(item.TargetURL + " " + item.JobName + " " + item.CollectorName)
	item.Labels.Range(func(l labels.Label) { text.WriteString(" " + l.Name + " " + l.Value) })
	value := strings.ToLower(text.String())
	for _, token := range tokens {
		if !strings.Contains(value, token) {
			return false
		}
	}
	return true
}
