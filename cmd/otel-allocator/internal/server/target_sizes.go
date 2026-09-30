// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"cmp"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/allocation"
	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/target"
)

type sizeTarget struct {
	ID     string        `json:"id"`
	Job    string        `json:"job"`
	URL    string        `json:"url"`
	Labels labels.Labels `json:"labels"`
}

// SizeTargetsHandler lists identities for submitting estimates without changing HTTP SD labels.
func (s *Server) SizeTargetsHandler(c *gin.Context) {
	items := s.allocator.TargetItems()
	targets := make([]sizeTarget, 0, len(items))
	for id, item := range items {
		targets = append(targets, sizeTarget{id.String(), item.JobName, item.TargetURL, item.Labels})
	}
	slices.SortFunc(targets, func(a, b sizeTarget) int { return cmp.Compare(a.ID, b.ID) })
	c.JSON(http.StatusOK, targets)
}

// TargetSizesHandler accepts a partial batch of estimates, keyed by target identity.
func (s *Server) TargetSizesHandler(c *gin.Context) {
	reporter, ok := s.allocator.(allocation.SizeReporter)
	if !ok {
		c.Status(http.StatusNotFound)
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
	decoder := json.NewDecoder(c.Request.Body)
	var reports map[string]float64
	if err := decoder.Decode(&reports); err != nil || reports == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "expected an object mapping target IDs to sizes (maximum 1 MiB)"})
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		c.JSON(http.StatusBadRequest, gin.H{"error": "expected one JSON object"})
		return
	}
	sizes := make(map[target.ItemHash]float64, len(reports))
	for id, size := range reports {
		hash, err := strconv.ParseUint(id, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "target IDs must be unsigned decimal integers"})
			return
		}
		sizes[target.ItemHash(hash)] = size
	}
	accepted, err := reporter.ReportSizes(sizes)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"accepted": accepted, "ignored": len(sizes) - accepted})
}
