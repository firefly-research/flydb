/*
 * Copyright (c) 2026 Firefly Software Solutions Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
Package health provides health check endpoints for FlyDB.

ENDPOINTS:
==========

	GET /health       - Overall health check
	GET /health/live  - Liveness check (is the process running?)
	GET /health/ready - Readiness check (is the service ready for traffic?)

STATUS VALUES:
==============
  - healthy: All checks pass
  - degraded: Some non-critical checks fail
  - unhealthy: Critical checks fail

KUBERNETES INTEGRATION:
=======================
Configure probes in your deployment:

	livenessProbe:
	  httpGet:
	    path: /health/live
	    port: 9095
	readinessProbe:
	  httpGet:
	    path: /health/ready
	    port: 9095
*/
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"flydb/internal/config"
	"flydb/internal/logging"
)

// Status represents the health status.
type Status string

const (
	StatusHealthy   Status = "healthy"
	StatusUnhealthy Status = "unhealthy"
	StatusDegraded  Status = "degraded"
)

// CheckResult represents the result of a health check.
type CheckResult struct {
	Name    string `json:"name"`
	Status  Status `json:"status"`
	Message string `json:"message,omitempty"`
	Latency int64  `json:"latency_ms"`
}

// HealthResponse represents the health check response.
type HealthResponse struct {
	Status    Status        `json:"status"`
	Timestamp string        `json:"timestamp"`
	Version   string        `json:"version,omitempty"`
	Checks    []CheckResult `json:"checks,omitempty"`
}

// Check is a function that performs a health check.
type Check func() CheckResult

// Checker manages health checks.
type Checker struct {
	mu      sync.RWMutex
	checks  map[string]Check
	version string
	logger  *logging.Logger
}

// NewChecker creates a new health checker.
func NewChecker(version string) *Checker {
	return &Checker{
		checks:  make(map[string]Check),
		version: version,
		logger:  logging.NewLogger("health"),
	}
}

// RegisterCheck registers a health check.
func (c *Checker) RegisterCheck(name string, check Check) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checks[name] = check
}

// RunChecks runs all registered health checks.
func (c *Checker) RunChecks() HealthResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()

	response := HealthResponse{
		Status:    StatusHealthy,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Version:   c.version,
		Checks:    make([]CheckResult, 0, len(c.checks)),
	}

	for name, check := range c.checks {
		start := time.Now()
		result := check()
		result.Name = name
		result.Latency = time.Since(start).Milliseconds()
		response.Checks = append(response.Checks, result)

		// Update overall status
		if result.Status == StatusUnhealthy {
			response.Status = StatusUnhealthy
		} else if result.Status == StatusDegraded && response.Status == StatusHealthy {
			response.Status = StatusDegraded
		}
	}

	return response
}

// IsHealthy returns true if all checks pass.
func (c *Checker) IsHealthy() bool {
	response := c.RunChecks()
	return response.Status == StatusHealthy
}


// Server provides HTTP health check endpoints.
type Server struct {
	config  *config.HealthConfig
	checker *Checker
	server  *http.Server
	logger  *logging.Logger
}

// NewServer creates a new health check server.
func NewServer(cfg *config.HealthConfig, checker *Checker) *Server {
	return &Server{
		config:  cfg,
		checker: checker,
		logger:  logging.NewLogger("health"),
	}
}

// Start starts the health check HTTP server.
func (s *Server) Start() error {
	if !s.config.Enabled {
		s.logger.Info("Health check server disabled")
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/health/live", s.handleLiveness)
	mux.HandleFunc("/health/ready", s.handleReadiness)

	s.server = &http.Server{
		Addr:    s.config.Addr,
		Handler: mux,
	}

	go func() {
		s.logger.Info("Starting health check server", "addr", s.config.Addr)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("Health check server error", "error", err)
		}
	}()

	return nil
}

// Stop stops the health check HTTP server.
func (s *Server) Stop() error {
	if s.server == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s.logger.Info("Stopping health check server")
	return s.server.Shutdown(ctx)
}

// handleHealth handles the /health endpoint.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	response := s.checker.RunChecks()

	w.Header().Set("Content-Type", "application/json")
	if response.Status != StatusHealthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(response)
}

// handleLiveness handles the /health/live endpoint (Kubernetes liveness probe).
func (s *Server) handleLiveness(w http.ResponseWriter, r *http.Request) {
	// Liveness just checks if the process is running
	response := HealthResponse{
		Status:    StatusHealthy,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Version:   s.checker.version,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleReadiness handles the /health/ready endpoint (Kubernetes readiness probe).
func (s *Server) handleReadiness(w http.ResponseWriter, r *http.Request) {
	response := s.checker.RunChecks()

	w.Header().Set("Content-Type", "application/json")
	if response.Status == StatusUnhealthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(response)
}

// Common health checks

// StorageCheck creates a storage health check.
func StorageCheck(checkFn func() error) Check {
	return func() CheckResult {
		if err := checkFn(); err != nil {
			return CheckResult{
				Status:  StatusUnhealthy,
				Message: err.Error(),
			}
		}
		return CheckResult{
			Status: StatusHealthy,
		}
	}
}

// ClusterCheck creates a cluster health check.
func ClusterCheck(checkFn func() (bool, string)) Check {
	return func() CheckResult {
		healthy, message := checkFn()
		if !healthy {
			return CheckResult{
				Status:  StatusDegraded,
				Message: message,
			}
		}
		return CheckResult{
			Status:  StatusHealthy,
			Message: message,
		}
	}
}

// ReplicationCheck creates a replication health check.
func ReplicationCheck(checkFn func() (bool, int64)) Check {
	return func() CheckResult {
		synced, _ := checkFn()
		if !synced {
			return CheckResult{
				Status:  StatusDegraded,
				Message: "replication lag detected",
			}
		}
		return CheckResult{
			Status:  StatusHealthy,
			Message: "replication synced",
		}
	}
}


