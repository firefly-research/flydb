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
Package metrics provides Prometheus-compatible metrics for FlyDB.

METRIC CATEGORIES:
==================
- Queries: executed (total, by type: SELECT, INSERT, UPDATE, DELETE)
- Query Latency: histogram of query execution times
- Connections: active, total
- Databases: count
- Transactions: active, committed, aborted
- Storage: database size, WAL size
- Replication: lag, sync status
- Cluster: nodes, leader status

PROMETHEUS ENDPOINT:
====================
Metrics are exposed at /metrics in Prometheus text format.

EXAMPLE METRICS:
================

	flydb_queries_total{type="SELECT"} 12345
	flydb_queries_total{type="INSERT"} 1234
	flydb_query_latency_seconds{quantile="0.99"} 0.005
	flydb_active_connections 42
	flydb_databases_count 5

GRAFANA DASHBOARD:
==================
Import the provided dashboard JSON for visualization.
*/
package metrics

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"flydb/internal/config"
	"flydb/internal/logging"
)

// Metrics holds all FlyDB metrics.
type Metrics struct {
	// Query metrics
	QueriesTotal       atomic.Uint64 // Total queries executed
	QueriesSelect      atomic.Uint64 // SELECT queries
	QueriesInsert      atomic.Uint64 // INSERT queries
	QueriesUpdate      atomic.Uint64 // UPDATE queries
	QueriesDelete      atomic.Uint64 // DELETE queries
	QueriesFailed      atomic.Uint64 // Failed queries

	// Query latency metrics (in microseconds)
	QueryLatencySum    atomic.Uint64
	QueryLatencyCount  atomic.Uint64

	// Connection metrics
	ActiveConnections  atomic.Int64
	TotalConnections   atomic.Uint64

	// Database metrics
	DatabaseCount      atomic.Int64

	// Transaction metrics
	TransactionsActive    atomic.Int64
	TransactionsCommitted atomic.Uint64
	TransactionsAborted   atomic.Uint64

	// Storage metrics (in bytes)
	StorageSize        atomic.Uint64
	WALSize            atomic.Uint64

	// Replication metrics
	ReplicationLag     atomic.Int64  // Replication lag in milliseconds
	ReplicationSynced  atomic.Bool   // Is replication synced?

	// Cluster metrics
	ClusterNodes       atomic.Int64
	IsLeader           atomic.Bool

	// Per-database metrics
	dbMetrics sync.Map // database name -> *DatabaseMetrics
}

// DatabaseMetrics holds metrics for a specific database.
type DatabaseMetrics struct {
	QueriesTotal  atomic.Uint64
	StorageSize   atomic.Uint64
	TableCount    atomic.Int64
}

// Global metrics instance
var globalMetrics = &Metrics{}

// Get returns the global metrics instance.
func Get() *Metrics {
	return globalMetrics
}

// GetDatabaseMetrics returns metrics for a specific database.
func (m *Metrics) GetDatabaseMetrics(dbName string) *DatabaseMetrics {
	if dm, ok := m.dbMetrics.Load(dbName); ok {
		return dm.(*DatabaseMetrics)
	}
	dm := &DatabaseMetrics{}
	actual, _ := m.dbMetrics.LoadOrStore(dbName, dm)
	return actual.(*DatabaseMetrics)
}

// RecordQuery records a query execution.
func (m *Metrics) RecordQuery(queryType string, latency time.Duration) {
	m.QueriesTotal.Add(1)
	m.QueryLatencySum.Add(uint64(latency.Microseconds()))
	m.QueryLatencyCount.Add(1)

	switch queryType {
	case "SELECT":
		m.QueriesSelect.Add(1)
	case "INSERT":
		m.QueriesInsert.Add(1)
	case "UPDATE":
		m.QueriesUpdate.Add(1)
	case "DELETE":
		m.QueriesDelete.Add(1)
	}
}

// RecordQueryError records a failed query.
func (m *Metrics) RecordQueryError() {
	m.QueriesFailed.Add(1)
}

// ConnectionOpened records a new connection.
func (m *Metrics) ConnectionOpened() {
	m.ActiveConnections.Add(1)
	m.TotalConnections.Add(1)
}

// ConnectionClosed records a closed connection.
func (m *Metrics) ConnectionClosed() {
	m.ActiveConnections.Add(-1)
}

// AverageQueryLatency returns the average query latency in microseconds.
func (m *Metrics) AverageQueryLatency() float64 {
	count := m.QueryLatencyCount.Load()
	if count == 0 {
		return 0
	}
	return float64(m.QueryLatencySum.Load()) / float64(count)
}

// Server provides an HTTP server for Prometheus metrics.
type Server struct {
	config *config.MetricsConfig
	server *http.Server
	logger *logging.Logger
}

// NewServer creates a new metrics server.
func NewServer(cfg *config.MetricsConfig) *Server {
	return &Server{
		config: cfg,
		logger: logging.NewLogger("metrics"),
	}
}

// Start starts the metrics HTTP server.
func (s *Server) Start() error {
	if !s.config.Enabled {
		s.logger.Info("Metrics server disabled")
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", s.handleMetrics)

	s.server = &http.Server{
		Addr:    s.config.Addr,
		Handler: mux,
	}

	go func() {
		s.logger.Info("Starting metrics server", "addr", s.config.Addr)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("Metrics server error", "error", err)
		}
	}()

	return nil
}

// Stop stops the metrics HTTP server.
func (s *Server) Stop() error {
	if s.server == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s.logger.Info("Stopping metrics server")
	return s.server.Shutdown(ctx)
}

// handleMetrics handles the /metrics endpoint in Prometheus format.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	m := Get()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	// Query metrics
	fmt.Fprintf(w, "# HELP flydb_queries_total Total queries executed\n")
	fmt.Fprintf(w, "# TYPE flydb_queries_total counter\n")
	fmt.Fprintf(w, "flydb_queries_total %d\n", m.QueriesTotal.Load())

	fmt.Fprintf(w, "# HELP flydb_queries_by_type_total Queries by type\n")
	fmt.Fprintf(w, "# TYPE flydb_queries_by_type_total counter\n")
	fmt.Fprintf(w, "flydb_queries_by_type_total{type=\"SELECT\"} %d\n", m.QueriesSelect.Load())
	fmt.Fprintf(w, "flydb_queries_by_type_total{type=\"INSERT\"} %d\n", m.QueriesInsert.Load())
	fmt.Fprintf(w, "flydb_queries_by_type_total{type=\"UPDATE\"} %d\n", m.QueriesUpdate.Load())
	fmt.Fprintf(w, "flydb_queries_by_type_total{type=\"DELETE\"} %d\n", m.QueriesDelete.Load())

	fmt.Fprintf(w, "# HELP flydb_queries_failed_total Failed queries\n")
	fmt.Fprintf(w, "# TYPE flydb_queries_failed_total counter\n")
	fmt.Fprintf(w, "flydb_queries_failed_total %d\n", m.QueriesFailed.Load())

	// Query latency
	fmt.Fprintf(w, "# HELP flydb_query_latency_avg_microseconds Average query latency\n")
	fmt.Fprintf(w, "# TYPE flydb_query_latency_avg_microseconds gauge\n")
	fmt.Fprintf(w, "flydb_query_latency_avg_microseconds %.2f\n", m.AverageQueryLatency())

	// Connection metrics
	fmt.Fprintf(w, "# HELP flydb_connections_active Current active connections\n")
	fmt.Fprintf(w, "# TYPE flydb_connections_active gauge\n")
	fmt.Fprintf(w, "flydb_connections_active %d\n", m.ActiveConnections.Load())

	fmt.Fprintf(w, "# HELP flydb_connections_total Total connections\n")
	fmt.Fprintf(w, "# TYPE flydb_connections_total counter\n")
	fmt.Fprintf(w, "flydb_connections_total %d\n", m.TotalConnections.Load())

	// Database metrics
	fmt.Fprintf(w, "# HELP flydb_databases_count Number of databases\n")
	fmt.Fprintf(w, "# TYPE flydb_databases_count gauge\n")
	fmt.Fprintf(w, "flydb_databases_count %d\n", m.DatabaseCount.Load())

	// Transaction metrics
	fmt.Fprintf(w, "# HELP flydb_transactions_active Active transactions\n")
	fmt.Fprintf(w, "# TYPE flydb_transactions_active gauge\n")
	fmt.Fprintf(w, "flydb_transactions_active %d\n", m.TransactionsActive.Load())

	fmt.Fprintf(w, "# HELP flydb_transactions_committed_total Committed transactions\n")
	fmt.Fprintf(w, "# TYPE flydb_transactions_committed_total counter\n")
	fmt.Fprintf(w, "flydb_transactions_committed_total %d\n", m.TransactionsCommitted.Load())

	fmt.Fprintf(w, "# HELP flydb_transactions_aborted_total Aborted transactions\n")
	fmt.Fprintf(w, "# TYPE flydb_transactions_aborted_total counter\n")
	fmt.Fprintf(w, "flydb_transactions_aborted_total %d\n", m.TransactionsAborted.Load())

	// Storage metrics
	fmt.Fprintf(w, "# HELP flydb_storage_size_bytes Total storage size\n")
	fmt.Fprintf(w, "# TYPE flydb_storage_size_bytes gauge\n")
	fmt.Fprintf(w, "flydb_storage_size_bytes %d\n", m.StorageSize.Load())

	fmt.Fprintf(w, "# HELP flydb_wal_size_bytes WAL size\n")
	fmt.Fprintf(w, "# TYPE flydb_wal_size_bytes gauge\n")
	fmt.Fprintf(w, "flydb_wal_size_bytes %d\n", m.WALSize.Load())

	// Replication metrics
	fmt.Fprintf(w, "# HELP flydb_replication_lag_ms Replication lag in milliseconds\n")
	fmt.Fprintf(w, "# TYPE flydb_replication_lag_ms gauge\n")
	fmt.Fprintf(w, "flydb_replication_lag_ms %d\n", m.ReplicationLag.Load())

	synced := 0
	if m.ReplicationSynced.Load() {
		synced = 1
	}
	fmt.Fprintf(w, "# HELP flydb_replication_synced Replication sync status (1=synced, 0=not synced)\n")
	fmt.Fprintf(w, "# TYPE flydb_replication_synced gauge\n")
	fmt.Fprintf(w, "flydb_replication_synced %d\n", synced)

	// Cluster metrics
	fmt.Fprintf(w, "# HELP flydb_cluster_nodes Number of cluster nodes\n")
	fmt.Fprintf(w, "# TYPE flydb_cluster_nodes gauge\n")
	fmt.Fprintf(w, "flydb_cluster_nodes %d\n", m.ClusterNodes.Load())

	isLeader := 0
	if m.IsLeader.Load() {
		isLeader = 1
	}
	fmt.Fprintf(w, "# HELP flydb_is_leader Is this node the cluster leader (1=yes, 0=no)\n")
	fmt.Fprintf(w, "# TYPE flydb_is_leader gauge\n")
	fmt.Fprintf(w, "flydb_is_leader %d\n", isLeader)
}

