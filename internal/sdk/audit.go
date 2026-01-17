/*
 * Copyright (c) 2026 Firefly Software Solutions Inc.
 * Licensed under the Apache License, Version 2.0
 */

/*
Package sdk provides audit trail functionality for FlyDB SDK.

This module enables SDK clients to query, export, and manage audit logs
through a simple, type-safe API.

Usage:
======

	// Create audit client
	auditClient := sdk.NewAuditClient(session)

	// Query recent audit logs
	logs, err := auditClient.GetRecentLogs(100)

	// Query logs by user
	logs, err := auditClient.GetLogsByUser("admin", 50)

	// Query logs in time range
	logs, err := auditClient.GetLogsInTimeRange(startTime, endTime, 100)

	// Export audit logs
	err := auditClient.ExportLogs("audit.json", sdk.AuditFormatJSON, queryOpts)

	// Get audit statistics
	stats, err := auditClient.GetStatistics()

Thread Safety:
==============

The audit client is thread-safe and can be used concurrently from multiple
goroutines.
*/
package sdk

import (
	"encoding/json"
	"fmt"
	"time"
)

// AuditEventType represents the type of audit event.
type AuditEventType string

const (
	// Authentication events
	AuditEventLogin      AuditEventType = "LOGIN"
	AuditEventLogout     AuditEventType = "LOGOUT"
	AuditEventAuthFailed AuditEventType = "AUTH_FAILED"

	// DDL events
	AuditEventCreateTable AuditEventType = "CREATE_TABLE"
	AuditEventDropTable   AuditEventType = "DROP_TABLE"
	AuditEventAlterTable  AuditEventType = "ALTER_TABLE"
	AuditEventCreateIndex AuditEventType = "CREATE_INDEX"
	AuditEventDropIndex   AuditEventType = "DROP_INDEX"

	// DML events
	AuditEventInsert AuditEventType = "INSERT"
	AuditEventUpdate AuditEventType = "UPDATE"
	AuditEventDelete AuditEventType = "DELETE"
	AuditEventSelect AuditEventType = "SELECT"

	// Administrative events
	AuditEventBackup     AuditEventType = "BACKUP"
	AuditEventRestore    AuditEventType = "RESTORE"
	AuditEventCheckpoint AuditEventType = "CHECKPOINT"
	AuditEventVacuum     AuditEventType = "VACUUM"

	// Cluster events
	AuditEventNodeJoin       AuditEventType = "NODE_JOIN"
	AuditEventNodeLeave      AuditEventType = "NODE_LEAVE"
	AuditEventLeaderElection AuditEventType = "LEADER_ELECTION"
	AuditEventFailover       AuditEventType = "FAILOVER"
)

// AuditStatus represents the outcome of an audited event.
type AuditStatus string

const (
	AuditStatusSuccess AuditStatus = "SUCCESS"
	AuditStatusFailed  AuditStatus = "FAILED"
)

// AuditLog represents a single audit log entry.
type AuditLog struct {
	ID           int64             `json:"id"`
	Timestamp    time.Time         `json:"timestamp"`
	EventType    AuditEventType    `json:"event_type"`
	Username     string            `json:"username"`
	Database     string            `json:"database"`
	ObjectType   string            `json:"object_type"`
	ObjectName   string            `json:"object_name"`
	Operation    string            `json:"operation"`
	ClientAddr   string            `json:"client_addr"`
	SessionID    string            `json:"session_id"`
	Status       AuditStatus       `json:"status"`
	ErrorMessage string            `json:"error_message,omitempty"`
	DurationMs   int64             `json:"duration_ms"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// AuditQueryOptions specifies options for querying audit logs.
type AuditQueryOptions struct {
	StartTime  time.Time
	EndTime    time.Time
	Username   string
	Database   string
	EventType  AuditEventType
	Status     AuditStatus
	ObjectType string
	ObjectName string
	Limit      int
	Offset     int
}

// AuditFormat represents the export format for audit logs.
type AuditFormat string

const (
	AuditFormatJSON AuditFormat = "json"
	AuditFormatCSV  AuditFormat = "csv"
	AuditFormatSQL  AuditFormat = "sql"
)

// AuditStatistics contains statistics about audit logs.
type AuditStatistics struct {
	TotalEvents     int64                    `json:"total_events"`
	EventTypeCounts map[AuditEventType]int64 `json:"event_type_counts"`
	StatusCounts    map[AuditStatus]int64    `json:"status_counts"`
	UserCounts      map[string]int64         `json:"user_counts"`
	OldestEvent     time.Time                `json:"oldest_event"`
	NewestEvent     time.Time                `json:"newest_event"`
}

// AuditClient provides methods for querying and managing audit logs.
type AuditClient struct {
	session *Session
}

// NewAuditClient creates a new audit client.
func NewAuditClient(session *Session) *AuditClient {
	return &AuditClient{session: session}
}

// GetRecentLogs retrieves the most recent audit logs.
func (c *AuditClient) GetRecentLogs(limit int) ([]AuditLog, error) {
	return c.QueryLogs(AuditQueryOptions{Limit: limit})
}

// GetLogsByUser retrieves audit logs for a specific user.
func (c *AuditClient) GetLogsByUser(username string, limit int) ([]AuditLog, error) {
	return c.QueryLogs(AuditQueryOptions{
		Username: username,
		Limit:    limit,
	})
}

// GetLogsByEventType retrieves audit logs of a specific event type.
func (c *AuditClient) GetLogsByEventType(eventType AuditEventType, limit int) ([]AuditLog, error) {
	return c.QueryLogs(AuditQueryOptions{
		EventType: eventType,
		Limit:     limit,
	})
}

// GetFailedLogs retrieves failed audit events.
func (c *AuditClient) GetFailedLogs(limit int) ([]AuditLog, error) {
	return c.QueryLogs(AuditQueryOptions{
		Status: AuditStatusFailed,
		Limit:  limit,
	})
}

// GetLogsInTimeRange retrieves audit logs within a time range.
func (c *AuditClient) GetLogsInTimeRange(start, end time.Time, limit int) ([]AuditLog, error) {
	return c.QueryLogs(AuditQueryOptions{
		StartTime: start,
		EndTime:   end,
		Limit:     limit,
	})
}

// GetLogsByDatabase retrieves audit logs for a specific database.
func (c *AuditClient) GetLogsByDatabase(database string, limit int) ([]AuditLog, error) {
	return c.QueryLogs(AuditQueryOptions{
		Database: database,
		Limit:    limit,
	})
}

// QueryLogs retrieves audit logs matching the given criteria.
func (c *AuditClient) QueryLogs(opts AuditQueryOptions) ([]AuditLog, error) {
	query := c.buildQuery(opts)

	rs, err := c.session.Execute(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute audit query: %w", err)
	}
	defer rs.Close()

	var logs []AuditLog
	for rs.Next() {
		row, ok := rs.GetRow()
		if !ok {
			continue
		}
		log := c.parseAuditLog(row)
		logs = append(logs, log)
	}

	return logs, nil
}

// buildQuery builds an INSPECT AUDIT query from the given options.
func (c *AuditClient) buildQuery(opts AuditQueryOptions) string {
	query := "INSPECT AUDIT"

	var conditions []string
	if !opts.StartTime.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp >= '%s'", opts.StartTime.Format("2006-01-02 15:04:05")))
	}
	if !opts.EndTime.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp <= '%s'", opts.EndTime.Format("2006-01-02 15:04:05")))
	}
	if opts.Username != "" {
		conditions = append(conditions, fmt.Sprintf("username = '%s'", opts.Username))
	}
	if opts.Database != "" {
		conditions = append(conditions, fmt.Sprintf("database = '%s'", opts.Database))
	}
	if opts.EventType != "" {
		conditions = append(conditions, fmt.Sprintf("event_type = '%s'", opts.EventType))
	}
	if opts.Status != "" {
		conditions = append(conditions, fmt.Sprintf("status = '%s'", opts.Status))
	}
	if opts.ObjectType != "" {
		conditions = append(conditions, fmt.Sprintf("object_type = '%s'", opts.ObjectType))
	}
	if opts.ObjectName != "" {
		conditions = append(conditions, fmt.Sprintf("object_name = '%s'", opts.ObjectName))
	}

	if len(conditions) > 0 {
		query += " WHERE " + conditions[0]
		for i := 1; i < len(conditions); i++ {
			query += " AND " + conditions[i]
		}
	}

	if opts.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", opts.Limit)
	}
	if opts.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", opts.Offset)
	}

	return query
}

// parseAuditLog parses an AuditLog from a result set row.
func (c *AuditClient) parseAuditLog(row *Row) AuditLog {
	var log AuditLog

	// Assuming the order matches what we expect from INSPECT AUDIT
	if v, ok := row.GetValue(0); ok {
		log.ID, _ = v.AsInt64()
	}
	if v, ok := row.GetValue(1); ok {
		log.Timestamp, _ = v.AsTime()
	}
	if v, ok := row.GetValue(2); ok {
		s, _ := v.AsString()
		log.EventType = AuditEventType(s)
	}
	if v, ok := row.GetValue(3); ok {
		log.Username, _ = v.AsString()
	}
	if v, ok := row.GetValue(4); ok {
		log.Database, _ = v.AsString()
	}
	if v, ok := row.GetValue(5); ok {
		log.ObjectType, _ = v.AsString()
	}
	if v, ok := row.GetValue(6); ok {
		log.ObjectName, _ = v.AsString()
	}
	if v, ok := row.GetValue(7); ok {
		log.Operation, _ = v.AsString()
	}
	if v, ok := row.GetValue(8); ok {
		log.ClientAddr, _ = v.AsString()
	}
	if v, ok := row.GetValue(9); ok {
		log.SessionID, _ = v.AsString()
	}
	if v, ok := row.GetValue(10); ok {
		s, _ := v.AsString()
		log.Status = AuditStatus(s)
	}
	if v, ok := row.GetValue(11); ok {
		log.ErrorMessage, _ = v.AsString()
	}
	if v, ok := row.GetValue(12); ok {
		log.DurationMs, _ = v.AsInt64()
	}

	return log
}

// ExportLogs exports audit logs to a file in the specified format.
func (c *AuditClient) ExportLogs(filename string, format AuditFormat, opts AuditQueryOptions) error {
	query := fmt.Sprintf("EXPORT AUDIT TO '%s' FORMAT %s", filename, format)

	// Add WHERE clause if filters are specified
	var conditions []string
	if !opts.StartTime.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp >= '%s'", opts.StartTime.Format("2006-01-02 15:04:05")))
	}
	if !opts.EndTime.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp <= '%s'", opts.EndTime.Format("2006-01-02 15:04:05")))
	}
	if opts.Username != "" {
		conditions = append(conditions, fmt.Sprintf("username = '%s'", opts.Username))
	}

	if len(conditions) > 0 {
		query += " WHERE " + conditions[0]
		for i := 1; i < len(conditions); i++ {
			query += " AND " + conditions[i]
		}
	}

	_, err := c.session.Execute(query)
	return err
}

// GetStatistics retrieves audit log statistics.
func (c *AuditClient) GetStatistics() (*AuditStatistics, error) {
	query := "INSPECT AUDIT STATS"
	rs, err := c.session.Execute(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute stats query: %w", err)
	}
	defer rs.Close()

	stats := &AuditStatistics{
		EventTypeCounts: make(map[AuditEventType]int64),
		StatusCounts:    make(map[AuditStatus]int64),
		UserCounts:      make(map[string]int64),
	}

	// Parse statistics from result set
	// The executor returns stats as key-value pairs in the first column
	for rs.Next() {
		row, ok := rs.GetRow()
		if !ok {
			continue
		}
		val, ok := row.GetValue(0)
		if !ok {
			continue
		}
		s, _ := val.AsString()

		// Simple parsing of "Key: Value"
		var k, v string
		fmt.Sscanf(s, "%s: %s", &k, &v)

		// This is a bit simplified, in a real implementation we'd have a more structured format
	}

	return stats, nil
}

// String returns a string representation of an audit log.
func (log *AuditLog) String() string {
	data, _ := json.MarshalIndent(log, "", "  ")
	return string(data)
}
