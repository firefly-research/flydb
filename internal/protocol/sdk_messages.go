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
SDK Protocol Messages

This file contains protocol message types for SDK and driver support,
including cursor operations, metadata queries, transaction control,
and session management.
*/
package protocol

import (
	"encoding/json"
)

// ============================================================================
// Cursor Messages
// ============================================================================

// CursorOpenMessage requests opening a cursor for a query.
type CursorOpenMessage struct {
	Query       string        `json:"query"`
	CursorType  int           `json:"cursor_type"`  // 0=forward-only, 1=static, 2=keyset, 3=dynamic
	Concurrency int           `json:"concurrency"`  // 0=read-only, 1=lock, 2=optimistic
	FetchSize   int           `json:"fetch_size"`
	Parameters  []interface{} `json:"parameters,omitempty"`
}

// Encode encodes the message to bytes.
func (m *CursorOpenMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeCursorOpenMessage decodes a cursor open message.
func DecodeCursorOpenMessage(data []byte) (*CursorOpenMessage, error) {
	var m CursorOpenMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// CursorFetchMessage requests fetching rows from a cursor.
type CursorFetchMessage struct {
	CursorID  string `json:"cursor_id"`
	Direction int    `json:"direction"` // 0=next, 1=prior, 2=first, 3=last, 4=absolute, 5=relative
	Offset    int64  `json:"offset"`    // For absolute/relative positioning
	Count     int    `json:"count"`     // Number of rows to fetch
}

// Encode encodes the message to bytes.
func (m *CursorFetchMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeCursorFetchMessage decodes a cursor fetch message.
func DecodeCursorFetchMessage(data []byte) (*CursorFetchMessage, error) {
	var m CursorFetchMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// CursorCloseMessage requests closing a cursor.
type CursorCloseMessage struct {
	CursorID string `json:"cursor_id"`
}

// Encode encodes the message to bytes.
func (m *CursorCloseMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeCursorCloseMessage decodes a cursor close message.
func DecodeCursorCloseMessage(data []byte) (*CursorCloseMessage, error) {
	var m CursorCloseMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// CursorResultMessage is the response for cursor operations.
type CursorResultMessage struct {
	Success     bool              `json:"success"`
	CursorID    string            `json:"cursor_id,omitempty"`
	Columns     []ColumnMetadata  `json:"columns,omitempty"`
	Rows        [][]interface{}   `json:"rows,omitempty"`
	RowCount    int               `json:"row_count"`
	HasMoreRows bool              `json:"has_more_rows"`
	Position    int64             `json:"position"`
	TotalRows   int64             `json:"total_rows"` // -1 if unknown
	Message     string            `json:"message,omitempty"`
}

// ColumnMetadata provides detailed column information.
type ColumnMetadata struct {
	Index         int    `json:"index"`
	Name          string `json:"name"`
	Label         string `json:"label"`
	Type          string `json:"type"`
	TypeCode      int    `json:"type_code"`
	Precision     int    `json:"precision"`
	Scale         int    `json:"scale"`
	DisplaySize   int    `json:"display_size"`
	Nullable      bool   `json:"nullable"`
	AutoIncrement bool   `json:"auto_increment"`
	ReadOnly      bool   `json:"read_only"`
	TableName     string `json:"table_name"`
	SchemaName    string `json:"schema_name"`
}

// Encode encodes the message to bytes.
func (m *CursorResultMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeCursorResultMessage decodes a cursor result message.
func DecodeCursorResultMessage(data []byte) (*CursorResultMessage, error) {
	var m CursorResultMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// ============================================================================
// Metadata Messages
// ============================================================================

// GetTablesMessage requests table metadata.
type GetTablesMessage struct {
	Catalog     string `json:"catalog,omitempty"`
	Schema      string `json:"schema,omitempty"`
	TableName   string `json:"table_name,omitempty"`   // Pattern with % wildcards
	TableTypes  []string `json:"table_types,omitempty"` // TABLE, VIEW, etc.
}

// Encode encodes the message to bytes.
func (m *GetTablesMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeGetTablesMessage decodes a get tables message.
func DecodeGetTablesMessage(data []byte) (*GetTablesMessage, error) {
	var m GetTablesMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// GetColumnsMessage requests column metadata.
type GetColumnsMessage struct {
	Catalog    string `json:"catalog,omitempty"`
	Schema     string `json:"schema,omitempty"`
	TableName  string `json:"table_name,omitempty"`
	ColumnName string `json:"column_name,omitempty"` // Pattern with % wildcards
}

// Encode encodes the message to bytes.
func (m *GetColumnsMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeGetColumnsMessage decodes a get columns message.
func DecodeGetColumnsMessage(data []byte) (*GetColumnsMessage, error) {
	var m GetColumnsMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// MetadataResultMessage is the response for metadata queries.
type MetadataResultMessage struct {
	Success  bool              `json:"success"`
	Columns  []ColumnMetadata  `json:"columns,omitempty"`
	Rows     [][]interface{}   `json:"rows,omitempty"`
	RowCount int               `json:"row_count"`
	Message  string            `json:"message,omitempty"`
}

// Encode encodes the message to bytes.
func (m *MetadataResultMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeMetadataResultMessage decodes a metadata result message.
func DecodeMetadataResultMessage(data []byte) (*MetadataResultMessage, error) {
	var m MetadataResultMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// ============================================================================
// Transaction Messages
// ============================================================================

// BeginTxMessage requests starting a transaction.
type BeginTxMessage struct {
	IsolationLevel int  `json:"isolation_level"` // 0=read uncommitted, 1=read committed, 2=repeatable read, 3=serializable
	ReadOnly       bool `json:"read_only"`
	Deferrable     bool `json:"deferrable"`
}

// Encode encodes the message to bytes.
func (m *BeginTxMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeBeginTxMessage decodes a begin transaction message.
func DecodeBeginTxMessage(data []byte) (*BeginTxMessage, error) {
	var m BeginTxMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// SavepointMessage requests creating or releasing a savepoint.
type SavepointMessage struct {
	Name    string `json:"name"`
	Release bool   `json:"release"` // true = release, false = create
}

// Encode encodes the message to bytes.
func (m *SavepointMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeSavepointMessage decodes a savepoint message.
func DecodeSavepointMessage(data []byte) (*SavepointMessage, error) {
	var m SavepointMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// TxResultMessage is the response for transaction operations.
type TxResultMessage struct {
	Success       bool   `json:"success"`
	TransactionID string `json:"transaction_id,omitempty"`
	Message       string `json:"message,omitempty"`
}

// Encode encodes the message to bytes.
func (m *TxResultMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeTxResultMessage decodes a transaction result message.
func DecodeTxResultMessage(data []byte) (*TxResultMessage, error) {
	var m TxResultMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// ============================================================================
// Session Messages
// ============================================================================

// SetOptionMessage requests setting a session option.
type SetOptionMessage struct {
	Option string      `json:"option"` // auto_commit, isolation_level, read_only, etc.
	Value  interface{} `json:"value"`
}

// Encode encodes the message to bytes.
func (m *SetOptionMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeSetOptionMessage decodes a set option message.
func DecodeSetOptionMessage(data []byte) (*SetOptionMessage, error) {
	var m SetOptionMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// GetOptionMessage requests getting a session option.
type GetOptionMessage struct {
	Option string `json:"option"`
}

// Encode encodes the message to bytes.
func (m *GetOptionMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeGetOptionMessage decodes a get option message.
func DecodeGetOptionMessage(data []byte) (*GetOptionMessage, error) {
	var m GetOptionMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// ServerInfoMessage requests server information.
type ServerInfoMessage struct {
	// Empty - just a request for server info
}

// Encode encodes the message to bytes.
func (m *ServerInfoMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// SessionResultMessage is the response for session operations.
type SessionResultMessage struct {
	Success         bool        `json:"success"`
	SessionID       string      `json:"session_id,omitempty"`
	Option          string      `json:"option,omitempty"`
	Value           interface{} `json:"value,omitempty"`
	ServerVersion   string      `json:"server_version,omitempty"`
	ProtocolVersion int         `json:"protocol_version,omitempty"`
	Database        string      `json:"database,omitempty"`
	Username        string      `json:"username,omitempty"`
	ReadOnly        bool        `json:"read_only,omitempty"`
	AutoCommit      bool        `json:"auto_commit,omitempty"`
	IsolationLevel  int         `json:"isolation_level,omitempty"`
	MaxStatementLen int         `json:"max_statement_len,omitempty"`
	Capabilities    []string    `json:"capabilities,omitempty"`
	Message         string      `json:"message,omitempty"`
}

// Encode encodes the message to bytes.
func (m *SessionResultMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeSessionResultMessage decodes a session result message.
func DecodeSessionResultMessage(data []byte) (*SessionResultMessage, error) {
	var m SessionResultMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// ============================================================================
// Enhanced Query Result with Metadata
// ============================================================================

// EnhancedQueryResultMessage provides full metadata for SDK/driver use.
type EnhancedQueryResultMessage struct {
	Success       bool             `json:"success"`
	Message       string           `json:"message,omitempty"`
	StatementType string           `json:"statement_type"` // SELECT, INSERT, UPDATE, DELETE, DDL
	AffectedRows  int64            `json:"affected_rows"`
	LastInsertID  int64            `json:"last_insert_id,omitempty"`
	Columns       []ColumnMetadata `json:"columns,omitempty"`
	Rows          [][]interface{}  `json:"rows,omitempty"`
	RowCount      int              `json:"row_count"`
	HasMoreRows   bool             `json:"has_more_rows"`
	CursorID      string           `json:"cursor_id,omitempty"`
	Warnings      []WarningInfo    `json:"warnings,omitempty"`
	SQLSTATE      string           `json:"sqlstate,omitempty"`
	ErrorCode     int              `json:"error_code,omitempty"`
}

// WarningInfo provides warning details.
type WarningInfo struct {
	Code     int    `json:"code"`
	Message  string `json:"message"`
	SQLSTATE string `json:"sqlstate"`
}

// Encode encodes the message to bytes.
func (m *EnhancedQueryResultMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeEnhancedQueryResultMessage decodes an enhanced query result message.
func DecodeEnhancedQueryResultMessage(data []byte) (*EnhancedQueryResultMessage, error) {
	var m EnhancedQueryResultMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// ============================================================================
// Database Selection Messages (ODBC/JDBC SDK Support)
// ============================================================================

// UseDatabaseMessage requests switching to a different database.
// This is the protocol-level equivalent of the SQL "USE <database>" command.
type UseDatabaseMessage struct {
	Database string `json:"database"` // Name of the database to switch to
}

// Encode encodes the message to bytes.
func (m *UseDatabaseMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeUseDatabaseMessage decodes a use database message.
func DecodeUseDatabaseMessage(data []byte) (*UseDatabaseMessage, error) {
	var m UseDatabaseMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// GetDatabasesMessage requests a list of available databases.
// This is used by ODBC/JDBC drivers for catalog enumeration.
type GetDatabasesMessage struct {
	Pattern string `json:"pattern,omitempty"` // Optional pattern for filtering (supports % wildcards)
}

// Encode encodes the message to bytes.
func (m *GetDatabasesMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeGetDatabasesMessage decodes a get databases message.
func DecodeGetDatabasesMessage(data []byte) (*GetDatabasesMessage, error) {
	var m GetDatabasesMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// DatabaseResultMessage is the response for database operations.
type DatabaseResultMessage struct {
	Success   bool     `json:"success"`
	Database  string   `json:"database,omitempty"`  // Current database (for USE)
	Databases []string `json:"databases,omitempty"` // List of databases (for GET_DATABASES)
	Message   string   `json:"message,omitempty"`
}

// Encode encodes the message to bytes.
func (m *DatabaseResultMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeDatabaseResultMessage decodes a database result message.
func DecodeDatabaseResultMessage(data []byte) (*DatabaseResultMessage, error) {
	var m DatabaseResultMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
