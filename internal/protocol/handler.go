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
Protocol Handler Implementation
================================

The protocol handler is responsible for processing client connections using
FlyDB's binary wire protocol. It bridges the gap between the network layer
and the database engine, handling message parsing, authentication, and
query execution.

Handler Architecture:
=====================

	┌─────────────────────────────────────────────────────────────┐
	│                    Client Connection                         │
	└─────────────────────────────────────────────────────────────┘
	                              │
	                              ▼
	┌─────────────────────────────────────────────────────────────┐
	│                    Protocol Handler                          │
	│  ┌─────────────────────────────────────────────────────────┐ │
	│  │                  Message Parser                          │ │
	│  │  Reads binary messages, validates headers, decodes JSON  │ │
	│  └─────────────────────────────────────────────────────────┘ │
	│  ┌─────────────────────────────────────────────────────────┐ │
	│  │                  Message Router                          │ │
	│  │  Routes messages to appropriate handlers by type         │ │
	│  └─────────────────────────────────────────────────────────┘ │
	│  ┌─────────────────────────────────────────────────────────┐ │
	│  │                  Response Encoder                        │ │
	│  │  Encodes responses and writes to connection              │ │
	│  └─────────────────────────────────────────────────────────┘ │
	└─────────────────────────────────────────────────────────────┘
	                              │
	                              ▼
	┌─────────────────────────────────────────────────────────────┐
	│                    Database Engine                           │
	└─────────────────────────────────────────────────────────────┘

Dependency Injection:
=====================

The handler uses interfaces for all its dependencies, enabling:
  - Easy testing with mock implementations
  - Flexible configuration for different deployment scenarios
  - Clean separation of concerns

Key interfaces:
  - QueryExecutor: Executes SQL queries
  - Authenticator: Validates user credentials
  - PreparedStatementManager: Manages prepared statements
  - CursorManager: Handles server-side cursors
  - TransactionManager: Manages transactions
  - MetadataProvider: Provides schema information for drivers

Connection Lifecycle:
=====================

  1. Client connects (TCP or TLS)
  2. Handler reads authentication message
  3. Handler validates credentials via Authenticator
  4. Handler enters message loop:
     a. Read message header
     b. Read message payload
     c. Route to appropriate handler
     d. Send response
  5. Connection closes (client disconnect or error)

Thread Safety:
==============

Each connection is handled in its own goroutine. The handler maintains
per-connection state (authenticated user, current database, etc.) that
is not shared between connections.

Shared resources (query executor, authenticator, etc.) must be thread-safe.

Error Handling:
===============

Errors are returned to clients as ErrorMessage responses with:
  - Error code (for programmatic handling)
  - Error message (for human-readable display)

The handler logs errors for debugging but does not expose internal
details to clients for security reasons.

References:
===========

  - See protocol.go for message format specification
  - See messages.go for message type definitions
  - See sdk_messages.go for SDK-specific messages
*/
package protocol

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"flydb/internal/logging"
)

// Package-level logger for the protocol component.
var log = logging.NewLogger("protocol")

// QueryExecutor is the interface for executing SQL queries.
type QueryExecutor interface {
	Execute(query string) (string, error)
}

// DatabaseAwareQueryExecutor extends QueryExecutor with database context support.
// This allows queries to be executed against a specific database.
type DatabaseAwareQueryExecutor interface {
	QueryExecutor
	// ExecuteInDatabase executes a query in the context of a specific database.
	// If database is empty, the default database is used.
	ExecuteInDatabase(query, database string) (string, error)
}

// PreparedStatementManager is the interface for managing prepared statements.
type PreparedStatementManager interface {
	Prepare(name, query string) error
	Execute(name string, params []interface{}) (string, error)
	Deallocate(name string) error
}

// Authenticator is the interface for authentication.
type Authenticator interface {
	Authenticate(username, password string) bool
}

// MetadataProvider provides database metadata for drivers.
type MetadataProvider interface {
	GetTables(catalog, schema, tablePattern string, tableTypes []string) ([][]interface{}, error)
	GetColumns(catalog, schema, tablePattern, columnPattern string) ([][]interface{}, error)
	GetPrimaryKeys(catalog, schema, table string) ([][]interface{}, error)
	GetForeignKeys(catalog, schema, table string) ([][]interface{}, error)
	GetIndexes(catalog, schema, table string, unique bool) ([][]interface{}, error)
	GetTypeInfo() ([][]interface{}, error)
}

// SessionManager manages session state for connections.
type SessionManager interface {
	SetOption(sessionID, option string, value interface{}) error
	GetOption(sessionID, option string) (interface{}, error)
	GetServerInfo() *SessionResultMessage
}

// CursorManager manages server-side cursors.
type CursorManager interface {
	OpenCursor(query string, cursorType, concurrency, fetchSize int, params []interface{}) (string, []ColumnMetadata, error)
	FetchRows(cursorID string, direction int, offset int64, count int) ([][]interface{}, bool, int64, error)
	CloseCursor(cursorID string) error
}

// TransactionManager manages transactions.
type TransactionManager interface {
	Begin(isolationLevel int, readOnly bool) (string, error)
	Commit(txID string) error
	Rollback(txID string) error
	CreateSavepoint(txID, name string) error
	ReleaseSavepoint(txID, name string) error
	RollbackToSavepoint(txID, name string) error
}

// DatabaseManager manages database selection for ODBC/JDBC drivers.
type DatabaseManager interface {
	// UseDatabase switches the connection to a different database.
	// Returns an error if the database doesn't exist.
	UseDatabase(database string) error
	// DatabaseExists checks if a database exists.
	DatabaseExists(database string) bool
	// ListDatabases returns a list of available database names.
	ListDatabases() []string
}

// connectionState holds per-connection state.
type connectionState struct {
	authenticated    bool
	username         string
	sessionID        string
	transactionID    string
	autoCommit       bool
	isolationLevel   int
	readOnly         bool
	currentDatabase  string // Current database for this connection
}

// BinaryHandler handles binary protocol connections.
type BinaryHandler struct {
	executor    QueryExecutor
	prepMgr     PreparedStatementManager
	auth        Authenticator
	metadata    MetadataProvider
	sessions    SessionManager
	cursors     CursorManager
	txMgr       TransactionManager
	dbMgr       DatabaseManager
	mu          sync.RWMutex
	connections map[net.Conn]*connectionState
}

// NewBinaryHandler creates a new binary protocol handler.
func NewBinaryHandler(executor QueryExecutor, prepMgr PreparedStatementManager, auth Authenticator) *BinaryHandler {
	return &BinaryHandler{
		executor:    executor,
		prepMgr:     prepMgr,
		auth:        auth,
		connections: make(map[net.Conn]*connectionState),
	}
}

// SetMetadataProvider sets the metadata provider for driver support.
func (h *BinaryHandler) SetMetadataProvider(mp MetadataProvider) {
	h.metadata = mp
}

// SetSessionManager sets the session manager.
func (h *BinaryHandler) SetSessionManager(sm SessionManager) {
	h.sessions = sm
}

// SetCursorManager sets the cursor manager.
func (h *BinaryHandler) SetCursorManager(cm CursorManager) {
	h.cursors = cm
}

// SetTransactionManager sets the transaction manager.
func (h *BinaryHandler) SetTransactionManager(tm TransactionManager) {
	h.txMgr = tm
}

// SetDatabaseManager sets the database manager for multi-database support.
func (h *BinaryHandler) SetDatabaseManager(dm DatabaseManager) {
	h.dbMgr = dm
}

// HandleConnection handles a single binary protocol connection.
func (h *BinaryHandler) HandleConnection(conn net.Conn) {
	remoteAddr := conn.RemoteAddr().String()
	connStart := time.Now()

	log.Info("Binary connection established",
		"remote_addr", remoteAddr,
		"local_addr", conn.LocalAddr().String(),
	)

	// Initialize connection state
	connState := &connectionState{
		authenticated:  false,
		autoCommit:     true,
		isolationLevel: 1, // READ COMMITTED
	}

	h.mu.Lock()
	h.connections[conn] = connState
	activeConns := len(h.connections)
	h.mu.Unlock()

	log.Debug("Active binary connections", "count", activeConns)

	defer func() {
		connDuration := time.Since(connStart)

		h.mu.Lock()
		delete(h.connections, conn)
		remainingConns := len(h.connections)
		h.mu.Unlock()

		conn.Close()
		log.Info("Binary connection terminated",
			"remote_addr", remoteAddr,
			"duration", connDuration,
			"remaining_connections", remainingConns,
		)
	}()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	authenticated := false

	for {
		msg, err := ReadMessage(reader)
		if err != nil {
			if err != io.EOF {
				log.Debug("Binary read error", "remote_addr", remoteAddr, "error", err)
				h.sendError(writer, 500, fmt.Sprintf("read error: %v", err))
			}
			return
		}

		// Get command name for logging (without sensitive data)
		cmdName := msgTypeToString(msg.Header.Type)

		// Create request context for logging
		reqCtx := logging.NewRequestContext(remoteAddr, cmdName)

		// Handle authentication first
		if !authenticated && msg.Header.Type != MsgAuth && msg.Header.Type != MsgPing {
			reqCtx.LogError(log, "authentication required")
			h.sendError(writer, 401, "authentication required")
			continue
		}

		var success bool
		switch msg.Header.Type {
		case MsgAuth:
			authenticated = h.handleAuth(writer, msg.Payload, remoteAddr, connState)
			success = authenticated

		case MsgQuery:
			success = h.handleQuery(writer, msg.Payload, remoteAddr, connState)

		case MsgPrepare:
			success = h.handlePrepare(writer, msg.Payload, remoteAddr)

		case MsgExecute:
			success = h.handleExecute(writer, msg.Payload, remoteAddr)

		case MsgDeallocate:
			success = h.handleDeallocate(writer, msg.Payload, remoteAddr)

		case MsgPing:
			h.handlePing(writer)
			success = true

		// Cursor operations for ODBC/JDBC driver support
		case MsgCursorOpen:
			success = h.handleCursorOpen(writer, msg.Payload, remoteAddr)

		case MsgCursorFetch:
			success = h.handleCursorFetch(writer, msg.Payload, remoteAddr)

		case MsgCursorClose:
			success = h.handleCursorClose(writer, msg.Payload, remoteAddr)

		// Metadata operations for ODBC/JDBC driver support
		case MsgGetTables:
			success = h.handleGetTables(writer, msg.Payload, remoteAddr)

		case MsgGetColumns:
			success = h.handleGetColumns(writer, msg.Payload, remoteAddr)

		case MsgGetPrimaryKeys:
			success = h.handleGetPrimaryKeys(writer, msg.Payload, remoteAddr)

		case MsgGetForeignKeys:
			success = h.handleGetForeignKeys(writer, msg.Payload, remoteAddr)

		case MsgGetIndexes:
			success = h.handleGetIndexes(writer, msg.Payload, remoteAddr)

		case MsgGetTypeInfo:
			success = h.handleGetTypeInfo(writer, remoteAddr)

		// Transaction operations
		case MsgBeginTx:
			success = h.handleBeginTx(writer, msg.Payload, remoteAddr, connState)

		case MsgCommitTx:
			success = h.handleCommitTx(writer, remoteAddr, connState)

		case MsgRollbackTx:
			success = h.handleRollbackTx(writer, remoteAddr, connState)

		// Session operations
		case MsgSetOption:
			success = h.handleSetOption(writer, msg.Payload, remoteAddr, connState)

		case MsgGetOption:
			success = h.handleGetOption(writer, msg.Payload, remoteAddr, connState)

		case MsgGetServerInfo:
			success = h.handleGetServerInfo(writer, remoteAddr)

		// Database operations for ODBC/JDBC driver support
		case MsgUseDatabase:
			success = h.handleUseDatabase(writer, msg.Payload, remoteAddr, connState)

		case MsgGetDatabases:
			success = h.handleGetDatabases(writer, msg.Payload, remoteAddr)

		default:
			reqCtx.LogError(log, "unknown message type")
			h.sendError(writer, 400, "unknown message type")
			continue
		}

		// Log the completed request
		if success {
			reqCtx.LogComplete(log, "success")
		} else {
			reqCtx.LogError(log, "command failed")
		}
	}
}

// msgTypeToString converts a message type to a string for logging.
func msgTypeToString(t MessageType) string {
	switch t {
	case MsgQuery:
		return "QUERY"
	case MsgQueryResult:
		return "QUERY_RESULT"
	case MsgError:
		return "ERROR"
	case MsgPrepare:
		return "PREPARE"
	case MsgPrepareResult:
		return "PREPARE_RESULT"
	case MsgExecute:
		return "EXECUTE"
	case MsgDeallocate:
		return "DEALLOCATE"
	case MsgAuth:
		return "AUTH"
	case MsgAuthResult:
		return "AUTH_RESULT"
	case MsgPing:
		return "PING"
	case MsgPong:
		return "PONG"
	case MsgCursorOpen:
		return "CURSOR_OPEN"
	case MsgCursorFetch:
		return "CURSOR_FETCH"
	case MsgCursorClose:
		return "CURSOR_CLOSE"
	case MsgGetTables:
		return "GET_TABLES"
	case MsgGetColumns:
		return "GET_COLUMNS"
	case MsgGetPrimaryKeys:
		return "GET_PRIMARY_KEYS"
	case MsgGetForeignKeys:
		return "GET_FOREIGN_KEYS"
	case MsgGetIndexes:
		return "GET_INDEXES"
	case MsgGetTypeInfo:
		return "GET_TYPE_INFO"
	case MsgBeginTx:
		return "BEGIN_TX"
	case MsgCommitTx:
		return "COMMIT_TX"
	case MsgRollbackTx:
		return "ROLLBACK_TX"
	case MsgSetOption:
		return "SET_OPTION"
	case MsgGetOption:
		return "GET_OPTION"
	case MsgGetServerInfo:
		return "GET_SERVER_INFO"
	case MsgUseDatabase:
		return "USE_DATABASE"
	case MsgGetDatabases:
		return "GET_DATABASES"
	case MsgDatabaseResult:
		return "DATABASE_RESULT"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", t)
	}
}

// handleAuth handles authentication messages.
func (h *BinaryHandler) handleAuth(w *bufio.Writer, payload []byte, remoteAddr string, state *connectionState) bool {
	authMsg, err := DecodeAuthMessage(payload)
	if err != nil {
		log.Debug("Invalid auth message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid auth message")
		return false
	}

	success := h.auth.Authenticate(authMsg.Username, authMsg.Password)
	result := &AuthResultMessage{
		Success: success,
	}
	if !success {
		result.Message = "authentication failed"
		log.Warn("Binary auth failed", "remote_addr", remoteAddr, "user", authMsg.Username)
	} else {
		result.Message = "authenticated"
		state.authenticated = true
		state.username = authMsg.Username

		// Handle database selection during authentication
		requestedDB := authMsg.Database
		if requestedDB == "" {
			requestedDB = "default"
		}

		// Validate and set the database if database manager is available
		if h.dbMgr != nil && requestedDB != "default" {
			if !h.dbMgr.DatabaseExists(requestedDB) {
				result.Success = false
				result.Message = "database '" + requestedDB + "' does not exist"
				log.Warn("Binary auth failed - database not found",
					"remote_addr", remoteAddr,
					"user", authMsg.Username,
					"database", requestedDB)
				data, _ := result.Encode()
				WriteMessage(w, MsgAuthResult, data)
				w.Flush()
				return false
			}
		}

		state.currentDatabase = requestedDB
		result.Database = requestedDB
		log.Info("Binary auth success",
			"remote_addr", remoteAddr,
			"user", authMsg.Username,
			"database", requestedDB)
	}

	data, _ := result.Encode()
	WriteMessage(w, MsgAuthResult, data)
	w.Flush()
	return success
}

// handleQuery handles query messages.
func (h *BinaryHandler) handleQuery(w *bufio.Writer, payload []byte, remoteAddr string, state *connectionState) bool {
	queryMsg, err := DecodeQueryMessage(payload)
	if err != nil {
		log.Debug("Invalid query message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid query message")
		return false
	}

	log.Debug("Executing binary query", "remote_addr", remoteAddr, "database", state.currentDatabase)

	// Use database-aware executor if available, otherwise fall back to default
	var result string
	if dbExec, ok := h.executor.(DatabaseAwareQueryExecutor); ok {
		result, err = dbExec.ExecuteInDatabase(queryMsg.Query, state.currentDatabase)
	} else {
		result, err = h.executor.Execute(queryMsg.Query)
	}

	if err != nil {
		log.Debug("Binary query error", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 500, err.Error())
		return false
	}

	// Check if this was a USE statement and update connection state
	// Result format: "USE <database> OK"
	if strings.HasPrefix(result, "USE ") && strings.HasSuffix(result, " OK") {
		dbName := strings.TrimSuffix(strings.TrimPrefix(result, "USE "), " OK")
		state.currentDatabase = dbName
		log.Debug("Database changed via USE statement", "remote_addr", remoteAddr, "database", dbName)
	}

	// Check if this was a DROP DATABASE that switched us to a different database
	// Result format: "DROP DATABASE OK (switched to <database>)"
	if strings.HasPrefix(result, "DROP DATABASE OK (switched to ") {
		// Extract the new database name
		if idx := strings.Index(result, "switched to "); idx != -1 {
			newDb := strings.TrimSuffix(result[idx+len("switched to "):], ")")
			state.currentDatabase = newDb
			log.Debug("Database changed via DROP DATABASE", "remote_addr", remoteAddr, "database", newDb)
		}
	}

	log.Debug("Binary query success", "remote_addr", remoteAddr)
	resultMsg := &QueryResultMessage{
		Success: true,
		Message: result,
	}
	data, _ := resultMsg.Encode()
	WriteMessage(w, MsgQueryResult, data)
	w.Flush()
	return true
}

// handlePrepare handles prepare statement messages.
func (h *BinaryHandler) handlePrepare(w *bufio.Writer, payload []byte, remoteAddr string) bool {
	prepMsg, err := DecodePrepareMessage(payload)
	if err != nil {
		log.Debug("Invalid prepare message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid prepare message")
		return false
	}

	if h.prepMgr == nil {
		h.sendError(w, 501, "prepared statements not supported")
		return false
	}

	log.Debug("Preparing statement", "remote_addr", remoteAddr, "name", prepMsg.Name)
	err = h.prepMgr.Prepare(prepMsg.Name, prepMsg.Query)
	if err != nil {
		log.Debug("Prepare error", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 500, err.Error())
		return false
	}

	resultMsg := &PrepareResultMessage{
		Success: true,
		Name:    prepMsg.Name,
	}
	data, _ := resultMsg.Encode()
	WriteMessage(w, MsgPrepareResult, data)
	w.Flush()
	return true
}

// handleExecute handles execute prepared statement messages.
func (h *BinaryHandler) handleExecute(w *bufio.Writer, payload []byte, remoteAddr string) bool {
	execMsg, err := DecodeExecuteMessage(payload)
	if err != nil {
		log.Debug("Invalid execute message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid execute message")
		return false
	}

	if h.prepMgr == nil {
		h.sendError(w, 501, "prepared statements not supported")
		return false
	}

	log.Debug("Executing prepared statement", "remote_addr", remoteAddr, "name", execMsg.Name)
	result, err := h.prepMgr.Execute(execMsg.Name, execMsg.Params)
	if err != nil {
		log.Debug("Execute error", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 500, err.Error())
		return false
	}

	resultMsg := &QueryResultMessage{
		Success: true,
		Message: result,
	}
	data, _ := resultMsg.Encode()
	WriteMessage(w, MsgQueryResult, data)
	w.Flush()
	return true
}

// handleDeallocate handles deallocate prepared statement messages.
func (h *BinaryHandler) handleDeallocate(w *bufio.Writer, payload []byte, remoteAddr string) bool {
	deallocMsg, err := DecodeDeallocateMessage(payload)
	if err != nil {
		log.Debug("Invalid deallocate message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid deallocate message")
		return false
	}

	if h.prepMgr == nil {
		h.sendError(w, 501, "prepared statements not supported")
		return false
	}

	log.Debug("Deallocating statement", "remote_addr", remoteAddr, "name", deallocMsg.Name)
	err = h.prepMgr.Deallocate(deallocMsg.Name)
	if err != nil {
		log.Debug("Deallocate error", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 500, err.Error())
		return false
	}

	resultMsg := &QueryResultMessage{
		Success: true,
		Message: "DEALLOCATE OK",
	}
	data, _ := resultMsg.Encode()
	WriteMessage(w, MsgQueryResult, data)
	w.Flush()
	return true
}

// handlePing handles ping messages.
func (h *BinaryHandler) handlePing(w *bufio.Writer) {
	WriteMessage(w, MsgPong, nil)
	w.Flush()
}

// sendError sends an error message.
func (h *BinaryHandler) sendError(w *bufio.Writer, code int, message string) {
	errMsg := &ErrorMessage{
		Code:    code,
		Message: message,
	}
	data, _ := errMsg.Encode()
	WriteMessage(w, MsgError, data)
	w.Flush()
}

// Close closes all connections.
func (h *BinaryHandler) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for conn := range h.connections {
		conn.Close()
	}
}

// ============================================================================
// Cursor handlers for ODBC/JDBC driver support
// ============================================================================

// handleCursorOpen handles cursor open messages.
func (h *BinaryHandler) handleCursorOpen(w *bufio.Writer, payload []byte, remoteAddr string) bool {
	if h.cursors == nil {
		h.sendError(w, 501, "cursors not supported")
		return false
	}

	msg, err := DecodeCursorOpenMessage(payload)
	if err != nil {
		log.Debug("Invalid cursor open message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid cursor open message")
		return false
	}

	cursorID, columns, err := h.cursors.OpenCursor(msg.Query, msg.CursorType, msg.Concurrency, msg.FetchSize, msg.Parameters)
	if err != nil {
		log.Debug("Cursor open error", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 500, err.Error())
		return false
	}

	result := &CursorResultMessage{
		Success:  true,
		CursorID: cursorID,
		Columns:  columns,
	}
	data, _ := result.Encode()
	WriteMessage(w, MsgCursorResult, data)
	w.Flush()
	return true
}

// handleCursorFetch handles cursor fetch messages.
func (h *BinaryHandler) handleCursorFetch(w *bufio.Writer, payload []byte, remoteAddr string) bool {
	if h.cursors == nil {
		h.sendError(w, 501, "cursors not supported")
		return false
	}

	msg, err := DecodeCursorFetchMessage(payload)
	if err != nil {
		log.Debug("Invalid cursor fetch message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid cursor fetch message")
		return false
	}

	rows, hasMore, position, err := h.cursors.FetchRows(msg.CursorID, msg.Direction, msg.Offset, msg.Count)
	if err != nil {
		log.Debug("Cursor fetch error", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 500, err.Error())
		return false
	}

	result := &CursorResultMessage{
		Success:     true,
		CursorID:    msg.CursorID,
		Rows:        rows,
		RowCount:    len(rows),
		HasMoreRows: hasMore,
		Position:    position,
	}
	data, _ := result.Encode()
	WriteMessage(w, MsgCursorResult, data)
	w.Flush()
	return true
}

// handleCursorClose handles cursor close messages.
func (h *BinaryHandler) handleCursorClose(w *bufio.Writer, payload []byte, remoteAddr string) bool {
	if h.cursors == nil {
		h.sendError(w, 501, "cursors not supported")
		return false
	}

	msg, err := DecodeCursorCloseMessage(payload)
	if err != nil {
		log.Debug("Invalid cursor close message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid cursor close message")
		return false
	}

	err = h.cursors.CloseCursor(msg.CursorID)
	if err != nil {
		log.Debug("Cursor close error", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 500, err.Error())
		return false
	}

	result := &CursorResultMessage{
		Success:  true,
		CursorID: msg.CursorID,
		Message:  "cursor closed",
	}
	data, _ := result.Encode()
	WriteMessage(w, MsgCursorResult, data)
	w.Flush()
	return true
}

// ============================================================================
// Metadata handlers for ODBC/JDBC driver support
// ============================================================================

// handleGetTables handles get tables metadata messages.
func (h *BinaryHandler) handleGetTables(w *bufio.Writer, payload []byte, remoteAddr string) bool {
	if h.metadata == nil {
		h.sendError(w, 501, "metadata not supported")
		return false
	}

	msg, err := DecodeGetTablesMessage(payload)
	if err != nil {
		log.Debug("Invalid get tables message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid get tables message")
		return false
	}

	rows, err := h.metadata.GetTables(msg.Catalog, msg.Schema, msg.TableName, msg.TableTypes)
	if err != nil {
		log.Debug("Get tables error", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 500, err.Error())
		return false
	}

	result := &MetadataResultMessage{
		Success:  true,
		Rows:     rows,
		RowCount: len(rows),
	}
	data, _ := result.Encode()
	WriteMessage(w, MsgMetadataResult, data)
	w.Flush()
	return true
}

// handleGetColumns handles get columns metadata messages.
func (h *BinaryHandler) handleGetColumns(w *bufio.Writer, payload []byte, remoteAddr string) bool {
	if h.metadata == nil {
		h.sendError(w, 501, "metadata not supported")
		return false
	}

	msg, err := DecodeGetColumnsMessage(payload)
	if err != nil {
		log.Debug("Invalid get columns message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid get columns message")
		return false
	}

	rows, err := h.metadata.GetColumns(msg.Catalog, msg.Schema, msg.TableName, msg.ColumnName)
	if err != nil {
		log.Debug("Get columns error", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 500, err.Error())
		return false
	}

	result := &MetadataResultMessage{
		Success:  true,
		Rows:     rows,
		RowCount: len(rows),
	}
	data, _ := result.Encode()
	WriteMessage(w, MsgMetadataResult, data)
	w.Flush()
	return true
}

// handleGetTypeInfo handles get type info metadata messages.
func (h *BinaryHandler) handleGetTypeInfo(w *bufio.Writer, remoteAddr string) bool {
	if h.metadata == nil {
		h.sendError(w, 501, "metadata not supported")
		return false
	}

	rows, err := h.metadata.GetTypeInfo()
	if err != nil {
		log.Debug("Get type info error", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 500, err.Error())
		return false
	}

	result := &MetadataResultMessage{
		Success:  true,
		Rows:     rows,
		RowCount: len(rows),
	}
	data, _ := result.Encode()
	WriteMessage(w, MsgMetadataResult, data)
	w.Flush()
	return true
}

// handleGetPrimaryKeys handles get primary keys metadata messages.
func (h *BinaryHandler) handleGetPrimaryKeys(w *bufio.Writer, payload []byte, remoteAddr string) bool {
	if h.metadata == nil {
		h.sendError(w, 501, "metadata not supported")
		return false
	}

	msg, err := DecodeGetPrimaryKeysMessage(payload)
	if err != nil {
		log.Debug("Invalid get primary keys message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid get primary keys message")
		return false
	}

	rows, err := h.metadata.GetPrimaryKeys(msg.Catalog, msg.Schema, msg.TableName)
	if err != nil {
		log.Debug("Get primary keys error", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 500, err.Error())
		return false
	}

	result := &MetadataResultMessage{
		Success:  true,
		Rows:     rows,
		RowCount: len(rows),
	}
	data, _ := result.Encode()
	WriteMessage(w, MsgMetadataResult, data)
	w.Flush()
	return true
}

// handleGetForeignKeys handles get foreign keys metadata messages.
func (h *BinaryHandler) handleGetForeignKeys(w *bufio.Writer, payload []byte, remoteAddr string) bool {
	if h.metadata == nil {
		h.sendError(w, 501, "metadata not supported")
		return false
	}

	msg, err := DecodeGetForeignKeysMessage(payload)
	if err != nil {
		log.Debug("Invalid get foreign keys message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid get foreign keys message")
		return false
	}

	rows, err := h.metadata.GetForeignKeys(msg.Catalog, msg.Schema, msg.TableName)
	if err != nil {
		log.Debug("Get foreign keys error", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 500, err.Error())
		return false
	}

	result := &MetadataResultMessage{
		Success:  true,
		Rows:     rows,
		RowCount: len(rows),
	}
	data, _ := result.Encode()
	WriteMessage(w, MsgMetadataResult, data)
	w.Flush()
	return true
}

// handleGetIndexes handles get indexes metadata messages.
func (h *BinaryHandler) handleGetIndexes(w *bufio.Writer, payload []byte, remoteAddr string) bool {
	if h.metadata == nil {
		h.sendError(w, 501, "metadata not supported")
		return false
	}

	msg, err := DecodeGetIndexesMessage(payload)
	if err != nil {
		log.Debug("Invalid get indexes message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid get indexes message")
		return false
	}

	rows, err := h.metadata.GetIndexes(msg.Catalog, msg.Schema, msg.TableName, msg.Unique)
	if err != nil {
		log.Debug("Get indexes error", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 500, err.Error())
		return false
	}

	result := &MetadataResultMessage{
		Success:  true,
		Rows:     rows,
		RowCount: len(rows),
	}
	data, _ := result.Encode()
	WriteMessage(w, MsgMetadataResult, data)
	w.Flush()
	return true
}

// ============================================================================
// Transaction handlers
// ============================================================================

// handleBeginTx handles begin transaction messages.
func (h *BinaryHandler) handleBeginTx(w *bufio.Writer, payload []byte, remoteAddr string, state *connectionState) bool {
	if h.txMgr == nil {
		h.sendError(w, 501, "transactions not supported")
		return false
	}

	msg, err := DecodeBeginTxMessage(payload)
	if err != nil {
		log.Debug("Invalid begin tx message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid begin tx message")
		return false
	}

	txID, err := h.txMgr.Begin(msg.IsolationLevel, msg.ReadOnly)
	if err != nil {
		log.Debug("Begin tx error", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 500, err.Error())
		return false
	}

	state.transactionID = txID
	result := &TxResultMessage{
		Success:       true,
		TransactionID: txID,
		Message:       "transaction started",
	}
	data, _ := result.Encode()
	WriteMessage(w, MsgTxResult, data)
	w.Flush()
	return true
}

// handleCommitTx handles commit transaction messages.
func (h *BinaryHandler) handleCommitTx(w *bufio.Writer, remoteAddr string, state *connectionState) bool {
	if h.txMgr == nil {
		h.sendError(w, 501, "transactions not supported")
		return false
	}

	if state.transactionID == "" {
		h.sendError(w, 400, "no active transaction")
		return false
	}

	err := h.txMgr.Commit(state.transactionID)
	if err != nil {
		log.Debug("Commit tx error", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 500, err.Error())
		return false
	}

	txID := state.transactionID
	state.transactionID = ""
	result := &TxResultMessage{
		Success:       true,
		TransactionID: txID,
		Message:       "transaction committed",
	}
	data, _ := result.Encode()
	WriteMessage(w, MsgTxResult, data)
	w.Flush()
	return true
}

// handleRollbackTx handles rollback transaction messages.
func (h *BinaryHandler) handleRollbackTx(w *bufio.Writer, remoteAddr string, state *connectionState) bool {
	if h.txMgr == nil {
		h.sendError(w, 501, "transactions not supported")
		return false
	}

	if state.transactionID == "" {
		h.sendError(w, 400, "no active transaction")
		return false
	}

	err := h.txMgr.Rollback(state.transactionID)
	if err != nil {
		log.Debug("Rollback tx error", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 500, err.Error())
		return false
	}

	txID := state.transactionID
	state.transactionID = ""
	result := &TxResultMessage{
		Success:       true,
		TransactionID: txID,
		Message:       "transaction rolled back",
	}
	data, _ := result.Encode()
	WriteMessage(w, MsgTxResult, data)
	w.Flush()
	return true
}

// ============================================================================
// Session handlers
// ============================================================================

// handleSetOption handles set session option messages.
func (h *BinaryHandler) handleSetOption(w *bufio.Writer, payload []byte, remoteAddr string, state *connectionState) bool {
	msg, err := DecodeSetOptionMessage(payload)
	if err != nil {
		log.Debug("Invalid set option message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid set option message")
		return false
	}

	// Handle common options locally
	switch msg.Option {
	case "auto_commit":
		if v, ok := msg.Value.(bool); ok {
			state.autoCommit = v
		}
	case "isolation_level":
		if v, ok := msg.Value.(float64); ok {
			state.isolationLevel = int(v)
		}
	case "read_only":
		if v, ok := msg.Value.(bool); ok {
			state.readOnly = v
		}
	default:
		// Delegate to session manager if available
		if h.sessions != nil {
			err = h.sessions.SetOption(state.sessionID, msg.Option, msg.Value)
			if err != nil {
				h.sendError(w, 500, err.Error())
				return false
			}
		}
	}

	result := &SessionResultMessage{
		Success: true,
		Option:  msg.Option,
		Value:   msg.Value,
		Message: "option set",
	}
	data, _ := result.Encode()
	WriteMessage(w, MsgSessionResult, data)
	w.Flush()
	return true
}

// handleGetOption handles get session option messages.
func (h *BinaryHandler) handleGetOption(w *bufio.Writer, payload []byte, remoteAddr string, state *connectionState) bool {
	msg, err := DecodeGetOptionMessage(payload)
	if err != nil {
		log.Debug("Invalid get option message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid get option message")
		return false
	}

	var value interface{}
	switch msg.Option {
	case "auto_commit":
		value = state.autoCommit
	case "isolation_level":
		value = state.isolationLevel
	case "read_only":
		value = state.readOnly
	case "username":
		value = state.username
	default:
		if h.sessions != nil {
			value, err = h.sessions.GetOption(state.sessionID, msg.Option)
			if err != nil {
				h.sendError(w, 500, err.Error())
				return false
			}
		}
	}

	result := &SessionResultMessage{
		Success: true,
		Option:  msg.Option,
		Value:   value,
	}
	data, _ := result.Encode()
	WriteMessage(w, MsgSessionResult, data)
	w.Flush()
	return true
}

// handleGetServerInfo handles get server info messages.
func (h *BinaryHandler) handleGetServerInfo(w *bufio.Writer, remoteAddr string) bool {
	result := &SessionResultMessage{
		Success:         true,
		ServerVersion:   "1.0.0",
		ProtocolVersion: int(ProtocolVersion),
		Capabilities:    []string{"sql", "prepared_statements", "transactions", "cursors"},
		MaxStatementLen: MaxMessageSize,
	}

	if h.sessions != nil {
		info := h.sessions.GetServerInfo()
		if info != nil {
			result = info
		}
	}

	data, _ := result.Encode()
	WriteMessage(w, MsgSessionResult, data)
	w.Flush()
	return true
}

// handleUseDatabase handles database selection requests from ODBC/JDBC drivers.
func (h *BinaryHandler) handleUseDatabase(w *bufio.Writer, payload []byte, remoteAddr string, state *connectionState) bool {
	msg, err := DecodeUseDatabaseMessage(payload)
	if err != nil {
		log.Debug("Invalid use database message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid use database message")
		return false
	}

	result := &DatabaseResultMessage{
		Success: true,
	}

	database := msg.Database
	if database == "" {
		database = "default"
	}

	// Validate database exists if database manager is available
	if h.dbMgr != nil && database != "default" {
		if !h.dbMgr.DatabaseExists(database) {
			result.Success = false
			result.Message = "database '" + database + "' does not exist"
			log.Debug("Use database failed - not found",
				"remote_addr", remoteAddr,
				"database", database)
			data, _ := result.Encode()
			WriteMessage(w, MsgDatabaseResult, data)
			w.Flush()
			return false
		}
	}

	// Update connection state
	state.currentDatabase = database
	result.Database = database
	result.Message = "database changed"

	log.Debug("Database changed",
		"remote_addr", remoteAddr,
		"database", database)

	data, _ := result.Encode()
	WriteMessage(w, MsgDatabaseResult, data)
	w.Flush()
	return true
}

// handleGetDatabases handles requests to list available databases.
func (h *BinaryHandler) handleGetDatabases(w *bufio.Writer, payload []byte, remoteAddr string) bool {
	msg, err := DecodeGetDatabasesMessage(payload)
	if err != nil {
		log.Debug("Invalid get databases message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid get databases message")
		return false
	}

	result := &DatabaseResultMessage{
		Success:   true,
		Databases: []string{"default"},
	}

	// Get database list from database manager if available
	if h.dbMgr != nil {
		databases := h.dbMgr.ListDatabases()
		if len(databases) > 0 {
			// Filter by pattern if provided
			if msg.Pattern != "" {
				filtered := make([]string, 0)
				for _, db := range databases {
					if matchPattern(db, msg.Pattern) {
						filtered = append(filtered, db)
					}
				}
				result.Databases = filtered
			} else {
				result.Databases = databases
			}
		}
	}

	log.Debug("Get databases",
		"remote_addr", remoteAddr,
		"count", len(result.Databases))

	data, _ := result.Encode()
	WriteMessage(w, MsgDatabaseResult, data)
	w.Flush()
	return true
}

// matchPattern matches a string against a SQL-like pattern with % wildcards.
func matchPattern(s, pattern string) bool {
	if pattern == "" || pattern == "%" {
		return true
	}

	// Simple pattern matching - convert SQL pattern to basic matching
	// This handles common cases like "prefix%", "%suffix", and "%contains%"
	if pattern[0] == '%' && pattern[len(pattern)-1] == '%' {
		// Contains match
		substr := pattern[1 : len(pattern)-1]
		return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
	} else if pattern[0] == '%' {
		// Suffix match
		suffix := pattern[1:]
		return strings.HasSuffix(strings.ToLower(s), strings.ToLower(suffix))
	} else if pattern[len(pattern)-1] == '%' {
		// Prefix match
		prefix := pattern[:len(pattern)-1]
		return strings.HasPrefix(strings.ToLower(s), strings.ToLower(prefix))
	}

	// Exact match (case-insensitive)
	return strings.EqualFold(s, pattern)
}
