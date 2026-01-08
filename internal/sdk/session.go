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

package sdk

import (
	"sync"
	"time"
)

// IsolationLevel defines transaction isolation levels.
type IsolationLevel int

const (
	// IsolationReadUncommitted allows dirty reads.
	IsolationReadUncommitted IsolationLevel = iota
	// IsolationReadCommitted prevents dirty reads (default).
	IsolationReadCommitted
	// IsolationRepeatableRead prevents non-repeatable reads.
	IsolationRepeatableRead
	// IsolationSerializable provides full isolation.
	IsolationSerializable
)

// String returns the string representation of the isolation level.
func (il IsolationLevel) String() string {
	switch il {
	case IsolationReadUncommitted:
		return "READ UNCOMMITTED"
	case IsolationReadCommitted:
		return "READ COMMITTED"
	case IsolationRepeatableRead:
		return "REPEATABLE READ"
	case IsolationSerializable:
		return "SERIALIZABLE"
	default:
		return "UNKNOWN"
	}
}

// ODBCValue returns the ODBC constant for this isolation level.
func (il IsolationLevel) ODBCValue() int {
	switch il {
	case IsolationReadUncommitted:
		return 1 // SQL_TXN_READ_UNCOMMITTED
	case IsolationReadCommitted:
		return 2 // SQL_TXN_READ_COMMITTED
	case IsolationRepeatableRead:
		return 4 // SQL_TXN_REPEATABLE_READ
	case IsolationSerializable:
		return 8 // SQL_TXN_SERIALIZABLE
	default:
		return 2
	}
}

// JDBCValue returns the JDBC constant for this isolation level.
func (il IsolationLevel) JDBCValue() int {
	switch il {
	case IsolationReadUncommitted:
		return 1 // Connection.TRANSACTION_READ_UNCOMMITTED
	case IsolationReadCommitted:
		return 2 // Connection.TRANSACTION_READ_COMMITTED
	case IsolationRepeatableRead:
		return 4 // Connection.TRANSACTION_REPEATABLE_READ
	case IsolationSerializable:
		return 8 // Connection.TRANSACTION_SERIALIZABLE
	default:
		return 2
	}
}

// SessionState represents the state of a session.
type SessionState int

const (
	// SessionStateActive means the session is active and usable.
	SessionStateActive SessionState = iota
	// SessionStateIdle means the session is idle.
	SessionStateIdle
	// SessionStateClosed means the session is closed.
	SessionStateClosed
)

// Session represents a database session with its own state.
type Session struct {
	mu sync.RWMutex

	// Identification
	ID       string
	Username string
	Database string

	// Timing
	CreatedAt      time.Time
	LastActivityAt time.Time
	Timeout        time.Duration

	// State
	State SessionState

	// Settings
	AutoCommit     bool
	IsolationLevel IsolationLevel
	ReadOnly       bool

	// Active resources
	ActiveTransaction *Transaction
	Cursors           map[string]*Cursor
	PreparedStmts     map[string]*PreparedStatement

	// Server info (populated after connection)
	ServerVersion   string
	ProtocolVersion int
}

// NewSession creates a new session.
func NewSession(username, database string) *Session {
	return &Session{
		ID:             generateID("sess"),
		Username:       username,
		Database:       database,
		CreatedAt:      time.Now(),
		LastActivityAt: time.Now(),
		Timeout:        30 * time.Minute,
		State:          SessionStateActive,
		AutoCommit:     true,
		IsolationLevel: IsolationReadCommitted,
		ReadOnly:       false,
		Cursors:        make(map[string]*Cursor),
		PreparedStmts:  make(map[string]*PreparedStatement),
	}
}

// Touch updates the last activity time.
func (s *Session) Touch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastActivityAt = time.Now()
}

// IsExpired returns true if the session has expired.
func (s *Session) IsExpired() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return time.Since(s.LastActivityAt) > s.Timeout
}

// IsActive returns true if the session is active.
func (s *Session) IsActive() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.State == SessionStateActive
}

// Close closes the session.
func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.State = SessionStateClosed
	// Close all cursors
	for _, cursor := range s.Cursors {
		cursor.SetState(CursorStateClosed)
	}
	s.Cursors = make(map[string]*Cursor)
	s.PreparedStmts = make(map[string]*PreparedStatement)
	return nil
}

// AddCursor adds a cursor to the session.
func (s *Session) AddCursor(cursor *Cursor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Cursors[cursor.ID] = cursor
	s.LastActivityAt = time.Now()
}

// GetCursor returns a cursor by ID.
func (s *Session) GetCursor(id string) (*Cursor, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cursor, ok := s.Cursors[id]
	return cursor, ok
}

// RemoveCursor removes a cursor from the session.
func (s *Session) RemoveCursor(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.Cursors, id)
}

// AddPreparedStatement adds a prepared statement to the session.
func (s *Session) AddPreparedStatement(stmt *PreparedStatement) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.PreparedStmts[stmt.ID] = stmt
	s.LastActivityAt = time.Now()
}

// GetPreparedStatement returns a prepared statement by ID.
func (s *Session) GetPreparedStatement(id string) (*PreparedStatement, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stmt, ok := s.PreparedStmts[id]
	return stmt, ok
}

// RemovePreparedStatement removes a prepared statement from the session.
func (s *Session) RemovePreparedStatement(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.PreparedStmts, id)
}

// SetAutoCommit sets the auto-commit mode.
func (s *Session) SetAutoCommit(autoCommit bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.AutoCommit = autoCommit
}

// GetAutoCommit returns the auto-commit mode.
func (s *Session) GetAutoCommit() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.AutoCommit
}

// SetIsolationLevel sets the transaction isolation level.
func (s *Session) SetIsolationLevel(level IsolationLevel) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.IsolationLevel = level
}

// GetIsolationLevel returns the transaction isolation level.
func (s *Session) GetIsolationLevel() IsolationLevel {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.IsolationLevel
}

// SetReadOnly sets the read-only mode.
func (s *Session) SetReadOnly(readOnly bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ReadOnly = readOnly
}

// IsReadOnly returns true if the session is read-only.
func (s *Session) IsReadOnly() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ReadOnly
}

// HasActiveTransaction returns true if there's an active transaction.
func (s *Session) HasActiveTransaction() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ActiveTransaction != nil && s.ActiveTransaction.IsActive()
}

// PreparedStatement represents a prepared SQL statement.
type PreparedStatement struct {
	mu sync.RWMutex

	ID         string
	SessionID  string
	SQL        string
	Parameters []ParameterInfo
	CreatedAt  time.Time
}

// ParameterInfo describes a parameter in a prepared statement.
type ParameterInfo struct {
	Index     int
	Name      string   // For named parameters
	Type      DataType
	Precision int
	Scale     int
	Nullable  bool
	Mode      ParameterMode
}

// ParameterMode defines the parameter direction.
type ParameterMode int

const (
	// ParameterModeIn is an input parameter.
	ParameterModeIn ParameterMode = iota
	// ParameterModeOut is an output parameter.
	ParameterModeOut
	// ParameterModeInOut is an input/output parameter.
	ParameterModeInOut
)

// NewPreparedStatement creates a new prepared statement.
func NewPreparedStatement(sessionID, sql string) *PreparedStatement {
	return &PreparedStatement{
		ID:        generateID("stmt"),
		SessionID: sessionID,
		SQL:       sql,
		CreatedAt: time.Now(),
	}
}

// SessionInfo provides session metadata for drivers.
type SessionInfo struct {
	SessionID       string
	Username        string
	Database        string
	ServerVersion   string
	ProtocolVersion int
	Capabilities    []string
	MaxStatementLen int
	MaxConnections  int
	ReadOnly        bool
	AutoCommit      bool
	IsolationLevel  IsolationLevel
}

// SetDatabase changes the current database for the session.
// This is used by ODBC/JDBC drivers to switch databases.
func (s *Session) SetDatabase(database string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Database = database
	s.LastActivityAt = time.Now()
}

// GetDatabase returns the current database for the session.
func (s *Session) GetDatabase() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Database
}

// ConnectionConfig represents connection configuration for ODBC/JDBC drivers.
type ConnectionConfig struct {
	// Host is the server hostname or IP address.
	Host string
	// Port is the server port number.
	Port int
	// Username is the authentication username.
	Username string
	// Password is the authentication password.
	Password string
	// Database is the initial database to connect to (default: "default").
	Database string
	// ConnectTimeout is the connection timeout in seconds.
	ConnectTimeout int
	// ReadTimeout is the read timeout in seconds.
	ReadTimeout int
	// WriteTimeout is the write timeout in seconds.
	WriteTimeout int
	// AutoCommit sets the initial auto-commit mode.
	AutoCommit bool
	// IsolationLevel sets the initial transaction isolation level.
	IsolationLevel IsolationLevel
	// ReadOnly sets the initial read-only mode.
	ReadOnly bool
	// ApplicationName identifies the client application.
	ApplicationName string
	// ClientInfo provides additional client information.
	ClientInfo map[string]string
}

// NewConnectionConfig creates a new connection configuration with defaults.
func NewConnectionConfig() *ConnectionConfig {
	return &ConnectionConfig{
		Host:           "localhost",
		Port:           5433,
		Database:       "default",
		ConnectTimeout: 30,
		ReadTimeout:    0, // No timeout
		WriteTimeout:   0, // No timeout
		AutoCommit:     true,
		IsolationLevel: IsolationReadCommitted,
		ReadOnly:       false,
		ClientInfo:     make(map[string]string),
	}
}

// ParseConnectionString parses an ODBC/JDBC connection string into a ConnectionConfig.
// Supported formats:
//   - ODBC: "Driver={FlyDB};Server=host;Port=5433;Database=mydb;Uid=user;Pwd=pass"
//   - JDBC: "jdbc:flydb://host:5433/mydb?user=user&password=pass"
func ParseConnectionString(connStr string) (*ConnectionConfig, error) {
	config := NewConnectionConfig()

	// Check for JDBC URL format
	if len(connStr) > 5 && connStr[:5] == "jdbc:" {
		return parseJDBCConnectionString(connStr, config)
	}

	// Parse ODBC-style connection string
	return parseODBCConnectionString(connStr, config)
}

// parseODBCConnectionString parses an ODBC-style connection string.
func parseODBCConnectionString(connStr string, config *ConnectionConfig) (*ConnectionConfig, error) {
	// Split by semicolon
	parts := splitConnectionString(connStr, ';')
	for _, part := range parts {
		if part == "" {
			continue
		}
		kv := splitConnectionString(part, '=')
		if len(kv) != 2 {
			continue
		}
		key := toLower(trim(kv[0]))
		value := trim(kv[1])

		switch key {
		case "server", "host", "data source":
			config.Host = value
		case "port":
			config.Port = parseInt(value, 5433)
		case "database", "initial catalog":
			config.Database = value
		case "uid", "user id", "user":
			config.Username = value
		case "pwd", "password":
			config.Password = value
		case "connect timeout", "connection timeout":
			config.ConnectTimeout = parseInt(value, 30)
		case "application name", "app":
			config.ApplicationName = value
		case "readonly", "read only":
			config.ReadOnly = parseBool(value)
		case "autocommit", "auto commit":
			config.AutoCommit = parseBool(value)
		}
	}
	return config, nil
}

// parseJDBCConnectionString parses a JDBC-style connection string.
func parseJDBCConnectionString(connStr string, config *ConnectionConfig) (*ConnectionConfig, error) {
	// Format: jdbc:flydb://host:port/database?params
	// Remove "jdbc:flydb://" prefix
	if len(connStr) < 13 {
		return config, nil
	}
	rest := connStr[13:] // Skip "jdbc:flydb://"

	// Split host:port/database from params
	queryIdx := indexOf(rest, '?')
	var hostPart, paramPart string
	if queryIdx >= 0 {
		hostPart = rest[:queryIdx]
		paramPart = rest[queryIdx+1:]
	} else {
		hostPart = rest
	}

	// Parse host:port/database
	slashIdx := indexOf(hostPart, '/')
	var hostPort, database string
	if slashIdx >= 0 {
		hostPort = hostPart[:slashIdx]
		database = hostPart[slashIdx+1:]
	} else {
		hostPort = hostPart
	}

	// Parse host:port
	colonIdx := indexOf(hostPort, ':')
	if colonIdx >= 0 {
		config.Host = hostPort[:colonIdx]
		config.Port = parseInt(hostPort[colonIdx+1:], 5433)
	} else {
		config.Host = hostPort
	}

	if database != "" {
		config.Database = database
	}

	// Parse query parameters
	if paramPart != "" {
		params := splitConnectionString(paramPart, '&')
		for _, param := range params {
			kv := splitConnectionString(param, '=')
			if len(kv) != 2 {
				continue
			}
			key := toLower(kv[0])
			value := kv[1]

			switch key {
			case "user":
				config.Username = value
			case "password":
				config.Password = value
			case "database":
				config.Database = value
			case "connecttimeout":
				config.ConnectTimeout = parseInt(value, 30)
			case "applicationname":
				config.ApplicationName = value
			case "readonly":
				config.ReadOnly = parseBool(value)
			case "autocommit":
				config.AutoCommit = parseBool(value)
			}
		}
	}

	return config, nil
}

// Helper functions for connection string parsing

func splitConnectionString(s string, sep byte) []string {
	var result []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			result = append(result, s[start:i])
			start = i + 1
		}
	}
	result = append(result, s[start:])
	return result
}

func indexOf(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

func trim(s string) string {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}

func toLower(s string) string {
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}

func parseInt(s string, defaultVal int) int {
	result := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= '0' && c <= '9' {
			result = result*10 + int(c-'0')
		} else {
			return defaultVal
		}
	}
	if result == 0 && s != "0" {
		return defaultVal
	}
	return result
}

func parseBool(s string) bool {
	s = toLower(s)
	return s == "true" || s == "1" || s == "yes" || s == "on"
}
