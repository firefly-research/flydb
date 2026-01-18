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
Package server implements the FlyDB TCP server using the binary wire protocol.

Server Architecture Overview:
=============================

The FlyDB server is a multi-threaded TCP server that handles client connections
concurrently using a binary wire protocol. Each client connection is processed
in its own goroutine, allowing the server to handle many simultaneous clients
efficiently.

Connection Lifecycle:
=====================

 1. Client connects via TCP to the binary protocol port
 2. Server spawns a goroutine to handle the connection
 3. Client authenticates using the binary protocol
 4. Client sends binary-framed messages (queries, prepared statements, etc.)
 5. Server processes messages and sends binary-framed responses
 6. Connection closes when client disconnects or errors occur

Protocol:
=========

FlyDB uses a binary wire protocol with the following frame format:

	┌───────────┬─────────┬──────────┬───────────┬────────────┬─────────────────┐
	│ Magic (1) │ Ver (1) │ Type (1) │ Flags (1) │ Length (4) │ Payload (var)   │
	└───────────┴─────────┴──────────┴───────────┴────────────┴─────────────────┘

	- Magic: 0xFD (identifies FlyDB protocol)
	- Version: Protocol version (currently 0x01)
	- Type: Message type (Query, Auth, Prepare, Execute, etc.)
	- Flags: Reserved for compression/encryption
	- Length: Payload length in bytes (max 16 MB)
	- Payload: JSON-encoded message data

Thread Safety:
==============

The server uses sync.Mutex to protect shared state:
  - subMu: Protects the subscribers map
  - connsMu: Protects the connection-to-user map

This ensures safe concurrent access from multiple goroutines.
*/
package server

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"flydb/internal/auth"
	"flydb/internal/errors"
	"flydb/internal/logging"
	"flydb/internal/protocol"
	"flydb/internal/sql"
	"flydb/internal/storage"
)

// Package-level logger for the server component.
var log = logging.NewLogger("server")

// Server represents the FlyDB TCP server using the binary wire protocol.
// It handles client connections, command dispatching, authentication,
// and reactive subscriptions.
//
// The server is designed for concurrent access - each client connection
// is handled in a separate goroutine, and shared state is protected
// by mutexes.
type Server struct {
	// addr is the TCP address to listen on for the binary protocol (e.g., ":8889").
	addr string

	// executor handles SQL statement execution against the storage engine.
	// It is shared across all connections but is thread-safe.
	executor *sql.Executor

	// store is the underlying storage engine.
	// Used for direct access when needed (e.g., replication).
	store storage.Engine

	// auth handles user authentication and authorization.
	// It verifies credentials and checks table permissions.
	auth *auth.AuthManager

	// dbManager manages multiple databases.
	dbManager *storage.DatabaseManager

	// connDatabases maps connections to their current database name.
	// Default is "default" for new connections.
	connDatabases map[net.Conn]string

	// connDbMu protects the connDatabases map from concurrent access.
	connDbMu sync.Mutex

	// subscribers maps table names to sets of connections watching that table.
	// When a row is inserted, all subscribers are notified.
	// Structure: map[tableName]map[connection]struct{}
	subscribers map[string]map[net.Conn]struct{}

	// schemaSubscribers is a set of connections watching schema changes.
	// These connections receive notifications for CREATE/DROP/ALTER TABLE, etc.
	schemaSubscribers map[net.Conn]struct{}

	// subMu protects the subscribers and schemaSubscribers maps from concurrent access.
	subMu sync.Mutex

	// conns maps connections to authenticated usernames.
	// Empty string means unauthenticated or admin.
	conns map[net.Conn]string

	// connsMu protects the conns map from concurrent access.
	connsMu sync.Mutex

	// transactions maps connections to their active transactions.
	// Each connection can have at most one active transaction.
	transactions map[net.Conn]*storage.Transaction

	// txMu protects the transactions map from concurrent access.
	txMu sync.Mutex

	// binaryHandler handles binary protocol connections.
	binaryHandler *protocol.BinaryHandler

	// preparedStmts manages prepared statements for the server.
	preparedStmts *sql.PreparedStatementManager

	// tlsConfig holds the TLS configuration for encrypted connections.
	// If nil, TLS is disabled.
	tlsConfig *tls.Config

	// tlsAddr is the TCP address for TLS connections (e.g., ":8889").
	tlsAddr string

	// listeners holds all active listeners for graceful shutdown.
	listeners []net.Listener

	// listenersMu protects the listeners slice.
	listenersMu sync.Mutex

	// stopCh signals the server to stop accepting connections.
	stopCh chan struct{}

	// stopped indicates if the server has been stopped.
	stopped bool
}

// NewServerWithStore creates a new Server using an existing storage engine.
// This constructor is used when the storage engine is already initialized,
// such as when sharing storage with the replication system.
//
// The function performs the following initialization:
//  1. Creates an AuthManager for user authentication
//  2. Creates an Executor for SQL statement execution
//  3. Initializes the binary protocol handler
//  4. Wires up the OnInsert callback for reactive notifications
//
// Parameters:
//   - addr: TCP address to listen on for binary protocol (e.g., ":8889")
//   - store: Pre-initialized storage engine instance
//
// Returns a fully configured Server ready to start.
func NewServerWithStore(addr string, store storage.Engine) *Server {
	// Create the authentication manager backed by the same storage.
	authMgr := auth.NewAuthManager(store)

	// Initialize built-in RBAC roles (admin, reader, writer, owner).
	// This is idempotent - roles are only created if they don't exist.
	if err := authMgr.InitializeBuiltInRoles(); err != nil {
		log.Error("Failed to initialize built-in roles", "error", err)
	}

	// Create the SQL executor with storage and auth dependencies.
	exec := sql.NewExecutor(store, authMgr)

	// Create the prepared statement manager.
	prepMgr := sql.NewPreparedStatementManager(exec)

	// Initialize the server with all components.
	srv := &Server{
		addr:              addr,
		executor:          exec,
		store:             store,
		auth:              authMgr,
		subscribers:       make(map[string]map[net.Conn]struct{}),
		schemaSubscribers: make(map[net.Conn]struct{}),
		conns:             make(map[net.Conn]string),
		connDatabases:     make(map[net.Conn]string),
		transactions:      make(map[net.Conn]*storage.Transaction),
		preparedStmts:     prepMgr,
		listeners:         make([]net.Listener, 0),
		stopCh:            make(chan struct{}),
	}

	// Create the binary protocol handler.
	srv.binaryHandler = protocol.NewBinaryHandler(
		&serverQueryExecutor{srv: srv},
		prepMgr,
		&serverAuthenticator{auth: authMgr},
	)

	// Wire up optional dependencies for ODBC/JDBC driver support
	srv.binaryHandler.SetMetadataProvider(&serverMetadataProvider{srv: srv})
	srv.binaryHandler.SetDatabaseManager(&serverDatabaseManager{srv: srv})

	// Wire up the OnInsert callback for reactive WATCH functionality.
	// When the executor inserts a row, it calls this callback,
	// which broadcasts the event to all subscribers of that table.
	exec.OnInsert = srv.broadcastInsert

	// Wire up the OnUpdate callback for reactive WATCH functionality.
	exec.OnUpdate = srv.broadcastUpdate

	// Wire up the OnDelete callback for reactive WATCH functionality.
	exec.OnDelete = srv.broadcastDelete

	// Wire up the OnSchemaChange callback for reactive WATCH functionality.
	exec.OnSchemaChange = srv.broadcastSchemaChange

	return srv
}

// NewServerWithStoreAndBinary creates a new Server with the binary protocol.
// This is an alias for NewServerWithStore for backward compatibility.
// The binaryAddr parameter is ignored - use addr for the binary protocol port.
//
// Parameters:
//   - addr: TCP address for binary protocol (e.g., ":8889")
//   - binaryAddr: Ignored (kept for backward compatibility)
//   - store: Pre-initialized storage engine instance
//
// Returns a fully configured Server ready to start.
func NewServerWithStoreAndBinary(addr string, binaryAddr string, store storage.Engine) *Server {
	// Use binaryAddr if provided, otherwise use addr
	if binaryAddr != "" {
		return NewServerWithStore(binaryAddr, store)
	}
	return NewServerWithStore(addr, store)
}

// NewServerWithDatabaseManager creates a new Server with multi-database support.
// This constructor uses a DatabaseManager to manage multiple databases.
//
// Parameters:
//   - addr: TCP address for binary protocol (e.g., ":8889")
//   - binaryAddr: Ignored (kept for backward compatibility, use addr instead)
//   - dbManager: Pre-initialized DatabaseManager instance
//
// Returns a fully configured Server ready to start.
func NewServerWithDatabaseManager(addr string, binaryAddr string, dbManager *storage.DatabaseManager) *Server {
	// Use binaryAddr if provided, otherwise use addr
	effectiveAddr := addr
	if binaryAddr != "" {
		effectiveAddr = binaryAddr
	}

	// Get the default database for initial setup
	defaultDB, _ := dbManager.GetDatabase(storage.DefaultDatabaseName)
	store := defaultDB.Store

	// Get the system database for global authentication
	systemDB, _ := dbManager.GetSystemDatabase()
	systemStore := systemDB.Store

	// Create the authentication manager backed by the system database storage.
	// This ensures users are global across all databases.
	authMgr := auth.NewAuthManager(systemStore)

	// Initialize built-in RBAC roles (admin, reader, writer, owner).
	// This is idempotent - roles are only created if they don't exist.
	if err := authMgr.InitializeBuiltInRoles(); err != nil {
		log.Error("Failed to initialize built-in roles", "error", err)
	}

	// Create the SQL executor with storage and auth dependencies.
	exec := sql.NewExecutor(store, authMgr)

	// Create the prepared statement manager.
	prepMgr := sql.NewPreparedStatementManager(exec)

	// Initialize the server with all components.
	srv := &Server{
		addr:              effectiveAddr,
		executor:          exec,
		store:             store,
		auth:              authMgr,
		dbManager:         dbManager,
		subscribers:       make(map[string]map[net.Conn]struct{}),
		schemaSubscribers: make(map[net.Conn]struct{}),
		conns:             make(map[net.Conn]string),
		connDatabases:     make(map[net.Conn]string),
		transactions:      make(map[net.Conn]*storage.Transaction),
		preparedStmts:     prepMgr,
		listeners:         make([]net.Listener, 0),
		stopCh:            make(chan struct{}),
	}

	// Create the binary protocol handler.
	srv.binaryHandler = protocol.NewBinaryHandler(
		&serverQueryExecutor{srv: srv},
		prepMgr,
		&serverAuthenticator{auth: authMgr},
	)

	// Wire up optional dependencies for ODBC/JDBC driver support
	srv.binaryHandler.SetMetadataProvider(&serverMetadataProvider{srv: srv})
	srv.binaryHandler.SetDatabaseManager(&serverDatabaseManager{srv: srv})

	// Wire up the OnInsert callback for reactive WATCH functionality.
	// When the executor inserts a row, it calls this callback,
	// which broadcasts the event to all subscribers of that table.
	exec.OnInsert = srv.broadcastInsert

	// Wire up the OnUpdate callback for reactive WATCH functionality.
	exec.OnUpdate = srv.broadcastUpdate

	// Wire up the OnDelete callback for reactive WATCH functionality.
	exec.OnDelete = srv.broadcastDelete

	// Wire up the OnSchemaChange callback for reactive WATCH functionality.
	exec.OnSchemaChange = srv.broadcastSchemaChange

	return srv
}

// serverDatabaseManager implements the protocol.DatabaseManager interface.
type serverDatabaseManager struct {
	srv *Server
}

// UseDatabase switches to a different database.
func (m *serverDatabaseManager) UseDatabase(database string) error {
	if m.srv.dbManager == nil {
		if database == "" || database == "default" {
			return nil
		}
		return fmt.Errorf("database '%s' does not exist", database)
	}
	if !m.srv.dbManager.DatabaseExists(database) {
		return fmt.Errorf("database '%s' does not exist", database)
	}
	return nil
}

// DatabaseExists checks if a database exists.
func (m *serverDatabaseManager) DatabaseExists(database string) bool {
	if m.srv.dbManager == nil {
		return database == "" || database == "default"
	}
	return m.srv.dbManager.DatabaseExists(database)
}

// ListDatabases returns a list of available database names.
func (m *serverDatabaseManager) ListDatabases() []string {
	if m.srv.dbManager == nil {
		return []string{"default"}
	}
	return m.srv.dbManager.ListDatabases()
}

// serverQueryExecutor adapts the server for the QueryExecutor interface.
// It implements both QueryExecutor and DatabaseAwareQueryExecutor.
type serverQueryExecutor struct {
	srv *Server
}

// Execute executes a query using the default database.
func (e *serverQueryExecutor) Execute(query string, user string) (string, error) {
	return e.ExecuteInDatabase(query, "", user)
}

// ExecuteInDatabase executes a query in the context of a specific database.
// If database is empty, the default database is used.
func (e *serverQueryExecutor) ExecuteInDatabase(query, database string, user string) (string, error) {
	lexer := sql.NewLexer(query)
	parser := sql.NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		return "", err
	}

	// Handle database management statements at the server level
	// These require access to the DatabaseManager which is only available here
	switch dbStmt := stmt.(type) {
	case *sql.InspectStmt:
		if dbStmt.Target == "DATABASES" || dbStmt.Target == "DATABASE" {
			return e.handleInspectDatabase(dbStmt)
		}
		if dbStmt.Target == "USERS" {
			return e.handleInspectUsers()
		}
		// Handle user-related inspection using system database
		if dbStmt.Target == "USER" || dbStmt.Target == "USER_ROLES" || dbStmt.Target == "USER_PRIVILEGES" {
			return e.handleInspectUserInfo(dbStmt)
		}
		// Handle role-related inspection using system database
		if dbStmt.Target == "ROLES" || dbStmt.Target == "ROLE" || dbStmt.Target == "PRIVILEGES" {
			return e.handleInspectRoleInfo(dbStmt)
		}
	case *sql.CreateDatabaseStmt:
		return e.handleCreateDatabase(dbStmt)
	case *sql.DropDatabaseStmt:
		return e.handleDropDatabase(dbStmt)
	case *sql.UseDatabaseStmt:
		return e.handleUseDatabase(dbStmt)
	}

	// Get the executor for the specified database
	executor := e.getExecutorForDatabase(database)

	// Thread-safe: sql.Executor.Execute now handles user context if modified,
	// but wait, sql.Executor.Execute doesn't take a user parameter yet.
	// We need to set it on a cloned or thread-safe way.
	// For now, let's use a mutex or a new approach.
	// Actually, the best way is to pass user to executor.Execute.
	return executor.ExecuteWithUser(stmt, user)
}

// getExecutorForDatabase returns the executor for the specified database.
// If database is empty or "default", returns the default executor.
func (e *serverQueryExecutor) getExecutorForDatabase(database string) *sql.Executor {
	if e.srv.dbManager == nil {
		return e.srv.executor
	}

	if database == "" || database == storage.DefaultDatabaseName {
		return e.srv.executor
	}

	db, err := e.srv.dbManager.GetDatabase(database)
	if err != nil {
		// Fall back to default executor if database not found
		return e.srv.executor
	}

	// Create an executor for this database using the global auth manager
	// The auth manager is backed by the system database for global user management
	executor := sql.NewExecutor(db.Store, e.srv.auth)
	return executor
}

// handleInspectDatabase handles INSPECT DATABASES and INSPECT DATABASE <name>
// for the binary protocol path.
func (e *serverQueryExecutor) handleInspectDatabase(stmt *sql.InspectStmt) (string, error) {
	if e.srv.dbManager == nil {
		// If no database manager, return just the default database info
		if stmt.Target == "DATABASES" {
			return "name, owner, encoding, locale, collation, tables, size\ndefault, admin, UTF8, en_US, default, 0, 0\n(1 rows)", nil
		}
		return "Database: default\nOwner: admin\nEncoding: UTF8\nLocale: en_US\nCollation: default\nStatus: Active\nStorage: WAL-backed", nil
	}

	if stmt.Target == "DATABASES" {
		// List all databases with detailed info
		databases := e.srv.dbManager.ListDatabases()
		if len(databases) == 0 {
			return "name, owner, encoding, locale, collation, tables, size\n(0 rows)", nil
		}

		result := "name, owner, encoding, locale, collation, tables, size\n"
		count := 0
		for _, dbName := range databases {
			// Skip system database - it's internal
			if dbName == storage.SystemDatabaseName {
				continue
			}
			db, err := e.srv.dbManager.GetDatabase(dbName)
			if err != nil {
				continue
			}
			size := db.GetSize()
			sizeStr := formatSize(size)
			result += fmt.Sprintf("%s, %s, %s, %s, %s, %d, %s\n",
				db.Name,
				db.GetOwner(),
				db.GetEncoding(),
				db.GetLocale(),
				db.GetCollation(),
				db.GetTableCount(),
				sizeStr,
			)
			count++
		}
		result += fmt.Sprintf("(%d rows)", count)
		return result, nil
	}

	// INSPECT DATABASE <name>
	db, err := e.srv.dbManager.GetDatabase(stmt.ObjectName)
	if err != nil {
		return "", err
	}

	size := db.GetSize()
	sizeStr := formatSize(size)
	createdAt := db.GetCreatedAt()
	createdStr := "unknown"
	if !createdAt.IsZero() {
		createdStr = createdAt.Format("2006-01-02 15:04:05")
	}

	return fmt.Sprintf("Database: %s\nOwner: %s\nEncoding: %s\nLocale: %s\nCollation: %s\nTables: %d\nSize: %s\nCreated: %s\nPath: %s\nStatus: Active\nStorage: WAL-backed",
		db.Name, db.GetOwner(), db.GetEncoding(), db.GetLocale(), db.GetCollation(),
		db.GetTableCount(), sizeStr, createdStr, db.Path), nil
}

// handleInspectUsers handles INSPECT USERS for the binary protocol path.
// Users are stored in the system database, so we need to query it directly.
func (e *serverQueryExecutor) handleInspectUsers() (string, error) {
	if e.srv.dbManager == nil {
		return "username, role, created_at, last_login\n(0 rows)", nil
	}

	systemDB, err := e.srv.dbManager.GetSystemDatabase()
	if err != nil {
		return "", err
	}

	// Scan for all user keys with the _sys_users: prefix
	users, err := systemDB.Store.Scan("_sys_users:")
	if err != nil {
		return "", err
	}

	header := "username, role, created_at, last_login"
	if len(users) == 0 {
		return fmt.Sprintf("%s\n(0 rows)", header), nil
	}

	// Parse user data to get metadata
	type userInfo struct {
		username  string
		role      string
		createdAt string
		lastLogin string
	}
	var userInfos []userInfo

	for key, data := range users {
		username := strings.TrimPrefix(key, "_sys_users:")
		role := "user"

		// Parse user JSON to get metadata
		var userData struct {
			IsAdmin   bool   `json:"is_admin"`
			CreatedAt string `json:"created_at"`
			LastLogin string `json:"last_login"`
		}
		createdAt := "-"
		lastLogin := "-"
		if err := json.Unmarshal(data, &userData); err == nil {
			if userData.IsAdmin {
				role = "admin"
			}
			if userData.CreatedAt != "" {
				// Parse and format the timestamp
				if t, err := time.Parse(time.RFC3339, userData.CreatedAt); err == nil {
					createdAt = t.Format("2006-01-02 15:04:05")
				} else {
					createdAt = userData.CreatedAt
				}
			}
			if userData.LastLogin != "" {
				if t, err := time.Parse(time.RFC3339, userData.LastLogin); err == nil {
					lastLogin = t.Format("2006-01-02 15:04:05")
				} else {
					lastLogin = userData.LastLogin
				}
			}
		} else if username == "admin" {
			role = "admin"
		}

		userInfos = append(userInfos, userInfo{
			username:  username,
			role:      role,
			createdAt: createdAt,
			lastLogin: lastLogin,
		})
	}

	// Sort by username for consistent output
	sort.Slice(userInfos, func(i, j int) bool {
		return userInfos[i].username < userInfos[j].username
	})

	var results []string
	for _, u := range userInfos {
		results = append(results, fmt.Sprintf("%s, %s, %s, %s", u.username, u.role, u.createdAt, u.lastLogin))
	}

	return fmt.Sprintf("%s\n%s\n(%d rows)", header, strings.Join(results, "\n"), len(results)), nil
}

// handleInspectUserInfo handles INSPECT USER, USER_ROLES, USER_PRIVILEGES for binary protocol.
func (e *serverQueryExecutor) handleInspectUserInfo(stmt *sql.InspectStmt) (string, error) {
	if e.srv.dbManager == nil {
		return "", fmt.Errorf("database manager not initialized")
	}

	systemDB, err := e.srv.dbManager.GetSystemDatabase()
	if err != nil {
		return "", err
	}

	// Create an executor using the system database
	// Note: User context is managed by the binary protocol handler
	// These commands require admin privileges which are checked at the protocol level
	systemExecutor := sql.NewExecutor(systemDB.Store, e.srv.auth)

	return systemExecutor.Execute(stmt)
}

// handleInspectRoleInfo handles INSPECT ROLES, ROLE, PRIVILEGES for binary protocol.
func (e *serverQueryExecutor) handleInspectRoleInfo(stmt *sql.InspectStmt) (string, error) {
	if e.srv.dbManager == nil {
		return "", fmt.Errorf("database manager not initialized")
	}

	systemDB, err := e.srv.dbManager.GetSystemDatabase()
	if err != nil {
		return "", err
	}

	// Create an executor using the system database
	// Note: User context is managed by the binary protocol handler
	// These commands require admin privileges which are checked at the protocol level
	systemExecutor := sql.NewExecutor(systemDB.Store, e.srv.auth)

	return systemExecutor.Execute(stmt)
}

// formatSize formats a byte size into a human-readable string.
func formatSize(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// handleCreateDatabase handles CREATE DATABASE for the binary protocol path.
func (e *serverQueryExecutor) handleCreateDatabase(stmt *sql.CreateDatabaseStmt) (string, error) {
	if e.srv.dbManager == nil {
		err := errors.NewExecutionError("multi-database support not enabled")
		err.Hint = "Start the server with --data-dir to enable multi-database mode"
		return "", err
	}

	// Check if database already exists
	if e.srv.dbManager.DatabaseExists(stmt.DatabaseName) {
		if stmt.IfNotExists {
			return "CREATE DATABASE OK", nil
		}
		return "", errors.NewExecutionError(fmt.Sprintf("database '%s' already exists", stmt.DatabaseName))
	}

	// Create the database
	if err := e.srv.dbManager.CreateDatabase(stmt.DatabaseName); err != nil {
		return "", errors.NewExecutionError(err.Error())
	}

	return "CREATE DATABASE OK", nil
}

// handleDropDatabase handles DROP DATABASE for the binary protocol path.
func (e *serverQueryExecutor) handleDropDatabase(stmt *sql.DropDatabaseStmt) (string, error) {
	if e.srv.dbManager == nil {
		err := errors.NewExecutionError("multi-database support not enabled")
		err.Hint = "Start the server with --data-dir to enable multi-database mode"
		return "", err
	}

	// Check if database exists
	if !e.srv.dbManager.DatabaseExists(stmt.DatabaseName) {
		if stmt.IfExists {
			return "DROP DATABASE OK", nil
		}
		return "", errors.NewExecutionError(fmt.Sprintf("database '%s' does not exist", stmt.DatabaseName))
	}

	// Drop the database
	if err := e.srv.dbManager.DropDatabase(stmt.DatabaseName); err != nil {
		return "", errors.NewExecutionError(err.Error())
	}

	return "DROP DATABASE OK", nil
}

// handleUseDatabase handles USE <database> for the binary protocol path.
func (e *serverQueryExecutor) handleUseDatabase(stmt *sql.UseDatabaseStmt) (string, error) {
	if e.srv.dbManager == nil {
		err := errors.NewExecutionError("multi-database support not enabled")
		err.Hint = "Start the server with --data-dir to enable multi-database mode"
		return "", err
	}

	// Check if database exists
	if !e.srv.dbManager.DatabaseExists(stmt.DatabaseName) {
		err := errors.NewExecutionError(fmt.Sprintf("database '%s' does not exist", stmt.DatabaseName))
		err.Hint = "Use INSPECT DATABASES to see available databases"
		return "", err
	}

	return fmt.Sprintf("USE %s OK", stmt.DatabaseName), nil
}

// serverAuthenticator adapts the auth manager for the Authenticator interface.
type serverAuthenticator struct {
	auth *auth.AuthManager
}

func (a *serverAuthenticator) Authenticate(username, password string) bool {
	// Authenticate against the database.
	// Admin credentials are stored in the database like any other user,
	// initialized during first-time setup.
	return a.auth.Authenticate(username, password)
}

// NewServer creates a new Server and initializes a new storage engine at dataDir.
// This is a convenience constructor for standalone server usage.
//
// Parameters:
//   - addr: TCP address to listen on (e.g., ":8889")
//   - dataDir: Path to the data directory for persistence
//
// Returns the server and any error from storage initialization.
func NewServer(addr string, dataDir string) (*Server, error) {
	config := storage.StorageConfig{
		DataDir:            dataDir,
		BufferPoolSize:     0, // Auto-size
		CheckpointInterval: 60 * time.Second,
	}
	store, err := storage.NewStorageEngine(config)
	if err != nil {
		return nil, err
	}
	return NewServerWithStore(addr, store), nil
}

// TLSConfig holds the configuration for TLS connections.
type TLSConfig struct {
	// CertFile is the path to the TLS certificate file.
	CertFile string

	// KeyFile is the path to the TLS private key file.
	KeyFile string

	// Address is the TCP address for TLS connections (e.g., ":8889").
	Address string

	// ClientAuth specifies the policy for client certificate authentication.
	// Use tls.NoClientCert for no client auth, tls.RequireAndVerifyClientCert for mutual TLS.
	ClientAuth tls.ClientAuthType

	// ClientCAs is the path to the CA certificate file for verifying client certificates.
	// Only used when ClientAuth requires client certificates.
	ClientCAs string
}

// EnableTLS configures TLS for the server.
// Call this before Start() to enable encrypted connections.
func (s *Server) EnableTLS(config TLSConfig) error {
	cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
	if err != nil {
		return fmt.Errorf("failed to load TLS certificate: %w", err)
	}

	s.tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		ClientAuth:   config.ClientAuth,
	}

	// Load client CA if specified
	if config.ClientCAs != "" {
		// Note: In a full implementation, we would load the CA file here
		// For now, we just set the client auth type
	}

	s.tlsAddr = config.Address
	return nil
}

// startTLSListener starts the TLS listener for encrypted connections.
func (s *Server) startTLSListener(ready chan<- struct{}) {
	if s.tlsConfig == nil || s.tlsAddr == "" {
		close(ready)
		return
	}

	ln, err := tls.Listen("tcp", s.tlsAddr, s.tlsConfig)
	if err != nil {
		log.Error("Failed to start TLS listener", "address", s.tlsAddr, "error", err)
		close(ready)
		return
	}

	// Track the listener for graceful shutdown
	s.listenersMu.Lock()
	s.listeners = append(s.listeners, ln)
	s.listenersMu.Unlock()

	log.Info("TLS protocol listening", "address", s.tlsAddr)
	close(ready) // Signal that we're ready

	for {
		conn, err := ln.Accept()
		if err != nil {
			// Check if we're shutting down
			select {
			case <-s.stopCh:
				return
			default:
			}
			log.Warn("TLS accept error", "error", err)
			continue
		}
		log.Debug("New TLS connection accepted", "remote_addr", conn.RemoteAddr().String())
		go s.binaryHandler.HandleConnection(conn)
	}
}

// Start begins listening for TCP connections using the binary protocol.
// This method blocks indefinitely, accepting and handling client connections.
// Each connection is handled in a separate goroutine for concurrency.
//
// The accept loop continues until an unrecoverable error occurs or Stop is called.
// Individual connection errors are logged but don't stop the server.
//
// Returns an error only if the initial Listen call fails.
func (s *Server) Start() error {
	// If TLS is configured, use TLS listener exclusively
	if s.tlsConfig != nil && s.tlsAddr != "" {
		// Use a channel to signal when TLS listener is ready
		tlsReady := make(chan struct{})
		go s.startTLSListener(tlsReady)
		<-tlsReady

		// Block forever - the TLS listener runs in its own goroutine
		// Wait for stop signal
		<-s.stopCh
		log.Info("Server stopped")
		return nil
	}

	// Create a TCP listener on the configured address for binary protocol.
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		log.Error("Failed to start binary protocol listener", "address", s.addr, "error", err)
		return err
	}

	// Track the listener for graceful shutdown
	s.listenersMu.Lock()
	s.listeners = append(s.listeners, ln)
	s.listenersMu.Unlock()

	log.Info("Binary protocol listening", "address", s.addr)

	// Accept loop: continuously accept new connections.
	// This loop runs until the server is stopped.
	for {
		conn, err := ln.Accept()
		if err != nil {
			// Check if we're shutting down
			select {
			case <-s.stopCh:
				log.Info("Server stopped, exiting accept loop")
				return nil
			default:
			}
			// Log and continue on accept errors.
			// These are usually transient (e.g., too many open files).
			log.Warn("Accept error", "error", err)
			continue
		}

		// Handle each connection in a separate goroutine using binary protocol.
		// This allows the server to handle many clients concurrently.
		log.Debug("New binary connection accepted", "remote_addr", conn.RemoteAddr().String())
		go s.binaryHandler.HandleConnection(conn)
	}
}

// Stop gracefully stops the server by closing all listeners.
// This causes the accept loops to exit and prevents new connections.
func (s *Server) Stop() error {
	s.listenersMu.Lock()
	defer s.listenersMu.Unlock()

	if s.stopped {
		return nil
	}
	s.stopped = true

	// Signal all goroutines to stop
	close(s.stopCh)

	// Close all listeners
	var lastErr error
	for _, ln := range s.listeners {
		if err := ln.Close(); err != nil {
			log.Warn("Error closing listener", "error", err)
			lastErr = err
		}
	}

	log.Info("Server stopped")
	return lastErr
}

// broadcastInsert notifies all subscribers of a table about a new INSERT.
// This function is called by the executor's OnInsert callback whenever
// a row is inserted into a table.
//
// The notification is sent asynchronously to avoid blocking the INSERT.
// Each subscriber receives a message in the format: "EVENT <table> <json>\n"
//
// Parameters:
//   - table: Name of the table where the INSERT occurred
//   - data: JSON representation of the inserted row
func (s *Server) broadcastInsert(table string, data string) {
	s.subMu.Lock()
	defer s.subMu.Unlock()

	// Check if there are any subscribers for this table.
	if subs, ok := s.subscribers[table]; ok {
		// Format the event message.
		msg := fmt.Sprintf("EVENT INSERT %s %s\n", table, data)

		// Send to each subscriber asynchronously.
		// Using goroutines prevents slow clients from blocking others.
		for conn := range subs {
			go conn.Write([]byte(msg))
		}
	}
}

// broadcastUpdate notifies all subscribers of a table about an UPDATE.
// This function is called by the executor's OnUpdate callback whenever
// a row is updated in a table.
//
// The notification is sent asynchronously to avoid blocking the UPDATE.
// Each subscriber receives a message in the format: "EVENT UPDATE <table> <old_json> <new_json>\n"
//
// Parameters:
//   - table: Name of the table where the UPDATE occurred
//   - oldData: JSON representation of the row before update
//   - newData: JSON representation of the row after update
func (s *Server) broadcastUpdate(table string, oldData string, newData string) {
	s.subMu.Lock()
	defer s.subMu.Unlock()

	// Check if there are any subscribers for this table.
	if subs, ok := s.subscribers[table]; ok {
		// Format the event message with both old and new data.
		msg := fmt.Sprintf("EVENT UPDATE %s %s %s\n", table, oldData, newData)

		// Send to each subscriber asynchronously.
		for conn := range subs {
			go conn.Write([]byte(msg))
		}
	}
}

// broadcastDelete notifies all subscribers of a table about a DELETE.
// This function is called by the executor's OnDelete callback whenever
// a row is deleted from a table.
//
// The notification is sent asynchronously to avoid blocking the DELETE.
// Each subscriber receives a message in the format: "EVENT DELETE <table> <json>\n"
//
// Parameters:
//   - table: Name of the table where the DELETE occurred
//   - data: JSON representation of the deleted row
func (s *Server) broadcastDelete(table string, data string) {
	s.subMu.Lock()
	defer s.subMu.Unlock()

	// Check if there are any subscribers for this table.
	if subs, ok := s.subscribers[table]; ok {
		// Format the event message.
		msg := fmt.Sprintf("EVENT DELETE %s %s\n", table, data)

		// Send to each subscriber asynchronously.
		for conn := range subs {
			go conn.Write([]byte(msg))
		}
	}
}

// broadcastSchemaChange notifies all schema subscribers about a schema change.
// This function is called by the executor's OnSchemaChange callback whenever
// a DDL operation (CREATE TABLE, DROP TABLE, ALTER TABLE, etc.) is executed.
//
// The notification is sent asynchronously to avoid blocking the DDL operation.
// Each subscriber receives a message in the format: "EVENT SCHEMA <event_type> <object_name> <details>\n"
//
// Parameters:
//   - eventType: Type of schema change (CREATE_TABLE, DROP_TABLE, ALTER_TABLE, etc.)
//   - objectName: Name of the affected object (table, view, index, etc.)
//   - details: JSON representation of additional details
func (s *Server) broadcastSchemaChange(eventType string, objectName string, details string) {
	s.subMu.Lock()
	defer s.subMu.Unlock()

	// Format the event message.
	msg := fmt.Sprintf("EVENT SCHEMA %s %s %s\n", eventType, objectName, details)

	// Send to all schema subscribers asynchronously.
	for conn := range s.schemaSubscribers {
		go conn.Write([]byte(msg))
	}
}

// getExecutorForConnection returns the executor for the connection's current database.
// If no database manager is configured or the connection has no database set,
// it returns the default executor.
func (s *Server) getExecutorForConnection(conn net.Conn) *sql.Executor {
	if s.dbManager == nil {
		return s.executor
	}

	s.connDbMu.Lock()
	dbName := s.connDatabases[conn]
	s.connDbMu.Unlock()

	if dbName == "" || dbName == storage.DefaultDatabaseName {
		return s.executor
	}

	db, err := s.dbManager.GetDatabase(dbName)
	if err != nil {
		// Fall back to default executor if database not found
		return s.executor
	}

	// Create an executor for this database using the global auth manager
	// The auth manager is backed by the system database for global user management
	exec := sql.NewExecutor(db.Store, s.auth)

	// Set collation and encoding from database metadata
	if db.Metadata != nil {
		exec.SetCollation(db.Metadata.Collation, db.Metadata.Locale)
		exec.SetEncoding(db.Metadata.Encoding)
	}

	return exec
}

// handleUseDatabase handles the USE <database> statement.
func (s *Server) handleUseDatabase(conn net.Conn, stmt *sql.UseDatabaseStmt, user string) string {
	remoteAddr := conn.RemoteAddr().String()

	if s.dbManager == nil {
		err := errors.NewExecutionError("multi-database support not enabled")
		err.Hint = "Start the server with --data-dir to enable multi-database mode"
		return err.UserMessage()
	}

	// Check if database exists
	if !s.dbManager.DatabaseExists(stmt.DatabaseName) {
		err := errors.NewExecutionError(fmt.Sprintf("database '%s' does not exist", stmt.DatabaseName))
		err.Hint = "Use INSPECT DATABASES to see available databases"
		return err.UserMessage()
	}

	// Update the connection's current database
	s.connDbMu.Lock()
	s.connDatabases[conn] = stmt.DatabaseName
	s.connDbMu.Unlock()

	log.Debug("Database switched", "remote_addr", remoteAddr, "database", stmt.DatabaseName)
	return fmt.Sprintf("USE %s OK", stmt.DatabaseName)
}

// handleCreateDatabase handles the CREATE DATABASE statement.
func (s *Server) handleCreateDatabase(conn net.Conn, stmt *sql.CreateDatabaseStmt, user string) string {
	remoteAddr := conn.RemoteAddr().String()

	// CREATE DATABASE requires admin privileges
	if user != "" && user != "admin" {
		err := errors.NewAuthError("permission denied for CREATE DATABASE")
		err.Hint = "Only admin users can create databases"
		return err.UserMessage()
	}

	if s.dbManager == nil {
		err := errors.NewExecutionError("multi-database support not enabled")
		err.Hint = "Start the server with --data-dir to enable multi-database mode"
		return err.UserMessage()
	}

	// Check if database already exists
	if s.dbManager.DatabaseExists(stmt.DatabaseName) {
		if stmt.IfNotExists {
			return "CREATE DATABASE OK"
		}
		err := errors.NewExecutionError(fmt.Sprintf("database '%s' already exists", stmt.DatabaseName))
		err.Hint = "Use CREATE DATABASE IF NOT EXISTS to avoid this error"
		return err.UserMessage()
	}

	// Build options from statement
	opts := storage.DefaultCreateDatabaseOptions()
	if stmt.Owner != "" {
		opts.Owner = stmt.Owner
	} else {
		opts.Owner = user
	}
	if stmt.Encoding != "" {
		opts.Encoding = storage.CharacterEncoding(stmt.Encoding)
	}
	if stmt.Locale != "" {
		opts.Locale = stmt.Locale
	}
	if stmt.Collation != "" {
		opts.Collation = storage.Collation(stmt.Collation)
	}
	if stmt.Description != "" {
		opts.Description = stmt.Description
	}

	// Create the database with options
	err := s.dbManager.CreateDatabaseWithOptions(stmt.DatabaseName, opts)
	if err != nil {
		return errors.NewExecutionError(err.Error()).UserMessage()
	}

	log.Info("Database created", "remote_addr", remoteAddr, "database", stmt.DatabaseName,
		"encoding", opts.Encoding, "locale", opts.Locale)
	return "CREATE DATABASE OK"
}

// handleDropDatabase handles the DROP DATABASE statement.
func (s *Server) handleDropDatabase(conn net.Conn, stmt *sql.DropDatabaseStmt, user string) string {
	remoteAddr := conn.RemoteAddr().String()

	// DROP DATABASE requires admin privileges
	if user != "" && user != "admin" {
		err := errors.NewAuthError("permission denied for DROP DATABASE")
		err.Hint = "Only admin users can drop databases"
		return err.UserMessage()
	}

	if s.dbManager == nil {
		err := errors.NewExecutionError("multi-database support not enabled")
		err.Hint = "Start the server with --data-dir to enable multi-database mode"
		return err.UserMessage()
	}

	// Check if database exists
	if !s.dbManager.DatabaseExists(stmt.DatabaseName) {
		if stmt.IfExists {
			return "DROP DATABASE OK"
		}
		err := errors.NewExecutionError(fmt.Sprintf("database '%s' does not exist", stmt.DatabaseName))
		err.Hint = "Use DROP DATABASE IF EXISTS to avoid this error"
		return err.UserMessage()
	}

	// Check if any connection is using this database
	s.connDbMu.Lock()
	for c, dbName := range s.connDatabases {
		if dbName == stmt.DatabaseName && c != conn {
			s.connDbMu.Unlock()
			err := errors.NewExecutionError(fmt.Sprintf("database '%s' is in use by other connections", stmt.DatabaseName))
			err.Hint = "Disconnect other clients from this database first"
			return err.UserMessage()
		}
	}
	// Track if this connection was using the database being dropped
	wasUsingDroppedDb := s.connDatabases[conn] == stmt.DatabaseName
	if wasUsingDroppedDb {
		s.connDatabases[conn] = storage.DefaultDatabaseName
	}
	s.connDbMu.Unlock()

	// Drop the database
	err := s.dbManager.DropDatabase(stmt.DatabaseName)
	if err != nil {
		return errors.NewExecutionError(err.Error()).UserMessage()
	}

	log.Info("Database dropped", "remote_addr", remoteAddr, "database", stmt.DatabaseName)

	// If the connection was using the dropped database, inform the client
	if wasUsingDroppedDb {
		return fmt.Sprintf("DROP DATABASE OK (switched to %s)", storage.DefaultDatabaseName)
	}
	return "DROP DATABASE OK"
}

// handleInspectDatabase handles INSPECT DATABASES and INSPECT DATABASE <name>.
func (s *Server) handleInspectDatabase(conn net.Conn, stmt *sql.InspectStmt, user string) string {
	// INSPECT requires admin privileges
	if user != "" && user != "admin" {
		err := errors.NewAuthError("permission denied for INSPECT")
		err.Hint = "Only admin users can inspect databases"
		return err.UserMessage()
	}

	if s.dbManager == nil {
		// If no database manager, return just the default database info
		if stmt.Target == "DATABASES" {
			return "name, owner, encoding, locale, collation, tables, size\ndefault, admin, UTF8, en_US, default, 0, 0\n(1 rows)"
		}
		return "Database: default\nOwner: admin\nEncoding: UTF8\nLocale: en_US\nCollation: default\nStatus: Active\nStorage: WAL-backed"
	}

	if stmt.Target == "DATABASES" {
		// List all databases with detailed info
		databases := s.dbManager.ListDatabases()
		if len(databases) == 0 {
			return "name, owner, encoding, locale, collation, tables, size\n(0 rows)"
		}

		result := "name, owner, encoding, locale, collation, tables, size\n"
		for _, dbName := range databases {
			db, err := s.dbManager.GetDatabase(dbName)
			if err != nil {
				continue
			}
			size := db.GetSize()
			sizeStr := formatSize(size)
			result += fmt.Sprintf("%s, %s, %s, %s, %s, %d, %s\n",
				db.Name,
				db.GetOwner(),
				db.GetEncoding(),
				db.GetLocale(),
				db.GetCollation(),
				db.GetTableCount(),
				sizeStr,
			)
		}
		result += fmt.Sprintf("(%d rows)", len(databases))
		return result
	}

	// INSPECT DATABASE <name>
	db, err := s.dbManager.GetDatabase(stmt.ObjectName)
	if err != nil {
		return errors.NewExecutionError(err.Error()).UserMessage()
	}

	size := db.GetSize()
	sizeStr := formatSize(size)
	createdAt := db.GetCreatedAt()
	createdStr := "unknown"
	if !createdAt.IsZero() {
		createdStr = createdAt.Format("2006-01-02 15:04:05")
	}

	return fmt.Sprintf("Database: %s\nOwner: %s\nEncoding: %s\nLocale: %s\nCollation: %s\nTables: %d\nSize: %s\nCreated: %s\nPath: %s\nStatus: Active\nStorage: WAL-backed",
		db.Name, db.GetOwner(), db.GetEncoding(), db.GetLocale(), db.GetCollation(),
		db.GetTableCount(), sizeStr, createdStr, db.Path)
}

// handleInspectUsers handles INSPECT USERS.
// Users are stored in the system database, so we need to query it directly.
func (s *Server) handleInspectUsers(conn net.Conn, user string) string {
	// INSPECT requires admin privileges
	if user != "" && user != "admin" {
		err := errors.NewAuthError("permission denied for INSPECT")
		err.Hint = "Only admin users can inspect users"
		return err.UserMessage()
	}

	// Get the system database
	if s.dbManager == nil {
		return "username, role, created_at, last_login\n(0 rows)"
	}

	systemDB, err := s.dbManager.GetSystemDatabase()
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	// Scan for all user keys with the _sys_users: prefix
	users, err := systemDB.Store.Scan("_sys_users:")
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	header := "username, role, created_at, last_login"
	if len(users) == 0 {
		return fmt.Sprintf("%s\n(0 rows)", header)
	}

	// Parse user data to get metadata
	type userInfo struct {
		username  string
		role      string
		createdAt string
		lastLogin string
	}
	var userInfos []userInfo

	for key, data := range users {
		username := strings.TrimPrefix(key, "_sys_users:")
		role := "user"

		// Parse user JSON to get metadata
		var userData struct {
			IsAdmin   bool   `json:"is_admin"`
			CreatedAt string `json:"created_at"`
			LastLogin string `json:"last_login"`
		}
		createdAt := "-"
		lastLogin := "-"
		if err := json.Unmarshal(data, &userData); err == nil {
			if userData.IsAdmin {
				role = "admin"
			}
			if userData.CreatedAt != "" {
				// Parse and format the timestamp
				if t, err := time.Parse(time.RFC3339, userData.CreatedAt); err == nil {
					createdAt = t.Format("2006-01-02 15:04:05")
				} else {
					createdAt = userData.CreatedAt
				}
			}
			if userData.LastLogin != "" {
				if t, err := time.Parse(time.RFC3339, userData.LastLogin); err == nil {
					lastLogin = t.Format("2006-01-02 15:04:05")
				} else {
					lastLogin = userData.LastLogin
				}
			}
		} else if username == "admin" {
			role = "admin"
		}

		userInfos = append(userInfos, userInfo{
			username:  username,
			role:      role,
			createdAt: createdAt,
			lastLogin: lastLogin,
		})
	}

	// Sort by username for consistent output
	sort.Slice(userInfos, func(i, j int) bool {
		return userInfos[i].username < userInfos[j].username
	})

	var results []string
	for _, u := range userInfos {
		results = append(results, fmt.Sprintf("%s, %s, %s, %s", u.username, u.role, u.createdAt, u.lastLogin))
	}

	return fmt.Sprintf("%s\n%s\n(%d rows)", header, strings.Join(results, "\n"), len(results))
}

// handleInspectUserInfo handles INSPECT USER, USER_ROLES, USER_PRIVILEGES.
// These need to use the system database where users and RBAC data are stored.
func (s *Server) handleInspectUserInfo(conn net.Conn, stmt *sql.InspectStmt, user string) string {
	// INSPECT requires admin privileges
	if user != "" && user != "admin" {
		err := errors.NewAuthError("permission denied for INSPECT")
		err.Hint = "Only admin users can inspect user information"
		return err.UserMessage()
	}

	// Get the system database
	if s.dbManager == nil {
		return "Error: database manager not initialized"
	}

	systemDB, err := s.dbManager.GetSystemDatabase()
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	// Create an executor using the system database
	systemExecutor := sql.NewExecutor(systemDB.Store, s.auth)
	systemExecutor.SetUser(user)

	// Execute the inspect command
	result, execErr := systemExecutor.Execute(stmt)
	if execErr != nil {
		return fmt.Sprintf("Error: %v", execErr)
	}

	return result
}

// handleInspectRoleInfo handles INSPECT ROLES, ROLE, PRIVILEGES.
// These need to use the system database where RBAC data is stored.
func (s *Server) handleInspectRoleInfo(conn net.Conn, stmt *sql.InspectStmt, user string) string {
	// INSPECT requires admin privileges
	if user != "" && user != "admin" {
		err := errors.NewAuthError("permission denied for INSPECT")
		err.Hint = "Only admin users can inspect role information"
		return err.UserMessage()
	}

	// Get the system database
	if s.dbManager == nil {
		return "Error: database manager not initialized"
	}

	systemDB, err := s.dbManager.GetSystemDatabase()
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	// Create an executor using the system database
	systemExecutor := sql.NewExecutor(systemDB.Store, s.auth)
	systemExecutor.SetUser(user)

	// Execute the inspect command
	result, execErr := systemExecutor.Execute(stmt)
	if execErr != nil {
		return fmt.Sprintf("Error: %v", execErr)
	}

	return result
}
