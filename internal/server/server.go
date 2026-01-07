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
Package server implements the FlyDB TCP server and command dispatcher.

Server Architecture Overview:
=============================

The FlyDB server is a multi-threaded TCP server that handles client connections
concurrently. Each client connection is processed in its own goroutine, allowing
the server to handle many simultaneous clients efficiently.

Connection Lifecycle:
=====================

  1. Client connects via TCP
  2. Server spawns a goroutine to handle the connection
  3. Client sends newline-terminated commands
  4. Server parses and executes commands
  5. Server sends newline-terminated responses
  6. Connection closes when client disconnects or errors occur

Protocol:
=========

FlyDB uses a simple text-based protocol:

  Request:  <COMMAND> [arguments]\n
  Response: <RESULT>\n

Supported Commands:
  - PING              : Health check, returns "PONG"
  - AUTH <user> <pwd> : Authenticate, returns "AUTH OK" or error
  - SQL <statement>   : Execute SQL, returns result or error
  - WATCH <table>     : Subscribe to INSERT events on a table

Reactive Subscriptions (WATCH):
===============================

The WATCH command enables real-time notifications when rows are inserted
into a table. This is useful for building reactive applications.

When a client executes "WATCH users", they receive "EVENT users <json>"
messages whenever a new row is inserted into the users table.

Implementation uses a pub/sub pattern:
  1. Client sends "WATCH <table>"
  2. Server adds connection to subscribers map
  3. On INSERT, executor calls OnInsert callback
  4. Server broadcasts to all subscribers of that table

Thread Safety:
==============

The server uses sync.Mutex to protect shared state:
  - subMu: Protects the subscribers map
  - connsMu: Protects the connection-to-user map

This ensures safe concurrent access from multiple goroutines.
*/
package server

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"sync"

	"flydb/internal/auth"
	"flydb/internal/errors"
	"flydb/internal/logging"
	"flydb/internal/protocol"
	"flydb/internal/sql"
	"flydb/internal/storage"
)

// Package-level logger for the server component.
var log = logging.NewLogger("server")

// Server represents the FlyDB TCP server.
// It handles client connections, command dispatching, authentication,
// and reactive subscriptions (WATCH).
//
// The server is designed for concurrent access - each client connection
// is handled in a separate goroutine, and shared state is protected
// by mutexes.
type Server struct {
	// addr is the TCP address to listen on (e.g., ":8888").
	addr string

	// executor handles SQL statement execution against the storage engine.
	// It is shared across all connections but is thread-safe.
	executor *sql.Executor

	// store is the underlying storage engine (KVStore).
	// Used for direct access when needed (e.g., replication).
	store storage.Engine

	// kvStore is the concrete KVStore for transaction support.
	kvStore *storage.KVStore

	// auth handles user authentication and authorization.
	// It verifies credentials and checks table permissions.
	auth *auth.AuthManager

	// subscribers maps table names to sets of connections watching that table.
	// When a row is inserted, all subscribers are notified.
	// Structure: map[tableName]map[connection]struct{}
	subscribers map[string]map[net.Conn]struct{}

	// subMu protects the subscribers map from concurrent access.
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

	// binaryAddr is the TCP address for binary protocol connections.
	binaryAddr string

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

// NewServerWithStore creates a new Server using an existing KVStore.
// This constructor is used when the KVStore is already initialized,
// such as when sharing storage with the replication system.
//
// The function performs the following initialization:
//  1. Creates an AuthManager for user authentication
//  2. Creates an Executor for SQL statement execution
//  3. Initializes the subscribers map for WATCH functionality
//  4. Wires up the OnInsert callback for reactive notifications
//
// Parameters:
//   - addr: TCP address to listen on (e.g., ":8888")
//   - kv: Pre-initialized KVStore instance
//
// Returns a fully configured Server ready to start.
func NewServerWithStore(addr string, kv *storage.KVStore) *Server {
	return NewServerWithStoreAndBinary(addr, "", kv)
}

// NewServerWithStoreAndBinary creates a new Server with both text and binary protocol support.
// This constructor is used when both protocols are needed.
//
// Parameters:
//   - addr: TCP address for text protocol (e.g., ":8888")
//   - binaryAddr: TCP address for binary protocol (e.g., ":8889"), empty to disable
//   - kv: Pre-initialized KVStore instance
//
// Returns a fully configured Server ready to start.
func NewServerWithStoreAndBinary(addr string, binaryAddr string, kv *storage.KVStore) *Server {
	// Create the authentication manager backed by the same storage.
	authMgr := auth.NewAuthManager(kv)

	// Create the SQL executor with storage and auth dependencies.
	exec := sql.NewExecutor(kv, authMgr)

	// Create the prepared statement manager.
	prepMgr := sql.NewPreparedStatementManager(exec)

	// Initialize the server with all components.
	srv := &Server{
		addr:          addr,
		binaryAddr:    binaryAddr,
		executor:      exec,
		store:         kv,
		kvStore:       kv,
		auth:          authMgr,
		subscribers:   make(map[string]map[net.Conn]struct{}),
		conns:         make(map[net.Conn]string),
		transactions:  make(map[net.Conn]*storage.Transaction),
		preparedStmts: prepMgr,
		listeners:     make([]net.Listener, 0),
		stopCh:        make(chan struct{}),
	}

	// Create the binary protocol handler if binary address is specified.
	if binaryAddr != "" {
		srv.binaryHandler = protocol.NewBinaryHandler(
			&serverQueryExecutor{srv: srv},
			prepMgr,
			&serverAuthenticator{auth: authMgr},
		)
	}

	// Wire up the OnInsert callback for reactive WATCH functionality.
	// When the executor inserts a row, it calls this callback,
	// which broadcasts the event to all subscribers of that table.
	exec.OnInsert = srv.broadcastInsert

	return srv
}

// serverQueryExecutor adapts the server for the QueryExecutor interface.
type serverQueryExecutor struct {
	srv *Server
}

func (e *serverQueryExecutor) Execute(query string) (string, error) {
	lexer := sql.NewLexer(query)
	parser := sql.NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		return "", err
	}
	return e.srv.executor.Execute(stmt)
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

// NewServer creates a new Server and initializes a new KVStore at storePath.
// This is a convenience constructor for standalone server usage.
//
// Parameters:
//   - addr: TCP address to listen on (e.g., ":8888")
//   - storePath: Path to the WAL file for persistence
//
// Returns the server and any error from storage initialization.
func NewServer(addr string, storePath string) (*Server, error) {
	kv, err := storage.NewKVStore(storePath)
	if err != nil {
		return nil, err
	}
	return NewServerWithStore(addr, kv), nil
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
		go s.handleConnection(conn)
	}
}

// Start begins listening for TCP connections and enters the accept loop.
// This method blocks indefinitely, accepting and handling client connections.
// Each connection is handled in a separate goroutine for concurrency.
//
// The accept loop continues until an unrecoverable error occurs or Stop is called.
// Individual connection errors are logged but don't stop the server.
//
// Returns an error only if the initial Listen call fails.
func (s *Server) Start() error {
	// Use a channel to signal when listeners are ready
	binaryReady := make(chan struct{})
	tlsReady := make(chan struct{})

	// Start binary protocol listener if configured.
	if s.binaryAddr != "" {
		go s.startBinaryListener(binaryReady)
	} else {
		close(binaryReady)
	}

	// Start TLS listener if configured.
	if s.tlsConfig != nil && s.tlsAddr != "" {
		go s.startTLSListener(tlsReady)
	} else {
		close(tlsReady)
	}

	// Create a TCP listener on the configured address for text protocol.
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		log.Error("Failed to start text protocol listener", "address", s.addr, "error", err)
		return err
	}

	// Track the listener for graceful shutdown
	s.listenersMu.Lock()
	s.listeners = append(s.listeners, ln)
	s.listenersMu.Unlock()

	log.Info("Text protocol listening", "address", s.addr)

	// Wait for other listeners to be ready before accepting connections
	<-binaryReady
	<-tlsReady

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

		// Handle each connection in a separate goroutine.
		// This allows the server to handle many clients concurrently.
		log.Debug("New connection accepted", "remote_addr", conn.RemoteAddr().String())
		go s.handleConnection(conn)
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

// startBinaryListener starts the binary protocol listener.
func (s *Server) startBinaryListener(ready chan<- struct{}) {
	ln, err := net.Listen("tcp", s.binaryAddr)
	if err != nil {
		log.Error("Failed to start binary protocol listener", "address", s.binaryAddr, "error", err)
		close(ready)
		return
	}

	// Track the listener for graceful shutdown
	s.listenersMu.Lock()
	s.listeners = append(s.listeners, ln)
	s.listenersMu.Unlock()

	log.Info("Binary protocol listening", "address", s.binaryAddr)
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
			log.Warn("Binary accept error", "error", err)
			continue
		}
		log.Debug("New binary connection accepted", "remote_addr", conn.RemoteAddr().String())
		go s.binaryHandler.HandleConnection(conn)
	}
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
		msg := fmt.Sprintf("EVENT %s %s\n", table, data)

		// Send to each subscriber asynchronously.
		// Using goroutines prevents slow clients from blocking others.
		for conn := range subs {
			go conn.Write([]byte(msg))
		}
	}
}

// handleConnection processes a single client connection.
// It reads commands from the connection, dispatches them to the appropriate
// handler, and sends responses back to the client.
//
// The function runs until the client disconnects or an error occurs.
// On exit, it cleans up subscriptions and connection state.
//
// Connection cleanup is handled in a deferred function to ensure
// resources are released even if a panic occurs.
func (s *Server) handleConnection(conn net.Conn) {
	remoteAddr := conn.RemoteAddr().String()
	connLog := log.With("remote_addr", remoteAddr)

	// Deferred cleanup: remove subscriptions, rollback transactions, and close connection.
	defer func() {
		// Remove this connection from all subscription lists.
		s.subMu.Lock()
		for _, subs := range s.subscribers {
			delete(subs, conn)
		}
		s.subMu.Unlock()

		// Remove the connection from the authenticated users map.
		s.connsMu.Lock()
		delete(s.conns, conn)
		s.connsMu.Unlock()

		// Rollback any active transaction for this connection.
		s.txMu.Lock()
		if tx, ok := s.transactions[conn]; ok && tx != nil && tx.IsActive() {
			connLog.Debug("Rolling back active transaction on disconnect")
			tx.Rollback()
		}
		delete(s.transactions, conn)
		s.txMu.Unlock()

		// Close the TCP connection.
		connLog.Debug("Connection closed")
		conn.Close()
	}()

	// Create a scanner for reading newline-terminated commands.
	scanner := bufio.NewScanner(conn)

	// Command processing loop: read and execute commands until disconnect.
	for scanner.Scan() {
		// Parse the command line into command and arguments.
		// Format: <COMMAND> [arguments]
		line := scanner.Text()
		parts := strings.SplitN(line, " ", 2)
		if len(parts) == 0 {
			continue
		}

		// Extract and normalize the command (case-insensitive).
		cmd := strings.ToUpper(parts[0])

		// Create request context for logging (do not log sensitive data)
		reqCtx := logging.NewRequestContext(remoteAddr, cmd)

		// Process the command and generate a response.
		var response string
		var isError bool

		switch cmd {
		case "PING":
			// PING: Simple health check command.
			// Returns "PONG" to confirm the server is responsive.
			response = "PONG"

		case "AUTH":
			// AUTH: Authenticate the connection with username and password.
			// Format: AUTH <username> <password>
			// On success, associates the connection with the user for permission checks.
			response = s.handleAuth(conn, parts)
			isError = strings.HasPrefix(response, "ERROR")

		case "WATCH":
			// WATCH: Subscribe to INSERT events on a table.
			// Format: WATCH <table>
			// After subscribing, the client receives "EVENT <table> <json>"
			// messages whenever a row is inserted into the table.
			response = s.handleWatch(conn, parts)
			isError = strings.HasPrefix(response, "ERROR")

		case "SQL":
			// SQL: Execute a SQL statement.
			// Format: SQL <statement>
			// Supports: SELECT, INSERT, UPDATE, DELETE, CREATE TABLE,
			//           CREATE USER, GRANT
			response = s.handleSQL(conn, parts)
			isError = strings.HasPrefix(response, "ERROR")

		default:
			// Unknown command - return an error with structured error handling.
			err := errors.InvalidCommand(cmd)
			response = err.UserMessage()
			isError = true
		}

		// Log the completed request (without sensitive data)
		if isError {
			reqCtx.LogError(log, "command failed")
		} else {
			reqCtx.LogComplete(log, "success")
		}

		// Send the response back to the client.
		// All responses are newline-terminated.
		conn.Write([]byte(response + "\n"))
	}

	// Check for scanner errors
	if err := scanner.Err(); err != nil {
		connLog.Debug("Connection read error", "error", err)
	}
}

// handleAuth processes the AUTH command for user authentication.
// It supports a hardcoded admin account for bootstrap and database-backed
// user accounts for regular users.
//
// Parameters:
//   - conn: The client connection to authenticate
//   - parts: Command parts [command, arguments]
//
// Returns the response string to send to the client.
func (s *Server) handleAuth(conn net.Conn, parts []string) string {
	remoteAddr := conn.RemoteAddr().String()

	// Validate command format.
	if len(parts) < 2 {
		err := errors.NewValidationError("missing credentials")
		err.Hint = "Usage: AUTH <user> <pass>"
		log.Debug("AUTH command missing credentials", "remote_addr", remoteAddr)
		return err.UserMessage()
	}

	// Parse username and password from arguments.
	creds := strings.Split(parts[1], " ")
	if len(creds) != 2 {
		err := errors.NewValidationError("invalid credentials format")
		err.Hint = "Usage: AUTH <user> <pass>"
		log.Debug("AUTH command invalid format", "remote_addr", remoteAddr)
		return err.UserMessage()
	}

	user := creds[0]
	pass := creds[1]

	// Authenticate against the database.
	// Admin credentials are stored in the database like any other user,
	// initialized during first-time setup.
	if s.auth.Authenticate(user, pass) {
		s.connsMu.Lock()
		s.conns[conn] = user
		s.connsMu.Unlock()
		log.Info("User authenticated", "user", user, "remote_addr", remoteAddr)
		// Return special message for admin user
		if auth.IsAdmin(user) {
			return "AUTH OK (admin)"
		}
		return "AUTH OK"
	}

	log.Warn("Authentication failed", "user", user, "remote_addr", remoteAddr)
	return errors.AuthenticationFailed().UserMessage()
}

// handleWatch processes the WATCH command for reactive subscriptions.
// It adds the connection to the subscribers list for the specified table.
//
// Parameters:
//   - conn: The client connection to subscribe
//   - parts: Command parts [command, table]
//
// Returns the response string to send to the client.
func (s *Server) handleWatch(conn net.Conn, parts []string) string {
	// Validate command format.
	if len(parts) < 2 {
		return "ERROR: Missing table name. Usage: WATCH <table>"
	}

	table := parts[1]

	// Add the connection to the subscribers map.
	// Create the table's subscriber set if it doesn't exist.
	s.subMu.Lock()
	if _, ok := s.subscribers[table]; !ok {
		s.subscribers[table] = make(map[net.Conn]struct{})
	}
	s.subscribers[table][conn] = struct{}{}
	s.subMu.Unlock()

	return "WATCH OK"
}

// handleSQL processes the SQL command for statement execution.
// It parses the SQL statement, checks permissions, and executes it.
//
// The execution flow is:
//  1. Tokenize the SQL with the Lexer
//  2. Parse tokens into an AST with the Parser
//  3. Set the user context for permission checks
//  4. Set the transaction context for this connection
//  5. Execute the AST with the Executor
//  6. Update transaction state if needed
//
// Parameters:
//   - conn: The client connection (for user context)
//   - parts: Command parts [command, sql_statement]
//
// Returns the response string to send to the client.
func (s *Server) handleSQL(conn net.Conn, parts []string) string {
	remoteAddr := conn.RemoteAddr().String()

	// Validate command format.
	if len(parts) < 2 {
		err := errors.MissingRequired("SQL statement")
		err.Hint = "Usage: SQL <statement>"
		log.Debug("SQL command missing query", "remote_addr", remoteAddr)
		return err.UserMessage()
	}

	query := parts[1]
	log.Debug("Executing SQL", "remote_addr", remoteAddr, "query", query)

	// Parse the SQL statement.
	// The Lexer tokenizes the input, and the Parser builds an AST.
	lexer := sql.NewLexer(query)
	parser := sql.NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		// Create a structured syntax error
		syntaxErr := errors.NewSyntaxError(err.Error())
		syntaxErr.Hint = "Check your SQL syntax and try again"
		log.Debug("SQL parse error", "remote_addr", remoteAddr, "error", err, "query", query)
		return syntaxErr.UserMessage()
	}

	// Set the user context for permission checks.
	// The executor uses this to enforce table access and RLS.
	s.connsMu.Lock()
	user := s.conns[conn]
	s.connsMu.Unlock()
	s.executor.SetUser(user)

	// Set the transaction context for this connection.
	s.txMu.Lock()
	tx := s.transactions[conn]
	s.txMu.Unlock()
	s.executor.SetTransaction(tx)

	// Execute the statement and return the result.
	res, execErr := s.executor.Execute(stmt)
	if execErr != nil {
		// Wrap execution errors with structured error handling
		var flyErr *errors.FlyDBError
		if e, ok := execErr.(*errors.FlyDBError); ok {
			flyErr = e
		} else {
			flyErr = errors.NewExecutionError(execErr.Error())
		}
		log.Debug("SQL execution error",
			"remote_addr", remoteAddr,
			"error", execErr,
			"query", query,
			"user", user,
		)
		return flyErr.UserMessage()
	}

	log.Debug("SQL executed successfully", "remote_addr", remoteAddr, "query", query)

	// Update transaction state after execution.
	// If BEGIN was executed, store the new transaction.
	// If COMMIT or ROLLBACK was executed, clear the transaction.
	switch stmt.(type) {
	case *sql.BeginStmt:
		s.txMu.Lock()
		s.transactions[conn] = s.executor.GetTransaction()
		s.txMu.Unlock()
		log.Debug("Transaction started", "remote_addr", remoteAddr)
	case *sql.CommitStmt, *sql.RollbackStmt:
		s.txMu.Lock()
		delete(s.transactions, conn)
		s.txMu.Unlock()
		log.Debug("Transaction ended", "remote_addr", remoteAddr)
	}

	return res
}
