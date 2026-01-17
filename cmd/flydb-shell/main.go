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
Package main is the entry point for the FlyDB command-line interface (CLI) client.

FlyDB CLI Overview:
===================

The flydb-shell (fsql) is an interactive REPL (Read-Eval-Print Loop) client that connects
to a FlyDB server over TCP. It provides a user-friendly interface for executing
SQL commands and managing the database.

Architecture:
=============

The CLI follows a simple synchronous request-response model:

 1. Read user input from stdin
 2. Parse and optionally transform the command
 3. Send the command to the server over TCP
 4. Read and display the server's response
 5. Repeat

Command Types:
==============

The CLI supports two types of commands:

 1. Local Commands (prefixed with \):
    - \q or \quit : Exit the CLI
    - \h or \help : Display help information

 2. Server Commands:
    - PING        : Test server connectivity
    - AUTH <u> <p>: Authenticate with username and password
    - SQL <stmt>  : Execute a SQL statement
    - WATCH <tbl> : Subscribe to INSERT events on a table

Smart Command Detection:
========================

The CLI automatically prefixes SQL keywords with "SQL " for convenience:
  - SELECT, INSERT, CREATE, GRANT, UPDATE, DELETE

This allows users to type natural SQL without the "SQL " prefix.

Usage Examples:
===============

	Connect to local server:
	  fsql

	Connect to remote server:
	  fsql -h 192.168.1.100 -p 8889

	Example session:
	  flydb> AUTH admin admin
	  AUTH OK (admin)
	  flydb> CREATE TABLE users (id INT, name TEXT)
	  CREATE TABLE OK
	  flydb> INSERT INTO users VALUES (1, 'Alice')
	  INSERT OK
	  flydb> SELECT name FROM users
	  Alice
*/
package main

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/chzyer/readline"
	"golang.org/x/term"

	"flydb/internal/banner"
	"flydb/internal/protocol"
	"flydb/pkg/cli"
)

// isTerminal returns true if stdin is a terminal.
func isTerminal() bool {
	return term.IsTerminal(int(os.Stdin.Fd()))
}

// Connection configuration constants
const (
	// ConnectionTimeout is the maximum time to wait for initial connection
	ConnectionTimeout = 5 * time.Second
	// PingTimeout is the maximum time to wait for a PING response
	PingTimeout = 3 * time.Second
	// MaxRetries is the maximum number of connection retry attempts
	MaxRetries = 3
	// InitialRetryDelay is the initial delay between retry attempts
	InitialRetryDelay = 500 * time.Millisecond
	// MaxRetryDelay is the maximum delay between retry attempts
	MaxRetryDelay = 5 * time.Second
)

// CLIConfig holds the configuration for the CLI client.
// This struct encapsulates all connection parameters, making it easy
// to pass configuration between functions and extend in the future.
type CLIConfig struct {
	Host          string           // Server hostname or IP address (supports comma-separated for HA)
	Hosts         []string         // Parsed list of hosts for HA cluster connections
	Port          string           // Server port number (binary protocol)
	Database      string           // Database to connect to (empty = use server default)
	Verbose       bool             // Enable verbose output
	Debug         bool             // Enable debug mode
	Format        cli.OutputFormat // Output format (table, json, plain)
	Execute       string           // Command to execute and exit
	TargetPrimary bool             // When true, always connect to primary/leader (for writes)
	UseTLS        bool             // When true, use TLS for connection
	TLSInsecure   bool             // When true, skip TLS certificate verification
}

// REPLState holds the runtime state for the REPL session.
// These are toggleable options that can be changed during the session.
type REPLState struct {
	Timing          bool   // Show query execution time
	ExpandedOutput  bool   // Use expanded (vertical) output format
	OutputFile      string // File to write output to (empty = stdout)
	outputWriter    *os.File
	SQLMode         bool   // When true, all input is treated as SQL
	CurrentDatabase string // Current database name (empty until authenticated)
	Authenticated   bool   // Whether the user has authenticated
	Username        string // The authenticated username
}

// Global REPL state
var replState = &REPLState{
	CurrentDatabase: "",    // Empty until authenticated
	Authenticated:   false, // Must authenticate before SQL commands
}

// sqlKeywords defines the SQL keywords that trigger automatic SQL query handling.
// When user input starts with any of these keywords, the CLI sends it as a query.
// This list is synchronized with the lexer's keyword recognition.
var sqlKeywords = []string{
	// DML statements
	"SELECT", "INSERT", "UPDATE", "DELETE",
	// DDL statements
	"CREATE", "DROP", "ALTER", "TRUNCATE",
	// Transaction control
	"BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE",
	// Prepared statements
	"PREPARE", "EXECUTE", "DEALLOCATE",
	// User management
	"GRANT", "REVOKE",
	// Stored procedures
	"CALL",
	// Inspection
	"INSPECT",
	// Set operations (can start a query)
	"WITH",
	// Database management
	"USE",
}

// allCompletions contains all completable commands and keywords for tab completion.
var allCompletions = []string{
	// Local commands
	"\\q", "\\quit", "\\h", "\\help", "\\c", "\\connect", "\\clear", "\\s", "\\status", "\\v", "\\version",
	"\\timing", "\\x", "\\!", "\\o", "\\conninfo", "\\dt", "\\du", "\\db", "\\l", "\\sql", "\\normal", "\\di",
	"\\cd", "\\current",
	// Server commands
	"PING", "AUTH", "SQL", "USE",
	// SQL statement keywords (can start a statement)
	"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TRUNCATE",
	"BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE",
	"PREPARE", "EXECUTE", "DEALLOCATE",
	"GRANT", "REVOKE", "CALL", "WITH",
	// SQL clause keywords
	"FROM", "WHERE", "AND", "OR", "NOT", "IN", "LIKE", "ORDER", "BY", "ASC", "DESC",
	"LIMIT", "OFFSET", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS", "ON", "AS",
	"GROUP", "HAVING", "DISTINCT", "UNION", "INTERSECT", "EXCEPT", "ALL",
	"VALUES", "INTO", "SET", "TABLE", "INDEX", "VIEW", "TRIGGER", "PROCEDURE",
	"USER", "DATABASE", "IF", "EXISTS",
	// Constraints
	"PRIMARY", "KEY", "FOREIGN", "REFERENCES", "UNIQUE", "NULL", "DEFAULT",
	"AUTO_INCREMENT", "CHECK", "CONSTRAINT",
	// Data types
	"INT", "INTEGER", "BIGINT", "SMALLINT", "SERIAL",
	"TEXT", "VARCHAR", "CHAR",
	"BOOLEAN", "BOOL",
	"FLOAT", "DOUBLE", "REAL", "DECIMAL", "NUMERIC",
	"TIMESTAMP", "DATETIME", "DATE", "TIME",
	"BLOB", "BYTEA", "UUID", "JSONB", "JSON",
	// Inspection
	"INSPECT", "INSPECT USERS", "INSPECT TABLES", "INSPECT TABLE",
	"INSPECT INDEXES", "INSPECT SERVER", "INSPECT STATUS",
	"INSPECT DATABASES", "INSPECT DATABASE",
	// Database management
	"USE", "CREATE DATABASE", "DROP DATABASE",
}

// getHistoryFilePath returns the path to the history file.
func getHistoryFilePath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".flydb_history")
}

// createCompleter creates a readline completer for tab completion.
func createCompleter() *readline.PrefixCompleter {
	// Build completion items
	items := make([]readline.PrefixCompleterInterface, 0, len(allCompletions))
	for _, cmd := range allCompletions {
		items = append(items, readline.PcItem(cmd))
	}
	return readline.NewPrefixCompleter(items...)
}

// createReadlineInstance creates a configured readline instance.
func createReadlineInstance() (*readline.Instance, error) {
	historyFile := getHistoryFilePath()

	config := &readline.Config{
		Prompt:          cli.Info("flydb") + cli.Dimmed(">") + " ",
		HistoryFile:     historyFile,
		AutoComplete:    createCompleter(),
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",

		HistorySearchFold:   true,
		FuncFilterInputRune: filterInput,
	}

	return readline.NewEx(config)
}

// filterInput filters input runes for readline.
func filterInput(r rune) (rune, bool) {
	switch r {
	case readline.CharCtrlZ:
		return r, false // Disable Ctrl+Z
	}
	return r, true
}

// isNonSQLCommand checks if the input is a command that doesn't require semicolon termination.
// This includes local commands (\q, \h, etc.), server commands (PING, AUTH), and USE.
func isNonSQLCommand(input string) bool {
	if input == "" {
		return true
	}

	// Local commands start with backslash
	if strings.HasPrefix(input, "\\") {
		return true
	}

	// Get the first word (command)
	upper := strings.ToUpper(input)
	parts := strings.Fields(upper)
	if len(parts) == 0 {
		return true
	}
	cmd := parts[0]

	// Server commands that don't require semicolons
	nonSQLCommands := map[string]bool{
		"PING":    true,
		"AUTH":    true,
		"USE":     true,
		"WATCH":   true,
		"UNWATCH": true,
	}

	return nonSQLCommands[cmd]
}

// requiresSemicolon checks if the input is a SQL statement that requires semicolon termination.
// Returns true for SQL statements, false for local commands and server commands.
func requiresSemicolon(input string) bool {
	if input == "" {
		return false
	}

	// Local commands don't require semicolons
	if strings.HasPrefix(input, "\\") {
		return false
	}

	// Get the first word (command)
	upper := strings.ToUpper(input)
	parts := strings.Fields(upper)
	if len(parts) == 0 {
		return false
	}
	cmd := parts[0]

	// Commands that don't require semicolons
	nonSQLCommands := map[string]bool{
		"PING":    true,
		"AUTH":    true,
		"USE":     true,
		"WATCH":   true,
		"UNWATCH": true,
	}

	if nonSQLCommands[cmd] {
		return false
	}

	// SQL keywords that start statements requiring semicolons
	sqlKeywords := map[string]bool{
		"SELECT":     true,
		"INSERT":     true,
		"UPDATE":     true,
		"DELETE":     true,
		"CREATE":     true,
		"DROP":       true,
		"ALTER":      true,
		"TRUNCATE":   true,
		"BEGIN":      true,
		"COMMIT":     true,
		"ROLLBACK":   true,
		"SAVEPOINT":  true,
		"RELEASE":    true,
		"PREPARE":    true,
		"EXECUTE":    true,
		"DEALLOCATE": true,
		"GRANT":      true,
		"REVOKE":     true,
		"CALL":       true,
		"WITH":       true,
		"INSPECT":    true,
		"SQL":        true,
		"EXPLAIN":    true,
		"ANALYZE":    true,
		"VACUUM":     true,
		"REINDEX":    true,
		"CLUSTER":    true,
		"COPY":       true,
		"SET":        true,
		"SHOW":       true,
		"RESET":      true,
		"LOCK":       true,
		"UNLOCK":     true,
	}

	return sqlKeywords[cmd]
}

// BinaryClient wraps the binary protocol connection.
type BinaryClient struct {
	conn       net.Conn
	reader     *bufio.Reader
	writer     *bufio.Writer
	authed     bool
	serverAddr string
}

// NewBinaryClient creates a new binary protocol client.
func NewBinaryClient(conn net.Conn, serverAddr string) *BinaryClient {
	return &BinaryClient{
		conn:       conn,
		reader:     bufio.NewReader(conn),
		writer:     bufio.NewWriter(conn),
		authed:     false,
		serverAddr: serverAddr,
	}
}

// HAClient wraps BinaryClient with HA/cluster failover support.
// Similar to PostgreSQL's libpq multi-host connection, it tries multiple
// hosts and automatically fails over when the current connection fails.
type HAClient struct {
	*BinaryClient
	hosts          []string // List of host:port addresses to try
	currentHostIdx int      // Index of currently connected host
	targetPrimary  bool     // If true, only connect to primary/leader
	authUsername   string   // Cached auth credentials for reconnection
	authPassword   string   // Cached auth credentials for reconnection
	lastAuth       bool     // Whether we successfully authenticated
	useTLS         bool     // Whether to use TLS for connections
	tlsInsecure    bool     // Whether to skip TLS certificate verification
}

// NewHAClient creates a new HA-aware client that can fail over between hosts.
// hosts is a list of "host:port" addresses to try in order.
func NewHAClient(hosts []string, targetPrimary bool, useTLS bool, tlsInsecure bool) *HAClient {
	return &HAClient{
		hosts:         hosts,
		targetPrimary: targetPrimary,
		useTLS:        useTLS,
		tlsInsecure:   tlsInsecure,
	}
}

// Connect establishes connection to one of the configured hosts.
// It tries each host in order until one succeeds.
func (h *HAClient) Connect() error {
	var lastErr error

	for i, host := range h.hosts {
		var conn net.Conn
		var err error

		if h.useTLS {
			// Create TLS configuration
			tlsConfig := &tls.Config{
				ServerName: extractHostname(host),
			}

			if h.tlsInsecure {
				tlsConfig.InsecureSkipVerify = true
			} else {
				// Load system CA certificates
				certPool, err := x509.SystemCertPool()
				if err != nil {
					certPool = x509.NewCertPool()
				}
				tlsConfig.RootCAs = certPool
			}

			// Dial with TLS
			dialer := &net.Dialer{Timeout: ConnectionTimeout}
			conn, err = tls.DialWithDialer(dialer, "tcp", host, tlsConfig)
		} else {
			// Plain TCP connection
			conn, err = net.DialTimeout("tcp", host, ConnectionTimeout)
		}

		if err != nil {
			lastErr = err
			continue
		}

		// Create the binary protocol client
		h.BinaryClient = NewBinaryClient(conn, host)
		h.currentHostIdx = i

		// Validate connection with a PING
		if err := h.Ping(); err != nil {
			h.BinaryClient.Close()
			lastErr = fmt.Errorf("server not responding to PING: %w", err)
			continue
		}

		return nil
	}

	return fmt.Errorf("failed to connect to any host: %w", lastErr)
}

// extractHostname extracts the hostname from a "host:port" address.
func extractHostname(addr string) string {
	if idx := strings.LastIndex(addr, ":"); idx != -1 {
		return addr[:idx]
	}
	return addr
}

// Reconnect attempts to reconnect to the cluster, trying alternate hosts.
// It starts from the next host in the list after the current one.
func (h *HAClient) Reconnect() error {
	if h.BinaryClient != nil {
		h.BinaryClient.Close()
	}

	// Rotate hosts: try from next host onwards, then wrap around
	hostsToTry := make([]string, 0, len(h.hosts))
	for i := 1; i <= len(h.hosts); i++ {
		idx := (h.currentHostIdx + i) % len(h.hosts)
		hostsToTry = append(hostsToTry, h.hosts[idx])
	}

	var lastErr error
	for _, host := range hostsToTry {
		var conn net.Conn
		var err error

		if h.useTLS {
			// Create TLS configuration
			tlsConfig := &tls.Config{
				ServerName: extractHostname(host),
			}

			if h.tlsInsecure {
				tlsConfig.InsecureSkipVerify = true
			} else {
				// Load system CA certificates
				certPool, err := x509.SystemCertPool()
				if err != nil {
					certPool = x509.NewCertPool()
				}
				tlsConfig.RootCAs = certPool
			}

			// Dial with TLS
			dialer := &net.Dialer{Timeout: ConnectionTimeout}
			conn, err = tls.DialWithDialer(dialer, "tcp", host, tlsConfig)
		} else {
			// Plain TCP connection
			conn, err = net.DialTimeout("tcp", host, ConnectionTimeout)
		}

		if err != nil {
			lastErr = err
			continue
		}

		// Create the binary protocol client
		h.BinaryClient = NewBinaryClient(conn, host)
		// Update current host index
		for i, hst := range h.hosts {
			if hst == host {
				h.currentHostIdx = i
				break
			}
		}

		// Validate connection with a PING
		if err := h.Ping(); err != nil {
			h.BinaryClient.Close()
			lastErr = fmt.Errorf("server not responding to PING: %w", err)
			continue
		}

		// Re-authenticate if we had previously authenticated
		if h.lastAuth && h.authUsername != "" {
			_, err := h.BinaryClient.Auth(h.authUsername, h.authPassword)
			if err != nil {
				// Authentication failed on new host - continue to next
				h.BinaryClient.Close()
				lastErr = fmt.Errorf("re-authentication failed: %w", err)
				continue
			}
		}

		return nil
	}

	return fmt.Errorf("failed to reconnect to any host: %w", lastErr)
}

// Auth authenticates with the server and caches credentials for reconnection.
func (h *HAClient) Auth(username, password string) (string, error) {
	result, err := h.BinaryClient.Auth(username, password)
	if err != nil {
		return result, err
	}
	// Cache credentials for reconnection
	h.authUsername = username
	h.authPassword = password
	h.lastAuth = true
	return result, nil
}

// QueryWithFailover executes a query with automatic failover on connection errors.
func (h *HAClient) QueryWithFailover(query string) (string, error) {
	result, err := h.BinaryClient.Query(query)
	if err == nil {
		return result, nil
	}

	// Check if this is a connection error that warrants failover
	if !isConnectionError(err) {
		return result, err
	}

	// Only attempt failover if we have multiple hosts
	if len(h.hosts) <= 1 {
		return result, err
	}

	// Attempt failover
	if reconnErr := h.Reconnect(); reconnErr != nil {
		return "", fmt.Errorf("query failed and reconnection failed: %w (original error: %v)", reconnErr, err)
	}

	// Retry the query on the new connection
	return h.BinaryClient.Query(query)
}

// CurrentHost returns the currently connected host address.
func (h *HAClient) CurrentHost() string {
	if h.BinaryClient != nil {
		return h.serverAddr
	}
	return ""
}

// AllHosts returns the list of all configured hosts.
func (h *HAClient) AllHosts() []string {
	return h.hosts
}

// Ping sends a ping message and waits for pong with timeout.
func (c *BinaryClient) Ping() error {
	// Set read deadline for ping response
	c.conn.SetReadDeadline(time.Now().Add(PingTimeout))
	defer c.conn.SetReadDeadline(time.Time{}) // Clear deadline

	if err := protocol.WriteMessage(c.writer, protocol.MsgPing, nil); err != nil {
		return fmt.Errorf("failed to send PING: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush PING: %w", err)
	}

	msg, err := protocol.ReadMessage(c.reader)
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return fmt.Errorf("server did not respond within %v", PingTimeout)
		}
		return fmt.Errorf("failed to read PONG: %w", err)
	}

	if msg.Header.Type != protocol.MsgPong {
		return fmt.Errorf("expected PONG, got message type %d", msg.Header.Type)
	}
	return nil
}

// IsConnected checks if the connection is still alive.
func (c *BinaryClient) IsConnected() bool {
	if c.conn == nil {
		return false
	}
	// Try a quick ping to verify connection
	c.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	defer c.conn.SetReadDeadline(time.Time{})

	if err := protocol.WriteMessage(c.writer, protocol.MsgPing, nil); err != nil {
		return false
	}
	if err := c.writer.Flush(); err != nil {
		return false
	}

	msg, err := protocol.ReadMessage(c.reader)
	if err != nil {
		return false
	}
	return msg.Header.Type == protocol.MsgPong
}

// Auth authenticates with the server.
func (c *BinaryClient) Auth(username, password string) (string, error) {
	authMsg := &protocol.AuthMessage{
		Username: username,
		Password: password,
	}
	data, err := authMsg.Encode()
	if err != nil {
		return "", err
	}

	if err := protocol.WriteMessage(c.writer, protocol.MsgAuth, data); err != nil {
		return "", err
	}
	if err := c.writer.Flush(); err != nil {
		return "", err
	}

	msg, err := protocol.ReadMessage(c.reader)
	if err != nil {
		return "", err
	}

	if msg.Header.Type == protocol.MsgError {
		errMsg, _ := protocol.DecodeErrorMessage(msg.Payload)
		return "", fmt.Errorf("auth failed: %s", errMsg.Message)
	}

	if msg.Header.Type != protocol.MsgAuthResult {
		return "", fmt.Errorf("unexpected response type: %d", msg.Header.Type)
	}

	result, err := protocol.DecodeAuthResultMessage(msg.Payload)
	if err != nil {
		return "", err
	}

	if result.Success {
		c.authed = true
		return "AUTH OK", nil
	}
	return "", fmt.Errorf("authentication failed: %s", result.Message)
}

// Query executes a SQL query.
func (c *BinaryClient) Query(query string) (string, error) {
	queryMsg := &protocol.QueryMessage{
		Query: query,
	}
	data, err := queryMsg.Encode()
	if err != nil {
		return "", err
	}

	if err := protocol.WriteMessage(c.writer, protocol.MsgQuery, data); err != nil {
		return "", err
	}
	if err := c.writer.Flush(); err != nil {
		return "", err
	}

	msg, err := protocol.ReadMessage(c.reader)
	if err != nil {
		return "", err
	}

	if msg.Header.Type == protocol.MsgError {
		errMsg, _ := protocol.DecodeErrorMessage(msg.Payload)
		return "", fmt.Errorf("%s", errMsg.Message)
	}

	if msg.Header.Type != protocol.MsgQueryResult {
		return "", fmt.Errorf("unexpected response type: %d", msg.Header.Type)
	}

	result, err := protocol.DecodeQueryResultMessage(msg.Payload)
	if err != nil {
		return "", err
	}

	if !result.Success {
		return "", fmt.Errorf("%s", result.Message)
	}

	return result.Message, nil
}

// Close closes the connection.
func (c *BinaryClient) Close() error {
	return c.conn.Close()
}

// CLIFlags holds all command-line flags for the CLI.
type CLIFlags struct {
	Host          string
	Port          string
	Database      string
	Version       bool
	Help          bool
	Verbose       bool
	Debug         bool
	Format        string
	NoColor       bool
	Execute       string
	ConfigFile    string
	TargetPrimary bool // When true, prefer connecting to primary/leader
	NoTLS         bool // When true, disable TLS and use plain TCP
	TLSInsecure   bool // When true, skip TLS certificate verification
}

// parseFlags parses command-line flags and returns the configuration.
func parseFlags() CLIFlags {
	flags := CLIFlags{}

	flag.StringVar(&flags.Host, "host", "localhost", "Server hostname(s) - comma-separated for HA cluster")
	flag.StringVar(&flags.Host, "H", "localhost", "Server hostname(s) (shorthand)")
	flag.StringVar(&flags.Port, "port", "8889", "Server port number (binary protocol)")
	flag.StringVar(&flags.Port, "p", "8889", "Server port number (shorthand)")
	flag.StringVar(&flags.Database, "database", "", "Database to connect to (default: 'default')")
	flag.StringVar(&flags.Database, "d", "", "Database to connect to (shorthand)")
	flag.BoolVar(&flags.Version, "version", false, "Print version information and exit")
	flag.BoolVar(&flags.Version, "v", false, "Print version information (shorthand)")
	flag.BoolVar(&flags.Help, "help", false, "Show help information")
	flag.BoolVar(&flags.Verbose, "verbose", false, "Enable verbose output")
	flag.BoolVar(&flags.Debug, "debug", false, "Enable debug mode with detailed logging")
	flag.StringVar(&flags.Format, "format", "table", "Output format: table, json, plain")
	flag.StringVar(&flags.Format, "f", "table", "Output format (shorthand)")
	flag.BoolVar(&flags.NoColor, "no-color", false, "Disable colored output")
	flag.StringVar(&flags.Execute, "execute", "", "Execute a command and exit")
	flag.StringVar(&flags.Execute, "e", "", "Execute a command and exit (shorthand)")
	flag.StringVar(&flags.ConfigFile, "config", "", "Path to configuration file")
	flag.StringVar(&flags.ConfigFile, "c", "", "Path to configuration file (shorthand)")
	flag.BoolVar(&flags.TargetPrimary, "target-primary", false, "Prefer connecting to primary/leader in cluster")
	flag.BoolVar(&flags.NoTLS, "no-tls", false, "Disable TLS and use plain TCP connection")
	flag.BoolVar(&flags.TLSInsecure, "tls-insecure", false, "Skip TLS certificate verification (insecure)")

	// Custom usage function
	flag.Usage = printUsage

	flag.Parse()
	return flags
}

// printUsage prints comprehensive help information.
func printUsage() {
	banner.Print()

	fmt.Println("    fsql [flags]")
	fmt.Println("    fsql -e \"<command>\"")
	fmt.Println()

	fmt.Println("  " + cli.Highlight("Flags"))
	fmt.Println()
	fmt.Printf("    %s, %s <host>      Server hostname(s), comma-separated for HA cluster\n", cli.Info("-H"), cli.Info("--host"))
	fmt.Printf("    %s, %s <port>      Server port number (default: 8889)\n", cli.Info("-p"), cli.Info("--port"))
	fmt.Printf("    %s, %s <name>  Database to connect to (default: 'default')\n", cli.Info("-d"), cli.Info("--database"))
	fmt.Printf("    %s, %s          Print version information and exit\n", cli.Info("-v"), cli.Info("--version"))
	fmt.Printf("        %s             Show this help message\n", cli.Info("--help"))
	fmt.Printf("        %s          Enable verbose output with timing\n", cli.Info("--verbose"))
	fmt.Printf("        %s            Enable debug mode with detailed logging\n", cli.Info("--debug"))
	fmt.Printf("    %s, %s <format>  Output format: table, json, plain\n", cli.Info("-f"), cli.Info("--format"))
	fmt.Printf("        %s         Disable colored output\n", cli.Info("--no-color"))
	fmt.Printf("    %s, %s <cmd>    Execute a command and exit\n", cli.Info("-e"), cli.Info("--execute"))
	fmt.Printf("    %s, %s <file>    Path to configuration file\n", cli.Info("-c"), cli.Info("--config"))
	fmt.Printf("        %s    Prefer connecting to primary in cluster\n", cli.Info("--target-primary"))
	fmt.Printf("        %s             Disable TLS and use plain TCP connection\n", cli.Info("--no-tls"))
	fmt.Printf("        %s        Skip TLS certificate verification (insecure)\n", cli.Info("--tls-insecure"))
	fmt.Println()

	fmt.Println("  " + cli.Highlight("Examples"))
	fmt.Println()
	fmt.Println("    " + cli.Dimmed("# Connect to local server (uses 'default' database)"))
	fmt.Println("    " + cli.Success("fsql"))
	fmt.Println()
	fmt.Println("    " + cli.Dimmed("# Connect to a specific database"))
	fmt.Println("    " + cli.Success("fsql -d mydb"))
	fmt.Println()
	fmt.Println("    " + cli.Dimmed("# Connect to remote server"))
	fmt.Println("    " + cli.Success("fsql -H 192.168.1.100 -p 8889"))
	fmt.Println()
	fmt.Println("    " + cli.Dimmed("# Connect to HA cluster (multiple hosts)"))
	fmt.Println("    " + cli.Success("fsql -H node1,node2,node3 -p 8889"))
	fmt.Println()
	fmt.Println("    " + cli.Dimmed("# Execute a query and exit"))
	fmt.Println("    " + cli.Success("fsql -e \"SELECT * FROM users\""))
	fmt.Println()
	fmt.Println("    " + cli.Dimmed("# Query a specific database"))
	fmt.Println("    " + cli.Success("fsql -d mydb -e \"SELECT * FROM users\""))
	fmt.Println()
	fmt.Println("    " + cli.Dimmed("# Connect with TLS (enabled by default)"))
	fmt.Println("    " + cli.Success("fsql -H example.com"))
	fmt.Println()
	fmt.Println("    " + cli.Dimmed("# Connect skipping TLS verification (for self-signed certs)"))
	fmt.Println("    " + cli.Success("fsql -H example.com --tls-insecure"))
	fmt.Println()

	fmt.Println("  " + cli.Highlight("Interactive Commands"))
	fmt.Println()
	fmt.Printf("    %s, %s         Exit the CLI\n", cli.Info("\\q"), cli.Info("\\quit"))
	fmt.Printf("    %s, %s         Display help information\n", cli.Info("\\h"), cli.Info("\\help"))
	fmt.Printf("    %s <db>          Switch to database\n", cli.Info("\\c"))
	fmt.Printf("    %s            Clear the screen\n", cli.Info("\\clear"))
	fmt.Printf("    %s, %s       Show connection status\n", cli.Info("\\s"), cli.Info("\\status"))
	fmt.Println()

	fmt.Println("  " + cli.Highlight("Environment Variables"))
	fmt.Println()
	fmt.Printf("    %s        Default server host\n", cli.Info("FLYDB_HOST"))
	fmt.Printf("    %s        Default server port\n", cli.Info("FLYDB_PORT"))
	fmt.Printf("    %s    Default database name\n", cli.Info("FLYDB_DATABASE"))
	fmt.Printf("    %s          Disable colored output\n", cli.Info("NO_COLOR"))
	fmt.Println()

	fmt.Println("  " + cli.Dimmed("For more information, visit: https://github.com/firefly-oss/flydb"))
	fmt.Println()
}

// main is the entry point for the flydb-shell (fsql) application.
// It parses command-line flags and initiates the REPL loop.
func main() {
	flags := parseFlags()

	// Handle --no-color flag (also check NO_COLOR env var and terminal detection)
	if flags.NoColor || os.Getenv("NO_COLOR") != "" || !isTerminal() {
		cli.SetColorsEnabled(false)
	}

	// Handle --version flag
	if flags.Version {
		fmt.Printf("fsql version %s\n", banner.Version)
		fmt.Printf("%s\n", banner.Copyright)
		os.Exit(0)
	}

	// Handle --help flag
	if flags.Help {
		printUsage()
		os.Exit(0)
	}

	// Load configuration from file if specified
	if flags.ConfigFile != "" {
		if err := loadConfigFile(flags.ConfigFile, &flags); err != nil {
			cli.PrintError("Failed to load config file: %v", err)
			os.Exit(1)
		}
	}

	// Check environment variables for defaults (override config file)
	if envHost := os.Getenv("FLYDB_HOST"); envHost != "" && flags.Host == "localhost" {
		flags.Host = envHost
	}
	if envPort := os.Getenv("FLYDB_PORT"); envPort != "" && flags.Port == "8889" {
		flags.Port = envPort
	}
	if envDB := os.Getenv("FLYDB_DATABASE"); envDB != "" && flags.Database == "" {
		flags.Database = envDB
	}

	// Parse hosts list for HA cluster support (comma-separated)
	hosts := parseHosts(flags.Host, flags.Port)

	// Determine TLS settings (enabled by default, unless --no-tls is specified)
	useTLS := !flags.NoTLS

	// Check environment variable for TLS setting
	if envTLS := os.Getenv("FLYDB_TLS_ENABLED"); envTLS != "" {
		useTLS = strings.ToLower(envTLS) == "true" || envTLS == "1"
	}

	// Override with flag if explicitly set
	if flags.NoTLS {
		useTLS = false
	}

	// Create configuration struct and start the REPL.
	config := CLIConfig{
		Host:          flags.Host,
		Hosts:         hosts,
		Port:          flags.Port,
		Database:      flags.Database,
		Verbose:       flags.Verbose,
		Debug:         flags.Debug,
		Format:        cli.ParseOutputFormat(flags.Format),
		Execute:       flags.Execute,
		TargetPrimary: flags.TargetPrimary,
		UseTLS:        useTLS,
		TLSInsecure:   flags.TLSInsecure,
	}

	startREPL(config)
}

// parseHosts parses the host string (possibly comma-separated) and returns a list of host:port addresses.
// Supports formats:
//   - "host1,host2,host3" - comma-separated hosts, all use the same port
//   - "host1:8889,host2:8890" - comma-separated with individual ports
func parseHosts(hostStr, defaultPort string) []string {
	hostStr = strings.TrimSpace(hostStr)
	if hostStr == "" {
		return []string{"localhost:" + defaultPort}
	}

	parts := strings.Split(hostStr, ",")
	hosts := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Check if host already has a port
		if strings.Contains(part, ":") {
			hosts = append(hosts, part)
		} else {
			hosts = append(hosts, part+":"+defaultPort)
		}
	}

	if len(hosts) == 0 {
		return []string{"localhost:" + defaultPort}
	}

	return hosts
}

// loadConfigFile loads CLI configuration from a file.
func loadConfigFile(path string, flags *CLIFlags) error {
	path = os.ExpandEnv(path)

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("cannot read config file: %w", err)
	}

	// Parse simple key=value format
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		// Remove quotes if present
		value = strings.Trim(value, "\"'")

		switch key {
		case "host", "hosts":
			flags.Host = value
		case "port":
			flags.Port = value
		case "database":
			flags.Database = value
		case "format":
			flags.Format = value
		case "verbose":
			flags.Verbose = value == "true"
		case "debug":
			flags.Debug = value == "true"
		case "no_color":
			flags.NoColor = value == "true"
		case "target_primary":
			flags.TargetPrimary = value == "true"
		}
	}

	return nil
}

// connectWithRetry attempts to connect to the server with exponential backoff.
func connectWithRetry(addr string, useTLS bool, tlsInsecure bool) (*BinaryClient, error) {
	var lastErr error
	delay := InitialRetryDelay

	for attempt := 1; attempt <= MaxRetries; attempt++ {
		if attempt > 1 {
			fmt.Printf("Retrying connection (attempt %d/%d)...\n", attempt, MaxRetries)
		}

		// Attempt connection with timeout
		var conn net.Conn
		var err error

		if useTLS {
			// Create TLS configuration
			tlsConfig := &tls.Config{
				ServerName: extractHostname(addr),
			}

			if tlsInsecure {
				tlsConfig.InsecureSkipVerify = true
			} else {
				// Load system CA certificates
				certPool, err := x509.SystemCertPool()
				if err != nil {
					certPool = x509.NewCertPool()
				}
				tlsConfig.RootCAs = certPool
			}

			// Dial with TLS
			dialer := &net.Dialer{Timeout: ConnectionTimeout}
			conn, err = tls.DialWithDialer(dialer, "tcp", addr, tlsConfig)
		} else {
			// Plain TCP connection
			conn, err = net.DialTimeout("tcp", addr, ConnectionTimeout)
		}

		if err != nil {
			lastErr = err
			if attempt < MaxRetries {
				fmt.Printf("Connection failed: %v. Retrying in %v...\n", err, delay)
				time.Sleep(delay)
				// Exponential backoff with cap
				delay = delay * 2
				if delay > MaxRetryDelay {
					delay = MaxRetryDelay
				}
			}
			continue
		}

		// Create the binary protocol client
		client := NewBinaryClient(conn, addr)

		// Validate connection with a PING
		if err := client.Ping(); err != nil {
			client.Close()
			lastErr = fmt.Errorf("server not responding to PING: %w", err)
			if attempt < MaxRetries {
				fmt.Printf("Server validation failed: %v. Retrying in %v...\n", lastErr, delay)
				time.Sleep(delay)
				delay = delay * 2
				if delay > MaxRetryDelay {
					delay = MaxRetryDelay
				}
			}
			continue
		}

		// Connection successful
		return client, nil
	}

	return nil, fmt.Errorf("failed to connect after %d attempts: %w", MaxRetries, lastErr)
}

// startREPL initiates the Read-Eval-Print Loop for interactive database access.
// It establishes a binary protocol connection to the server and processes user commands
// until the user exits or the connection is lost.
func startREPL(config CLIConfig) {
	// Display the startup banner for visual branding (unless executing a command).
	if config.Execute == "" {
		banner.Print()
	}

	// Use HA cluster connection if multiple hosts are configured
	isHAMode := len(config.Hosts) > 1
	var client *BinaryClient
	var haClient *HAClient
	var addr string

	if isHAMode {
		// HA mode: use HAClient for multi-host failover
		var spinner *cli.Spinner
		if config.Execute == "" {
			hostList := strings.Join(config.Hosts, ", ")
			spinner = cli.NewSpinner(fmt.Sprintf("Connecting to cluster (%s)...", hostList))
			spinner.Start()
		} else if config.Verbose {
			fmt.Printf("Connecting to cluster (%s)...\n", strings.Join(config.Hosts, ", "))
		}

		haClient = NewHAClient(config.Hosts, config.TargetPrimary, config.UseTLS, config.TLSInsecure)
		if err := haClient.Connect(); err != nil {
			if spinner != nil {
				spinner.StopWithError("Connection failed")
			}
			cli.PrintError("Failed to connect to any cluster node: %v", err)
			os.Exit(1)
		}

		addr = haClient.CurrentHost()
		client = haClient.BinaryClient

		protocol := "tcp"
		if config.UseTLS {
			protocol = "tls"
		}
		if spinner != nil {
			spinner.StopWithSuccess(fmt.Sprintf("Connected to %s://%s (cluster mode: %d nodes)", protocol, addr, len(config.Hosts)))
		}
	} else {
		// Single host mode: use direct connection
		addr = config.Hosts[0]

		// Show connection spinner
		var spinner *cli.Spinner
		if config.Execute == "" {
			spinner = cli.NewSpinner(fmt.Sprintf("Connecting to %s...", addr))
			spinner.Start()
		} else if config.Verbose {
			fmt.Printf("Connecting to %s...\n", addr)
		}

		var err error
		client, err = connectWithRetry(addr, config.UseTLS, config.TLSInsecure)
		if err != nil {
			if spinner != nil {
				spinner.StopWithError("Connection failed")
			}
			cli.ErrConnectionFailed(config.Host, config.Port, err).Exit()
		}

		protocol := "tcp"
		if config.UseTLS {
			protocol = "tls"
		}

		if spinner != nil {
			spinner.StopWithSuccess(fmt.Sprintf("Connected to %s://%s", protocol, addr))
		}
	}
	defer client.Close()

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Handle signals in a goroutine
	go func() {
		<-sigChan
		fmt.Println()
		cli.PrintInfo("Interrupted. Closing connection...")
		client.Close()
		os.Exit(0)
	}()

	// Store the requested database for later (after authentication)
	// Database selection will happen after successful AUTH
	requestedDatabase := config.Database

	// If executing a single command, do it and exit
	// Note: The command must include AUTH if authentication is required
	if config.Execute != "" {
		response, err := processCommand(client, config.Execute, config)
		if err != nil {
			cli.PrintError("%v", err)
			os.Exit(1)
		}
		printResponseWithFormat(response, config.Format)
		os.Exit(0)
	}

	// Store requested database in config for use after authentication
	_ = requestedDatabase // Will be used after AUTH succeeds

	fmt.Println()
	// If not running in a terminal (piped input), use simple REPL
	if !isTerminal() {
		runSimpleREPL(config, client, addr)
		return
	}

	fmt.Println(cli.Success("✓ Connected to FlyDB server"))
	fmt.Println()
	cli.PrintWarning("Authentication required before executing SQL commands.")
	fmt.Printf("  Use %s to authenticate\n", cli.Highlight("AUTH <username> <password>"))
	fmt.Printf("  Or use %s for interactive login\n", cli.Highlight("AUTH"))
	fmt.Println()
	fmt.Printf("  Type %s to quit, %s for help, %s for completion\n",
		cli.Highlight("\\q"),
		cli.Highlight("\\h"),
		cli.Highlight("Tab"))
	fmt.Println()

	// Create readline instance for advanced line editing
	rl, err := createReadlineInstance()
	if err != nil {
		// Fall back to simple scanner if readline fails
		cli.PrintWarning("Advanced line editing unavailable: %v", err)
		runSimpleREPL(config, client, addr)
		return
	}
	defer rl.Close()

	// Multi-line input buffer
	var multiLineBuffer strings.Builder
	inMultiLine := false

	// Main REPL loop: continuously read, evaluate, and print.
	for {
		// Set prompt based on multi-line state, SQL mode, authentication, and current database
		if inMultiLine {
			rl.SetPrompt(cli.Dimmed("        -> "))
		} else {
			// Build the prompt based on authentication state
			var prompt string
			if !replState.Authenticated {
				// Not authenticated - show login prompt
				prompt = cli.Warning("flydb") + cli.Dimmed("[not authenticated]>") + " "
			} else {
				// Authenticated - show database context
				dbName := replState.CurrentDatabase
				if dbName == "" {
					dbName = "default"
				}
				prompt = cli.Info("flydb") + cli.Dimmed(":") + cli.Success(dbName)
				if replState.SQLMode {
					prompt += cli.Dimmed("[") + cli.Warning("sql") + cli.Dimmed("]")
				}
				prompt += cli.Dimmed(">") + " "
			}
			rl.SetPrompt(prompt)
		}

		// Read user input with readline (supports history, completion, editing)
		line, err := rl.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				// Ctrl+C pressed
				if inMultiLine {
					// Cancel multi-line input
					multiLineBuffer.Reset()
					inMultiLine = false
					fmt.Println()
					continue
				}
				// Ask for confirmation to exit
				fmt.Println()
				fmt.Println(cli.Dimmed("(Use \\q to quit or Ctrl+D to exit)"))
				continue
			}
			if err == io.EOF {
				// Ctrl+D pressed - exit gracefully
				fmt.Println()
				cli.PrintInfo("Goodbye!")
				break
			}
			// Other error - exit
			fmt.Println()
			cli.PrintInfo("Goodbye!")
			break
		}

		// Trim whitespace from input for clean processing.
		input := strings.TrimSpace(line)

		// Handle explicit multi-line continuation (lines ending with \)
		if strings.HasSuffix(input, "\\") {
			multiLineBuffer.WriteString(strings.TrimSuffix(input, "\\"))
			multiLineBuffer.WriteString(" ")
			inMultiLine = true
			continue
		}

		// If in multi-line mode, append this line
		if inMultiLine {
			multiLineBuffer.WriteString(input)
			// Check if the statement is complete (ends with semicolon)
			accumulated := strings.TrimSpace(multiLineBuffer.String())
			if !strings.HasSuffix(accumulated, ";") && !isNonSQLCommand(accumulated) {
				// Still incomplete, continue reading
				multiLineBuffer.WriteString(" ")
				continue
			}
			// Statement is complete
			input = accumulated
			multiLineBuffer.Reset()
			inMultiLine = false
		} else {
			// Check if this is a SQL statement that needs semicolon termination
			if input != "" && requiresSemicolon(input) && !strings.HasSuffix(input, ";") {
				// Start multi-line mode for incomplete SQL statements
				multiLineBuffer.WriteString(input)
				multiLineBuffer.WriteString(" ")
				inMultiLine = true
				continue
			}
		}

		// Strip trailing semicolon for command processing (server doesn't need it)
		input = strings.TrimSuffix(input, ";")
		input = strings.TrimSpace(input)

		// Skip empty lines to avoid sending unnecessary requests.
		if input == "" {
			continue
		}

		// Handle local commands (prefixed with backslash).
		// These commands are processed locally without server communication.
		if strings.HasPrefix(input, "\\") {
			handleLocalCommand(input, config, client)
			continue
		}

		// Handle AUTH command with secure password input
		if isAuthCommand(input) {
			// Remove AUTH command from history to prevent password exposure
			// The history is saved automatically, so we need to remove the last entry
			// if it contains a password (AUTH user pass format)
			parts := strings.Fields(input)
			if len(parts) >= 3 {
				// AUTH with password on command line - remove from history
				// We can't easily remove from readline history, so we'll save a sanitized version
				sanitized := fmt.Sprintf("AUTH %s ********", parts[1])
				rl.SaveHistory(sanitized)
			}

			response, err := handleAuthCommand(rl, client, haClient, input, config)
			if err != nil {
				printErrorMessage(err.Error())
				continue
			}
			printResponseWithFormat(response, config.Format)
			continue
		}

		// Process the command using the binary protocol
		response, err := processCommand(client, input, config)
		if err != nil {
			// Check if this is a connection error
			if isConnectionError(err) {
				cli.PrintWarning("Connection lost. Attempting to reconnect...")
				client.Close()

				// Try to reconnect with spinner
				spinner := cli.NewSpinner("Reconnecting...")
				spinner.Start()

				if isHAMode && haClient != nil {
					// HA mode: use haClient.Reconnect() to try other nodes
					reconnErr := haClient.Reconnect()
					if reconnErr != nil {
						spinner.StopWithError("Failed to reconnect to any cluster node")
						cli.PrintError("Exiting...")
						os.Exit(1)
					}
					// Update client reference to new connection
					client = haClient.BinaryClient
					addr = haClient.CurrentHost()
					spinner.StopWithSuccess(fmt.Sprintf("Reconnected to %s", addr))
				} else {
					// Single host mode: retry the same address
					newClient, reconnErr := connectWithRetry(addr, config.UseTLS, config.TLSInsecure)
					if reconnErr != nil {
						spinner.StopWithError("Failed to reconnect")
						cli.PrintError("Exiting...")
						os.Exit(1)
					}
					client = newClient
					spinner.StopWithSuccess("Reconnected successfully!")
				}

				// Retry the command
				response, err = processCommand(client, input, config)
				if err != nil {
					printErrorMessage(err.Error())
					continue
				}
			} else {
				printErrorMessage(err.Error())
				continue
			}
		}

		printResponseWithFormat(response, config.Format)
	}
}

// isAuthCommand checks if the input is an AUTH command.
func isAuthCommand(input string) bool {
	upper := strings.ToUpper(strings.TrimSpace(input))
	return strings.HasPrefix(upper, "AUTH")
}

// handleAuthCommand handles the AUTH command with secure password input.
// It supports three modes:
// 1. "AUTH" - prompts for both username and password
// 2. "AUTH username" - prompts for password only
// 3. "AUTH username password" - warns about security and proceeds
// When haClient is non-nil (HA mode), credentials are cached for reconnection.
func handleAuthCommand(rl *readline.Instance, client *BinaryClient, haClient *HAClient, input string, config CLIConfig) (string, error) {
	parts := strings.Fields(input)

	var username, password string

	switch len(parts) {
	case 1:
		// Just "AUTH" - prompt for both username and password
		fmt.Print(cli.Dimmed("Username: "))
		usernameBytes, err := rl.Readline()
		if err != nil {
			return "", fmt.Errorf("cancelled")
		}
		username = strings.TrimSpace(usernameBytes)
		if username == "" {
			return "", cli.ErrMissingArgument("username", "AUTH <username> <password>")
		}

		// Read password with masking
		password, err = readPasswordMasked(rl, "Password: ")
		if err != nil {
			return "", fmt.Errorf("cancelled")
		}

	case 2:
		// "AUTH username" - prompt for password only
		username = parts[1]

		// Read password with masking
		var err error
		password, err = readPasswordMasked(rl, "Password: ")
		if err != nil {
			return "", fmt.Errorf("cancelled")
		}

	default:
		// "AUTH username password" - warn about security but proceed
		username = parts[1]
		password = strings.Join(parts[2:], " ")

		// Show security warning
		cli.PrintWarning("Security: Passwords typed on command line may be visible in history.")
		fmt.Println(cli.Dimmed("  Tip: Use 'AUTH' or 'AUTH <username>' for secure password entry."))
		fmt.Println()
	}

	if password == "" {
		return "", cli.ErrMissingArgument("password", "AUTH <username> <password>")
	}

	// Perform authentication
	var result string
	var err error

	if haClient != nil {
		// HA mode: use haClient.Auth() to cache credentials for reconnection
		result, err = haClient.Auth(username, password)
	} else {
		// Single host mode: use direct client
		result, err = client.Auth(username, password)
	}

	if err != nil {
		return "", err
	}

	// If authentication succeeded, update state
	if strings.Contains(result, "OK") {
		replState.Authenticated = true
		replState.Username = username
		replState.CurrentDatabase = "default" // Set default database after auth
	}

	return result, nil
}

// readPasswordMasked reads a password with asterisk masking.
func readPasswordMasked(rl *readline.Instance, prompt string) (string, error) {
	// Use readline's built-in password reading with custom mask rune
	rl.SetMaskRune('*')
	password, err := rl.ReadPassword(cli.Dimmed(prompt))
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(password)), nil
}

// runSimpleREPL runs a simple REPL without readline (fallback/piped mode).
func runSimpleREPL(config CLIConfig, client *BinaryClient, addr string) {
	scanner := bufio.NewScanner(os.Stdin)
	interactive := isTerminal()

	// Multi-line input buffer for simple REPL
	var multiLineBuffer strings.Builder
	inMultiLine := false

	for {
		// Only show prompt in interactive mode
		if interactive {
			// Build the prompt based on authentication state and multi-line state
			var prompt string
			if inMultiLine {
				prompt = cli.Dimmed("        -> ")
			} else if !replState.Authenticated {
				// Not authenticated - show login prompt
				prompt = cli.Warning("flydb") + cli.Dimmed("[not authenticated]>") + " "
			} else {
				// Authenticated - show database context
				dbName := replState.CurrentDatabase
				if dbName == "" {
					dbName = "default"
				}
				prompt = cli.Info("flydb") + cli.Dimmed(":") + cli.Success(dbName)
				if replState.SQLMode {
					prompt += cli.Dimmed("[") + cli.Warning("sql") + cli.Dimmed("]")
				}
				prompt += cli.Dimmed(">") + " "
			}
			fmt.Print(prompt)
		}
		if !scanner.Scan() {
			if interactive {
				fmt.Println()
				cli.PrintInfo("Goodbye!")
			}
			break
		}

		input := strings.TrimSpace(scanner.Text())

		// Handle explicit multi-line continuation (lines ending with \)
		if strings.HasSuffix(input, "\\") {
			multiLineBuffer.WriteString(strings.TrimSuffix(input, "\\"))
			multiLineBuffer.WriteString(" ")
			inMultiLine = true
			continue
		}

		// If in multi-line mode, append this line
		if inMultiLine {
			multiLineBuffer.WriteString(input)
			// Check if the statement is complete (ends with semicolon)
			accumulated := strings.TrimSpace(multiLineBuffer.String())
			if !strings.HasSuffix(accumulated, ";") && !isNonSQLCommand(accumulated) {
				// Still incomplete, continue reading
				multiLineBuffer.WriteString(" ")
				continue
			}
			// Statement is complete
			input = accumulated
			multiLineBuffer.Reset()
			inMultiLine = false
		} else {
			// Check if this is a SQL statement that needs semicolon termination
			if input != "" && requiresSemicolon(input) && !strings.HasSuffix(input, ";") {
				// Start multi-line mode for incomplete SQL statements
				multiLineBuffer.WriteString(input)
				multiLineBuffer.WriteString(" ")
				inMultiLine = true
				continue
			}
		}

		// Strip trailing semicolon for command processing (server doesn't need it)
		input = strings.TrimSuffix(input, ";")
		input = strings.TrimSpace(input)

		if input == "" {
			continue
		}

		// Skip local commands in piped mode (except quit)
		if strings.HasPrefix(input, "\\") {
			if !interactive && input != "\\q" && input != "\\quit" {
				continue
			}
			handleLocalCommand(input, config, client)
			continue
		}

		// Handle AUTH command with secure password input in interactive mode
		if interactive && isAuthCommand(input) {
			response, err := handleAuthCommandSimple(client, input, config)
			if err != nil {
				printErrorMessage(err.Error())
				continue
			}
			printResponseWithFormat(response, config.Format)
			continue
		}

		response, err := processCommand(client, input, config)
		if err != nil {
			if isConnectionError(err) && interactive {
				cli.PrintWarning("Connection lost. Attempting to reconnect...")
				client.Close()

				spinner := cli.NewSpinner("Reconnecting...")
				spinner.Start()
				newClient, reconnErr := connectWithRetry(addr, config.UseTLS, config.TLSInsecure)
				if reconnErr != nil {
					spinner.StopWithError("Failed to reconnect")
					cli.PrintError("Exiting...")
					os.Exit(1)
				}
				client = newClient
				spinner.StopWithSuccess("Reconnected successfully!")
				response, err = processCommand(client, input, config)
				if err != nil {
					printErrorMessage(err.Error())
					continue
				}
			} else {
				printErrorMessage(err.Error())
				continue
			}
		}

		printResponseWithFormat(response, config.Format)
	}
}

// handleAuthCommandSimple handles AUTH command in simple REPL mode (without readline).
func handleAuthCommandSimple(client *BinaryClient, input string, config CLIConfig) (string, error) {
	parts := strings.Fields(input)

	var username, password string

	switch len(parts) {
	case 1:
		// Just "AUTH" - prompt for both username and password
		fmt.Print(cli.Dimmed("Username: "))
		reader := bufio.NewReader(os.Stdin)
		usernameInput, err := reader.ReadString('\n')
		if err != nil {
			return "", fmt.Errorf("cancelled")
		}
		username = strings.TrimSpace(usernameInput)
		if username == "" {
			return "", cli.ErrMissingArgument("username", "AUTH <username> <password>")
		}

		// Read password with masking using terminal raw mode
		password, err = readPasswordTerminal("Password: ")
		if err != nil {
			return "", fmt.Errorf("cancelled")
		}

	case 2:
		// "AUTH username" - prompt for password only
		username = parts[1]

		// Read password with masking
		var err error
		password, err = readPasswordTerminal("Password: ")
		if err != nil {
			return "", fmt.Errorf("cancelled")
		}

	default:
		// "AUTH username password" - warn about security but proceed
		username = parts[1]
		password = strings.Join(parts[2:], " ")

		// Show security warning
		cli.PrintWarning("Security: Passwords typed on command line may be visible in history.")
		fmt.Println(cli.Dimmed("  Tip: Use 'AUTH' or 'AUTH <username>' for secure password entry."))
		fmt.Println()
	}

	if password == "" {
		return "", cli.ErrMissingArgument("password", "AUTH <username> <password>")
	}

	// Perform authentication
	result, err := client.Auth(username, password)
	if err != nil {
		return "", err
	}

	// If authentication succeeded, update state
	if strings.Contains(result, "OK") {
		replState.Authenticated = true
		replState.Username = username
		replState.CurrentDatabase = "default" // Set default database after auth
	}

	return result, nil
}

// readPasswordTerminal reads a password from terminal with masking.
func readPasswordTerminal(prompt string) (string, error) {
	fmt.Print(cli.Dimmed(prompt))

	// Use golang.org/x/term for secure password reading
	fd := int(os.Stdin.Fd())
	if term.IsTerminal(fd) {
		password, err := term.ReadPassword(fd)
		fmt.Println() // Print newline after password input
		if err != nil {
			return "", err
		}
		return string(password), nil
	}

	// Fallback for non-terminal (piped input)
	reader := bufio.NewReader(os.Stdin)
	password, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(password), nil
}

// isConnectionError checks if an error indicates a connection problem.
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "connection") ||
		strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "reset by peer") ||
		strings.Contains(errStr, "timeout")
}

// processCommand handles a user command and returns the response.
func processCommand(client *BinaryClient, input string, config CLIConfig) (string, error) {
	upper := strings.ToUpper(input)
	parts := strings.SplitN(input, " ", 2)
	cmd := strings.ToUpper(parts[0])

	// Debug logging
	if config.Debug {
		fmt.Printf("%s Command: %s\n", cli.Dimmed("[DEBUG]"), input)
	}

	// Handle PING command
	if cmd == "PING" {
		start := time.Now()
		if err := client.Ping(); err != nil {
			return "", err
		}
		if config.Verbose {
			return fmt.Sprintf("PONG (latency: %v)", time.Since(start)), nil
		}
		return "PONG", nil
	}

	// Handle AUTH command
	if cmd == "AUTH" {
		if len(parts) < 2 {
			return "", cli.ErrMissingArgument("credentials", "AUTH <username> <password>")
		}
		creds := strings.SplitN(parts[1], " ", 2)
		if len(creds) != 2 {
			return "", cli.ErrMissingArgument("password", "AUTH <username> <password>")
		}
		result, err := client.Auth(creds[0], creds[1])
		if err != nil {
			return "", err
		}
		// If authentication succeeded, update state
		if strings.Contains(result, "OK") {
			replState.Authenticated = true
			replState.Username = creds[0]
			replState.CurrentDatabase = "default" // Set default database after auth
		}
		return result, nil
	}

	// Block all SQL commands until authenticated
	if !replState.Authenticated {
		return "", cli.NewCLIError("Authentication required").
			WithSuggestion("Use 'AUTH <username> <password>' to authenticate").
			WithSuggestion("Or use 'AUTH' for interactive login")
	}

	// Handle WATCH command (not supported yet)
	if cmd == "WATCH" {
		return "", cli.NewCLIError("WATCH is not supported yet").
			WithSuggestion("WATCH functionality is planned for a future release")
	}

	// Handle USE command directly (without SQL prefix requirement)
	if cmd == "USE" {
		if len(parts) < 2 || strings.TrimSpace(parts[1]) == "" {
			return "", cli.ErrMissingArgument("database_name", "USE <database_name>")
		}
		dbName := strings.TrimSpace(parts[1])
		// Execute the USE command on the server
		result, err := client.Query("USE " + dbName)
		if err != nil {
			return "", err
		}
		// If successful, update local state
		if strings.Contains(result, "OK") {
			replState.CurrentDatabase = dbName
		}
		return result, nil
	}

	// Determine the SQL query to execute
	var query string

	// Check for explicit SQL prefix (e.g., "SQL SELECT * FROM users")
	if cmd == "SQL" {
		if len(parts) > 1 {
			query = parts[1]
		} else {
			return "", cli.ErrMissingArgument("query", "SQL <query>")
		}
	} else if replState.SQLMode {
		// In SQL mode, treat all input as SQL
		query = input
	} else {
		// Normal mode: check if it starts with a SQL keyword
		isSQLKeyword := false
		for _, keyword := range sqlKeywords {
			if strings.HasPrefix(upper, keyword+" ") || upper == keyword {
				isSQLKeyword = true
				break
			}
		}
		if !isSQLKeyword {
			return "", cli.NewCLIError(fmt.Sprintf("Unknown command: %s", cmd)).
				WithSuggestion("Use 'SQL <query>' prefix to execute any SQL statement").
				WithSuggestion("Use '\\sql' to enter SQL mode where all input is treated as SQL").
				WithSuggestion("Type '\\h' for help on available commands")
		}
		query = input
	}

	// Execute the query with timing
	start := time.Now()
	result, err := client.Query(query)
	elapsed := time.Since(start)
	if err != nil {
		return "", err
	}

	// Update current database if USE command was executed via SQL mode
	if strings.HasPrefix(strings.ToUpper(query), "USE ") && strings.Contains(result, "OK") {
		dbName := strings.TrimSpace(query[4:])
		replState.CurrentDatabase = dbName
	}

	// Handle DROP DATABASE - if we dropped the current database, update local state
	upperQuery := strings.ToUpper(query)
	if strings.HasPrefix(upperQuery, "DROP DATABASE") && strings.Contains(result, "OK") {
		// Check if the server switched us to a different database
		if strings.Contains(result, "switched to") {
			// Extract the new database name from "DROP DATABASE OK (switched to <db>)"
			if idx := strings.Index(result, "switched to "); idx != -1 {
				newDb := strings.TrimSuffix(result[idx+len("switched to "):], ")")
				replState.CurrentDatabase = newDb
				// Add a warning to the result
				result = result + "\n" + cli.Warning("⚠ You were connected to the dropped database. Now using: "+newDb)
				result = result + "\n" + cli.Dimmed("  Use 'USE <database>' to switch to a different database.")
			}
		}
	}

	// Add timing information if enabled (via \timing or --verbose)
	if config.Verbose || replState.Timing {
		result = fmt.Sprintf("%s\n%s", result, cli.Dimmed(fmt.Sprintf("Time: %v", elapsed)))
	}

	return result, nil
}

// printErrorMessage prints an error message with formatting.
// It handles both CLIError types and plain error strings, parsing server
// error messages for better display.
func printErrorMessage(msg string) {
	// Check if this is a server error with hint (format: "ERROR: message\nHint: suggestion")
	if strings.Contains(msg, "\nHint:") {
		parts := strings.SplitN(msg, "\nHint:", 2)
		cli.PrintError("%s", strings.TrimPrefix(parts[0], "ERROR: "))
		if len(parts) > 1 {
			fmt.Printf("  %s %s\n", cli.Dimmed("Hint:"), strings.TrimSpace(parts[1]))
		}
		return
	}

	// Check if this is a simple ERROR: prefix message
	if strings.HasPrefix(msg, "ERROR: ") {
		cli.PrintError("%s", strings.TrimPrefix(msg, "ERROR: "))
		return
	}

	cli.PrintError("%s", msg)
}

// isSQL checks if the input is a SQL command.
func isSQL(input string) bool {
	upper := strings.ToUpper(input)

	// Check if the input starts with a known SQL keyword.
	for _, keyword := range sqlKeywords {
		if strings.HasPrefix(upper, keyword) {
			return true
		}
	}

	return false
}

// handleLocalCommand processes commands that start with a backslash.
// These commands are handled locally by the CLI without server communication.
// This pattern is inspired by PostgreSQL's psql client.
func handleLocalCommand(cmd string, config CLIConfig, client *BinaryClient) {
	// Handle commands with arguments
	parts := strings.SplitN(cmd, " ", 2)
	baseCmd := parts[0]
	arg := ""
	if len(parts) > 1 {
		arg = strings.TrimSpace(parts[1])
	}

	switch baseCmd {
	case "\\q", "\\quit":
		// Exit the CLI gracefully.
		cli.PrintInfo("Goodbye!")
		os.Exit(0)

	case "\\h", "\\help":
		// Display help information about available commands.
		printHelp()

	case "\\clear":
		// Clear the screen
		fmt.Print("\033[H\033[2J")

	case "\\s", "\\status", "\\conninfo":
		// Show connection status
		printStatus(config, client)

	case "\\v", "\\version":
		// Show version
		fmt.Printf("fsql version %s\n", banner.Version)

	case "\\timing":
		// Toggle timing display
		replState.Timing = !replState.Timing
		if replState.Timing {
			cli.PrintSuccess("Timing is on.")
		} else {
			cli.PrintInfo("Timing is off.")
		}

	case "\\x":
		// Toggle expanded output
		replState.ExpandedOutput = !replState.ExpandedOutput
		if replState.ExpandedOutput {
			cli.PrintSuccess("Expanded display is on.")
		} else {
			cli.PrintInfo("Expanded display is off.")
		}

	case "\\o":
		// Set output file
		if arg == "" {
			// Close current output file and reset to stdout
			if replState.outputWriter != nil {
				replState.outputWriter.Close()
				replState.outputWriter = nil
				replState.OutputFile = ""
				cli.PrintInfo("Output reset to stdout.")
			} else {
				cli.PrintInfo("Output is currently to stdout.")
			}
		} else {
			// Open new output file
			f, err := os.Create(arg)
			if err != nil {
				cli.PrintError("Cannot open file: %v", err)
				return
			}
			if replState.outputWriter != nil {
				replState.outputWriter.Close()
			}
			replState.outputWriter = f
			replState.OutputFile = arg
			cli.PrintSuccess("Output set to file: %s", arg)
		}

	case "\\!":
		// Execute shell command
		if arg == "" {
			cli.PrintWarning("Usage: \\! <shell command>")
			return
		}
		executeShellCommand(arg)

	case "\\dt":
		// List tables (shortcut for INSPECT TABLES)
		response, err := processCommand(client, "INSPECT TABLES", config)
		if err != nil {
			printErrorMessage(err.Error())
			return
		}
		printResponseWithFormat(response, config.Format)

	case "\\du":
		// List users (shortcut for INSPECT USERS)
		response, err := processCommand(client, "INSPECT USERS", config)
		if err != nil {
			printErrorMessage(err.Error())
			return
		}
		printResponseWithFormat(response, config.Format)

	case "\\di":
		// List indexes (shortcut for INSPECT INDEXES)
		response, err := processCommand(client, "INSPECT INDEXES", config)
		if err != nil {
			printErrorMessage(err.Error())
			return
		}
		printResponseWithFormat(response, config.Format)

	case "\\db", "\\l":
		// List databases (shortcut for INSPECT DATABASES)
		response, err := processCommand(client, "INSPECT DATABASES", config)
		if err != nil {
			printErrorMessage(err.Error())
			return
		}
		// Show current database indicator
		fmt.Printf("Current database: %s\n\n", cli.Success(replState.CurrentDatabase))
		printResponseWithFormat(response, config.Format)

	case "\\cd", "\\current":
		// Show current database
		dbName := replState.CurrentDatabase
		if dbName == "" {
			dbName = "default"
		}
		fmt.Printf("Current database: %s\n", cli.Success(dbName))
		fmt.Println(cli.Dimmed("  Use '\\c <database>' or 'USE <database>' to switch databases"))

	case "\\c", "\\connect":
		// Connect to a different database (shortcut for USE <database>)
		if arg == "" {
			cli.PrintWarning("Usage: \\c <database_name>")
			fmt.Printf("Current database: %s\n", cli.Success(replState.CurrentDatabase))
			return
		}
		response, err := processCommand(client, "USE "+arg, config)
		if err != nil {
			printErrorMessage(err.Error())
			return
		}
		// Show a nice confirmation message
		if strings.Contains(response, "OK") {
			cli.PrintSuccess("Connected to database '%s'", arg)
		} else {
			printResponseWithFormat(response, config.Format)
		}

	case "\\sql":
		// Enter SQL mode - all input treated as SQL
		replState.SQLMode = true
		cli.PrintSuccess("Entering SQL mode. All input will be executed as SQL.")
		fmt.Println(cli.Dimmed("  Type \\normal to return to normal mode."))
		fmt.Println(cli.Dimmed("  Local commands (\\h, \\q, etc.) still work in SQL mode."))

	case "\\normal":
		// Exit SQL mode - return to normal mode with auto-detection
		if replState.SQLMode {
			replState.SQLMode = false
			cli.PrintInfo("Returning to normal mode with SQL auto-detection.")
		} else {
			cli.PrintInfo("Already in normal mode.")
		}

	case "\\audit":
		// Show recent audit logs (shortcut for INSPECT AUDIT)
		response, err := processCommand(client, "INSPECT AUDIT LIMIT 50", config)
		if err != nil {
			printErrorMessage(err.Error())
			return
		}
		printResponseWithFormat(response, config.Format)

	case "\\audit-user":
		// Show audit logs for a specific user
		if arg == "" {
			cli.PrintWarning("Usage: \\audit-user <username>")
			return
		}
		query := fmt.Sprintf("INSPECT AUDIT WHERE username = '%s' LIMIT 50", arg)
		response, err := processCommand(client, query, config)
		if err != nil {
			printErrorMessage(err.Error())
			return
		}
		printResponseWithFormat(response, config.Format)

	case "\\audit-export":
		// Export audit logs to a file
		if arg == "" {
			cli.PrintWarning("Usage: \\audit-export <filename> [format]")
			fmt.Println(cli.Dimmed("  Formats: json, csv, sql (default: json)"))
			return
		}
		parts := strings.Fields(arg)
		filename := parts[0]
		format := "json"
		if len(parts) > 1 {
			format = parts[1]
		}
		query := fmt.Sprintf("EXPORT AUDIT TO '%s' FORMAT %s", filename, format)
		response, err := processCommand(client, query, config)
		if err != nil {
			printErrorMessage(err.Error())
			return
		}
		cli.PrintSuccess("Audit logs exported to: %s", filename)
		printResponseWithFormat(response, config.Format)

	case "\\audit-stats":
		// Show audit statistics
		response, err := processCommand(client, "INSPECT AUDIT STATS", config)
		if err != nil {
			printErrorMessage(err.Error())
			return
		}
		printResponseWithFormat(response, config.Format)

	default:
		// Unknown local command - inform the user with suggestions.
		cli.PrintWarning("Unknown command: %s", cmd)
		fmt.Println(cli.Dimmed("  Type \\h for help on available commands."))
	}
}

// executeShellCommand executes a shell command and displays output.
func executeShellCommand(cmd string) {
	fmt.Println()
	// Use /bin/sh -c to execute the command
	proc := exec.Command("/bin/sh", "-c", cmd)
	proc.Stdout = os.Stdout
	proc.Stderr = os.Stderr
	proc.Stdin = os.Stdin
	err := proc.Run()
	if err != nil {
		cli.PrintError("Command failed: %v", err)
	}
	fmt.Println()
}

// printStatus displays the current connection status.
func printStatus(config CLIConfig, client *BinaryClient) {
	fmt.Println()
	fmt.Println("  " + cli.Highlight("Connection Status"))
	fmt.Println("  " + cli.Separator(30))
	fmt.Println()

	// Check connection status
	var statusIcon, statusText string
	if client.IsConnected() {
		statusIcon = cli.Success("●")
		statusText = cli.Success("Connected")
	} else {
		statusIcon = cli.Error("●")
		statusText = cli.Error("Disconnected")
	}

	// Get current database name
	dbName := replState.CurrentDatabase
	if dbName == "" {
		dbName = "default"
	}

	// Check if we're in HA/cluster mode
	isClusterMode := len(config.Hosts) > 1

	fmt.Printf("    %s %s %s\n", cli.Dimmed("Status:"), statusIcon, statusText)
	fmt.Printf("    %s %s\n", cli.Dimmed("Server:"), client.serverAddr)
	if isClusterMode {
		fmt.Printf("    %s %s (%d nodes)\n", cli.Dimmed("Mode:"), cli.Info("Cluster/HA"), len(config.Hosts))
		fmt.Printf("    %s %s\n", cli.Dimmed("Nodes:"), strings.Join(config.Hosts, ", "))
		if config.TargetPrimary {
			fmt.Printf("    %s %s\n", cli.Dimmed("Target:"), cli.Success("Primary/Leader"))
		}
	} else {
		fmt.Printf("    %s %s\n", cli.Dimmed("Mode:"), "Single node")
	}
	fmt.Printf("    %s %s\n", cli.Dimmed("Database:"), cli.Success(dbName))
	fmt.Printf("    %s %s\n", cli.Dimmed("Protocol:"), "Binary")

	tlsStatus := cli.Error("Disabled")
	if config.UseTLS {
		if config.TLSInsecure {
			tlsStatus = cli.Warning("Enabled (Insecure)")
		} else {
			tlsStatus = cli.Success("Enabled")
		}
	}
	fmt.Printf("    %s %s\n", cli.Dimmed("TLS:"), tlsStatus)

	fmt.Printf("    %s %s\n", cli.Dimmed("Format:"), string(config.Format))

	// Show session settings
	fmt.Println()
	fmt.Println("  " + cli.Highlight("Session Settings"))
	fmt.Println("  " + cli.Separator(30))
	fmt.Println()

	timingStatus := cli.Dimmed("off")
	if replState.Timing {
		timingStatus = cli.Success("on")
	}
	expandedStatus := cli.Dimmed("off")
	if replState.ExpandedOutput {
		expandedStatus = cli.Success("on")
	}
	sqlModeStatus := cli.Dimmed("off")
	if replState.SQLMode {
		sqlModeStatus = cli.Warning("on")
	}
	outputDest := "stdout"
	if replState.OutputFile != "" {
		outputDest = replState.OutputFile
	}

	fmt.Printf("    %s %s\n", cli.Dimmed("Timing:"), timingStatus)
	fmt.Printf("    %s %s\n", cli.Dimmed("Expanded:"), expandedStatus)
	fmt.Printf("    %s %s %s\n", cli.Dimmed("SQL Mode:"), sqlModeStatus, cli.Dimmed("(\\sql to enable, \\normal to disable)"))
	fmt.Printf("    %s %s\n", cli.Dimmed("Output:"), outputDest)
	fmt.Println()
}

// printHelp displays comprehensive help information about CLI usage.
// This function provides users with a quick reference for available commands.
func printHelp() {
	fmt.Println()
	fmt.Println("  " + cli.Highlight("FlyDB CLI Help") + " " + cli.Dimmed("(v"+banner.Version+")"))
	fmt.Println("  " + cli.Separator(50))
	fmt.Println()

	// Local Commands
	fmt.Println("  " + cli.Highlight("Local Commands"))
	fmt.Println()
	fmt.Printf("    %s, %s         Exit the CLI\n", cli.Info("\\q"), cli.Info("\\quit"))
	fmt.Printf("    %s, %s         Display this help message\n", cli.Info("\\h"), cli.Info("\\help"))
	fmt.Printf("    %s            Clear the screen\n", cli.Info("\\clear"))
	fmt.Printf("    %s, %s       Show connection status\n", cli.Info("\\s"), cli.Info("\\status"))
	fmt.Printf("    %s, %s      Show version information\n", cli.Info("\\v"), cli.Info("\\version"))
	fmt.Printf("    %s              Toggle query timing display\n", cli.Info("\\timing"))
	fmt.Printf("    %s                  Toggle expanded output mode\n", cli.Info("\\x"))
	fmt.Printf("    %s [file]          Set output to file (no arg = stdout)\n", cli.Info("\\o"))
	fmt.Printf("    %s <cmd>           Execute shell command\n", cli.Info("\\!"))
	fmt.Printf("    %s                 List tables (shortcut)\n", cli.Info("\\dt"))
	fmt.Printf("    %s                 List users (shortcut)\n", cli.Info("\\du"))
	fmt.Printf("    %s                 List indexes (shortcut)\n", cli.Info("\\di"))
	fmt.Printf("    %s, %s            List databases\n", cli.Info("\\db"), cli.Info("\\l"))
	fmt.Printf("    %s, %s <db>   Switch to database\n", cli.Info("\\c"), cli.Info("\\connect"))
	fmt.Printf("    %s, %s     Show current database\n", cli.Info("\\cd"), cli.Info("\\current"))
	fmt.Printf("    %s               Enter SQL mode (all input = SQL)\n", cli.Info("\\sql"))
	fmt.Printf("    %s            Return to normal mode\n", cli.Info("\\normal"))
	fmt.Printf("    %s             Show recent audit logs\n", cli.Info("\\audit"))
	fmt.Printf("    %s <user>  Show audit logs for user\n", cli.Info("\\audit-user"))
	fmt.Printf("    %s <file>  Export audit logs to file\n", cli.Info("\\audit-export"))
	fmt.Printf("    %s        Show audit statistics\n", cli.Info("\\audit-stats"))
	fmt.Println()

	// Server Commands
	fmt.Println("  " + cli.Highlight("Server Commands"))
	fmt.Println()
	fmt.Printf("    %s                   Test server connectivity\n", cli.Info("PING"))
	fmt.Printf("    %s <user> <pwd>      Authenticate with the server\n", cli.Info("AUTH"))
	fmt.Printf("    %s <query>           Execute any SQL query explicitly\n", cli.Info("SQL"))
	fmt.Println()

	// SQL Commands
	fmt.Println("  " + cli.Highlight("SQL Commands") + " " + cli.Dimmed("(auto-detected)"))
	fmt.Println()
	fmt.Printf("    %s ...             Query data from tables\n", cli.Info("SELECT"))
	fmt.Printf("    %s ...             Insert new rows\n", cli.Info("INSERT"))
	fmt.Printf("    %s ...             Modify existing rows\n", cli.Info("UPDATE"))
	fmt.Printf("    %s ...             Remove rows\n", cli.Info("DELETE"))
	fmt.Printf("    %s ...       Create a new table\n", cli.Info("CREATE TABLE"))
	fmt.Printf("    %s ...       Create an index on a column\n", cli.Info("CREATE INDEX"))
	fmt.Printf("    %s ...        Create a new user\n", cli.Info("CREATE USER"))
	fmt.Printf("    %s ...              Grant permissions to a user\n", cli.Info("GRANT"))
	fmt.Println()

	// Prepared Statements
	fmt.Println("  " + cli.Highlight("Prepared Statements"))
	fmt.Println()
	fmt.Printf("    %s <name> AS <query>    Compile a query for reuse\n", cli.Info("PREPARE"))
	fmt.Printf("    %s <name> [USING ...]   Run a prepared statement\n", cli.Info("EXECUTE"))
	fmt.Printf("    %s <name>            Remove a prepared statement\n", cli.Info("DEALLOCATE"))
	fmt.Println()

	// Transactions
	fmt.Println("  " + cli.Highlight("Transactions"))
	fmt.Println()
	fmt.Printf("    %s                  Start a transaction\n", cli.Info("BEGIN"))
	fmt.Printf("    %s                 Commit the transaction\n", cli.Info("COMMIT"))
	fmt.Printf("    %s               Rollback the transaction\n", cli.Info("ROLLBACK"))
	fmt.Println()

	// Inspection
	fmt.Println("  " + cli.Highlight("Inspection"))
	fmt.Println()
	fmt.Printf("    %s           List all database users\n", cli.Info("INSPECT USERS"))
	fmt.Printf("    %s <name>     Detailed info for a user (roles, access)\n", cli.Info("INSPECT USER"))
	fmt.Printf("    %s          List all tables with schemas\n", cli.Info("INSPECT TABLES"))
	fmt.Printf("    %s <name>    Detailed info for a table\n", cli.Info("INSPECT TABLE"))
	fmt.Printf("    %s         List all indexes\n", cli.Info("INSPECT INDEXES"))
	fmt.Printf("    %s          Show server/daemon information\n", cli.Info("INSPECT SERVER"))
	fmt.Printf("    %s          Show database status & statistics\n", cli.Info("INSPECT STATUS"))
	fmt.Printf("    %s       List all databases\n", cli.Info("INSPECT DATABASES"))
	fmt.Printf("    %s <name>  Detailed info for a database\n", cli.Info("INSPECT DATABASE"))
	fmt.Printf("    %s           List all RBAC roles\n", cli.Info("INSPECT ROLES"))
	fmt.Printf("    %s <name>     Detailed info for a role\n", cli.Info("INSPECT ROLE"))
	fmt.Printf("    %s [WHERE ...] Show audit trail logs\n", cli.Info("INSPECT AUDIT"))
	fmt.Printf("    %s          Show audit statistics\n", cli.Info("INSPECT AUDIT STATS"))
	fmt.Println()

	// Database Management
	fmt.Println("  " + cli.Highlight("Database Management"))
	fmt.Println()
	fmt.Printf("    %s <name>              Switch to a database\n", cli.Info("USE"))
	fmt.Printf("    %s <name>  Create a new database\n", cli.Info("CREATE DATABASE"))
	fmt.Printf("    %s <name>    Drop a database\n", cli.Info("DROP DATABASE"))
	fmt.Println()
	fmt.Println("    " + cli.Dimmed("Note: The prompt always shows the current database (e.g., flydb:mydb>)"))
	fmt.Println("    " + cli.Dimmed("      Use -d flag or FLYDB_DATABASE env var to set database on startup"))
	fmt.Println()

	// Quick Examples
	fmt.Println("  " + cli.Highlight("Quick Examples"))
	fmt.Println()
	fmt.Println("    " + cli.Dimmed("# Switch to a different database"))
	fmt.Println("    " + cli.Success("USE mydb"))
	fmt.Println("    " + cli.Dimmed("# Or use the shortcut:"))
	fmt.Println("    " + cli.Success("\\c mydb"))
	fmt.Println()
	fmt.Println("    " + cli.Dimmed("# Create a table (auto-detected)"))
	fmt.Println("    " + cli.Success("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"))
	fmt.Println()
	fmt.Println("    " + cli.Dimmed("# Insert data"))
	fmt.Println("    " + cli.Success("INSERT INTO users VALUES (1, 'Alice')"))
	fmt.Println()
	fmt.Println("    " + cli.Dimmed("# Query data"))
	fmt.Println("    " + cli.Success("SELECT * FROM users WHERE id = 1"))
	fmt.Println()

	fmt.Println("  " + cli.Highlight("SQL Execution Modes"))
	fmt.Println()
	fmt.Println("    " + cli.Dimmed("1. Auto-detection: Common SQL keywords are recognized automatically"))
	fmt.Println("    " + cli.Dimmed("2. SQL prefix: Use 'SQL <query>' to execute any SQL explicitly"))
	fmt.Println("    " + cli.Dimmed("3. SQL mode: Use '\\sql' to enter mode where all input is SQL"))
	fmt.Println()

	// Multi-line Editing
	fmt.Println("  " + cli.Highlight("Multi-line Editing"))
	fmt.Println()
	fmt.Println("    " + cli.Dimmed("SQL statements require a semicolon (;) to execute."))
	fmt.Println("    " + cli.Dimmed("Without a semicolon, the prompt changes to '->' for continuation."))
	fmt.Println()
	fmt.Println("    " + cli.Dimmed("Example:"))
	fmt.Println("    " + cli.Success("flydb:default> SELECT *"))
	fmt.Println("    " + cli.Success("        -> FROM users"))
	fmt.Println("    " + cli.Success("        -> WHERE id = 1;"))
	fmt.Println()
	fmt.Println("    " + cli.Dimmed("Tips:"))
	fmt.Println("    " + cli.Dimmed("  • Press Ctrl+C to cancel multi-line input"))
	fmt.Println("    " + cli.Dimmed("  • Use \\ at end of line for explicit continuation"))
	fmt.Println("    " + cli.Dimmed("  • Commands like PING, AUTH, USE don't need semicolons"))
	fmt.Println()
}

// printResponseWithFormat formats and displays the server's response based on format.
func printResponseWithFormat(response string, format cli.OutputFormat) {
	response = strings.TrimSpace(response)

	// Apply red color to error messages for visibility.
	if strings.HasPrefix(response, "ERROR") {
		cli.PrintError("%s", response)
		return
	}

	// Handle JSON format
	if format == cli.FormatJSON {
		printResponseAsJSON(response)
		return
	}

	// Handle plain format
	if format == cli.FormatPlain {
		fmt.Println(response)
		return
	}

	// Default table format
	// Detect DML results (INSERT/UPDATE/DELETE with affected row count)
	if isDMLResult(response) {
		formatDMLResult(response)
		return
	}

	// Detect SELECT results (contain row count at the end).
	if strings.HasSuffix(response, "rows)") || strings.HasSuffix(response, "row)") {
		formatSelectResult(response)
		return
	}

	// Detect key-value format (inspect results like "Key: Value")
	if isKeyValueFormat(response) {
		formatKeyValueResult(response)
		return
	}

	// Detect tabular data and format it nicely.
	if strings.Contains(response, ", ") || strings.Contains(response, "\n") {
		formatTable(response)
	} else {
		// Simple single-value response - print with success color.
		cli.PrintSuccess("%s", response)
	}
}

// isDMLResult checks if the response is a DML result (INSERT/UPDATE/DELETE with count).
func isDMLResult(response string) bool {
	// Match patterns like "INSERT 1", "UPDATE 5", "DELETE 3"
	parts := strings.SplitN(response, " ", 2)
	if len(parts) != 2 {
		return false
	}
	cmd := parts[0]
	if cmd != "INSERT" && cmd != "UPDATE" && cmd != "DELETE" {
		return false
	}
	// Check if the second part is a number
	for _, c := range parts[1] {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// formatDMLResult formats INSERT/UPDATE/DELETE results with proper "rows affected" message.
func formatDMLResult(response string) {
	parts := strings.SplitN(response, " ", 2)
	if len(parts) != 2 {
		cli.PrintSuccess("%s", response)
		return
	}

	cmd := parts[0]
	count := parts[1]

	// Format the message based on the count
	var msg string
	if count == "1" {
		msg = fmt.Sprintf("%s: 1 row affected", cmd)
	} else {
		msg = fmt.Sprintf("%s: %s rows affected", cmd, count)
	}

	cli.PrintSuccess("%s", msg)
}

// isKeyValueFormat checks if the response is in key-value format (like inspect results).
func isKeyValueFormat(response string) bool {
	lines := strings.Split(response, "\n")
	if len(lines) < 2 {
		return false
	}

	// Check if most lines contain ": " pattern
	kvCount := 0
	for _, line := range lines {
		if strings.Contains(line, ": ") {
			kvCount++
		}
	}

	// If more than half the lines are key-value pairs, treat as KV format
	return kvCount > len(lines)/2
}

// formatKeyValueResult formats key-value inspect results nicely.
func formatKeyValueResult(response string) {
	lines := strings.Split(response, "\n")

	// Find the title (first line that looks like a section header)
	title := ""
	startIdx := 0

	// Check if first line looks like a title (e.g., "Table: users")
	if len(lines) > 0 && strings.Contains(lines[0], ": ") {
		parts := strings.SplitN(lines[0], ": ", 2)
		if parts[0] == "Table" || parts[0] == "Server" {
			title = parts[1]
			startIdx = 0
		}
	}

	fmt.Println()

	// Determine section title based on content
	if title != "" {
		if strings.Contains(response, "Version:") && strings.Contains(response, "Protocol:") {
			fmt.Println("  " + cli.Highlight("Server Information"))
		} else if strings.Contains(response, "Columns:") {
			fmt.Println("  " + cli.Highlight("Table: "+title))
		} else {
			fmt.Println("  " + cli.Highlight(title))
		}
	} else if strings.Contains(response, "Tables:") && strings.Contains(response, "Users:") {
		fmt.Println("  " + cli.Highlight("Database Status"))
	} else {
		fmt.Println("  " + cli.Highlight("Result"))
	}
	fmt.Println("  " + cli.Separator(40))
	fmt.Println()

	// Format each line
	for i := startIdx; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}

		// Handle section headers (lines ending with ":")
		if strings.HasSuffix(line, ":") && !strings.Contains(line, ": ") {
			fmt.Println()
			fmt.Println("  " + cli.Info(line))
			continue
		}

		// Handle indented items (like column listings)
		if strings.HasPrefix(lines[i], "  ") {
			fmt.Println("  " + cli.Dimmed("  "+line))
			continue
		}

		// Handle key-value pairs
		if idx := strings.Index(line, ": "); idx != -1 {
			key := line[:idx]
			value := line[idx+2:]
			fmt.Printf("    %s %s\n", cli.Dimmed(key+":"), value)
		} else {
			fmt.Println("    " + line)
		}
	}

	fmt.Println()
}

// printResponseAsJSON outputs the response in JSON format.
func printResponseAsJSON(response string) {
	// Try to parse as tabular data
	lines := strings.Split(response, "\n")

	// Check if it's a SELECT result with row count
	if strings.HasSuffix(response, "rows)") || strings.HasSuffix(response, "row)") {
		if len(lines) > 1 {
			lines = lines[:len(lines)-1] // Remove row count line
		}
	}

	// Parse rows
	var rows [][]string
	for _, line := range lines {
		if line == "" {
			continue
		}
		cols := strings.Split(line, ", ")
		rows = append(rows, cols)
	}

	// Convert to JSON
	if len(rows) == 0 {
		fmt.Println("[]")
		return
	}

	// Create JSON array
	result := make([]map[string]string, len(rows))
	for i, row := range rows {
		rowMap := make(map[string]string)
		for j, val := range row {
			rowMap[fmt.Sprintf("col%d", j+1)] = val
		}
		result[i] = rowMap
	}

	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fmt.Println(response) // Fallback to plain
		return
	}
	fmt.Println(string(data))
}

// formatSelectResult formats SELECT query results as a professional grid table.
// It parses the response which contains a header row, data rows, and a row count.
// Format from server: "header1, header2\ndata1, data2\n(N rows)"
func formatSelectResult(response string) {
	lines := strings.Split(response, "\n")
	if len(lines) == 0 {
		return
	}

	// The last line is the row count (e.g., "(3 rows)")
	rowCountLine := lines[len(lines)-1]
	dataLines := lines[:len(lines)-1]

	// Handle empty result set (only header, no data)
	if len(dataLines) == 0 {
		fmt.Println()
		fmt.Println(cli.Dimmed("  (empty result set)"))
		fmt.Println(cli.Dimmed("  " + rowCountLine))
		fmt.Println()
		return
	}

	// Parse all rows (first row is header, rest are data)
	var allRows [][]string
	for _, line := range dataLines {
		if line == "" {
			continue
		}
		cols := strings.Split(line, ", ")
		allRows = append(allRows, cols)
	}

	if len(allRows) == 0 {
		fmt.Println()
		fmt.Println(cli.Dimmed("  (empty result set)"))
		fmt.Println(cli.Dimmed("  " + rowCountLine))
		fmt.Println()
		return
	}

	// First row is the header
	headers := allRows[0]
	dataRows := allRows[1:]

	// Calculate the maximum number of columns across ALL rows
	numCols := len(headers)
	for _, row := range dataRows {
		if len(row) > numCols {
			numCols = len(row)
		}
	}

	// Calculate maximum width for each column (minimum width of 3 for aesthetics)
	colWidths := make([]int, numCols)
	for i := range colWidths {
		colWidths[i] = 3 // Minimum column width
	}
	// Consider header widths
	for i, col := range headers {
		if i < numCols && len(col) > colWidths[i] {
			colWidths[i] = len(col)
		}
	}
	// Consider data widths
	for _, row := range dataRows {
		for i, col := range row {
			if i < numCols && len(col) > colWidths[i] {
				colWidths[i] = len(col)
			}
		}
	}

	// Unicode box-drawing characters for professional grid
	const (
		topLeft     = "┌"
		topRight    = "┐"
		bottomLeft  = "└"
		bottomRight = "┘"
		horizontal  = "─"
		vertical    = "│"
		topT        = "┬"
		bottomT     = "┴"
		leftT       = "├"
		rightT      = "┤"
		cross       = "┼"
	)

	// Build the top border
	var topParts []string
	for _, width := range colWidths {
		topParts = append(topParts, strings.Repeat(horizontal, width+2))
	}
	topBorder := topLeft + strings.Join(topParts, topT) + topRight

	// Build the separator line (between header and data)
	var sepParts []string
	for _, width := range colWidths {
		sepParts = append(sepParts, strings.Repeat(horizontal, width+2))
	}
	separator := leftT + strings.Join(sepParts, cross) + rightT

	// Build the bottom border
	var bottomParts []string
	for _, width := range colWidths {
		bottomParts = append(bottomParts, strings.Repeat(horizontal, width+2))
	}
	bottomBorder := bottomLeft + strings.Join(bottomParts, bottomT) + bottomRight

	fmt.Println()

	// Print the table with professional grid
	fmt.Println(cli.Dimmed(topBorder))

	// Print header row with bold styling
	var headerParts []string
	for i := 0; i < numCols; i++ {
		val := ""
		if i < len(headers) {
			val = headers[i]
		}
		padded := fmt.Sprintf(" %-*s ", colWidths[i], val)
		headerParts = append(headerParts, cli.Highlight(padded))
	}
	fmt.Println(cli.Dimmed(vertical) + strings.Join(headerParts, cli.Dimmed(vertical)) + cli.Dimmed(vertical))

	// Print separator after header if there are data rows
	if len(dataRows) > 0 {
		fmt.Println(cli.Dimmed(separator))
	}

	// Print data rows
	for _, row := range dataRows {
		var rowParts []string
		for i := 0; i < numCols; i++ {
			val := ""
			if i < len(row) {
				val = row[i]
			}
			padded := fmt.Sprintf(" %-*s ", colWidths[i], val)
			rowParts = append(rowParts, padded)
		}
		fmt.Println(cli.Dimmed(vertical) + strings.Join(rowParts, cli.Dimmed(vertical)) + cli.Dimmed(vertical))
	}

	fmt.Println(cli.Dimmed(bottomBorder))

	// Print the row count with formatting (data rows only, not header)
	rowCount := len(dataRows)
	if rowCount == 0 {
		fmt.Println(cli.Dimmed("  (0 rows)"))
	} else if rowCount == 1 {
		fmt.Println(cli.Success(fmt.Sprintf("  %d row returned", rowCount)))
	} else {
		fmt.Println(cli.Success(fmt.Sprintf("  %d rows returned", rowCount)))
	}
	fmt.Println()
}

// formatTable pretty-prints tabular data using a professional grid layout.
// It converts comma-separated values into a nicely formatted table.
// The first row is treated as a header row with a separator line below it.
func formatTable(data string) {
	// Process each line of the response.
	lines := strings.Split(data, "\n")
	var rows [][]string
	for _, line := range lines {
		if line == "" {
			continue
		}
		cols := strings.Split(line, ", ")
		rows = append(rows, cols)
	}

	if len(rows) == 0 {
		return
	}

	// Calculate maximum number of columns across all rows
	numCols := 0
	for _, row := range rows {
		if len(row) > numCols {
			numCols = len(row)
		}
	}

	// Calculate maximum width for each column (minimum width of 3 for aesthetics)
	colWidths := make([]int, numCols)
	for i := range colWidths {
		colWidths[i] = 3 // Minimum column width
	}
	for _, row := range rows {
		for i, col := range row {
			if i < numCols && len(col) > colWidths[i] {
				colWidths[i] = len(col)
			}
		}
	}

	// Unicode box-drawing characters for professional grid
	const (
		topLeft     = "┌"
		topRight    = "┐"
		bottomLeft  = "└"
		bottomRight = "┘"
		horizontal  = "─"
		vertical    = "│"
		topT        = "┬"
		bottomT     = "┴"
		leftT       = "├"
		rightT      = "┤"
		cross       = "┼"
	)

	// Build border strings
	var topParts, sepParts, bottomParts []string
	for _, width := range colWidths {
		topParts = append(topParts, strings.Repeat(horizontal, width+2))
		sepParts = append(sepParts, strings.Repeat(horizontal, width+2))
		bottomParts = append(bottomParts, strings.Repeat(horizontal, width+2))
	}
	topBorder := topLeft + strings.Join(topParts, topT) + topRight
	separator := leftT + strings.Join(sepParts, cross) + rightT
	bottomBorder := bottomLeft + strings.Join(bottomParts, bottomT) + bottomRight

	fmt.Println()
	fmt.Println(cli.Dimmed(topBorder))

	for rowIdx, row := range rows {
		var rowParts []string
		for i := 0; i < numCols; i++ {
			val := ""
			if i < len(row) {
				val = row[i]
			}
			padded := fmt.Sprintf(" %-*s ", colWidths[i], val)
			// Apply bold styling to header row (first row)
			if rowIdx == 0 {
				rowParts = append(rowParts, cli.Highlight(padded))
			} else {
				rowParts = append(rowParts, padded)
			}
		}
		fmt.Println(cli.Dimmed(vertical) + strings.Join(rowParts, cli.Dimmed(vertical)) + cli.Dimmed(vertical))

		// Print separator after first row (header) if there are multiple rows
		if rowIdx == 0 && len(rows) > 1 {
			fmt.Println(cli.Dimmed(separator))
		}
	}

	fmt.Println(cli.Dimmed(bottomBorder))
	fmt.Println()
}
