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

The fly-cli is an interactive REPL (Read-Eval-Print Loop) client that connects
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
    ./fly-cli

  Connect to remote server:
    ./fly-cli -h 192.168.1.100 -p 8888

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
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"flydb/internal/banner"
	"flydb/internal/protocol"
	"flydb/pkg/cli"
)

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
	Host    string           // Server hostname or IP address
	Port    string           // Server port number (binary protocol)
	Verbose bool             // Enable verbose output
	Debug   bool             // Enable debug mode
	Format  cli.OutputFormat // Output format (table, json, plain)
	Execute string           // Command to execute and exit
}

// sqlKeywords defines the SQL keywords that trigger automatic SQL query handling.
// When user input starts with any of these keywords, the CLI sends it as a query.
var sqlKeywords = []string{
	"SELECT", "INSERT", "CREATE", "GRANT", "UPDATE", "DELETE", "INTROSPECT",
	"BEGIN", "COMMIT", "ROLLBACK", "PREPARE", "EXECUTE", "DEALLOCATE",
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
	Host       string
	Port       string
	Version    bool
	Help       bool
	Verbose    bool
	Debug      bool
	Format     string
	NoColor    bool
	Execute    string
	ConfigFile string
}

// parseFlags parses command-line flags and returns the configuration.
func parseFlags() CLIFlags {
	flags := CLIFlags{}

	flag.StringVar(&flags.Host, "host", "localhost", "Server hostname or IP address")
	flag.StringVar(&flags.Host, "H", "localhost", "Server hostname or IP address (shorthand)")
	flag.StringVar(&flags.Port, "port", "8889", "Server port number (binary protocol)")
	flag.StringVar(&flags.Port, "p", "8889", "Server port number (shorthand)")
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

	// Custom usage function
	flag.Usage = printUsage

	flag.Parse()
	return flags
}

// printUsage prints comprehensive help information.
func printUsage() {
	fmt.Println()
	fmt.Println(cli.Highlight("FlyDB CLI - Interactive Database Client"))
	fmt.Printf("Version: %s\n\n", banner.Version)

	fmt.Println(cli.Highlight("USAGE:"))
	fmt.Println("  fly-cli [flags]")
	fmt.Println("  fly-cli -e \"<command>\"")
	fmt.Println()

	fmt.Println(cli.Highlight("FLAGS:"))
	fmt.Println("  -H, --host <host>      Server hostname or IP address (default: localhost)")
	fmt.Println("  -p, --port <port>      Server port number (default: 8889)")
	fmt.Println("  -v, --version          Print version information and exit")
	fmt.Println("      --help             Show this help message")
	fmt.Println("      --verbose          Enable verbose output")
	fmt.Println("      --debug            Enable debug mode with detailed logging")
	fmt.Println("  -f, --format <format>  Output format: table, json, plain (default: table)")
	fmt.Println("      --no-color         Disable colored output")
	fmt.Println("  -e, --execute <cmd>    Execute a command and exit")
	fmt.Println("  -c, --config <file>    Path to configuration file")
	fmt.Println()

	fmt.Println(cli.Highlight("EXAMPLES:"))
	fmt.Println(cli.Dimmed("  # Connect to local server"))
	fmt.Println(cli.Info("  fly-cli"))
	fmt.Println()
	fmt.Println(cli.Dimmed("  # Connect to remote server"))
	fmt.Println(cli.Info("  fly-cli -H 192.168.1.100 -p 8889"))
	fmt.Println()
	fmt.Println(cli.Dimmed("  # Execute a query and exit"))
	fmt.Println(cli.Info("  fly-cli -e \"SELECT * FROM users\""))
	fmt.Println()
	fmt.Println(cli.Dimmed("  # Get JSON output"))
	fmt.Println(cli.Info("  fly-cli -f json -e \"SELECT * FROM users\""))
	fmt.Println()

	fmt.Println(cli.Highlight("INTERACTIVE COMMANDS:"))
	fmt.Println("  \\q, \\quit              Exit the CLI")
	fmt.Println("  \\h, \\help              Display help information")
	fmt.Println("  \\c, \\clear             Clear the screen")
	fmt.Println("  \\s, \\status            Show connection status")
	fmt.Println()

	fmt.Println(cli.Highlight("ENVIRONMENT VARIABLES:"))
	fmt.Println("  FLYDB_HOST             Default server host")
	fmt.Println("  FLYDB_PORT             Default server port")
	fmt.Println("  NO_COLOR               Disable colored output")
	fmt.Println()

	fmt.Println("For more information, visit: https://github.com/flydb/flydb")
}

// main is the entry point for the fly-cli application.
// It parses command-line flags and initiates the REPL loop.
func main() {
	flags := parseFlags()

	// Handle --no-color flag
	if flags.NoColor {
		cli.SetColorsEnabled(false)
	}

	// Handle --version flag
	if flags.Version {
		fmt.Printf("fly-cli version %s\n", banner.Version)
		fmt.Printf("%s\n", banner.Copyright)
		os.Exit(0)
	}

	// Handle --help flag
	if flags.Help {
		printUsage()
		os.Exit(0)
	}

	// Check environment variables for defaults
	if envHost := os.Getenv("FLYDB_HOST"); envHost != "" && flags.Host == "localhost" {
		flags.Host = envHost
	}
	if envPort := os.Getenv("FLYDB_PORT"); envPort != "" && flags.Port == "8889" {
		flags.Port = envPort
	}

	// Create configuration struct and start the REPL.
	config := CLIConfig{
		Host:    flags.Host,
		Port:    flags.Port,
		Verbose: flags.Verbose,
		Debug:   flags.Debug,
		Format:  cli.ParseOutputFormat(flags.Format),
		Execute: flags.Execute,
	}

	startREPL(config)
}

// connectWithRetry attempts to connect to the server with exponential backoff.
func connectWithRetry(addr string) (*BinaryClient, error) {
	var lastErr error
	delay := InitialRetryDelay

	for attempt := 1; attempt <= MaxRetries; attempt++ {
		if attempt > 1 {
			fmt.Printf("Retrying connection (attempt %d/%d)...\n", attempt, MaxRetries)
		}

		// Attempt connection with timeout
		conn, err := net.DialTimeout("tcp", addr, ConnectionTimeout)
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

	// Construct the server address
	addr := config.Host + ":" + config.Port

	// Show connection spinner
	var spinner *cli.Spinner
	if config.Execute == "" {
		spinner = cli.NewSpinner(fmt.Sprintf("Connecting to %s...", addr))
		spinner.Start()
	} else if config.Verbose {
		fmt.Printf("Connecting to %s...\n", addr)
	}

	// Connect with retry logic
	client, err := connectWithRetry(addr)
	if err != nil {
		if spinner != nil {
			spinner.StopWithError("Connection failed")
		}
		cli.ErrConnectionFailed(config.Host, config.Port, err).Exit()
	}
	defer client.Close()

	if spinner != nil {
		spinner.StopWithSuccess(fmt.Sprintf("Connected to %s", addr))
	}

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

	// If executing a single command, do it and exit
	if config.Execute != "" {
		response, err := processCommand(client, config.Execute, config)
		if err != nil {
			cli.PrintError("%v", err)
			os.Exit(1)
		}
		printResponseWithFormat(response, config.Format)
		os.Exit(0)
	}

	fmt.Printf("%s Type %s to quit, %s for help.\n",
		cli.Success("Ready."),
		cli.Highlight("\\q"),
		cli.Highlight("\\h"))

	// Create scanner for reading user input
	scanner := bufio.NewScanner(os.Stdin)

	// Main REPL loop: continuously read, evaluate, and print.
	for {
		// Display the prompt and read user input.
		fmt.Print(cli.Info("flydb") + "> ")
		if !scanner.Scan() {
			// EOF or error reading input - exit gracefully.
			fmt.Println()
			cli.PrintInfo("Goodbye!")
			break
		}

		// Trim whitespace from input for clean processing.
		input := strings.TrimSpace(scanner.Text())

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
				newClient, reconnErr := connectWithRetry(addr)
				if reconnErr != nil {
					spinner.StopWithError("Failed to reconnect")
					cli.PrintError("Exiting...")
					os.Exit(1)
				}
				client = newClient
				spinner.StopWithSuccess("Reconnected successfully!")
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
		return result, nil
	}

	// Handle WATCH command (not supported in binary protocol yet)
	if cmd == "WATCH" {
		return "", cli.NewCLIError("WATCH is not supported in binary protocol mode").
			WithSuggestion("Use the text protocol for WATCH functionality")
	}

	// Check if it's a SQL command (with or without SQL prefix)
	query := input
	if cmd == "SQL" && len(parts) > 1 {
		query = parts[1]
	} else {
		// Check if it starts with a SQL keyword
		isSQLKeyword := false
		for _, keyword := range sqlKeywords {
			if strings.HasPrefix(upper, keyword) {
				isSQLKeyword = true
				break
			}
		}
		if !isSQLKeyword {
			return "", cli.ErrInvalidCommand(cmd)
		}
	}

	// Execute the query with timing in verbose mode
	start := time.Now()
	result, err := client.Query(query)
	if err != nil {
		return "", err
	}

	if config.Verbose {
		result = fmt.Sprintf("%s\n%s", result, cli.Dimmed(fmt.Sprintf("(executed in %v)", time.Since(start))))
	}

	return result, nil
}

// printErrorMessage prints an error message with formatting.
func printErrorMessage(msg string) {
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
	switch cmd {
	case "\\q", "\\quit":
		// Exit the CLI gracefully.
		cli.PrintInfo("Goodbye!")
		os.Exit(0)

	case "\\h", "\\help":
		// Display help information about available commands.
		printHelp()

	case "\\c", "\\clear":
		// Clear the screen
		fmt.Print("\033[H\033[2J")

	case "\\s", "\\status":
		// Show connection status
		printStatus(config, client)

	case "\\v", "\\version":
		// Show version
		fmt.Printf("fly-cli version %s\n", banner.Version)

	default:
		// Unknown local command - inform the user.
		cli.PrintWarning("Unknown command: %s. Type \\h for help.", cmd)
	}
}

// printStatus displays the current connection status.
func printStatus(config CLIConfig, client *BinaryClient) {
	fmt.Println()
	fmt.Println(cli.Highlight("Connection Status"))
	fmt.Println(cli.Separator(40))
	cli.KeyValue("Server", config.Host+":"+config.Port, 15)
	cli.KeyValue("Protocol", "Binary", 15)
	cli.KeyValue("Output Format", string(config.Format), 15)

	// Check connection
	if client.IsConnected() {
		cli.KeyValue("Status", cli.Success("Connected"), 15)
	} else {
		cli.KeyValue("Status", cli.Error("Disconnected"), 15)
	}
	fmt.Println()
}

// printHelp displays comprehensive help information about CLI usage.
// This function provides users with a quick reference for available commands.
func printHelp() {
	fmt.Println()
	fmt.Printf("%s (Binary Protocol)\n", cli.Highlight("FlyDB CLI v"+banner.Version))
	fmt.Println(cli.Separator(50))
	fmt.Println()

	fmt.Println(cli.Highlight("Local Commands:"))
	fmt.Println("  \\q, \\quit      Exit the CLI")
	fmt.Println("  \\h, \\help      Display this help message")
	fmt.Println("  \\c, \\clear     Clear the screen")
	fmt.Println("  \\s, \\status    Show connection status")
	fmt.Println("  \\v, \\version   Show version information")
	fmt.Println()

	fmt.Println(cli.Highlight("Server Commands:"))
	fmt.Println("  PING                Test server connectivity")
	fmt.Println("  AUTH <user> <pwd>   Authenticate with the server")
	fmt.Println()

	fmt.Println(cli.Highlight("SQL Commands (auto-detected):"))
	fmt.Println("  SELECT ...          Query data from tables")
	fmt.Println("  INSERT ...          Insert new rows")
	fmt.Println("  UPDATE ...          Modify existing rows")
	fmt.Println("  DELETE ...          Remove rows")
	fmt.Println("  CREATE TABLE ...    Create a new table")
	fmt.Println("  CREATE INDEX ...    Create an index on a column")
	fmt.Println("  CREATE USER ...     Create a new user")
	fmt.Println("  GRANT ...           Grant permissions to a user")
	fmt.Println()

	fmt.Println(cli.Highlight("Prepared Statements:"))
	fmt.Println("  PREPARE <name> AS <query>   Compile a query for reuse")
	fmt.Println("  EXECUTE <name> [USING ...]  Run a prepared statement")
	fmt.Println("  DEALLOCATE <name>           Remove a prepared statement")
	fmt.Println()

	fmt.Println(cli.Highlight("Transactions:"))
	fmt.Println("  BEGIN               Start a transaction")
	fmt.Println("  COMMIT              Commit the transaction")
	fmt.Println("  ROLLBACK            Rollback the transaction")
	fmt.Println()

	fmt.Println(cli.Highlight("Introspection:"))
	fmt.Println("  INTROSPECT USERS            List all database users")
	fmt.Println("  INTROSPECT DATABASES        Show database information")
	fmt.Println("  INTROSPECT DATABASE <name>  Detailed info for a database")
	fmt.Println("  INTROSPECT TABLES           List all tables with schemas")
	fmt.Println("  INTROSPECT TABLE <name>     Detailed info for a table")
	fmt.Println("  INTROSPECT INDEXES          List all indexes")
	fmt.Println()

	fmt.Println(cli.Dimmed("Tip: SQL commands are auto-detected, no prefix needed."))
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
	// Detect SELECT results (contain row count at the end).
	if strings.HasSuffix(response, "rows)") || strings.HasSuffix(response, "row)") {
		formatSelectResult(response)
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

// formatSelectResult formats SELECT query results as a pretty-printed table.
// It parses the response which contains data rows followed by a row count.
func formatSelectResult(response string) {
	lines := strings.Split(response, "\n")
	if len(lines) == 0 {
		return
	}

	// The last line is the row count (e.g., "(3 rows)")
	rowCountLine := lines[len(lines)-1]
	dataLines := lines[:len(lines)-1]

	// Handle empty result set
	if len(dataLines) == 0 {
		fmt.Println(rowCountLine)
		return
	}

	// Parse all rows to determine column widths
	var rows [][]string
	for _, line := range dataLines {
		if line == "" {
			continue
		}
		cols := strings.Split(line, ", ")
		rows = append(rows, cols)
	}

	if len(rows) == 0 {
		fmt.Println(rowCountLine)
		return
	}

	// Calculate maximum width for each column
	numCols := len(rows[0])
	colWidths := make([]int, numCols)
	for _, row := range rows {
		for i, col := range row {
			if i < numCols && len(col) > colWidths[i] {
				colWidths[i] = len(col)
			}
		}
	}

	// Build the separator line
	var sepParts []string
	for _, width := range colWidths {
		sepParts = append(sepParts, strings.Repeat("-", width+2))
	}
	separator := "+" + strings.Join(sepParts, "+") + "+"

	// Print the table
	fmt.Println(separator)
	for _, row := range rows {
		var rowParts []string
		for i, col := range row {
			if i < numCols {
				// Pad the column value to the required width
				padded := fmt.Sprintf(" %-*s ", colWidths[i], col)
				rowParts = append(rowParts, padded)
			}
		}
		fmt.Println("|" + strings.Join(rowParts, "|") + "|")
	}
	fmt.Println(separator)

	// Print the row count
	fmt.Println(rowCountLine)
}

// formatTable pretty-prints tabular data using aligned columns.
// It converts comma-separated values into a nicely formatted table
// using Go's tabwriter package for column alignment.
func formatTable(data string) {
	// Create a tabwriter for aligned column output.
	// Parameters: output, minwidth, tabwidth, padding, padchar, flags
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	// Process each line of the response.
	lines := strings.Split(data, "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		// Convert comma-space separators to tabs for tabwriter.
		// This assumes values don't contain ", " - a limitation of the simple protocol.
		row := strings.ReplaceAll(line, ", ", "\t")
		fmt.Fprintln(w, row)
	}

	// Flush the tabwriter to ensure all output is written.
	w.Flush()
}
