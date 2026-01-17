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
Package main is the entry point for the FlyDB dump utility (fdump).

FlyDB Dump Utility exports FlyDB databases to various formats for backup,
migration, and data analysis purposes. It supports both local mode (direct
file access) and remote mode (network connection to a running FlyDB server).

Features:
  - Full database dumps with schema and data
  - Table-specific exports
  - Schema-only exports (no data)
  - Data-only exports (no schema)
  - Multiple output formats: SQL, CSV, JSON
  - Import/restore functionality
  - Compression support (gzip)
  - Authentication support (username/password)
  - Encryption support for encrypted databases (local mode)
  - Remote mode with cluster support and automatic failover
  - Leader discovery for write operations in cluster mode

Usage:

	Local mode:  fdump -d <data_dir> [options]
	Remote mode: fdump --host <hostname> [--port <port>] [options]

Connection Options:

	-d <path>               Data directory path (local mode)
	--host <hostname>       Remote server hostname(s), comma-separated for cluster
	--port <port>           Remote server port (default: 8889)
	-db <name>              Database name (default: "default")
	--connect-timeout <dur> Connection timeout (default: 10s)
	--query-timeout <dur>   Query timeout (default: 30s)

Export Options:

	-o <file>           Output file path (default: stdout)
	-f <format>         Output format: sql, csv, json (default: sql)
	-t <tables>         Comma-separated list of tables to dump (default: all)
	--schema-only       Dump schema only, no data
	--data-only         Dump data only, no schema
	--no-owner          Do not output ownership commands
	-z                  Compress output with gzip
	-v                  Verbose output
	--version           Show version information
	--help              Show help

Import Options:

	--import <file>     Import data from SQL dump file

Authentication Options:

	-U <username>       Username for authentication
	-W <password>       Password for authentication
	-P                  Prompt for password interactively

Encryption Options (local mode only):

	--passphrase <pass>     Encryption passphrase for encrypted databases
	--prompt-passphrase     Prompt for encryption passphrase interactively

Environment Variables:

	FLYDB_USER                   Default username for authentication
	FLYDB_ADMIN_PASSWORD         Password for authentication
	FLYDB_ENCRYPTION_PASSPHRASE  Encryption passphrase (local mode)

Examples:

	# Local mode - Full SQL dump
	fdump -d ./data -o backup.sql

	# Local mode - Dump with authentication
	fdump -d ./data -U admin -P -o backup.sql

	# Local mode - Dump encrypted database
	fdump -d ./data --passphrase mysecret -o backup.sql

	# Remote mode - Dump from server
	fdump --host localhost --port 8889 -U admin -P -o backup.sql

	# Remote mode - Cluster dump (connects to any node)
	fdump --host node1.example.com,node2.example.com -U admin -P -o backup.sql

	# Remote mode - Import to cluster (discovers leader)
	fdump --host node1.example.com -U admin -P --import backup.sql

	# Export specific tables as JSON
	fdump --host localhost -t users,orders -f json -o data.json

	# Schema only dump
	fdump --host localhost --schema-only -o schema.sql

	# Compressed dump
	fdump --host localhost -z -o backup.sql.gz

	# Local mode - Import from dump file
	fdump -d ./data --import backup.sql
*/
package main

import (
	"bufio"
	"compress/gzip"
	"crypto/tls"
	"crypto/x509"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"flydb/internal/auth"
	"flydb/internal/protocol"
	"flydb/internal/sql"
	"flydb/internal/storage"
	"flydb/pkg/cli"

	"golang.org/x/term"
)

// Version information
const (
	Version   = "1.0.0"
	BuildDate = "2026-01-09"
)

// Network connection constants
const (
	DefaultPort       = "8889"
	ConnectionTimeout = 10 * time.Second
	PingTimeout       = 5 * time.Second
	QueryTimeout      = 30 * time.Second
	MaxRetries        = 3
	RetryDelay        = 1 * time.Second
	MaxRetryDelay     = 10 * time.Second
)

// Command-line flags
var (
	// Local access flags
	dataDir = flag.String("d", "", "Data directory path (for local access)")

	// Remote connection flags
	host = flag.String("host", "", "Remote server hostname(s) - comma-separated for cluster")
	port = flag.String("port", DefaultPort, "Remote server port number")

	// Common flags
	database    = flag.String("db", "default", "Database name to dump")
	outputFile  = flag.String("o", "", "Output file path (default: stdout)")
	format      = flag.String("f", "sql", "Output format: sql, csv, json")
	tables      = flag.String("t", "", "Comma-separated list of tables to dump (default: all)")
	schemaOnly  = flag.Bool("schema-only", false, "Dump schema only, no data")
	dataOnly    = flag.Bool("data-only", false, "Dump data only, no schema")
	noOwner     = flag.Bool("no-owner", false, "Do not output ownership commands")
	compress    = flag.Bool("z", false, "Compress output with gzip")
	importFile  = flag.String("import", "", "Import data from file")
	verbose     = flag.Bool("v", false, "Verbose output")
	showVersion = flag.Bool("version", false, "Show version information")
	showHelp    = flag.Bool("help", false, "Show help")

	// Authentication flags
	username   = flag.String("U", "", "Username for authentication")
	password   = flag.String("W", "", "Password for authentication (use -P for prompt)")
	promptPass = flag.Bool("P", false, "Prompt for password")

	// Encryption flags (local mode only)
	encryptionPassphrase = flag.String("passphrase", "", "Encryption passphrase for encrypted databases (local mode only)")
	promptPassphrase     = flag.Bool("prompt-passphrase", false, "Prompt for encryption passphrase (local mode only)")

	// Cluster options
	connectTimeout = flag.Duration("connect-timeout", ConnectionTimeout, "Connection timeout for remote connections")
	queryTimeout   = flag.Duration("query-timeout", QueryTimeout, "Query timeout for remote connections")

	// TLS options
	noTLS       = flag.Bool("no-tls", false, "Disable TLS and use plain TCP connection")
	tlsInsecure = flag.Bool("tls-insecure", false, "Skip TLS certificate verification (insecure)")
	tlsCA       = flag.String("tls-ca", "", "Path to CA certificate file for TLS verification")
	tlsCert     = flag.String("tls-cert", "", "Path to client certificate file for mutual TLS")
	tlsKey      = flag.String("tls-key", "", "Path to client key file for mutual TLS")
)

// isRemoteMode returns true if remote connection mode is enabled
func isRemoteMode() bool {
	return *host != ""
}

// isLocalMode returns true if local file access mode is enabled
func isLocalMode() bool {
	return *dataDir != ""
}

func main() {
	// Custom usage function
	flag.Usage = printUsage
	flag.Parse()

	// Handle --version flag
	if *showVersion {
		fmt.Printf("fdump version %s (built %s)\n", Version, BuildDate)
		os.Exit(0)
	}

	// Handle --help flag
	if *showHelp {
		printUsage()
		os.Exit(0)
	}

	// Validate connection mode - must specify either -d (local) or --host (remote)
	if !isLocalMode() && !isRemoteMode() {
		cli.NewCLIError("Connection mode required: specify -d for local access or --host for remote connection").
			WithDetail("Use -d <path> for direct file system access to a FlyDB data directory").
			WithDetail("Use --host <hostname> for remote connection to a FlyDB server").
			WithSuggestion("fdump -d ./data -o backup.sql").
			WithSuggestion("fdump --host localhost --port 8889 -U admin -P -o backup.sql").
			Exit()
	}

	// Validate conflicting connection modes
	if isLocalMode() && isRemoteMode() {
		cli.NewCLIError("Cannot use both -d (local) and --host (remote) together").
			WithDetail("Choose either local file access or remote connection, not both").
			WithSuggestion("Use -d for local access: fdump -d ./data -o backup.sql").
			WithSuggestion("Use --host for remote: fdump --host localhost -o backup.sql").
			Exit()
	}

	// Validate format
	validFormats := map[string]bool{"sql": true, "csv": true, "json": true}
	if !validFormats[*format] {
		cli.NewCLIError(fmt.Sprintf("Invalid format: %s", *format)).
			WithDetail("Supported formats are: sql, csv, json").
			WithSuggestion("Use -f sql for SQL dump format").
			WithSuggestion("Use -f json for JSON export").
			WithSuggestion("Use -f csv for CSV export (creates one file per table)").
			Exit()
	}

	// Validate conflicting options
	if *schemaOnly && *dataOnly {
		cli.NewCLIError("Cannot use --schema-only and --data-only together").
			WithDetail("These options are mutually exclusive").
			WithSuggestion("Use --schema-only to export only table definitions").
			WithSuggestion("Use --data-only to export only data (INSERT statements)").
			Exit()
	}

	// Warn about encryption options in remote mode
	if isRemoteMode() && (*encryptionPassphrase != "" || *promptPassphrase) {
		cli.PrintWarning("Encryption passphrase options are ignored in remote mode")
		cli.PrintInfo("The server handles encryption transparently")
	}

	// Handle import mode
	if *importFile != "" {
		if isRemoteMode() {
			if err := runRemoteImport(); err != nil {
				handleError("Import failed", err)
			}
		} else {
			if err := runImport(); err != nil {
				handleError("Import failed", err)
			}
		}
		os.Exit(0)
	}

	// Run export
	if isRemoteMode() {
		if err := runRemoteExport(); err != nil {
			handleError("Export failed", err)
		}
	} else {
		if err := runExport(); err != nil {
			handleError("Export failed", err)
		}
	}
}

// handleError formats and displays an error with helpful suggestions
func handleError(operation string, err error) {
	errStr := err.Error()

	cliErr := cli.NewCLIError(fmt.Sprintf("%s: %s", operation, errStr))

	// Add context-specific suggestions based on error type
	if strings.Contains(errStr, "database directory not found") {
		cliErr.WithSuggestion("Verify the data directory path is correct")
		cliErr.WithSuggestion("Ensure the database exists: fdump -d ./data -db <database_name>")
	} else if strings.Contains(errStr, "authentication failed") {
		cliErr.WithSuggestion("Check your username and password")
		cliErr.WithSuggestion("Use -P to prompt for password securely")
	} else if strings.Contains(errStr, "passphrase") || strings.Contains(errStr, "decrypt") {
		cliErr.WithSuggestion("Verify the encryption passphrase is correct")
		cliErr.WithSuggestion("Use --prompt-passphrase for secure input")
		cliErr.WithSuggestion("Set FLYDB_ENCRYPTION_PASSPHRASE environment variable")
	} else if strings.Contains(errStr, "permission denied") {
		cliErr.WithSuggestion("Check file and directory permissions")
		cliErr.WithSuggestion("Ensure you have read access to the data directory")
	} else if strings.Contains(errStr, "failed to create") || strings.Contains(errStr, "failed to open") {
		cliErr.WithSuggestion("Check that the output path is writable")
		cliErr.WithSuggestion("Ensure the parent directory exists")
	}

	cliErr.Exit()
}

func printUsage() {
	fmt.Println()
	fmt.Printf("%s - Database export and import tool\n", cli.Highlight("FlyDB Dump Utility v"+Version))
	fmt.Println(cli.Separator(60))
	fmt.Println()

	fmt.Println(cli.Highlight("USAGE:"))
	fmt.Println("  " + cli.Dimmed("Local mode (direct file access):"))
	fmt.Println("    fdump -d <data_dir> [options]")
	fmt.Println("  " + cli.Dimmed("Remote mode (network connection):"))
	fmt.Println("    fdump --host <hostname> [--port <port>] [options]")
	fmt.Println()

	fmt.Println(cli.Highlight("CONNECTION OPTIONS:"))
	fmt.Printf("  %-28s %s\n", cli.Info("-d <path>"), "Data directory path (local mode)")
	fmt.Printf("  %-28s %s\n", cli.Info("--host <hostname>"), "Remote server hostname(s), comma-separated for cluster")
	fmt.Printf("  %-28s %s\n", cli.Info("--port <port>"), "Remote server port (default: "+DefaultPort+")")
	fmt.Printf("  %-28s %s\n", cli.Info("-db <name>"), "Database name (default: default)")
	fmt.Printf("  %-28s %s\n", cli.Info("--connect-timeout <dur>"), "Connection timeout (default: 10s)")
	fmt.Printf("  %-28s %s\n", cli.Info("--query-timeout <dur>"), "Query timeout (default: 30s)")
	fmt.Println()

	fmt.Println(cli.Highlight("EXPORT OPTIONS:"))
	fmt.Printf("  %-28s %s\n", cli.Info("-o <file>"), "Output file path (default: stdout)")
	fmt.Printf("  %-28s %s\n", cli.Info("-f <format>"), "Output format: sql, csv, json (default: sql)")
	fmt.Printf("  %-28s %s\n", cli.Info("-t <tables>"), "Comma-separated list of tables (default: all)")
	fmt.Printf("  %-28s %s\n", cli.Info("--schema-only"), "Dump schema only, no data")
	fmt.Printf("  %-28s %s\n", cli.Info("--data-only"), "Dump data only, no schema")
	fmt.Printf("  %-28s %s\n", cli.Info("--no-owner"), "Do not output ownership commands")
	fmt.Printf("  %-28s %s\n", cli.Info("-z"), "Compress output with gzip")
	fmt.Println()

	fmt.Println(cli.Highlight("IMPORT OPTIONS:"))
	fmt.Printf("  %-28s %s\n", cli.Info("--import <file>"), "Import data from SQL dump file")
	fmt.Println()

	fmt.Println(cli.Highlight("AUTHENTICATION:"))
	fmt.Printf("  %-28s %s\n", cli.Info("-U <username>"), "Username for authentication")
	fmt.Printf("  %-28s %s\n", cli.Info("-W <password>"), "Password (use -P for secure prompt)")
	fmt.Printf("  %-28s %s\n", cli.Info("-P"), "Prompt for password interactively")
	fmt.Println()

	fmt.Println(cli.Highlight("TLS OPTIONS (remote mode):"))
	fmt.Printf("  %-28s %s\n", cli.Info("--no-tls"), "Disable TLS (use plain TCP)")
	fmt.Printf("  %-28s %s\n", cli.Info("--tls-insecure"), "Skip TLS certificate verification (insecure)")
	fmt.Printf("  %-28s %s\n", cli.Info("--tls-ca <file>"), "Path to CA certificate file for TLS verification")
	fmt.Printf("  %-28s %s\n", cli.Info("--tls-cert <file>"), "Path to client certificate file for mutual TLS")
	fmt.Printf("  %-28s %s\n", cli.Info("--tls-key <file>"), "Path to client key file for mutual TLS")
	fmt.Println()

	fmt.Println(cli.Highlight("ENCRYPTION (local mode only):"))
	fmt.Printf("  %-28s %s\n", cli.Info("--passphrase <pass>"), "Encryption passphrase")
	fmt.Printf("  %-28s %s\n", cli.Info("--prompt-passphrase"), "Prompt for passphrase interactively")
	fmt.Println()

	fmt.Println(cli.Highlight("OTHER OPTIONS:"))
	fmt.Printf("  %-28s %s\n", cli.Info("-v"), "Verbose output")
	fmt.Printf("  %-28s %s\n", cli.Info("--version"), "Show version information")
	fmt.Printf("  %-28s %s\n", cli.Info("--help"), "Show this help message")
	fmt.Println()

	fmt.Println(cli.Highlight("EXAMPLES:"))
	fmt.Println()
	fmt.Println("  " + cli.Dimmed("# Local mode - Full SQL dump"))
	fmt.Println("  " + cli.Info("fdump -d ./data -o backup.sql"))
	fmt.Println()
	fmt.Println("  " + cli.Dimmed("# Local mode - Dump with authentication"))
	fmt.Println("  " + cli.Info("fdump -d ./data -U admin -P -o backup.sql"))
	fmt.Println()
	fmt.Println("  " + cli.Dimmed("# Local mode - Dump encrypted database"))
	fmt.Println("  " + cli.Info("fdump -d ./data --passphrase secret -o backup.sql"))
	fmt.Println()
	fmt.Println("  " + cli.Dimmed("# Remote mode - Dump from server"))
	fmt.Println("  " + cli.Info("fdump --host localhost --port 8889 -U admin -P -o backup.sql"))
	fmt.Println()
	fmt.Println("  " + cli.Dimmed("# Remote mode - Cluster dump (connects to any node)"))
	fmt.Println("  " + cli.Info("fdump --host node1.example.com,node2.example.com -U admin -P -o backup.sql"))
	fmt.Println()
	fmt.Println("  " + cli.Dimmed("# Remote mode - Import to cluster (discovers leader)"))
	fmt.Println("  " + cli.Info("fdump --host node1.example.com -U admin -P --import backup.sql"))
	fmt.Println()
	fmt.Println("  " + cli.Dimmed("# Export specific tables as JSON"))
	fmt.Println("  " + cli.Info("fdump --host localhost -t users,orders -f json -o data.json"))
	fmt.Println()
	fmt.Println("  " + cli.Dimmed("# Schema only dump"))
	fmt.Println("  " + cli.Info("fdump --host localhost --schema-only -o schema.sql"))
	fmt.Println()
	fmt.Println("  " + cli.Dimmed("# Compressed dump"))
	fmt.Println("  " + cli.Info("fdump --host localhost -z -o backup.sql.gz"))
	fmt.Println()
	fmt.Println("  " + cli.Dimmed("# Local mode - Import from dump file"))
	fmt.Println("  " + cli.Info("fdump -d ./data --import backup.sql"))
	fmt.Println()

	fmt.Println(cli.Highlight("ENVIRONMENT VARIABLES:"))
	fmt.Println()
	fmt.Printf("  %-36s %s\n", cli.Info("FLYDB_USER"), "Default username for authentication")
	fmt.Printf("  %-36s %s\n", cli.Info("FLYDB_ADMIN_PASSWORD"), "Password for authentication")
	fmt.Printf("  %-36s %s\n", cli.Info("FLYDB_ENCRYPTION_PASSPHRASE"), "Encryption passphrase (local mode)")
	fmt.Println()

	fmt.Println("  " + cli.Dimmed("For more information, visit: https://github.com/firefly-oss/flydb"))
	fmt.Println()
}

// promptForPassword prompts the user for a password securely
func promptForPassword(prompt string) (string, error) {
	fmt.Fprint(os.Stderr, prompt)
	password, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Fprintln(os.Stderr) // Print newline after password input
	if err != nil {
		return "", err
	}
	return string(password), nil
}

// getPassword returns the password from flags, environment, or prompt
func getPassword() (string, error) {
	if *password != "" {
		return *password, nil
	}
	if envPass := os.Getenv("FLYDB_ADMIN_PASSWORD"); envPass != "" {
		return envPass, nil
	}
	if *promptPass {
		return promptForPassword("Password: ")
	}
	return "", nil
}

// getEncryptionPassphrase returns the passphrase from flags, environment, or prompt
func getEncryptionPassphrase() (string, error) {
	if *encryptionPassphrase != "" {
		return *encryptionPassphrase, nil
	}
	if envPass := os.Getenv("FLYDB_ENCRYPTION_PASSPHRASE"); envPass != "" {
		return envPass, nil
	}
	if *promptPassphrase {
		return promptForPassword("Encryption passphrase: ")
	}
	return "", nil
}

// createStorageConfig creates a storage configuration with encryption if needed
func createStorageConfig(dbPath string) (storage.StorageConfig, error) {
	config := storage.StorageConfig{
		DataDir: dbPath,
	}

	passphrase, err := getEncryptionPassphrase()
	if err != nil {
		return config, fmt.Errorf("failed to get encryption passphrase: %w", err)
	}

	if passphrase != "" {
		config.Encryption = storage.EncryptionConfig{
			Enabled:    true,
			Passphrase: passphrase,
		}
	}

	return config, nil
}

// authenticateIfNeeded performs authentication if credentials are provided
func authenticateIfNeeded(store storage.Engine) error {
	user := *username
	if user == "" {
		user = os.Getenv("FLYDB_USER")
	}
	if user == "" {
		// No authentication requested
		return nil
	}

	pass, err := getPassword()
	if err != nil {
		return fmt.Errorf("failed to get password: %w", err)
	}

	authMgr := auth.NewAuthManager(store)
	if !authMgr.Authenticate(user, pass) {
		return fmt.Errorf("authentication failed for user '%s'", user)
	}

	if *verbose {
		cli.PrintSuccess("Authenticated as user: %s", user)
	}

	return nil
}

// Dumper handles database export operations
type Dumper struct {
	store      storage.Engine
	catalog    *sql.Catalog
	tables     []string
	format     string
	writer     io.Writer
	verbose    bool
	tableCount int
	rowCount   int
	startTime  time.Time
	toStdout   bool
}

// NewDumper creates a new Dumper instance
func NewDumper(store storage.Engine, catalog *sql.Catalog) *Dumper {
	return &Dumper{
		store:   store,
		catalog: catalog,
		format:  "sql",
		writer:  os.Stdout,
	}
}

// runExport performs the database export
func runExport() error {
	startTime := time.Now()

	// Determine if outputting to stdout (no progress indicators in that case)
	toStdout := *outputFile == "" || *outputFile == "-"

	// Open the database with spinner
	dbPath := filepath.Join(*dataDir, *database)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("database directory not found: %s", dbPath)
	}

	var spinner *cli.Spinner
	if !toStdout {
		spinner = cli.NewSpinner(fmt.Sprintf("Opening database '%s'...", *database))
		spinner.Start()
	}

	// Initialize storage engine with encryption support
	config, err := createStorageConfig(dbPath)
	if err != nil {
		if spinner != nil {
			spinner.StopWithError("Failed to configure storage")
		}
		return err
	}

	store, err := storage.NewStorageEngine(config)
	if err != nil {
		if spinner != nil {
			spinner.StopWithError("Failed to open database")
		}
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer store.Close()

	// Authenticate if credentials provided
	if spinner != nil {
		spinner.UpdateMessage("Authenticating...")
	}
	if err := authenticateIfNeeded(store); err != nil {
		if spinner != nil {
			spinner.StopWithError("Authentication failed")
		}
		return err
	}

	if spinner != nil {
		spinner.StopWithSuccess(fmt.Sprintf("Connected to database '%s'", *database))
	}

	// Initialize catalog
	catalog := sql.NewCatalog(store)

	// Create dumper
	dumper := NewDumper(store, catalog)
	dumper.format = *format
	dumper.verbose = *verbose
	dumper.startTime = startTime
	dumper.toStdout = toStdout

	// Parse table list
	if *tables != "" {
		dumper.tables = strings.Split(*tables, ",")
		for i, t := range dumper.tables {
			dumper.tables[i] = strings.TrimSpace(t)
		}
	}

	// Set up output writer
	var output io.WriteCloser
	if toStdout {
		output = os.Stdout
	} else {
		f, err := os.Create(*outputFile)
		if err != nil {
			return fmt.Errorf("failed to create output file: %w", err)
		}
		defer f.Close()
		output = f
	}

	// Apply compression if requested
	if *compress {
		gzWriter := gzip.NewWriter(output)
		defer gzWriter.Close()
		dumper.writer = gzWriter
	} else {
		dumper.writer = output
	}

	// Show export info
	if !toStdout {
		tablesToDump := dumper.getTablesToDump()
		tableInfo := "all tables"
		if len(dumper.tables) > 0 {
			tableInfo = fmt.Sprintf("%d table(s)", len(dumper.tables))
		} else {
			tableInfo = fmt.Sprintf("%d table(s)", len(tablesToDump))
		}

		formatInfo := strings.ToUpper(*format)
		if *compress {
			formatInfo += " (gzip)"
		}

		cli.PrintInfo("Exporting %s in %s format...", tableInfo, formatInfo)
	}

	// Start export spinner
	if !toStdout {
		spinner = cli.NewSpinner("Exporting data...")
		spinner.Start()
	}

	// Perform the dump based on format
	var dumpErr error
	switch dumper.format {
	case "sql":
		dumpErr = dumper.dumpSQL()
	case "json":
		dumpErr = dumper.dumpJSON()
	case "csv":
		dumpErr = dumper.dumpCSV()
	default:
		if spinner != nil {
			spinner.StopWithError("Unsupported format")
		}
		return fmt.Errorf("unsupported format: %s", dumper.format)
	}

	if dumpErr != nil {
		if spinner != nil {
			spinner.StopWithError("Export failed")
		}
		return dumpErr
	}

	// Print summary (only if not outputting to stdout)
	if !toStdout {
		if spinner != nil {
			spinner.StopWithSuccess("Export completed successfully")
		}

		elapsed := time.Since(startTime)
		fmt.Println()
		cli.Box("Export Summary", fmt.Sprintf(
			"%s %d tables, %d rows\n%s %s\n%s %s\n%s %v",
			cli.Dimmed("Exported:"), dumper.tableCount, dumper.rowCount,
			cli.Dimmed("Format:"), strings.ToUpper(*format),
			cli.Dimmed("Output:"), *outputFile,
			cli.Dimmed("Duration:"), elapsed.Round(time.Millisecond),
		))
	}

	return nil
}

// getTablesToDump returns the list of tables to dump
func (d *Dumper) getTablesToDump() []string {
	if len(d.tables) > 0 {
		return d.tables
	}
	var tableNames []string
	for name := range d.catalog.Tables {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)
	return tableNames
}

// dumpSQL exports the database in SQL format
func (d *Dumper) dumpSQL() error {
	fmt.Fprintf(d.writer, "-- FlyDB Database Dump\n")
	fmt.Fprintf(d.writer, "-- Generated: %s\n", time.Now().Format(time.RFC3339))
	fmt.Fprintf(d.writer, "-- Database: %s\n", *database)
	fmt.Fprintf(d.writer, "--\n\n")

	tablesToDump := d.getTablesToDump()

	if !*dataOnly {
		fmt.Fprintf(d.writer, "-- Schema\n\n")
		for _, tableName := range tablesToDump {
			if err := d.dumpTableSchema(tableName); err != nil {
				return err
			}
		}
	}

	if !*schemaOnly {
		fmt.Fprintf(d.writer, "\n-- Data\n\n")
		for _, tableName := range tablesToDump {
			if err := d.dumpTableData(tableName); err != nil {
				return err
			}
		}
	}

	fmt.Fprintf(d.writer, "\n-- Dump completed\n")
	return nil
}

// dumpTableSchema writes the CREATE TABLE statement for a table
func (d *Dumper) dumpTableSchema(tableName string) error {
	table, ok := d.catalog.GetTable(tableName)
	if !ok {
		return fmt.Errorf("table not found: %s", tableName)
	}

	fmt.Fprintf(d.writer, "CREATE TABLE %s (\n", tableName)

	for i, col := range table.Columns {
		fmt.Fprintf(d.writer, "    %s %s", col.Name, col.Type)
		if col.IsNotNull() && !col.IsPrimaryKey() {
			fmt.Fprintf(d.writer, " NOT NULL")
		}
		if col.IsPrimaryKey() {
			fmt.Fprintf(d.writer, " PRIMARY KEY")
		}
		if col.IsUnique() && !col.IsPrimaryKey() {
			fmt.Fprintf(d.writer, " UNIQUE")
		}
		if col.IsAutoIncrement() && col.Type != "SERIAL" {
			fmt.Fprintf(d.writer, " AUTO_INCREMENT")
		}
		if defaultVal, hasDefault := col.GetDefaultValue(); hasDefault {
			fmt.Fprintf(d.writer, " DEFAULT %s", defaultVal)
		}
		// Handle foreign key references
		if fk := col.GetForeignKey(); fk != nil {
			fmt.Fprintf(d.writer, " REFERENCES %s(%s)", fk.Table, fk.Column)
			if fk.OnDelete != "" {
				fmt.Fprintf(d.writer, " ON DELETE %s", fk.OnDelete)
			}
			if fk.OnUpdate != "" {
				fmt.Fprintf(d.writer, " ON UPDATE %s", fk.OnUpdate)
			}
		}
		if i < len(table.Columns)-1 || len(table.Constraints) > 0 {
			fmt.Fprintf(d.writer, ",")
		}
		fmt.Fprintf(d.writer, "\n")
	}

	for i, constraint := range table.Constraints {
		switch string(constraint.Type) {
		case "PRIMARY KEY":
			fmt.Fprintf(d.writer, "    PRIMARY KEY (%s)", strings.Join(constraint.Columns, ", "))
		case "UNIQUE":
			fmt.Fprintf(d.writer, "    UNIQUE (%s)", strings.Join(constraint.Columns, ", "))
		case "FOREIGN KEY":
			if constraint.ForeignKey != nil {
				fmt.Fprintf(d.writer, "    FOREIGN KEY (%s) REFERENCES %s(%s)",
					strings.Join(constraint.Columns, ", "), constraint.ForeignKey.Table,
					constraint.ForeignKey.Column)
				if constraint.ForeignKey.OnDelete != "" {
					fmt.Fprintf(d.writer, " ON DELETE %s", constraint.ForeignKey.OnDelete)
				}
				if constraint.ForeignKey.OnUpdate != "" {
					fmt.Fprintf(d.writer, " ON UPDATE %s", constraint.ForeignKey.OnUpdate)
				}
			}
		}
		if i < len(table.Constraints)-1 {
			fmt.Fprintf(d.writer, ",")
		}
		fmt.Fprintf(d.writer, "\n")
	}

	fmt.Fprintf(d.writer, ");\n\n")
	if d.verbose && !d.toStdout {
		cli.PrintInfo("Exported schema: %s (%d columns)", tableName, len(table.Columns))
	}
	return nil
}

// dumpTableData writes INSERT statements for all rows in a table
func (d *Dumper) dumpTableData(tableName string) error {
	table, ok := d.catalog.GetTable(tableName)
	if !ok {
		return fmt.Errorf("table not found: %s", tableName)
	}

	prefix := "row:" + tableName + ":"
	rows, err := d.store.Scan(prefix)
	if err != nil {
		return fmt.Errorf("failed to scan table %s: %w", tableName, err)
	}

	if len(rows) == 0 {
		d.tableCount++
		return nil
	}

	colNames := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		colNames[i] = col.Name
	}

	tableRowCount := 0
	for _, rowData := range rows {
		var row map[string]interface{}
		if err := json.Unmarshal(rowData, &row); err != nil {
			continue
		}

		values := make([]string, len(colNames))
		for i, colName := range colNames {
			val, exists := row[colName]
			if !exists || val == nil {
				values[i] = "NULL"
			} else {
				values[i] = formatSQLValue(val)
			}
		}

		fmt.Fprintf(d.writer, "INSERT INTO %s (%s) VALUES (%s);\n",
			tableName, strings.Join(colNames, ", "), strings.Join(values, ", "))
		tableRowCount++
	}

	// Update global counters
	d.tableCount++
	d.rowCount += tableRowCount

	if d.verbose && !d.toStdout {
		cli.PrintSuccess("Exported data: %s (%d rows)", tableName, tableRowCount)
	}
	return nil
}

// formatSQLValue formats a value for SQL INSERT statement
func formatSQLValue(val interface{}) string {
	switch v := val.(type) {
	case string:
		escaped := strings.ReplaceAll(v, "'", "''")
		return fmt.Sprintf("'%s'", escaped)
	case float64:
		if v == float64(int64(v)) {
			return fmt.Sprintf("%d", int64(v))
		}
		return fmt.Sprintf("%g", v)
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	case nil:
		return "NULL"
	default:
		return fmt.Sprintf("'%v'", v)
	}
}

// dumpJSON exports the database in JSON format
func (d *Dumper) dumpJSON() error {
	result := make(map[string]interface{})
	result["database"] = *database
	result["generated"] = time.Now().Format(time.RFC3339)
	result["format"] = "json"

	tablesToDump := d.getTablesToDump()
	tablesData := make(map[string]interface{})

	for _, tableName := range tablesToDump {
		tableData := make(map[string]interface{})

		if !*dataOnly {
			table, ok := d.catalog.GetTable(tableName)
			if ok {
				schema := make(map[string]interface{})
				cols := make([]map[string]interface{}, len(table.Columns))
				for i, col := range table.Columns {
					defaultVal, _ := col.GetDefaultValue()
					cols[i] = map[string]interface{}{
						"name":           col.Name,
						"type":           col.Type,
						"not_null":       col.IsNotNull(),
						"primary_key":    col.IsPrimaryKey(),
						"unique":         col.IsUnique(),
						"auto_increment": col.IsAutoIncrement(),
						"default":        defaultVal,
					}
				}
				schema["columns"] = cols
				tableData["schema"] = schema
			}
		}

		if !*schemaOnly {
			prefix := "row:" + tableName + ":"
			rows, err := d.store.Scan(prefix)
			if err == nil {
				rowsData := make([]map[string]interface{}, 0, len(rows))
				for _, rowData := range rows {
					var row map[string]interface{}
					if err := json.Unmarshal(rowData, &row); err == nil {
						rowsData = append(rowsData, row)
					}
				}
				tableData["rows"] = rowsData
			}
		}

		tablesData[tableName] = tableData
	}

	result["tables"] = tablesData

	encoder := json.NewEncoder(d.writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

// dumpCSV exports each table to a separate CSV file
func (d *Dumper) dumpCSV() error {
	tablesToDump := d.getTablesToDump()

	// For CSV, if output is a directory, create one file per table
	outputDir := *outputFile
	if outputDir == "" {
		outputDir = "."
	}

	for _, tableName := range tablesToDump {
		table, ok := d.catalog.GetTable(tableName)
		if !ok {
			continue
		}

		// Create CSV file for this table
		csvPath := filepath.Join(outputDir, tableName+".csv")
		f, err := os.Create(csvPath)
		if err != nil {
			return fmt.Errorf("failed to create CSV file for %s: %w", tableName, err)
		}

		writer := csv.NewWriter(f)

		// Write header
		colNames := make([]string, len(table.Columns))
		for i, col := range table.Columns {
			colNames[i] = col.Name
		}
		if err := writer.Write(colNames); err != nil {
			f.Close()
			return err
		}

		// Write data rows
		prefix := "row:" + tableName + ":"
		rows, err := d.store.Scan(prefix)
		if err == nil {
			for _, rowData := range rows {
				var row map[string]interface{}
				if err := json.Unmarshal(rowData, &row); err != nil {
					continue
				}

				values := make([]string, len(colNames))
				for i, colName := range colNames {
					val, exists := row[colName]
					if !exists || val == nil {
						values[i] = ""
					} else {
						values[i] = fmt.Sprintf("%v", val)
					}
				}
				if err := writer.Write(values); err != nil {
					f.Close()
					return err
				}
			}
		}

		writer.Flush()
		f.Close()

		d.tableCount++

		if d.verbose && !d.toStdout {
			cli.PrintSuccess("Exported: %s → %s", tableName, csvPath)
		}
	}

	return nil
}

// runImport imports data from a SQL dump file
func runImport() error {
	startTime := time.Now()

	// Validate import file exists
	if _, err := os.Stat(*importFile); os.IsNotExist(err) {
		return fmt.Errorf("import file not found: %s", *importFile)
	}

	// Start connection spinner
	spinner := cli.NewSpinner(fmt.Sprintf("Opening database '%s'...", *database))
	spinner.Start()

	// Open the database
	dbPath := filepath.Join(*dataDir, *database)

	// Create database directory if it doesn't exist
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		spinner.StopWithError("Failed to create database directory")
		return fmt.Errorf("failed to create database directory: %w", err)
	}

	// Initialize storage engine with encryption support
	config, err := createStorageConfig(dbPath)
	if err != nil {
		spinner.StopWithError("Failed to configure storage")
		return err
	}

	store, err := storage.NewStorageEngine(config)
	if err != nil {
		spinner.StopWithError("Failed to open database")
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer store.Close()

	// Authenticate if credentials provided
	spinner.UpdateMessage("Authenticating...")
	if err := authenticateIfNeeded(store); err != nil {
		spinner.StopWithError("Authentication failed")
		return err
	}

	spinner.StopWithSuccess(fmt.Sprintf("Connected to database '%s'", *database))

	// Open import file
	f, err := os.Open(*importFile)
	if err != nil {
		return fmt.Errorf("failed to open import file: %w", err)
	}
	defer f.Close()

	// Get file info for display
	fileInfo, _ := f.Stat()
	fileSize := fileInfo.Size()
	fileSizeStr := formatFileSize(fileSize)

	// Check if file is gzipped
	var reader io.Reader = f
	isCompressed := strings.HasSuffix(*importFile, ".gz")
	if isCompressed {
		gzReader, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("failed to decompress file: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}

	// Show import info
	importInfo := filepath.Base(*importFile)
	if isCompressed {
		importInfo += " (compressed)"
	}
	cli.PrintInfo("Importing from %s (%s)...", importInfo, fileSizeStr)

	// Start import spinner
	spinner = cli.NewSpinner("Importing statements...")
	spinner.Start()

	// Initialize executor (it creates its own catalog)
	executor := sql.NewExecutor(store, nil)

	// Read and execute SQL statements
	sqlScanner := NewSQLScanner(reader)
	stmtCount := 0
	errorCount := 0
	createCount := 0
	insertCount := 0

	for sqlScanner.Scan() {
		stmtText := strings.TrimSpace(sqlScanner.Text())
		if stmtText == "" || strings.HasPrefix(stmtText, "--") {
			continue
		}

		// Parse the SQL statement
		lexer := sql.NewLexer(stmtText)
		parser := sql.NewParser(lexer)
		stmt, err := parser.Parse()
		if err != nil {
			errorCount++
			if *verbose {
				spinner.Stop()
				cli.PrintWarning("Parse error: %v", err)
				spinner = cli.NewSpinner(fmt.Sprintf("Importing statements... (%d executed)", stmtCount))
				spinner.Start()
			}
			continue
		}

		_, err = executor.Execute(stmt)
		if err != nil {
			errorCount++
			if *verbose {
				spinner.Stop()
				cli.PrintWarning("Execute error: %v", err)
				spinner = cli.NewSpinner(fmt.Sprintf("Importing statements... (%d executed)", stmtCount))
				spinner.Start()
			}
			continue
		}
		stmtCount++

		// Track statement types for summary
		upperStmt := strings.ToUpper(stmtText)
		if strings.HasPrefix(upperStmt, "CREATE") {
			createCount++
		} else if strings.HasPrefix(upperStmt, "INSERT") {
			insertCount++
		}

		// Update spinner periodically
		if stmtCount%100 == 0 {
			spinner.UpdateMessage(fmt.Sprintf("Importing statements... (%d executed)", stmtCount))
		}
	}

	if err := sqlScanner.Err(); err != nil {
		spinner.StopWithError("Error reading import file")
		return fmt.Errorf("error reading import file: %w", err)
	}

	elapsed := time.Since(startTime)

	// Show completion status
	if errorCount > 0 {
		spinner.StopWithWarning(fmt.Sprintf("Import completed with %d error(s)", errorCount))
	} else {
		spinner.StopWithSuccess("Import completed successfully")
	}

	// Print summary using Box
	fmt.Println()
	summaryContent := fmt.Sprintf(
		"%s %d statements executed\n%s %d CREATE, %d INSERT",
		cli.Dimmed("Imported:"), stmtCount,
		cli.Dimmed("Breakdown:"), createCount, insertCount,
	)
	if errorCount > 0 {
		summaryContent += fmt.Sprintf("\n%s %d statements", cli.Warning("Skipped:"), errorCount)
	}
	summaryContent += fmt.Sprintf("\n%s %s\n%s %v",
		cli.Dimmed("Source:"), *importFile,
		cli.Dimmed("Duration:"), elapsed.Round(time.Millisecond),
	)
	cli.Box("Import Summary", summaryContent)

	return nil
}

// formatFileSize formats a file size in bytes to a human-readable string
func formatFileSize(size int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case size >= GB:
		return fmt.Sprintf("%.2f GB", float64(size)/float64(GB))
	case size >= MB:
		return fmt.Sprintf("%.2f MB", float64(size)/float64(MB))
	case size >= KB:
		return fmt.Sprintf("%.2f KB", float64(size)/float64(KB))
	default:
		return fmt.Sprintf("%d bytes", size)
	}
}

// SQLScanner reads SQL statements from a reader
type SQLScanner struct {
	reader  *strings.Builder
	scanner *bufio.Scanner
	current string
}

// NewSQLScanner creates a new SQL scanner
func NewSQLScanner(r io.Reader) *SQLScanner {
	return &SQLScanner{
		reader:  &strings.Builder{},
		scanner: bufio.NewScanner(r),
	}
}

// Scan reads the next SQL statement
func (s *SQLScanner) Scan() bool {
	s.reader.Reset()

	for s.scanner.Scan() {
		line := s.scanner.Text()

		// Skip comments and empty lines
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}

		s.reader.WriteString(line)
		s.reader.WriteString(" ")

		// Check if statement is complete (ends with semicolon)
		if strings.HasSuffix(trimmed, ";") {
			s.current = strings.TrimSuffix(s.reader.String(), " ")
			return true
		}
	}

	// Handle any remaining content
	if s.reader.Len() > 0 {
		s.current = s.reader.String()
		return true
	}

	return false
}

// Text returns the current SQL statement
func (s *SQLScanner) Text() string {
	return s.current
}

// Err returns any error from scanning
func (s *SQLScanner) Err() error {
	return s.scanner.Err()
}

// =============================================================================
// Binary Protocol Client for Remote Connections
// =============================================================================

// BinaryClient wraps the binary protocol connection for remote database access.
type BinaryClient struct {
	conn       net.Conn
	reader     *bufio.Reader
	writer     *bufio.Writer
	authed     bool
	serverAddr string
	database   string
}

// NewBinaryClient creates a new binary protocol client.
func NewBinaryClient(conn net.Conn, serverAddr string) *BinaryClient {
	return &BinaryClient{
		conn:       conn,
		reader:     bufio.NewReader(conn),
		writer:     bufio.NewWriter(conn),
		authed:     false,
		serverAddr: serverAddr,
		database:   *database,
	}
}

// Ping sends a ping message and waits for pong with timeout.
func (c *BinaryClient) Ping() error {
	c.conn.SetReadDeadline(time.Now().Add(PingTimeout))
	defer c.conn.SetReadDeadline(time.Time{})

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
		return fmt.Errorf("unexpected response type: expected PONG, got %d", msg.Header.Type)
	}

	return nil
}

// Auth authenticates with the server.
func (c *BinaryClient) Auth(username, password string) error {
	authMsg := &protocol.AuthMessage{
		Username: username,
		Password: password,
		Database: c.database,
	}
	data, err := authMsg.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode auth message: %w", err)
	}

	if err := protocol.WriteMessage(c.writer, protocol.MsgAuth, data); err != nil {
		return fmt.Errorf("failed to send auth message: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush auth message: %w", err)
	}

	msg, err := protocol.ReadMessage(c.reader)
	if err != nil {
		return fmt.Errorf("failed to read auth response: %w", err)
	}

	if msg.Header.Type == protocol.MsgError {
		errMsg, _ := protocol.DecodeErrorMessage(msg.Payload)
		return fmt.Errorf("authentication failed: %s", errMsg.Message)
	}

	if msg.Header.Type != protocol.MsgAuthResult {
		return fmt.Errorf("unexpected response type: %d", msg.Header.Type)
	}

	result, err := protocol.DecodeAuthResultMessage(msg.Payload)
	if err != nil {
		return fmt.Errorf("failed to decode auth result: %w", err)
	}

	if !result.Success {
		return fmt.Errorf("authentication failed: %s", result.Message)
	}

	c.authed = true
	return nil
}

// Query executes a SQL query and returns the result message.
func (c *BinaryClient) Query(query string) (string, error) {
	c.conn.SetReadDeadline(time.Now().Add(*queryTimeout))
	defer c.conn.SetReadDeadline(time.Time{})

	queryMsg := &protocol.QueryMessage{
		Query: query,
	}
	data, err := queryMsg.Encode()
	if err != nil {
		return "", fmt.Errorf("failed to encode query: %w", err)
	}

	if err := protocol.WriteMessage(c.writer, protocol.MsgQuery, data); err != nil {
		return "", fmt.Errorf("failed to send query: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return "", fmt.Errorf("failed to flush query: %w", err)
	}

	msg, err := protocol.ReadMessage(c.reader)
	if err != nil {
		return "", fmt.Errorf("failed to read query response: %w", err)
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
		return "", fmt.Errorf("failed to decode query result: %w", err)
	}

	if !result.Success {
		return "", fmt.Errorf("%s", result.Message)
	}

	return result.Message, nil
}

// QueryWithResult executes a SQL query and returns the full result with rows.
func (c *BinaryClient) QueryWithResult(query string) (*protocol.QueryResultMessage, error) {
	c.conn.SetReadDeadline(time.Now().Add(*queryTimeout))
	defer c.conn.SetReadDeadline(time.Time{})

	queryMsg := &protocol.QueryMessage{
		Query: query,
	}
	data, err := queryMsg.Encode()
	if err != nil {
		return nil, fmt.Errorf("failed to encode query: %w", err)
	}

	if err := protocol.WriteMessage(c.writer, protocol.MsgQuery, data); err != nil {
		return nil, fmt.Errorf("failed to send query: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush query: %w", err)
	}

	msg, err := protocol.ReadMessage(c.reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read query response: %w", err)
	}

	if msg.Header.Type == protocol.MsgError {
		errMsg, _ := protocol.DecodeErrorMessage(msg.Payload)
		return nil, fmt.Errorf("%s", errMsg.Message)
	}

	if msg.Header.Type != protocol.MsgQueryResult {
		return nil, fmt.Errorf("unexpected response type: %d", msg.Header.Type)
	}

	result, err := protocol.DecodeQueryResultMessage(msg.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to decode query result: %w", err)
	}

	return result, nil
}

// Close closes the connection.
func (c *BinaryClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
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

// =============================================================================
// HA Client for Cluster Support
// =============================================================================

// HAClient wraps BinaryClient with HA/cluster failover support.
type HAClient struct {
	*BinaryClient
	hosts          []string
	currentHostIdx int
	authUsername   string
	authPassword   string
	lastAuth       bool
	useTLS         bool   // Whether to use TLS for connections
	tlsInsecure    bool   // Whether to skip TLS certificate verification
	tlsCA          string // Path to CA certificate file
	tlsCert        string // Path to client certificate file
	tlsKey         string // Path to client key file
}

// NewHAClient creates a new HA-aware client that can fail over between hosts.
func NewHAClient(hosts []string, useTLS bool, tlsInsecure bool, tlsCA, tlsCert, tlsKey string) *HAClient {
	return &HAClient{
		hosts:       hosts,
		useTLS:      useTLS,
		tlsInsecure: tlsInsecure,
		tlsCA:       tlsCA,
		tlsCert:     tlsCert,
		tlsKey:      tlsKey,
	}
}

// Connect establishes connection to one of the configured hosts.
func (h *HAClient) Connect() error {
	var lastErr error

	for i, hostAddr := range h.hosts {
		var conn net.Conn
		var err error

		if h.useTLS {
			// Create TLS configuration
			tlsConfig := &tls.Config{
				ServerName: extractHostname(hostAddr),
			}

			if h.tlsInsecure {
				tlsConfig.InsecureSkipVerify = true
			} else {
				// Load CA certificates
				var certPool *x509.CertPool
				if h.tlsCA != "" {
					caData, err := os.ReadFile(h.tlsCA)
					if err != nil {
						lastErr = fmt.Errorf("failed to read CA certificate: %w", err)
						continue
					}
					certPool = x509.NewCertPool()
					if !certPool.AppendCertsFromPEM(caData) {
						lastErr = fmt.Errorf("failed to append CA certificate")
						continue
					}
				} else {
					// Load system CA certificates
					var err error
					certPool, err = x509.SystemCertPool()
					if err != nil {
						certPool = x509.NewCertPool()
					}
				}
				tlsConfig.RootCAs = certPool
			}

			// Load client certificate if provided
			if h.tlsCert != "" && h.tlsKey != "" {
				cert, err := tls.LoadX509KeyPair(h.tlsCert, h.tlsKey)
				if err != nil {
					lastErr = fmt.Errorf("failed to load client certificate/key: %w", err)
					continue
				}
				tlsConfig.Certificates = []tls.Certificate{cert}
			}

			// Dial with TLS
			dialer := &net.Dialer{Timeout: *connectTimeout}
			conn, err = tls.DialWithDialer(dialer, "tcp", hostAddr, tlsConfig)
		} else {
			// Plain TCP connection
			conn, err = net.DialTimeout("tcp", hostAddr, *connectTimeout)
		}

		if err != nil {
			lastErr = err
			if *verbose {
				cli.PrintWarning("Failed to connect to %s: %v", hostAddr, err)
			}
			continue
		}

		h.BinaryClient = NewBinaryClient(conn, hostAddr)
		h.currentHostIdx = i

		// Validate connection with a PING
		if err := h.Ping(); err != nil {
			h.BinaryClient.Close()
			lastErr = fmt.Errorf("server not responding to PING: %w", err)
			if *verbose {
				cli.PrintWarning("Server %s not responding: %v", hostAddr, err)
			}
			continue
		}

		return nil
	}

	return fmt.Errorf("failed to connect to any host: %w", lastErr)
}

// extractHostname extracts the hostname from a "host:port" address.
func extractHostname(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

// Reconnect attempts to reconnect to the cluster, trying alternate hosts.
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
	for _, hostAddr := range hostsToTry {
		var conn net.Conn
		var err error

		if h.useTLS {
			// Create TLS configuration
			tlsConfig := &tls.Config{
				ServerName: extractHostname(hostAddr),
			}

			if h.tlsInsecure {
				tlsConfig.InsecureSkipVerify = true
			} else {
				// Load CA certificates
				var certPool *x509.CertPool
				if h.tlsCA != "" {
					caData, err := os.ReadFile(h.tlsCA)
					if err != nil {
						lastErr = fmt.Errorf("failed to read CA certificate: %w", err)
						continue
					}
					certPool = x509.NewCertPool()
					if !certPool.AppendCertsFromPEM(caData) {
						lastErr = fmt.Errorf("failed to append CA certificate")
						continue
					}
				} else {
					// Load system CA certificates
					var err error
					certPool, err = x509.SystemCertPool()
					if err != nil {
						certPool = x509.NewCertPool()
					}
				}
				tlsConfig.RootCAs = certPool
			}

			// Load client certificate if provided
			if h.tlsCert != "" && h.tlsKey != "" {
				cert, err := tls.LoadX509KeyPair(h.tlsCert, h.tlsKey)
				if err != nil {
					lastErr = fmt.Errorf("failed to load client certificate/key: %w", err)
					continue
				}
				tlsConfig.Certificates = []tls.Certificate{cert}
			}

			// Dial with TLS
			dialer := &net.Dialer{Timeout: *connectTimeout}
			conn, err = tls.DialWithDialer(dialer, "tcp", hostAddr, tlsConfig)
		} else {
			// Plain TCP connection
			conn, err = net.DialTimeout("tcp", hostAddr, *connectTimeout)
		}

		if err != nil {
			lastErr = err
			continue
		}

		h.BinaryClient = NewBinaryClient(conn, hostAddr)
		for i, hst := range h.hosts {
			if hst == hostAddr {
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
			if err := h.BinaryClient.Auth(h.authUsername, h.authPassword); err != nil {
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
func (h *HAClient) Auth(username, password string) error {
	err := h.BinaryClient.Auth(username, password)
	if err != nil {
		return err
	}
	h.authUsername = username
	h.authPassword = password
	h.lastAuth = true
	return nil
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

	if *verbose {
		cli.PrintWarning("Connection error, attempting failover...")
	}

	// Attempt failover
	if reconnErr := h.Reconnect(); reconnErr != nil {
		return "", fmt.Errorf("query failed and reconnection failed: %w (original error: %v)", reconnErr, err)
	}

	// Retry the query on the new connection
	return h.BinaryClient.Query(query)
}

// QueryWithResultAndFailover executes a query with failover and returns full result.
func (h *HAClient) QueryWithResultAndFailover(query string) (*protocol.QueryResultMessage, error) {
	result, err := h.BinaryClient.QueryWithResult(query)
	if err == nil {
		return result, nil
	}

	if !isConnectionError(err) {
		return result, err
	}

	if len(h.hosts) <= 1 {
		return result, err
	}

	if *verbose {
		cli.PrintWarning("Connection error, attempting failover...")
	}

	if reconnErr := h.Reconnect(); reconnErr != nil {
		return nil, fmt.Errorf("query failed and reconnection failed: %w (original error: %v)", reconnErr, err)
	}

	return h.BinaryClient.QueryWithResult(query)
}

// CurrentHost returns the currently connected host address.
func (h *HAClient) CurrentHost() string {
	if h.BinaryClient != nil {
		return h.serverAddr
	}
	return ""
}

// DiscoverLeader attempts to find the cluster leader for write operations.
// Returns the leader's address or empty string if not in a cluster.
func (h *HAClient) DiscoverLeader() (string, error) {
	// Query the server for cluster status
	result, err := h.BinaryClient.QueryWithResult("SHOW CLUSTER STATUS")
	if err != nil {
		// If cluster status query fails, assume single-node mode
		if strings.Contains(err.Error(), "unknown command") ||
			strings.Contains(err.Error(), "not supported") {
			return h.CurrentHost(), nil
		}
		return "", err
	}

	// Parse the result to find the leader
	// The result should contain role information
	if result.Message != "" && strings.Contains(strings.ToLower(result.Message), "leader") {
		return h.CurrentHost(), nil
	}

	// If we're connected to a follower, we may need to redirect
	// For now, return current host - the server will handle redirects
	return h.CurrentHost(), nil
}

// parseHosts parses the host string into a list of host:port addresses.
func parseHosts(hostStr, portStr string) []string {
	hosts := strings.Split(hostStr, ",")
	result := make([]string, 0, len(hosts))

	for _, h := range hosts {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}

		// Check if host already includes port
		if strings.Contains(h, ":") {
			result = append(result, h)
		} else {
			result = append(result, net.JoinHostPort(h, portStr))
		}
	}

	return result
}

// connectRemote establishes a remote connection to the FlyDB server.
func connectRemote() (*HAClient, error) {
	hosts := parseHosts(*host, *port)
	if len(hosts) == 0 {
		return nil, fmt.Errorf("no valid hosts specified")
	}

	// Determine TLS settings (enabled by default, unless --no-tls is specified)
	useTLS := !*noTLS

	// Check environment variable for TLS setting
	if envTLS := os.Getenv("FLYDB_TLS_ENABLED"); envTLS != "" {
		useTLS = strings.ToLower(envTLS) == "true" || envTLS == "1"
	}

	// Override with flag if explicitly set
	if *noTLS {
		useTLS = false
	}

	client := NewHAClient(hosts, useTLS, *tlsInsecure, *tlsCA, *tlsCert, *tlsKey)
	if err := client.Connect(); err != nil {
		return nil, err
	}

	// Authenticate if credentials provided
	user := *username
	if user == "" {
		user = os.Getenv("FLYDB_USER")
	}

	if user != "" {
		pass, err := getPassword()
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("failed to get password: %w", err)
		}

		if err := client.Auth(user, pass); err != nil {
			client.Close()
			return nil, err
		}
	}

	return client, nil
}

// =============================================================================
// Remote Dumper for Network-Based Exports
// =============================================================================

// RemoteDumper handles database export operations over the network.
type RemoteDumper struct {
	client     *HAClient
	tables     []string
	format     string
	writer     io.Writer
	verbose    bool
	tableCount int
	rowCount   int
	startTime  time.Time
	toStdout   bool
}

// NewRemoteDumper creates a new RemoteDumper instance.
func NewRemoteDumper(client *HAClient) *RemoteDumper {
	return &RemoteDumper{
		client: client,
		format: "sql",
		writer: os.Stdout,
	}
}

// getRemoteTablesToDump returns the list of tables to dump from the remote server.
func (d *RemoteDumper) getRemoteTablesToDump() ([]string, error) {
	if len(d.tables) > 0 {
		return d.tables, nil
	}

	// Query the server for table list
	result, err := d.client.QueryWithResultAndFailover("SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("failed to get table list: %w", err)
	}

	var tableNames []string
	for _, row := range result.Rows {
		if len(row) > 0 {
			if name, ok := row[0].(string); ok {
				tableNames = append(tableNames, name)
			}
		}
	}

	sort.Strings(tableNames)
	return tableNames, nil
}

// getTableSchema retrieves the CREATE TABLE statement for a table.
func (d *RemoteDumper) getTableSchema(tableName string) (string, error) {
	query := fmt.Sprintf("SHOW CREATE TABLE %s", tableName)
	result, err := d.client.QueryWithResultAndFailover(query)
	if err != nil {
		return "", fmt.Errorf("failed to get schema for %s: %w", tableName, err)
	}

	// The result should contain the CREATE TABLE statement
	if result.Message != "" {
		return result.Message, nil
	}

	// Try to extract from rows if message is empty
	if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
		if schema, ok := result.Rows[0][0].(string); ok {
			return schema, nil
		}
	}

	return "", fmt.Errorf("no schema returned for table %s", tableName)
}

// getTableData retrieves all rows from a table.
func (d *RemoteDumper) getTableData(tableName string) (*protocol.QueryResultMessage, error) {
	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	return d.client.QueryWithResultAndFailover(query)
}

// dumpRemoteSQL exports the database in SQL format over the network.
func (d *RemoteDumper) dumpRemoteSQL() error {
	fmt.Fprintf(d.writer, "-- FlyDB Database Dump (Remote)\n")
	fmt.Fprintf(d.writer, "-- Generated: %s\n", time.Now().Format(time.RFC3339))
	fmt.Fprintf(d.writer, "-- Database: %s\n", *database)
	fmt.Fprintf(d.writer, "-- Server: %s\n", d.client.CurrentHost())
	fmt.Fprintf(d.writer, "--\n\n")

	tablesToDump, err := d.getRemoteTablesToDump()
	if err != nil {
		return err
	}

	if !*dataOnly {
		fmt.Fprintf(d.writer, "-- Schema\n\n")
		for _, tableName := range tablesToDump {
			schema, err := d.getTableSchema(tableName)
			if err != nil {
				if d.verbose && !d.toStdout {
					cli.PrintWarning("Failed to get schema for %s: %v", tableName, err)
				}
				continue
			}
			fmt.Fprintf(d.writer, "%s;\n\n", schema)
			if d.verbose && !d.toStdout {
				cli.PrintInfo("Exported schema: %s", tableName)
			}
		}
	}

	if !*schemaOnly {
		fmt.Fprintf(d.writer, "\n-- Data\n\n")
		for _, tableName := range tablesToDump {
			if err := d.dumpRemoteTableData(tableName); err != nil {
				if d.verbose && !d.toStdout {
					cli.PrintWarning("Failed to dump data for %s: %v", tableName, err)
				}
				continue
			}
		}
	}

	fmt.Fprintf(d.writer, "\n-- Dump completed\n")
	return nil
}

// dumpRemoteTableData writes INSERT statements for all rows in a remote table.
func (d *RemoteDumper) dumpRemoteTableData(tableName string) error {
	result, err := d.getTableData(tableName)
	if err != nil {
		return err
	}

	if len(result.Rows) == 0 {
		d.tableCount++
		return nil
	}

	columns := result.Columns
	tableRowCount := 0

	for _, row := range result.Rows {
		values := make([]string, len(columns))
		for i := range columns {
			if i < len(row) {
				values[i] = formatSQLValue(row[i])
			} else {
				values[i] = "NULL"
			}
		}

		fmt.Fprintf(d.writer, "INSERT INTO %s (%s) VALUES (%s);\n",
			tableName, strings.Join(columns, ", "), strings.Join(values, ", "))
		tableRowCount++
	}

	d.tableCount++
	d.rowCount += tableRowCount

	if d.verbose && !d.toStdout {
		cli.PrintSuccess("Exported data: %s (%d rows)", tableName, tableRowCount)
	}
	return nil
}

// dumpRemoteJSON exports the database in JSON format over the network.
func (d *RemoteDumper) dumpRemoteJSON() error {
	result := make(map[string]interface{})
	result["database"] = *database
	result["generated"] = time.Now().Format(time.RFC3339)
	result["format"] = "json"
	result["server"] = d.client.CurrentHost()

	tablesToDump, err := d.getRemoteTablesToDump()
	if err != nil {
		return err
	}

	tablesData := make(map[string]interface{})

	for _, tableName := range tablesToDump {
		tableData := make(map[string]interface{})

		if !*dataOnly {
			schema, err := d.getTableSchema(tableName)
			if err == nil {
				tableData["create_statement"] = schema
			}
		}

		if !*schemaOnly {
			queryResult, err := d.getTableData(tableName)
			if err == nil {
				rowsData := make([]map[string]interface{}, 0, len(queryResult.Rows))
				for _, row := range queryResult.Rows {
					rowMap := make(map[string]interface{})
					for i, col := range queryResult.Columns {
						if i < len(row) {
							rowMap[col] = row[i]
						}
					}
					rowsData = append(rowsData, rowMap)
				}
				tableData["rows"] = rowsData
				d.rowCount += len(queryResult.Rows)
			}
		}

		tablesData[tableName] = tableData
		d.tableCount++
	}

	result["tables"] = tablesData

	encoder := json.NewEncoder(d.writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

// dumpRemoteCSV exports each table to a separate CSV file over the network.
func (d *RemoteDumper) dumpRemoteCSV() error {
	tablesToDump, err := d.getRemoteTablesToDump()
	if err != nil {
		return err
	}

	outputDir := *outputFile
	if outputDir == "" {
		outputDir = "."
	}

	for _, tableName := range tablesToDump {
		queryResult, err := d.getTableData(tableName)
		if err != nil {
			if d.verbose && !d.toStdout {
				cli.PrintWarning("Failed to get data for %s: %v", tableName, err)
			}
			continue
		}

		csvPath := filepath.Join(outputDir, tableName+".csv")
		f, err := os.Create(csvPath)
		if err != nil {
			return fmt.Errorf("failed to create CSV file for %s: %w", tableName, err)
		}

		writer := csv.NewWriter(f)

		// Write header
		if err := writer.Write(queryResult.Columns); err != nil {
			f.Close()
			return err
		}

		// Write data rows
		for _, row := range queryResult.Rows {
			values := make([]string, len(queryResult.Columns))
			for i := range queryResult.Columns {
				if i < len(row) {
					if row[i] == nil {
						values[i] = ""
					} else {
						values[i] = fmt.Sprintf("%v", row[i])
					}
				}
			}
			if err := writer.Write(values); err != nil {
				f.Close()
				return err
			}
		}

		writer.Flush()
		f.Close()

		d.tableCount++
		d.rowCount += len(queryResult.Rows)

		if d.verbose && !d.toStdout {
			cli.PrintSuccess("Exported: %s → %s (%d rows)", tableName, csvPath, len(queryResult.Rows))
		}
	}

	return nil
}

// runRemoteExport performs the database export over the network.
func runRemoteExport() error {
	startTime := time.Now()

	toStdout := *outputFile == "" || *outputFile == "-"

	var spinner *cli.Spinner
	if !toStdout {
		spinner = cli.NewSpinner(fmt.Sprintf("Connecting to %s...", *host))
		spinner.Start()
	}

	// Connect to remote server
	client, err := connectRemote()
	if err != nil {
		if spinner != nil {
			spinner.StopWithError("Connection failed")
		}
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer client.Close()

	if spinner != nil {
		spinner.StopWithSuccess(fmt.Sprintf("Connected to %s (database: %s)", client.CurrentHost(), *database))
	}

	// Create remote dumper
	dumper := NewRemoteDumper(client)
	dumper.format = *format
	dumper.verbose = *verbose
	dumper.startTime = startTime
	dumper.toStdout = toStdout

	// Parse table list
	if *tables != "" {
		dumper.tables = strings.Split(*tables, ",")
		for i, t := range dumper.tables {
			dumper.tables[i] = strings.TrimSpace(t)
		}
	}

	// Set up output writer
	var output io.WriteCloser
	if toStdout {
		output = os.Stdout
	} else {
		f, err := os.Create(*outputFile)
		if err != nil {
			return fmt.Errorf("failed to create output file: %w", err)
		}
		defer f.Close()
		output = f
	}

	// Apply compression if requested
	if *compress {
		gzWriter := gzip.NewWriter(output)
		defer gzWriter.Close()
		dumper.writer = gzWriter
	} else {
		dumper.writer = output
	}

	// Show export info
	if !toStdout {
		tablesToDump, _ := dumper.getRemoteTablesToDump()
		tableInfo := "all tables"
		if len(dumper.tables) > 0 {
			tableInfo = fmt.Sprintf("%d table(s)", len(dumper.tables))
		} else if len(tablesToDump) > 0 {
			tableInfo = fmt.Sprintf("%d table(s)", len(tablesToDump))
		}

		formatInfo := strings.ToUpper(*format)
		if *compress {
			formatInfo += " (gzip)"
		}

		cli.PrintInfo("Exporting %s in %s format...", tableInfo, formatInfo)
	}

	// Start export spinner
	if !toStdout {
		spinner = cli.NewSpinner("Exporting data...")
		spinner.Start()
	}

	// Perform the dump based on format
	var dumpErr error
	switch dumper.format {
	case "sql":
		dumpErr = dumper.dumpRemoteSQL()
	case "json":
		dumpErr = dumper.dumpRemoteJSON()
	case "csv":
		dumpErr = dumper.dumpRemoteCSV()
	default:
		if spinner != nil {
			spinner.StopWithError("Unsupported format")
		}
		return fmt.Errorf("unsupported format: %s", dumper.format)
	}

	if dumpErr != nil {
		if spinner != nil {
			spinner.StopWithError("Export failed")
		}
		return dumpErr
	}

	// Print summary
	if !toStdout {
		if spinner != nil {
			spinner.StopWithSuccess("Export completed successfully")
		}

		elapsed := time.Since(startTime)
		fmt.Println()
		cli.Box("Export Summary", fmt.Sprintf(
			"%s %d tables, %d rows\n%s %s\n%s %s\n%s %s\n%s %v",
			cli.Dimmed("Exported:"), dumper.tableCount, dumper.rowCount,
			cli.Dimmed("Format:"), strings.ToUpper(*format),
			cli.Dimmed("Server:"), client.CurrentHost(),
			cli.Dimmed("Output:"), *outputFile,
			cli.Dimmed("Duration:"), elapsed.Round(time.Millisecond),
		))
	}

	return nil
}

// =============================================================================
// Remote Import for Network-Based Imports
// =============================================================================

// runRemoteImport imports data from a SQL dump file over the network.
func runRemoteImport() error {
	startTime := time.Now()

	// Validate import file exists
	if _, err := os.Stat(*importFile); os.IsNotExist(err) {
		return fmt.Errorf("import file not found: %s", *importFile)
	}

	// Start connection spinner
	spinner := cli.NewSpinner(fmt.Sprintf("Connecting to %s...", *host))
	spinner.Start()

	// Connect to remote server
	client, err := connectRemote()
	if err != nil {
		spinner.StopWithError("Connection failed")
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer client.Close()

	// For imports (write operations), try to discover and connect to the leader
	if len(client.hosts) > 1 {
		spinner.UpdateMessage("Discovering cluster leader...")
		leaderAddr, err := client.DiscoverLeader()
		if err != nil {
			if *verbose {
				cli.PrintWarning("Could not discover leader: %v", err)
			}
		} else if leaderAddr != client.CurrentHost() {
			if *verbose {
				cli.PrintInfo("Redirecting to leader: %s", leaderAddr)
			}
			// The server should handle write forwarding, but we note it
		}
	}

	spinner.StopWithSuccess(fmt.Sprintf("Connected to %s (database: %s)", client.CurrentHost(), *database))

	// Open import file
	f, err := os.Open(*importFile)
	if err != nil {
		return fmt.Errorf("failed to open import file: %w", err)
	}
	defer f.Close()

	// Get file info for display
	fileInfo, _ := f.Stat()
	fileSize := fileInfo.Size()
	fileSizeStr := formatFileSize(fileSize)

	// Check if file is gzipped
	var reader io.Reader = f
	isCompressed := strings.HasSuffix(*importFile, ".gz")
	if isCompressed {
		gzReader, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("failed to decompress file: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}

	// Show import info
	importInfo := filepath.Base(*importFile)
	if isCompressed {
		importInfo += " (compressed)"
	}
	cli.PrintInfo("Importing from %s (%s) to %s...", importInfo, fileSizeStr, client.CurrentHost())

	// Start import spinner
	spinner = cli.NewSpinner("Importing statements...")
	spinner.Start()

	// Read and execute SQL statements
	sqlScanner := NewSQLScanner(reader)
	stmtCount := 0
	errorCount := 0
	createCount := 0
	insertCount := 0
	retryCount := 0

	for sqlScanner.Scan() {
		stmtText := strings.TrimSpace(sqlScanner.Text())
		if stmtText == "" || strings.HasPrefix(stmtText, "--") {
			continue
		}

		// Execute the statement over the network with retry logic
		var execErr error
		for attempt := 0; attempt < MaxRetries; attempt++ {
			_, execErr = client.QueryWithFailover(stmtText)
			if execErr == nil {
				break
			}

			// Check if this is a connection error that warrants retry
			if isConnectionError(execErr) && attempt < MaxRetries-1 {
				retryCount++
				time.Sleep(RetryDelay * time.Duration(attempt+1))
				continue
			}
			break
		}

		if execErr != nil {
			errorCount++
			if *verbose {
				spinner.Stop()
				cli.PrintWarning("Execute error: %v", execErr)
				spinner = cli.NewSpinner(fmt.Sprintf("Importing statements... (%d executed)", stmtCount))
				spinner.Start()
			}
			continue
		}

		stmtCount++

		// Track statement types for summary
		upperStmt := strings.ToUpper(stmtText)
		if strings.HasPrefix(upperStmt, "CREATE") {
			createCount++
		} else if strings.HasPrefix(upperStmt, "INSERT") {
			insertCount++
		}

		// Update spinner periodically
		if stmtCount%100 == 0 {
			spinner.UpdateMessage(fmt.Sprintf("Importing statements... (%d executed)", stmtCount))
		}
	}

	if err := sqlScanner.Err(); err != nil {
		spinner.StopWithError("Error reading import file")
		return fmt.Errorf("error reading import file: %w", err)
	}

	elapsed := time.Since(startTime)

	// Show completion status
	if errorCount > 0 {
		spinner.StopWithWarning(fmt.Sprintf("Import completed with %d error(s)", errorCount))
	} else {
		spinner.StopWithSuccess("Import completed successfully")
	}

	// Print summary
	fmt.Println()
	summaryContent := fmt.Sprintf(
		"%s %d statements executed\n%s %d CREATE, %d INSERT",
		cli.Dimmed("Imported:"), stmtCount,
		cli.Dimmed("Breakdown:"), createCount, insertCount,
	)
	if errorCount > 0 {
		summaryContent += fmt.Sprintf("\n%s %d statements", cli.Warning("Skipped:"), errorCount)
	}
	if retryCount > 0 {
		summaryContent += fmt.Sprintf("\n%s %d retries", cli.Dimmed("Retries:"), retryCount)
	}
	summaryContent += fmt.Sprintf("\n%s %s\n%s %s\n%s %v",
		cli.Dimmed("Server:"), client.CurrentHost(),
		cli.Dimmed("Source:"), *importFile,
		cli.Dimmed("Duration:"), elapsed.Round(time.Millisecond),
	)
	cli.Box("Import Summary", summaryContent)

	return nil
}
