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
migration, and data analysis purposes.

Features:
  - Full database dumps with schema and data
  - Table-specific exports
  - Schema-only exports (no data)
  - Data-only exports (no schema)
  - Multiple output formats: SQL, CSV, JSON
  - Import/restore functionality
  - Compression support (gzip)
  - Authentication support (username/password)
  - Encryption support for encrypted databases

Usage:

	fdump -d <data_dir> [options]

Options:

	-d <path>           Data directory path (required)
	-db <name>          Database name to dump (default: "default")
	-o <file>           Output file path (default: stdout)
	-f <format>         Output format: sql, csv, json (default: sql)
	-t <tables>         Comma-separated list of tables to dump (default: all)
	--schema-only       Dump schema only, no data
	--data-only         Dump data only, no schema
	--no-owner          Do not output ownership commands
	-z                  Compress output with gzip
	--import <file>     Import data from file
	-v                  Verbose output
	--version           Show version information
	-h                  Show help

Authentication Options:

	-U <username>       Username for authentication
	-W <password>       Password for authentication
	-P                  Prompt for password interactively

Encryption Options:

	--passphrase <pass>     Encryption passphrase for encrypted databases
	--prompt-passphrase     Prompt for encryption passphrase interactively

Environment Variables:

	FLYDB_USER                   Default username for authentication
	FLYDB_ADMIN_PASSWORD         Password for authentication
	FLYDB_ENCRYPTION_PASSPHRASE  Encryption passphrase

Examples:

	# Full SQL dump
	fdump -d ./data -o backup.sql

	# Dump with authentication
	fdump -d ./data -U admin -P -o backup.sql

	# Dump encrypted database
	fdump -d ./data --passphrase mysecret -o backup.sql

	# Dump with auth and encryption (using env vars)
	FLYDB_ADMIN_PASSWORD=pass FLYDB_ENCRYPTION_PASSPHRASE=secret fdump -d ./data -U admin -o backup.sql

	# Export specific tables as JSON
	fdump -d ./data -t users,orders -f json -o data.json

	# Schema only dump
	fdump -d ./data --schema-only -o schema.sql

	# Compressed dump
	fdump -d ./data -z -o backup.sql.gz

	# Import from dump file
	fdump -d ./data --import backup.sql

	# Import with authentication
	fdump -d ./data -U admin -P --import backup.sql
*/
package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"flydb/internal/auth"
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

// Command-line flags
var (
	dataDir     = flag.String("d", "", "Data directory path (required)")
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
	help        = flag.Bool("h", false, "Show help")

	// Authentication flags
	username   = flag.String("U", "", "Username for authentication")
	password   = flag.String("W", "", "Password for authentication (use -P for prompt)")
	promptPass = flag.Bool("P", false, "Prompt for password")

	// Encryption flags
	encryptionPassphrase = flag.String("passphrase", "", "Encryption passphrase for encrypted databases")
	promptPassphrase     = flag.Bool("prompt-passphrase", false, "Prompt for encryption passphrase")
)

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("flydb-dump version %s (built %s)\n", Version, BuildDate)
		os.Exit(0)
	}

	if *help {
		printUsage()
		os.Exit(0)
	}

	if *dataDir == "" {
		fmt.Fprintf(os.Stderr, "%s Data directory (-d) is required\n", cli.ErrorIcon())
		fmt.Fprintf(os.Stderr, "   %s fdump -d <data_dir> [options]\n", cli.Dimmed("Usage:"))
		os.Exit(1)
	}

	// Handle import mode
	if *importFile != "" {
		if err := runImport(); err != nil {
			fmt.Fprintf(os.Stderr, "%s Import failed: %v\n", cli.ErrorIcon(), err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	// Run export
	if err := runExport(); err != nil {
		fmt.Fprintf(os.Stderr, "%s Export failed: %v\n", cli.ErrorIcon(), err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("FlyDB Dump Utility - Database export and import tool")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  fdump -d <data_dir> [options]")
	fmt.Println()
	fmt.Println("Options:")
	flag.PrintDefaults()
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  fdump -d ./data -o backup.sql                          # Full SQL dump")
	fmt.Println("  fdump -d ./data -U admin -P -o backup.sql              # With authentication")
	fmt.Println("  fdump -d ./data --passphrase secret -o backup.sql      # With encryption")
	fmt.Println("  fdump -d ./data -db mydb -o mydb.sql                   # Specific database")
	fmt.Println("  fdump -d ./data -t users -f json -o u.json             # Export users table as JSON")
	fmt.Println("  fdump -d ./data --schema-only -o schema.sql            # Schema only")
	fmt.Println("  fdump -d ./data --import backup.sql                    # Import from dump")
	fmt.Println()
	fmt.Println("Environment Variables:")
	fmt.Println("  FLYDB_ADMIN_PASSWORD         Password for authentication")
	fmt.Println("  FLYDB_ENCRYPTION_PASSPHRASE  Encryption passphrase")
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
		fmt.Fprintf(os.Stderr, "Authenticated as user: %s\n", user)
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

	// Show what we're doing (only if not outputting to stdout)
	toStdout := *outputFile == "" || *outputFile == "-"
	if !toStdout {
		fmt.Fprintf(os.Stderr, "%s Exporting database '%s'...\n", cli.InfoIcon(), *database)
	}

	// Open the database
	dbPath := filepath.Join(*dataDir, *database)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("database directory not found: %s", dbPath)
	}

	// Initialize storage engine with encryption support
	config, err := createStorageConfig(dbPath)
	if err != nil {
		return err
	}

	store, err := storage.NewStorageEngine(config)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer store.Close()

	// Authenticate if credentials provided
	if err := authenticateIfNeeded(store); err != nil {
		return err
	}

	// Initialize catalog
	catalog := sql.NewCatalog(store)

	// Create dumper
	dumper := NewDumper(store, catalog)
	dumper.format = *format
	dumper.verbose = *verbose
	dumper.startTime = startTime

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
		return fmt.Errorf("unsupported format: %s", dumper.format)
	}

	if dumpErr != nil {
		return dumpErr
	}

	// Print summary (only if not outputting to stdout)
	if !toStdout {
		elapsed := time.Since(startTime)
		fmt.Fprintf(os.Stderr, "%s Export completed successfully\n", cli.SuccessIcon())
		fmt.Fprintf(os.Stderr, "   %s %d tables, %d rows\n", cli.Dimmed("Exported:"), dumper.tableCount, dumper.rowCount)
		fmt.Fprintf(os.Stderr, "   %s %s\n", cli.Dimmed("Output:"), *outputFile)
		fmt.Fprintf(os.Stderr, "   %s %v\n", cli.Dimmed("Duration:"), elapsed.Round(time.Millisecond))
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
	if d.verbose {
		fmt.Fprintf(os.Stderr, "Dumped schema for table: %s\n", tableName)
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

	if d.verbose {
		fmt.Fprintf(os.Stderr, "  %s Dumped %d rows from table: %s\n", cli.SuccessIcon(), tableRowCount, tableName)
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

		if d.verbose {
			fmt.Fprintf(os.Stderr, "Exported table %s to %s\n", tableName, csvPath)
		}
	}

	return nil
}

// runImport imports data from a SQL dump file
func runImport() error {
	startTime := time.Now()

	fmt.Fprintf(os.Stderr, "%s Importing from '%s' into database '%s'...\n",
		cli.InfoIcon(), *importFile, *database)

	// Open the database
	dbPath := filepath.Join(*dataDir, *database)

	// Create database directory if it doesn't exist
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return fmt.Errorf("failed to create database directory: %w", err)
	}

	// Initialize storage engine with encryption support
	config, err := createStorageConfig(dbPath)
	if err != nil {
		return err
	}

	store, err := storage.NewStorageEngine(config)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer store.Close()

	// Authenticate if credentials provided
	if err := authenticateIfNeeded(store); err != nil {
		return err
	}

	// Open import file
	f, err := os.Open(*importFile)
	if err != nil {
		return fmt.Errorf("failed to open import file: %w", err)
	}
	defer f.Close()

	// Check if file is gzipped
	var reader io.Reader = f
	if strings.HasSuffix(*importFile, ".gz") {
		gzReader, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("failed to decompress file: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}

	// Initialize executor (it creates its own catalog)
	executor := sql.NewExecutor(store, nil)

	// Read and execute SQL statements
	scanner := NewSQLScanner(reader)
	stmtCount := 0
	errorCount := 0

	for scanner.Scan() {
		stmtText := strings.TrimSpace(scanner.Text())
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
				fmt.Fprintf(os.Stderr, "  %s Parse error: %v\n", cli.WarningIcon(), err)
			}
			continue
		}

		_, err = executor.Execute(stmt)
		if err != nil {
			errorCount++
			if *verbose {
				fmt.Fprintf(os.Stderr, "  %s Execute error: %v\n", cli.WarningIcon(), err)
			}
			continue
		}
		stmtCount++
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading import file: %w", err)
	}

	elapsed := time.Since(startTime)

	// Print summary
	fmt.Fprintf(os.Stderr, "%s Import completed successfully\n", cli.SuccessIcon())
	fmt.Fprintf(os.Stderr, "   %s %d statements executed\n", cli.Dimmed("Imported:"), stmtCount)
	if errorCount > 0 {
		fmt.Fprintf(os.Stderr, "   %s %d statements skipped (errors)\n", cli.Dimmed("Skipped:"), errorCount)
	}
	fmt.Fprintf(os.Stderr, "   %s %v\n", cli.Dimmed("Duration:"), elapsed.Round(time.Millisecond))

	return nil
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

