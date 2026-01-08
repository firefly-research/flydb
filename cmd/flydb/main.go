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
Package main is the entry point for the FlyDB database server.

FlyDB Server Architecture Overview:
===================================

The FlyDB server is designed with a layered architecture that separates concerns:

  1. Storage Layer (internal/storage):
     - KVStore: In-memory key-value store with WAL-backed persistence
     - WAL: Write-Ahead Log for durability and crash recovery

  2. SQL Layer (internal/sql):
     - Lexer: Tokenizes SQL input into tokens
     - Parser: Builds Abstract Syntax Tree (AST) from tokens
     - Executor: Executes AST nodes against the storage engine
     - Catalog: Manages table schemas

  3. Server Layer (internal/server):
     - TCP Server: Handles client connections and command dispatch
     - Replicator: Implements Leader-Follower replication

  4. Auth Layer (internal/auth):
     - AuthManager: Handles user authentication and authorization
     - Row-Level Security (RLS): Fine-grained access control

Startup Flow:
=============

  1. Parse command-line flags for configuration
  2. Initialize the KVStore (which replays WAL for crash recovery)
  3. Create the Replicator based on the server role (master/slave)
  4. Start replication in a background goroutine
  5. Create and start the TCP server to accept client connections

Command-Line Flags:
===================

  -port      : TCP port for client connections (default: 8888)
  -repl-port : TCP port for replication (master only, default: 9999)
  -role      : Server role - "master" or "slave" (default: master)
  -master    : Master address for slave nodes (format: host:port)
  -data-dir  : Directory for database storage (default: ./data)

Usage Examples:
===============

  Start a standalone server:
    ./flydb -data-dir ./data

  Start a master node:
    ./flydb -port 8888 -repl-port 9999 -role master -data-dir ./data

  Start a slave node:
    ./flydb -port 8889 -role slave -master localhost:9999 -data-dir ./data
*/
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"flydb/internal/auth"
	"flydb/internal/banner"
	"flydb/internal/config"
	"flydb/internal/logging"
	"flydb/internal/server"
	"flydb/internal/storage"
	"flydb/internal/wizard"
	"flydb/pkg/cli"
)

// Note: Environment variable names are now defined in internal/config package.

// printUsage prints comprehensive help information.
func printUsage() {
	fmt.Println()
	fmt.Printf("%s - High-performance SQL database server\n", cli.Highlight("FlyDB Server v"+banner.Version))
	fmt.Println(cli.Separator(60))
	fmt.Println()

	fmt.Println(cli.Highlight("USAGE:"))
	fmt.Println("  flydb [options]")
	fmt.Println("  flydb                    # Start with interactive wizard")
	fmt.Println()

	fmt.Println(cli.Highlight("OPTIONS:"))
	fmt.Println("  -port <port>             Server port for text protocol (default: 8888)")
	fmt.Println("  -binary-port <port>      Server port for binary protocol (default: 8889)")
	fmt.Println("  -repl-port <port>        Replication port for master mode (default: 9999)")
	fmt.Println("  -role <role>             Server role: standalone, master, slave (default: master)")
	fmt.Println("  -master <host:port>      Master address for slave mode")
	fmt.Printf("  -data-dir <path>         Directory for database storage (default: %s)\n", config.GetDefaultDataDir())
	fmt.Println("  -log-level <level>       Log level: debug, info, warn, error (default: info)")
	fmt.Println("  -log-json                Enable JSON log output")
	fmt.Println("  -version                 Show version information")
	fmt.Println("  -help                    Show this help message")
	fmt.Println()

	fmt.Println(cli.Highlight("ENVIRONMENT VARIABLES:"))
	fmt.Println("  FLYDB_DATA_DIR           Data directory for database storage")
	fmt.Println("  FLYDB_PORT               Server port for text protocol")
	fmt.Println("  FLYDB_BINARY_PORT        Server port for binary protocol")
	fmt.Println("  FLYDB_ENCRYPTION_PASSPHRASE  Encryption passphrase (required if encryption enabled)")
	fmt.Println("  FLYDB_ADMIN_PASSWORD     Admin password for first-time setup")
	fmt.Println()

	fmt.Println(cli.Highlight("EXAMPLES:"))
	fmt.Println()
	fmt.Println("  " + cli.Dimmed("# Start with interactive wizard"))
	fmt.Println("  flydb")
	fmt.Println()
	fmt.Println("  " + cli.Dimmed("# Start standalone server (development)"))
	fmt.Println("  flydb -role standalone -port 8888")
	fmt.Println()
	fmt.Println("  " + cli.Dimmed("# Start master server (production)"))
	fmt.Println("  flydb -role master -port 8888 -repl-port 9999 -db /var/lib/flydb/data.fdb")
	fmt.Println()
	fmt.Println("  " + cli.Dimmed("# Start slave server"))
	fmt.Println("  flydb -role slave -master localhost:9999 -db /var/lib/flydb/slave.fdb")
	fmt.Println()
	fmt.Println("  " + cli.Dimmed("# Start with debug logging"))
	fmt.Println("  flydb -log-level debug -log-json")
	fmt.Println()

	fmt.Println(cli.Highlight("ENVIRONMENT VARIABLES:"))
	fmt.Println("  FLYDB_ADMIN_PASSWORD     Set initial admin password (first-time setup)")
	fmt.Println()

	fmt.Println(cli.Highlight("CONNECTING:"))
	fmt.Println("  Use fsql to connect to the server:")
	fmt.Println("    fsql -h localhost -p 8889")
	fmt.Println()
}

// main is the entry point for the FlyDB server application.
// It orchestrates the initialization of all subsystems and starts the server.
func main() {
	// Initialize the configuration manager and load from file/env
	cfgMgr := config.Global()
	if err := cfgMgr.Load(); err != nil {
		// Non-fatal: continue with defaults if config file not found
		// Only fail on parse errors
		if config.FindConfigFile() != "" {
			fmt.Fprintf(os.Stderr, "Warning: %v\n", err)
		}
	}

	// Get the loaded configuration (may have file/env values)
	cfg := cfgMgr.Get()

	// Define command-line flags for configuration.
	// These flags allow operators to customize the server behavior without
	// modifying code, following the 12-factor app methodology.
	// Default values come from the loaded configuration.
	port := flag.String("port", strconv.Itoa(cfg.Port), "Server port for client connections")
	binaryPort := flag.String("binary-port", strconv.Itoa(cfg.BinaryPort), "Server port for binary protocol connections")
	replPort := flag.String("repl-port", strconv.Itoa(cfg.ReplPort), "Replication port (master only)")
	role := flag.String("role", cfg.Role, "Server role: 'master', 'slave', or 'standalone'")
	masterAddr := flag.String("master", cfg.MasterAddr, "Master address (host:port) for slave mode")
	dbPath := flag.String("db", cfg.DBPath, "Path to the WAL database file")
	dataDir := flag.String("data-dir", cfg.DataDir, "Directory for multi-database storage (enables multi-database mode)")
	logLevel := flag.String("log-level", cfg.LogLevel, "Log level: debug, info, warn, error")
	logJSON := flag.Bool("log-json", cfg.LogJSON, "Enable JSON log output")
	configFile := flag.String("config", "", "Path to configuration file")
	showVersion := flag.Bool("version", false, "Show version information")
	showHelp := flag.Bool("help", false, "Show help message")

	// Custom usage function
	flag.Usage = printUsage
	flag.Parse()

	// Handle --version flag
	if *showVersion {
		fmt.Printf("flydb version %s\n", banner.Version)
		os.Exit(0)
	}

	// Handle --help flag
	if *showHelp {
		printUsage()
		os.Exit(0)
	}

	// If a specific config file was provided, load it
	if *configFile != "" {
		if err := cfgMgr.LoadFromFile(*configFile); err != nil {
			fmt.Fprintf(os.Stderr, "Error loading config file: %v\n", err)
			os.Exit(1)
		}
		// Re-apply environment variables (they have higher priority than config file)
		cfgMgr.LoadFromEnv()
		cfg = cfgMgr.Get()
	}

	// Track if we're running in interactive wizard mode
	useWizard := len(os.Args) == 1
	var wizardConfig *wizard.Config

	// If running in wizard mode, the wizard handles config loading and display
	if useWizard {
		// Run wizard - it will load existing config, display it, and prompt user
		wizardConfig = wizard.Run()
		if wizardConfig == nil {
			// User cancelled the wizard
			os.Exit(0)
		}
		// Convert wizard config to main config and use it directly
		// The wizard has already handled config file loading and saving
		cfg = wizardConfig.ToConfig()

		// If wizard saved a config file, update the ConfigFile path
		if wizardConfig.ConfigFile != "" {
			cfg.ConfigFile = wizardConfig.ConfigFile
		}

		// Apply environment variables for sensitive values that can't be set in wizard
		// (e.g., encryption passphrase which should not be stored in config files)
		cfgMgr.Set(cfg)
		cfgMgr.LoadFromEnv()
		cfg = cfgMgr.Get()
	} else {
		// Display the startup banner with version and copyright information.
		// This provides visual feedback that the server is starting.
		banner.Print()

		// Apply command-line flags to configuration (highest priority)
		// Only apply flags that were explicitly set by the user
		flag.Visit(func(f *flag.Flag) {
			switch f.Name {
			case "port":
				if portInt, err := strconv.Atoi(*port); err == nil {
					cfg.Port = portInt
				}
			case "binary-port":
				if binaryPortInt, err := strconv.Atoi(*binaryPort); err == nil {
					cfg.BinaryPort = binaryPortInt
				}
			case "repl-port":
				if replPortInt, err := strconv.Atoi(*replPort); err == nil {
					cfg.ReplPort = replPortInt
				}
			case "role":
				cfg.Role = *role
			case "master":
				cfg.MasterAddr = *masterAddr
			case "db":
				cfg.DBPath = *dbPath
			case "data-dir":
				cfg.DataDir = *dataDir
			case "log-level":
				cfg.LogLevel = *logLevel
			case "log-json":
				cfg.LogJSON = *logJSON
			}
		})
	}

	// Validate the final configuration
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
		os.Exit(1)
	}

	// Update the global configuration
	cfgMgr.Set(cfg)

	// Configure the logging system.
	logging.SetGlobalLevel(logging.ParseLevel(cfg.LogLevel))
	logging.SetJSONMode(cfg.LogJSON)

	// Create the main logger for the daemon.
	log := logging.NewLogger("main")

	// Log configuration source if a file was loaded
	if cfg.ConfigFile != "" {
		log.Info("Configuration loaded", "file", cfg.ConfigFile)
	}

	log.Info("FlyDB server starting",
		"version", banner.Version,
		"role", cfg.Role,
		"port", cfg.Port,
		"binary_port", cfg.BinaryPort,
		"data_dir", cfg.DataDir,
	)

	// Ensure DataDir is set (use default if not specified)
	if cfg.DataDir == "" {
		cfg.DataDir = config.GetDefaultDataDir()
	}

	// Check if this is a first-time setup (no existing database files)
	isFirstTimeSetup := !storage.DataDirectoryHasData(cfg.DataDir)

	// Configure encryption if enabled
	encConfig := storage.EncryptionConfig{
		Enabled:    cfg.EncryptionEnabled,
		Passphrase: cfg.EncryptionPassphrase,
	}

	// Handle missing passphrase when encryption is enabled
	if cfg.EncryptionEnabled && cfg.EncryptionPassphrase == "" {
		if isFirstTimeSetup {
			// First-time setup: generate a passphrase automatically
			generatedPassphrase, err := auth.GenerateSecurePassword(24)
			if err != nil {
				log.Error("Failed to generate encryption passphrase", "error", err)
				os.Exit(1)
			}

			cfg.EncryptionPassphrase = generatedPassphrase
			encConfig.Passphrase = generatedPassphrase

			fmt.Println()
			fmt.Println("  " + cli.Warning("═══════════════════════════════════════════════════════════"))
			fmt.Println("  " + cli.Warning("  FIRST-TIME SETUP: Encryption Passphrase Generated"))
			fmt.Println("  " + cli.Warning("═══════════════════════════════════════════════════════════"))
			fmt.Println()
			fmt.Printf("    %s %s\n", cli.Dimmed("Passphrase:"), cli.Highlight(generatedPassphrase))
			fmt.Println()
			fmt.Println("  " + cli.Warning("IMPORTANT: Save this passphrase securely!"))
			fmt.Println("  " + cli.Warning("Data cannot be recovered without it!"))
			fmt.Println()
			fmt.Printf("    %s\n", cli.Dimmed("Set this environment variable before starting:"))
			fmt.Printf("    %s\n", cli.Info("export FLYDB_ENCRYPTION_PASSPHRASE=\""+generatedPassphrase+"\""))
			fmt.Println()

			log.Info("Generated encryption passphrase for first-time setup")
		} else {
			// Existing data but no passphrase: FAIL - cannot decrypt existing data
			fmt.Println()
			cli.PrintError("Encryption is enabled but no passphrase was provided.")
			fmt.Println()
			fmt.Println("  " + cli.Warning("Existing encrypted data was found in: ") + cli.Highlight(cfg.DataDir))
			fmt.Println()
			fmt.Println("  " + cli.Highlight("You MUST provide the original passphrase to access your data."))
			fmt.Println()
			fmt.Println("  " + cli.Dimmed("Set the encryption passphrase environment variable:"))
			fmt.Println("    export FLYDB_ENCRYPTION_PASSPHRASE=\"your-passphrase\"")
			fmt.Println()
			fmt.Println("  " + cli.Warning("If you have lost your passphrase, your data cannot be recovered."))
			fmt.Println()
			log.Error("Encryption passphrase required for existing data", "data_dir", cfg.DataDir)
			os.Exit(1)
		}
	}

	// Initialize the Storage Layer.
	// FlyDB always uses multi-database mode with DatabaseManager.
	// Each database is stored in a separate directory under cfg.DataDir.
	//
	// The KVStore is the core storage engine that provides:
	//   - In-memory key-value storage for fast reads
	//   - WAL-backed persistence for durability
	//   - Automatic state recovery on startup by replaying the WAL
	var kv *storage.KVStore
	var dbManager *storage.DatabaseManager

	log.Info("Initializing database manager", "data_dir", cfg.DataDir)

	var err error
	dbManager, err = storage.NewDatabaseManager(cfg.DataDir, encConfig)
	if err != nil {
		// Check if this is an encryption error (wrong passphrase)
		if storage.IsEncryptionError(err) {
			fmt.Println()
			cli.PrintError("Failed to decrypt database - incorrect passphrase!")
			fmt.Println()
			fmt.Println("  " + cli.Warning("The passphrase you provided does not match the one used to encrypt the data."))
			fmt.Println()
			fmt.Println("  " + cli.Highlight("Please verify your passphrase and try again."))
			fmt.Println()
			fmt.Println("  " + cli.Dimmed("Current passphrase source:"))
			if os.Getenv(config.EnvEncryptionPassphrase) != "" {
				fmt.Println("    Environment variable: FLYDB_ENCRYPTION_PASSPHRASE")
			} else {
				fmt.Println("    Configuration file or wizard")
			}
			fmt.Println()
			fmt.Println("  " + cli.Warning("If you have lost your passphrase, your data cannot be recovered."))
			fmt.Println()
			log.Error("Encryption passphrase incorrect", "data_dir", cfg.DataDir)
			os.Exit(1)
		}
		log.Error("Failed to initialize database manager", "error", err, "data_dir", cfg.DataDir)
		os.Exit(1)
	}

	// Get the system database's KVStore for admin initialization
	// Users are stored in the system database for global access across all databases
	systemDB, err := dbManager.GetSystemDatabase()
	if err != nil {
		// Check if this is an encryption error (wrong passphrase)
		if storage.IsEncryptionError(err) {
			fmt.Println()
			cli.PrintError("Failed to decrypt database - incorrect passphrase!")
			fmt.Println()
			fmt.Println("  " + cli.Warning("The passphrase you provided does not match the one used to encrypt the data."))
			fmt.Println()
			fmt.Println("  " + cli.Highlight("Please verify your passphrase and try again."))
			fmt.Println()
			fmt.Println("  " + cli.Warning("If you have lost your passphrase, your data cannot be recovered."))
			fmt.Println()
			log.Error("Encryption passphrase incorrect", "data_dir", cfg.DataDir)
			os.Exit(1)
		}
		log.Error("Failed to get system database", "error", err)
		os.Exit(1)
	}
	kv = systemDB.Store

	log.Info("Database manager initialized", "data_dir", cfg.DataDir)

	// Initialize the authentication manager backed by the system database.
	// This ensures users are global across all databases.
	authMgr := auth.NewAuthManager(kv)

	// Initialize built-in RBAC roles BEFORE creating admin user.
	// This ensures the admin role exists when we try to assign it.
	if err := authMgr.InitializeBuiltInRoles(); err != nil {
		log.Error("Failed to initialize built-in roles", "error", err)
		// Continue anyway - roles can be created later
	}

	// Ensure existing admin user has the admin role (for upgrades from older versions)
	if err := authMgr.EnsureAdminHasRole(); err != nil {
		log.Error("Failed to ensure admin has admin role", "error", err)
		// Continue anyway - this is not critical
	}

	if !authMgr.AdminExists() {
		log.Info("First-time setup detected: admin user does not exist")

		// Check for admin password from configuration (loaded from env var)
		adminPassword := cfg.AdminPassword

		if adminPassword != "" {
			// Use password from environment variable
			log.Info("Using admin password from environment variable")
			if err := authMgr.InitializeAdmin(adminPassword); err != nil {
				log.Error("Failed to initialize admin user", "error", err)
				os.Exit(1)
			}
			log.Info("Admin user initialized successfully")
		} else if useWizard {
			// Interactive mode: prompt for password
			fmt.Println()
			cli.PrintWarning("First-Time Setup: Admin Password Configuration")
			fmt.Println()
			fmt.Println("  This is the first time FlyDB is starting with this database.")
			fmt.Println("  An admin user needs to be created.")
			fmt.Println()

			// Generate a random password
			generatedPassword, err := authMgr.InitializeAdminWithGeneratedPassword()
			if err != nil {
				log.Error("Failed to initialize admin user", "error", err)
				os.Exit(1)
			}

			fmt.Println()
			cli.PrintSuccess("Admin user created successfully!")
			fmt.Println()
			fmt.Println("  " + cli.Highlight("Admin Credentials"))
			fmt.Println("  " + cli.Separator(40))
			fmt.Println()
			fmt.Printf("    %s %s\n", cli.Dimmed("Username:"), cli.Info("admin"))
			fmt.Printf("    %s %s\n", cli.Dimmed("Password:"), cli.Info(generatedPassword))
			fmt.Println()
			cli.PrintWarning("IMPORTANT: Save this password securely!")
			cli.PrintWarning("This password will NOT be shown again.")
			cli.PrintWarning("You can change it later using: ALTER USER admin IDENTIFIED BY 'newpass'")
			fmt.Println()

			log.Info("Admin user initialized with generated password")
		} else {
			// Non-interactive mode without env var: generate and display password
			generatedPassword, err := authMgr.InitializeAdminWithGeneratedPassword()
			if err != nil {
				log.Error("Failed to initialize admin user", "error", err)
				os.Exit(1)
			}

			fmt.Println()
			fmt.Println("  FIRST-TIME SETUP: Admin credentials generated")
			fmt.Println("  " + strings.Repeat("─", 45))
			fmt.Println()
			fmt.Printf("    Username: admin\n")
			fmt.Printf("    Password: %s\n", generatedPassword)
			fmt.Println()
			fmt.Println("  IMPORTANT: Save this password securely!")
			fmt.Println("  This password will NOT be shown again.")
			fmt.Println("  Set FLYDB_ADMIN_PASSWORD env var to specify a custom password.")
			fmt.Println()

			log.Info("Admin user initialized with generated password")
		}
	} else {
		log.Debug("Admin user already exists")
	}

	// Print startup message after wizard (if applicable)
	if useWizard && wizardConfig != nil {
		wizard.PrintStartupMessage(wizardConfig)
	}

	// Determine the server role and initialize the Replicator.
	// FlyDB supports three operative modes:
	//   - Standalone: Single server mode (no replication)
	//   - Master: Accepts writes and streams WAL updates to slaves
	//   - Slave: Receives WAL updates from master and applies them locally
	//
	// This architecture provides read scalability and fault tolerance.
	replLog := logging.NewLogger("replication")

	switch cfg.Role {
	case "standalone":
		// Standalone Mode: No replication, single server for development.
		log.Info("Running in standalone mode (no replication)")

	case "master":
		// Master Mode: Start the replication server in a background goroutine.
		// The replication server listens for slave connections and streams
		// WAL updates to keep slaves synchronized.
		replicator := server.NewReplicator(kv.WAL(), kv, true)
		go func() {
			replLog.Info("Starting replication master", "port", cfg.ReplPort)
			if err := replicator.StartMaster(fmt.Sprintf(":%d", cfg.ReplPort)); err != nil {
				replLog.Error("Replication master error", "error", err)
			}
		}()

	case "slave":
		// Slave Mode: Validate configuration and start the replication client.
		// Note: Validation already done above, but double-check for safety
		if cfg.MasterAddr == "" {
			log.Error("Master address is required for slave mode")
			os.Exit(1)
		}

		// Start the replication client in a background goroutine.
		// The client connects to the master and receives WAL updates.
		// A retry loop ensures the slave reconnects if the connection is lost.
		replicator := server.NewReplicator(kv.WAL(), kv, false)
		go func() {
			for {
				replLog.Info("Connecting to master", "address", cfg.MasterAddr)
				if err := replicator.StartSlave(cfg.MasterAddr); err != nil {
					replLog.Warn("Replication slave error, retrying",
						"error", err,
						"retry_in", "5s",
					)
					// Wait before retrying to avoid overwhelming the master
					// and to allow transient network issues to resolve.
					time.Sleep(5 * time.Second)
				}
			}
		}()

	default:
		log.Error("Invalid role specified", "role", cfg.Role, "valid_roles", "standalone, master, slave")
		os.Exit(1)
	}

	// Initialize and start the TCP server for client connections.
	// The server uses dependency injection by accepting the DatabaseManager,
	// which ensures that the SQL executor and replicator share the same
	// storage instance and WAL file.
	//
	// This design pattern allows for:
	//   - Easier testing with mock storage
	//   - Consistent state across all components
	//   - Proper resource management
	srv := server.NewServerWithDatabaseManager(
		fmt.Sprintf(":%d", cfg.Port),
		fmt.Sprintf(":%d", cfg.BinaryPort),
		dbManager,
	)

	// Set up graceful shutdown handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Handle shutdown signals in a goroutine
	go func() {
		sig := <-sigChan
		log.Info("Received shutdown signal", "signal", sig.String())
		fmt.Println()
		cli.PrintInfo("Shutting down FlyDB server...")

		// Stop the server gracefully
		if err := srv.Stop(); err != nil {
			log.Error("Error during shutdown", "error", err)
		}

		// Close the database manager
		if err := dbManager.Close(); err != nil {
			log.Error("Error closing database manager", "error", err)
		}

		cli.PrintSuccess("FlyDB server stopped gracefully")
		os.Exit(0)
	}()

	// Print startup success message
	fmt.Println()
	cli.PrintSuccess("FlyDB server is ready!")
	fmt.Println()
	cli.KeyValue("Text Protocol", fmt.Sprintf("localhost:%d", cfg.Port), 20)
	cli.KeyValue("Binary Protocol", fmt.Sprintf("localhost:%d", cfg.BinaryPort), 20)
	cli.KeyValue("Role", cfg.Role, 20)
	cli.KeyValue("Data Directory", cfg.DataDir, 20)
	if cfg.EncryptionEnabled {
		cli.KeyValue("Encryption", "enabled", 20)
	}
	if cfg.ConfigFile != "" {
		cli.KeyValue("Config File", cfg.ConfigFile, 20)
	}
	fmt.Println()
	fmt.Println(cli.Dimmed("Press Ctrl+C to stop the server"))
	fmt.Println()

	// Start the server's main accept loop.
	// This call blocks and handles client connections until the server is stopped.
	// Each client connection is handled in a separate goroutine for concurrency.
	log.Info("Starting FlyDB server",
		"text_port", cfg.Port,
		"binary_port", cfg.BinaryPort,
		"role", cfg.Role,
	)
	if err := srv.Start(); err != nil {
		log.Error("Server error", "error", err)
		os.Exit(1)
	}
}
