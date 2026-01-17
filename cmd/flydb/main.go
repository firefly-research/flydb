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
     - UnifiedStorageEngine: Disk-based storage with intelligent buffer pool caching
     - BufferPool: LRU-K page caching with auto-sizing based on available memory
     - HeapFile: 8KB slotted pages for efficient variable-length record storage
     - WAL: Write-Ahead Log for durability and crash recovery

  2. SQL Layer (internal/sql):
     - Lexer: Tokenizes SQL input into tokens
     - Parser: Builds Abstract Syntax Tree (AST) from tokens
     - Executor: Executes AST nodes against the storage engine
     - Catalog: Manages table schemas

  3. Server Layer (internal/server):
     - TCP Server: Handles client connections and command dispatch
     - Replicator: Implements Cluster replication

  4. Auth Layer (internal/auth):
     - AuthManager: Handles user authentication and authorization
     - Row-Level Security (RLS): Fine-grained access control

Startup Flow:
=============

  1. Parse command-line flags and load configuration
  2. Initialize the storage engine (auto-sizes buffer pool, replays WAL)
  3. Create the Replicator based on the server role (cluster/standalone)
  4. Start replication in a background goroutine
  5. Create and start the TCP server to accept client connections

Command-Line Flags:
===================

  -port      : TCP port for client connections (default: 8889)
  -repl-port : TCP port for replication (cluster only, default: 9999)
  -role      : Server role - "standalone" or "cluster" (default: standalone)
  -data-dir  : Directory for database storage (default: ./data)

Usage Examples:
===============

  Start a standalone server:
    ./flydb -data-dir ./data

  Start a cluster node:
    ./flydb -port 8889 -repl-port 9999 -role cluster -data-dir ./data
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
	"flydb/internal/cluster"
	"flydb/internal/config"
	"flydb/internal/logging"
	"flydb/internal/server"
	"flydb/internal/storage"
	flydbtls "flydb/internal/tls"
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
	fmt.Println("  -port <port>             Server port for client connections (default: 8889)")
	fmt.Println("  -repl-port <port>        Replication port (default: 9999)")
	fmt.Println("  -role <role>             Server role: standalone, cluster (default: standalone)")
	fmt.Printf("  -data-dir <path>         Directory for database storage (default: %s)\n", config.GetDefaultDataDir())
	fmt.Println("  -cluster-port <port>     Cluster communication port (default: 9998)")
	fmt.Println("  -cluster-peers <peers>   Comma-separated list of cluster peers")
	fmt.Println("  -log-level <level>       Log level: debug, info, warn, error (default: info)")
	fmt.Println("  -log-json                Enable JSON log output")
	fmt.Println("  -version                 Show version information")
	fmt.Println("  -help                    Show this help message")
	fmt.Println()

	fmt.Println(cli.Highlight("PERFORMANCE OPTIONS (01.26.13+):"))
	fmt.Println("  -enable-raft             Enable Raft consensus for leader election (default: true)")
	fmt.Println("  -raft-election-timeout   Raft election timeout in ms (default: 1000)")
	fmt.Println("  -raft-heartbeat-interval Raft heartbeat interval in ms (default: 150)")
	fmt.Println("  -enable-compression      Enable compression for WAL and replication (default: false)")
	fmt.Println("  -compression-algorithm   Compression algorithm: gzip, lz4, snappy, zstd (default: gzip)")
	fmt.Println("  -compression-min-size    Minimum payload size to compress in bytes (default: 256)")
	fmt.Println("  -enable-zero-copy        Enable zero-copy buffer pooling (default: true)")
	fmt.Println("  -buffer-pool-size-bytes  Buffer pool size in bytes, 0 = auto (default: 0)")
	fmt.Println()

	fmt.Println(cli.Highlight("ENVIRONMENT VARIABLES:"))
	fmt.Println("  FLYDB_DATA_DIR           Data directory for database storage")
	fmt.Println("  FLYDB_PORT               Server port for client connections")
	fmt.Println("  FLYDB_ENCRYPTION_PASSPHRASE  Encryption passphrase (required if encryption enabled)")
	fmt.Println("  FLYDB_ADMIN_PASSWORD     Admin password for first-time setup")
	fmt.Println()

	fmt.Println(cli.Highlight("EXAMPLES:"))
	fmt.Println()
	fmt.Println("  " + cli.Dimmed("# Start with interactive wizard"))
	fmt.Println("  flydb")
	fmt.Println()
	fmt.Println("  " + cli.Dimmed("# Start standalone server (development)"))
	fmt.Println("  flydb -role standalone -port 8889")
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

// getHostname returns the hostname of the current machine, or "localhost" if it cannot be determined.
func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "localhost"
	}
	return hostname
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
	port := flag.String("port", strconv.Itoa(cfg.Port), "Server port for client connections (binary protocol)")
	replPort := flag.String("repl-port", strconv.Itoa(cfg.ReplPort), "Replication port")
	role := flag.String("role", cfg.Role, "Server role: 'cluster' or 'standalone'")
	dbPath := flag.String("db", cfg.DBPath, "Path to the WAL database file")
	dataDir := flag.String("data-dir", cfg.DataDir, "Directory for multi-database storage (enables multi-database mode)")
	clusterPort := flag.Int("cluster-port", cfg.ClusterPort, "Cluster communication port")
	clusterPeers := flag.String("cluster-peers", strings.Join(cfg.ClusterPeers, ","), "Comma-separated list of cluster peers")
	logLevel := flag.String("log-level", cfg.LogLevel, "Log level: debug, info, warn, error")
	logJSON := flag.Bool("log-json", cfg.LogJSON, "Enable JSON log output")
	configFile := flag.String("config", "", "Path to configuration file")
	showVersion := flag.Bool("version", false, "Show version information")
	showHelp := flag.Bool("help", false, "Show help message")

	// Raft consensus flags (01.26.13+)
	enableRaft := flag.Bool("enable-raft", cfg.EnableRaft, "Enable Raft consensus for leader election (replaces Bully)")
	raftElectionTimeout := flag.Int("raft-election-timeout", cfg.RaftElectionTimeout, "Raft election timeout in milliseconds")
	raftHeartbeatInterval := flag.Int("raft-heartbeat-interval", cfg.RaftHeartbeatInterval, "Raft heartbeat interval in milliseconds")

	// Compression flags (01.26.13+)
	enableCompression := flag.Bool("enable-compression", cfg.EnableCompression, "Enable compression for WAL and replication")
	compressionAlgorithm := flag.String("compression-algorithm", cfg.CompressionAlgorithm, "Compression algorithm: gzip, lz4, snappy, or zstd")
	compressionMinSize := flag.Int("compression-min-size", cfg.CompressionMinSize, "Minimum payload size in bytes to compress")

	// Performance flags (01.26.13+)
	enableZeroCopy := flag.Bool("enable-zero-copy", cfg.EnableZeroCopy, "Enable zero-copy buffer pooling")
	bufferPoolSizeBytes := flag.Int("buffer-pool-size-bytes", cfg.BufferPoolSizeBytes, "Buffer pool size in bytes (0 = auto)")

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
		// Apply command-line flags to configuration (highest priority)
		// Only apply flags that were explicitly set by the user
		flag.Visit(func(f *flag.Flag) {
			switch f.Name {
			case "port":
				if portInt, err := strconv.Atoi(*port); err == nil {
					cfg.Port = portInt
				}
			case "repl-port":
				if replPortInt, err := strconv.Atoi(*replPort); err == nil {
					cfg.ReplPort = replPortInt
				}
			case "role":
				cfg.Role = *role
			case "db":
				cfg.DBPath = *dbPath
			case "data-dir":
				cfg.DataDir = *dataDir
			case "cluster-port":
				cfg.ClusterPort = *clusterPort
			case "cluster-peers":
				if *clusterPeers != "" {
					cfg.ClusterPeers = strings.Split(*clusterPeers, ",")
				}
			case "log-level":
				cfg.LogLevel = *logLevel
			case "log-json":
				cfg.LogJSON = *logJSON
			// Raft consensus flags (01.26.13+)
			case "enable-raft":
				cfg.EnableRaft = *enableRaft
			case "raft-election-timeout":
				cfg.RaftElectionTimeout = *raftElectionTimeout
			case "raft-heartbeat-interval":
				cfg.RaftHeartbeatInterval = *raftHeartbeatInterval
			// Compression flags (01.26.13+)
			case "enable-compression":
				cfg.EnableCompression = *enableCompression
			case "compression-algorithm":
				cfg.CompressionAlgorithm = *compressionAlgorithm
			case "compression-min-size":
				cfg.CompressionMinSize = *compressionMinSize
			// Performance flags (01.26.13+)
			case "enable-zero-copy":
				cfg.EnableZeroCopy = *enableZeroCopy
			case "buffer-pool-size-bytes":
				cfg.BufferPoolSizeBytes = *bufferPoolSizeBytes
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

	// Display comprehensive startup banner with configuration (only when not in wizard mode)
	// The wizard already displays its own banner and configuration summary
	if !useWizard {
		banner.PrintServerWithConfig(cfg)
	}

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
	// The unified storage engine provides:
	//   - Page-based disk storage for datasets larger than RAM
	//   - Intelligent buffer pool with LRU-K caching
	//   - WAL-backed persistence for durability
	//   - Automatic state recovery on startup
	var store storage.Engine
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
	store = systemDB.Store

	log.Info("Database manager initialized", "data_dir", cfg.DataDir)

	// Initialize the authentication manager backed by the system database.
	// This ensures users are global across all databases.
	authMgr := auth.NewAuthManager(store)

	// Set the system store in DatabaseManager for replication
	dbManager.SetSystemStore(store)

	// Wire up replication hook for multi-database replication
	if unified, ok := store.(*storage.UnifiedStorageEngine); ok {
		unified.SetReplicationHook(dbManager.HandleReplicatedDatabaseEvent)
	}

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
	// FlyDB supports two operative modes:
	//   - Standalone: Single server mode (no replication)
	//   - Cluster: Distributed mode with automatic leader election and replication
	//
	// This architecture provides read scalability and fault tolerance.
	replLog := logging.NewLogger("replication")

	switch cfg.Role {
	case "standalone":
		// Standalone Mode: No replication, single server for development.
		log.Info("Running in standalone mode (no replication)")

	case "cluster":
		// Cluster Mode: Start the unified cluster manager for distributed operation
		// with automatic leader election, data partitioning, and integrated replication.
		unified, ok := store.(*storage.UnifiedStorageEngine)
		if !ok {
			log.Error("Cluster mode requires unified storage engine")
			os.Exit(1)
		}

		// Create unified cluster configuration
		clusterConfig := cluster.ClusterConfig{
			NodeID:              fmt.Sprintf("%s:%d", getHostname(), cfg.ClusterPort),
			NodeAddr:            getHostname(),
			ClusterPort:         cfg.ClusterPort,
			DataPort:            cfg.ReplPort,
			Seeds:               cfg.ClusterPeers,
			PartitionCount:      cfg.PartitionCount,
			ReplicationFactor:   cfg.ReplicationFactor,
			HeartbeatInterval:   time.Duration(cfg.HeartbeatInterval) * time.Millisecond,
			ElectionTimeout:     time.Duration(cfg.ElectionTimeout) * time.Millisecond,
			SyncTimeout:         time.Duration(cfg.SyncTimeout) * time.Millisecond,
			EnableAutoRebalance: true,
			DataDir:             cfg.DataDir,
		}

		// Set default consistency based on replication mode
		switch cfg.ReplicationMode {
		case "sync":
			clusterConfig.DefaultConsistency = cluster.ConsistencyAll
		case "semi_sync":
			clusterConfig.DefaultConsistency = cluster.ConsistencyQuorum
		default:
			clusterConfig.DefaultConsistency = cluster.ConsistencyOne
		}

		clusterMgr := cluster.NewUnifiedClusterManager(clusterConfig)

		// Wire up WAL and storage for integrated replication
		clusterMgr.SetWAL(unified.WAL())
		clusterMgr.SetStore(store)

		// Set up callback for leader transitions - start integrated replication leader
		clusterMgr.SetLeaderCallback(func() {
			replLog.Info("This node is now the LEADER - starting integrated replication leader")
			go func() {
				if err := clusterMgr.StartReplicationLeader(fmt.Sprintf(":%d", cfg.ReplPort)); err != nil {
					replLog.Error("Replication leader error", "error", err)
				}
			}()
		})

		// Set up callback for follower transitions - start integrated replication follower
		clusterMgr.SetFollowerCallback(func(leaderID string) {
			replLog.Info("This node is now a FOLLOWER", "leader", leaderID)
			
			// Get leader node to find its data port
			leaderNode := clusterMgr.GetNode(leaderID)
			if leaderNode == nil {
				replLog.Error("Failed to find leader node details", "leader_id", leaderID)
				return
			}
			
			leaderAddr := fmt.Sprintf("%s:%d", leaderNode.Addr, leaderNode.DataPort)
			
			go func() {
				if err := clusterMgr.StartReplicationFollower(leaderAddr); err != nil {
					replLog.Error("Replication follower error", "error", err)
				}
			}()
		})

		// Set up event callback for logging cluster events
		clusterMgr.OnEvent(func(event cluster.ClusterEvent) {
			switch event.Type {
			case cluster.EventLeaderElected:
				log.Info("Cluster event: leader elected", "node", event.NodeID, "term", event.Term)
			case cluster.EventNodeJoined:
				log.Info("Cluster event: node joined", "node", event.NodeID)
			case cluster.EventNodeLeft:
				log.Info("Cluster event: node left", "node", event.NodeID)
			case cluster.EventNodeFailed:
				log.Warn("Cluster event: node failed", "node", event.NodeID)
			case cluster.EventQuorumLost:
				log.Error("Cluster event: QUORUM LOST - cluster is degraded")
			case cluster.EventQuorumRestored:
				log.Info("Cluster event: quorum restored")
			case cluster.EventRebalanceStarted:
				log.Info("Cluster event: partition rebalancing started")
			case cluster.EventRebalanceComplete:
				log.Info("Cluster event: partition rebalancing complete")
			case cluster.EventPartitionMoved:
				log.Info("Cluster event: partition moved", "partition", event.PartitionID)
			}
		})

		// Start service discovery if enabled
		var discoveryService *cluster.DiscoveryService
		if cfg.DiscoveryEnabled {
			discoveryConfig := cluster.DiscoveryConfig{
				NodeID:      clusterConfig.NodeID,
				ClusterID:   cfg.DiscoveryClusterID,
				ClusterAddr: fmt.Sprintf("%s:%d", getHostname(), cfg.ClusterPort),
				RaftAddr:    fmt.Sprintf("%s:%d", getHostname(), cfg.ReplPort),
				HTTPAddr:    fmt.Sprintf("%s:%d", getHostname(), cfg.Port),
				Version:     "1.0.0", // TODO: Get from build info
				Enabled:     true,
			}
			discoveryService = cluster.NewDiscoveryService(discoveryConfig)
			if err := discoveryService.Start(); err != nil {
				log.Warn("Failed to start discovery service", "error", err)
			} else {
				log.Info("Service discovery started", "node_id", discoveryConfig.NodeID)

				// If no seeds configured, try to discover peers
				if len(cfg.ClusterPeers) == 0 {
					log.Info("No seeds configured, attempting peer discovery...")
					time.Sleep(2 * time.Second) // Give time for mDNS to propagate
					nodes, err := discoveryService.DiscoverNodes(5 * time.Second)
					if err != nil {
						log.Warn("Peer discovery failed", "error", err)
					} else if len(nodes) > 0 {
						log.Info("Discovered peers via mDNS", "count", len(nodes))
						// Add discovered nodes as seeds
						for _, node := range nodes {
							log.Info("Discovered peer", "node_id", node.NodeID, "addr", node.ClusterAddr)
						}
					}
				}
			}
		}

		// Start the unified cluster manager
		if err := clusterMgr.Start(); err != nil {
			log.Error("Failed to start unified cluster manager", "error", err)
			os.Exit(1)
		}

		// Handle graceful shutdown of cluster manager and discovery
		go func() {
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			<-sigCh
			log.Info("Shutting down unified cluster manager...")
			if discoveryService != nil {
				discoveryService.Stop()
			}
			clusterMgr.Stop()
		}()

		// Log cluster status
		status := clusterMgr.GetStatus()
		log.Info("Unified cluster mode started",
			"node_id", status.NodeID,
			"cluster_port", cfg.ClusterPort,
			"data_port", cfg.ReplPort,
			"partitions", cfg.PartitionCount,
			"replication_factor", cfg.ReplicationFactor,
			"peers", cfg.ClusterPeers,
		)

	default:
		log.Error("Invalid role specified", "role", cfg.Role, "valid_roles", "standalone, cluster")
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
		"", // binaryAddr is deprecated - all connections use binary protocol on Port
		dbManager,
	)

	// Configure TLS if enabled
	if cfg.TLSEnabled {
		// Determine certificate paths
		certPath := cfg.TLSCertFile
		keyPath := cfg.TLSKeyFile

		if certPath == "" || keyPath == "" {
			// Use default paths based on user privileges
			_, certPath, keyPath = flydbtls.GetDefaultCertPaths()
		}

		// Auto-generate certificates if enabled and they don't exist
		if cfg.TLSAutoGen {
			certConfig := flydbtls.DefaultCertConfig()
			if err := flydbtls.EnsureCertificates(certPath, keyPath, certConfig); err != nil {
				log.Error("Failed to ensure TLS certificates", "error", err)
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
		}

		// Enable TLS on the server
		tlsConfig := server.TLSConfig{
			CertFile: certPath,
			KeyFile:  keyPath,
			Address:  fmt.Sprintf(":%d", cfg.Port),
		}

		if err := srv.EnableTLS(tlsConfig); err != nil {
			log.Error("Failed to enable TLS", "error", err)
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			fmt.Fprintf(os.Stderr, "\nTo disable TLS, set tls_enabled = false in your config file\n")
			fmt.Fprintf(os.Stderr, "or set FLYDB_TLS_ENABLED=false environment variable\n")
			os.Exit(1)
		}

		log.Info("TLS enabled", "cert", certPath, "key", keyPath)
	}

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

	// Show connection protocol
	protocol := "tcp"
	if cfg.TLSEnabled {
		protocol = "tls"
	}
	cli.KeyValue("Server", fmt.Sprintf("%s://localhost:%d", protocol, cfg.Port), 20)
	cli.KeyValue("Role", cfg.Role, 20)
	cli.KeyValue("Data Directory", cfg.DataDir, 20)
	if cfg.EncryptionEnabled {
		cli.KeyValue("Encryption", "enabled", 20)
	}
	if cfg.TLSEnabled {
		cli.KeyValue("TLS", "enabled", 20)
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
		"port", cfg.Port,
		"role", cfg.Role,
	)
	if err := srv.Start(); err != nil {
		log.Error("Server error", "error", err)
		os.Exit(1)
	}
}
