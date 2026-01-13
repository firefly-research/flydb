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
Package config provides a comprehensive configuration management system for FlyDB.

The configuration system supports multiple sources with clear precedence:
 1. Command-line flags (highest priority)
 2. Environment variables
 3. Configuration file
 4. Default values (lowest priority)

Configuration File Format:
The configuration file uses TOML format for readability and ease of use.

Example configuration file:

	# FlyDB Configuration
	role = "standalone"
	port = 8889              # Client connections (binary protocol)
	replication_port = 9999
	data_dir = "/var/lib/flydb"
	buffer_pool_size = 0  # 0 = auto-size based on available memory
	checkpoint_secs = 60  # Checkpoint interval in seconds
	encryption_enabled = true
	log_level = "info"
	log_json = false

IMPORTANT - Data-at-Rest Encryption:
Encryption is ENABLED by default for security. When encryption is enabled, you MUST
provide a passphrase. There are two ways to set the passphrase:
 1. Environment variable: Set FLYDB_ENCRYPTION_PASSPHRASE before starting FlyDB
 2. Interactive wizard: Run FlyDB without arguments and enter passphrase when prompted
To disable encryption, set encryption_enabled = false in your config file.

Migration Note for Existing Users:
If you are upgrading from a version where encryption was disabled by default,
you have two options:
 1. Enable encryption: Set FLYDB_ENCRYPTION_PASSPHRASE and let FlyDB encrypt new data
 2. Disable encryption: Set encryption_enabled = false in your config file

Environment Variables:
  - FLYDB_PORT: Server port for client connections (binary protocol)
  - FLYDB_REPL_PORT: Replication port
  - FLYDB_ROLE: Server role (standalone, master, slave)
  - FLYDB_MASTER_ADDR: Master address for slave mode
  - FLYDB_DB_PATH: Path to database file
  - FLYDB_ENCRYPTION_ENABLED: Enable data-at-rest encryption (true/false, default: true)
  - FLYDB_ENCRYPTION_PASSPHRASE: Passphrase for encryption key derivation (REQUIRED when encryption enabled)
  - FLYDB_LOG_LEVEL: Log level (debug, info, warn, error)
  - FLYDB_LOG_JSON: Enable JSON logging (true/false)
  - FLYDB_ADMIN_PASSWORD: Initial admin password (first-time setup only)
  - FLYDB_CONFIG_FILE: Path to configuration file
*/
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// Environment variable names for configuration.
const (
	EnvPort     = "FLYDB_PORT"
	EnvReplPort = "FLYDB_REPL_PORT"
	EnvClusterPort          = "FLYDB_CLUSTER_PORT"
	EnvRole                 = "FLYDB_ROLE"
	EnvMasterAddr           = "FLYDB_MASTER_ADDR"
	EnvClusterPeers         = "FLYDB_CLUSTER_PEERS"
	EnvDBPath               = "FLYDB_DB_PATH"
	EnvDataDir              = "FLYDB_DATA_DIR"
	EnvStorageEngine        = "FLYDB_STORAGE_ENGINE"
	EnvBufferPoolSize       = "FLYDB_BUFFER_POOL_SIZE"
	EnvCheckpointSecs       = "FLYDB_CHECKPOINT_SECS"
	EnvLogLevel             = "FLYDB_LOG_LEVEL"
	EnvLogJSON              = "FLYDB_LOG_JSON"
	EnvAdminPassword        = "FLYDB_ADMIN_PASSWORD"
	EnvConfigFile           = "FLYDB_CONFIG_FILE"
	EnvEncryptionEnabled    = "FLYDB_ENCRYPTION_ENABLED"
	EnvEncryptionPassphrase = "FLYDB_ENCRYPTION_PASSPHRASE"
	EnvReplicationMode      = "FLYDB_REPLICATION_MODE"
	EnvHeartbeatInterval    = "FLYDB_HEARTBEAT_INTERVAL_MS"
	EnvHeartbeatTimeout     = "FLYDB_HEARTBEAT_TIMEOUT_MS"
	EnvElectionTimeout      = "FLYDB_ELECTION_TIMEOUT_MS"
	EnvMinQuorum            = "FLYDB_MIN_QUORUM"
)

// GetDefaultDataDir returns the default directory for database storage.
// For root users, it uses /var/lib/flydb (Filesystem Hierarchy Standard).
// For non-root users, it uses ~/.local/share/flydb (XDG Base Directory).
func GetDefaultDataDir() string {
	// Check if running as root
	if os.Getuid() == 0 {
		return "/var/lib/flydb"
	}
	// Use XDG data directory for non-root users
	if xdgData := os.Getenv("XDG_DATA_HOME"); xdgData != "" {
		return filepath.Join(xdgData, "flydb")
	}
	// Fall back to ~/.local/share/flydb
	if home := os.Getenv("HOME"); home != "" {
		return filepath.Join(home, ".local", "share", "flydb")
	}
	// Last resort: current directory
	return "./data"
}

// Default configuration file paths (searched in order).
var DefaultConfigPaths = []string{
	"/etc/flydb/flydb.conf",
	"$HOME/.config/flydb/flydb.conf",
	"./flydb.conf",
}

// Config holds all configuration values for FlyDB.
type Config struct {
	// Server configuration
	// Port is the server port for client connections (binary protocol).
	// The legacy text protocol has been removed - all connections use the binary protocol.
	Port        int    `toml:"port" json:"port"`
	ReplPort    int    `toml:"replication_port" json:"replication_port"`
	ClusterPort int    `toml:"cluster_port" json:"cluster_port"`
	Role        string `toml:"role" json:"role"`
	MasterAddr  string `toml:"master_addr" json:"master_addr"`

	// Cluster configuration
	ClusterPeers      []string `toml:"cluster_peers" json:"cluster_peers"`                 // List of peer addresses for cluster mode
	HeartbeatInterval int      `toml:"heartbeat_interval_ms" json:"heartbeat_interval_ms"` // Heartbeat interval in milliseconds
	HeartbeatTimeout  int      `toml:"heartbeat_timeout_ms" json:"heartbeat_timeout_ms"`   // Heartbeat timeout in milliseconds
	ElectionTimeout   int      `toml:"election_timeout_ms" json:"election_timeout_ms"`     // Election timeout in milliseconds
	MinQuorum         int      `toml:"min_quorum" json:"min_quorum"`                       // Minimum nodes for quorum (0 = auto)
	EnablePreVote     bool     `toml:"enable_pre_vote" json:"enable_pre_vote"`             // Enable pre-vote protocol
	PartitionCount    int      `toml:"partition_count" json:"partition_count"`             // Number of data partitions (power of 2)
	ReplicationFactor int      `toml:"replication_factor" json:"replication_factor"`       // Number of replicas per partition

	// Replication configuration
	ReplicationMode   string `toml:"replication_mode" json:"replication_mode"`             // async, semi_sync, or sync
	SyncTimeout       int    `toml:"sync_timeout_ms" json:"sync_timeout_ms"`               // Timeout for sync replication in ms
	MaxReplicationLag int    `toml:"max_replication_lag_ms" json:"max_replication_lag_ms"` // Max acceptable replication lag in ms

	// Raft consensus configuration
	EnableRaft           bool `toml:"enable_raft" json:"enable_raft"`                         // Enable Raft-based consensus (replaces Bully)
	RaftElectionTimeout  int  `toml:"raft_election_timeout_ms" json:"raft_election_timeout_ms"` // Raft election timeout in ms
	RaftHeartbeatInterval int `toml:"raft_heartbeat_interval_ms" json:"raft_heartbeat_interval_ms"` // Raft heartbeat interval in ms

	// Compression configuration
	EnableCompression      bool   `toml:"enable_compression" json:"enable_compression"`           // Enable compression for WAL and replication
	CompressionAlgorithm   string `toml:"compression_algorithm" json:"compression_algorithm"`     // gzip, lz4, snappy, or zstd
	CompressionMinSize     int    `toml:"compression_min_size" json:"compression_min_size"`       // Minimum size to compress (bytes)

	// Performance configuration
	EnableZeroCopy         bool `toml:"enable_zero_copy" json:"enable_zero_copy"`               // Enable zero-copy buffer pooling
	BufferPoolSizeBytes    int  `toml:"buffer_pool_size_bytes" json:"buffer_pool_size_bytes"`   // Buffer pool size in bytes (0 = auto)

	// Storage configuration
	DBPath         string `toml:"db_path" json:"db_path"`
	DataDir        string `toml:"data_dir" json:"data_dir"`               // Directory for multi-database storage
	StorageEngine  string `toml:"storage_engine" json:"storage_engine"`   // Deprecated: FlyDB now uses disk storage exclusively
	BufferPoolSize int    `toml:"buffer_pool_size" json:"buffer_pool_size"` // Buffer pool size in pages (0 = auto-size based on available memory)
	CheckpointSecs int    `toml:"checkpoint_secs" json:"checkpoint_secs"` // Checkpoint interval in seconds (0 = disabled)

	// Multi-database configuration
	DefaultDatabase  string `toml:"default_database" json:"default_database"`   // Default database for new connections
	DefaultEncoding  string `toml:"default_encoding" json:"default_encoding"`   // Default encoding for new databases
	DefaultLocale    string `toml:"default_locale" json:"default_locale"`       // Default locale for new databases
	DefaultCollation string `toml:"default_collation" json:"default_collation"` // Default collation for new databases

	// Encryption configuration for data at rest
	EncryptionEnabled    bool   `toml:"encryption_enabled" json:"encryption_enabled"`
	EncryptionPassphrase string `toml:"-" json:"-"` // Not persisted to file for security

	// Logging configuration
	LogLevel string `toml:"log_level" json:"log_level"`
	LogJSON  bool   `toml:"log_json" json:"log_json"`

	// Authentication (not persisted to file for security)
	AdminPassword string `toml:"-" json:"-"`

	// Metadata
	ConfigFile string `toml:"-" json:"-"` // Path to loaded config file
}

// DefaultConfig returns a Config with sensible default values.
// Note: Encryption is enabled by default for security. Users must provide a passphrase
// via the FLYDB_ENCRYPTION_PASSPHRASE environment variable, or explicitly disable
// encryption by setting encryption_enabled = false in the config file.
func DefaultConfig() *Config {
	return &Config{
		// Server - Port is for binary protocol (text protocol has been removed)
		Port:        8889,
		ReplPort:    9999,
		ClusterPort: 9998,
		Role:        "standalone",
		MasterAddr:  "",

		// Cluster
		ClusterPeers:      []string{},
		HeartbeatInterval: 500,  // 500ms
		HeartbeatTimeout:  2000, // 2s
		ElectionTimeout:   1000, // 1s
		MinQuorum:         0,    // 0 = auto-calculate based on cluster size
		EnablePreVote:     true,
		PartitionCount:    256,  // Number of data partitions
		ReplicationFactor: 3,    // Number of replicas per partition

		// Replication
		ReplicationMode:   "async",
		SyncTimeout:       5000,  // 5s
		MaxReplicationLag: 10000, // 10s

		// Raft consensus
		EnableRaft:            true,  // Enable Raft by default for cluster mode
		RaftElectionTimeout:   1000,  // 1s
		RaftHeartbeatInterval: 150,   // 150ms

		// Compression
		EnableCompression:    false,  // Disabled by default for compatibility
		CompressionAlgorithm: "gzip", // Default to gzip
		CompressionMinSize:   256,    // Minimum 256 bytes to compress

		// Performance
		EnableZeroCopy:      true, // Enable zero-copy by default
		BufferPoolSizeBytes: 0,    // 0 = auto-size

		// Storage
		DBPath:         "flydb.fdb",
		DataDir:        GetDefaultDataDir(),
		StorageEngine:  "disk", // FlyDB uses disk storage exclusively
		BufferPoolSize: 0,      // 0 = auto-size based on available memory
		CheckpointSecs: 60,     // 1 minute

		// Multi-database
		DefaultDatabase:  "default",
		DefaultEncoding:  "UTF8",
		DefaultLocale:    "en_US",
		DefaultCollation: "default",

		// Encryption
		EncryptionEnabled:    true, // Encryption enabled by default for security
		EncryptionPassphrase: "",

		// Logging
		LogLevel: "info",
		LogJSON:  false,
	}
}

// Manager handles configuration loading, validation, and access.
type Manager struct {
	config *Config
	mu     sync.RWMutex

	// Callbacks for configuration changes (for hot-reload support)
	onReload []func(*Config)
}

// NewManager creates a new configuration manager with default values.
func NewManager() *Manager {
	return &Manager{
		config:   DefaultConfig(),
		onReload: make([]func(*Config), 0),
	}
}

// Global manager instance for convenience.
var globalManager = NewManager()

// Global returns the global configuration manager.
func Global() *Manager {
	return globalManager
}

// Get returns the current configuration.
func (m *Manager) Get() *Config {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// Return a copy to prevent external modification
	cfg := *m.config
	return &cfg
}

// Set updates the configuration.
func (m *Manager) Set(cfg *Config) {
	m.mu.Lock()
	m.config = cfg
	m.mu.Unlock()
}

// OnReload registers a callback to be called when configuration is reloaded.
func (m *Manager) OnReload(fn func(*Config)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onReload = append(m.onReload, fn)
}

// notifyReload calls all registered reload callbacks.
func (m *Manager) notifyReload() {
	m.mu.RLock()
	callbacks := make([]func(*Config), len(m.onReload))
	copy(callbacks, m.onReload)
	cfg := m.config
	m.mu.RUnlock()

	for _, fn := range callbacks {
		fn(cfg)
	}
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	var errs []string

	// Validate port numbers
	if c.Port < 1 || c.Port > 65535 {
		errs = append(errs, fmt.Sprintf("invalid port: %d (must be 1-65535)", c.Port))
	}
	if c.ReplPort < 1 || c.ReplPort > 65535 {
		errs = append(errs, fmt.Sprintf("invalid replication_port: %d (must be 1-65535)", c.ReplPort))
	}
	if c.ClusterPort < 1 || c.ClusterPort > 65535 {
		errs = append(errs, fmt.Sprintf("invalid cluster_port: %d (must be 1-65535)", c.ClusterPort))
	}

	// Check for port conflicts
	if c.Role == "master" && c.Port == c.ReplPort {
		errs = append(errs, "replication_port must be different from port")
	}
	if c.Role == "cluster" {
		ports := map[int]string{c.Port: "port", c.ReplPort: "replication_port", c.ClusterPort: "cluster_port"}
		if len(ports) < 3 {
			errs = append(errs, "all ports (port, replication_port, cluster_port) must be different in cluster mode")
		}
	}

	// Validate role
	switch c.Role {
	case "standalone", "master", "slave", "cluster":
		// Valid roles
	default:
		errs = append(errs, fmt.Sprintf("invalid role: %s (must be standalone, master, slave, or cluster)", c.Role))
	}

	// Validate slave configuration
	if c.Role == "slave" && c.MasterAddr == "" {
		errs = append(errs, "master_addr is required for slave role")
	}

	// Validate cluster configuration
	if c.Role == "cluster" && len(c.ClusterPeers) == 0 {
		errs = append(errs, "cluster_peers is required for cluster role (at least one peer)")
	}

	// Validate replication mode
	switch strings.ToLower(c.ReplicationMode) {
	case "async", "semi_sync", "sync":
		// Valid modes
	default:
		errs = append(errs, fmt.Sprintf("invalid replication_mode: %s (must be async, semi_sync, or sync)", c.ReplicationMode))
	}

	// Validate timing values
	if c.HeartbeatInterval < 100 {
		errs = append(errs, "heartbeat_interval_ms must be at least 100ms")
	}
	if c.HeartbeatTimeout < c.HeartbeatInterval*2 {
		errs = append(errs, "heartbeat_timeout_ms should be at least 2x heartbeat_interval_ms")
	}
	if c.ElectionTimeout < 500 {
		errs = append(errs, "election_timeout_ms must be at least 500ms")
	}

	// Validate log level
	switch strings.ToLower(c.LogLevel) {
	case "debug", "info", "warn", "error":
		// Valid log levels
	default:
		errs = append(errs, fmt.Sprintf("invalid log_level: %s (must be debug, info, warn, or error)", c.LogLevel))
	}

	// Validate db_path
	if c.DBPath == "" {
		errs = append(errs, "db_path cannot be empty")
	}

	// Note: Encryption passphrase validation is intentionally NOT done here.
	// The passphrase check is done at startup in main.go to provide a more
	// user-friendly error message with guidance on how to fix the issue.
	// This allows the config to be valid for display/save purposes even
	// without a passphrase set.

	if len(errs) > 0 {
		return fmt.Errorf("configuration validation failed:\n  - %s", strings.Join(errs, "\n  - "))
	}

	return nil
}

// LoadFromFile loads configuration from a TOML file.
func (m *Manager) LoadFromFile(path string) error {
	// Expand environment variables in path
	path = os.ExpandEnv(path)

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	cfg := DefaultConfig()
	if err := parseTOML(string(data), cfg); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	cfg.ConfigFile = path
	m.Set(cfg)
	return nil
}

// LoadFromEnv loads configuration from environment variables.
// This merges with existing configuration (env vars override file values).
func (m *Manager) LoadFromEnv() {
	cfg := m.Get()

	if v := os.Getenv(EnvPort); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Port = port
		}
	}
	if v := os.Getenv(EnvReplPort); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.ReplPort = port
		}
	}
	if v := os.Getenv(EnvRole); v != "" {
		cfg.Role = v
	}
	if v := os.Getenv(EnvMasterAddr); v != "" {
		cfg.MasterAddr = v
	}
	if v := os.Getenv(EnvDBPath); v != "" {
		cfg.DBPath = v
	}
	if v := os.Getenv(EnvDataDir); v != "" {
		cfg.DataDir = v
	}
	if v := os.Getenv(EnvStorageEngine); v != "" {
		cfg.StorageEngine = v
	}
	if v := os.Getenv(EnvBufferPoolSize); v != "" {
		if size, err := strconv.Atoi(v); err == nil {
			cfg.BufferPoolSize = size
		}
	}
	if v := os.Getenv(EnvCheckpointSecs); v != "" {
		if secs, err := strconv.Atoi(v); err == nil {
			cfg.CheckpointSecs = secs
		}
	}
	if v := os.Getenv(EnvLogLevel); v != "" {
		cfg.LogLevel = v
	}
	if v := os.Getenv(EnvLogJSON); v != "" {
		cfg.LogJSON = strings.ToLower(v) == "true" || v == "1"
	}
	if v := os.Getenv(EnvAdminPassword); v != "" {
		cfg.AdminPassword = v
	}
	if v := os.Getenv(EnvEncryptionEnabled); v != "" {
		cfg.EncryptionEnabled = strings.ToLower(v) == "true" || v == "1"
	}
	if v := os.Getenv(EnvEncryptionPassphrase); v != "" {
		cfg.EncryptionPassphrase = v
	}

	// Cluster configuration
	if v := os.Getenv(EnvClusterPort); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.ClusterPort = port
		}
	}
	if v := os.Getenv(EnvClusterPeers); v != "" {
		// Parse comma-separated list of peers
		peers := strings.Split(v, ",")
		cfg.ClusterPeers = make([]string, 0, len(peers))
		for _, peer := range peers {
			peer = strings.TrimSpace(peer)
			if peer != "" {
				cfg.ClusterPeers = append(cfg.ClusterPeers, peer)
			}
		}
	}
	if v := os.Getenv(EnvReplicationMode); v != "" {
		cfg.ReplicationMode = strings.ToLower(v)
	}
	if v := os.Getenv(EnvHeartbeatInterval); v != "" {
		if ms, err := strconv.Atoi(v); err == nil {
			cfg.HeartbeatInterval = ms
		}
	}
	if v := os.Getenv(EnvHeartbeatTimeout); v != "" {
		if ms, err := strconv.Atoi(v); err == nil {
			cfg.HeartbeatTimeout = ms
		}
	}
	if v := os.Getenv(EnvElectionTimeout); v != "" {
		if ms, err := strconv.Atoi(v); err == nil {
			cfg.ElectionTimeout = ms
		}
	}
	if v := os.Getenv(EnvMinQuorum); v != "" {
		if q, err := strconv.Atoi(v); err == nil {
			cfg.MinQuorum = q
		}
	}

	m.Set(cfg)
}

// FindConfigFile searches for a configuration file in default locations.
// Returns the path to the first file found, or empty string if none found.
func FindConfigFile() string {
	// Check environment variable first
	if envPath := os.Getenv(EnvConfigFile); envPath != "" {
		if _, err := os.Stat(os.ExpandEnv(envPath)); err == nil {
			return os.ExpandEnv(envPath)
		}
	}

	// Search default paths
	for _, path := range DefaultConfigPaths {
		expandedPath := os.ExpandEnv(path)
		if _, err := os.Stat(expandedPath); err == nil {
			return expandedPath
		}
	}

	return ""
}

// Load loads configuration from all sources with proper precedence.
// Order: defaults -> config file -> environment variables
// Command-line flags should be applied after calling this function.
func (m *Manager) Load() error {
	// Start with defaults (already set in NewManager)

	// Try to load from config file
	configPath := FindConfigFile()
	if configPath != "" {
		if err := m.LoadFromFile(configPath); err != nil {
			return err
		}
	}

	// Apply environment variables (override file values)
	m.LoadFromEnv()

	return nil
}

// Reload reloads configuration from file and environment.
func (m *Manager) Reload() error {
	cfg := m.Get()
	configPath := cfg.ConfigFile

	if configPath == "" {
		configPath = FindConfigFile()
	}

	// Reset to defaults
	m.Set(DefaultConfig())

	// Reload from file if available
	if configPath != "" {
		if err := m.LoadFromFile(configPath); err != nil {
			return err
		}
	}

	// Apply environment variables
	m.LoadFromEnv()

	// Notify listeners
	m.notifyReload()

	return nil
}

// parseTOML is a simple TOML parser for our configuration format.
// It handles the subset of TOML we need without external dependencies.
func parseTOML(data string, cfg *Config) error {
	lines := strings.Split(data, "\n")

	for lineNum, line := range lines {
		// Remove comments
		if idx := strings.Index(line, "#"); idx != -1 {
			line = line[:idx]
		}
		line = strings.TrimSpace(line)

		// Skip empty lines
		if line == "" {
			continue
		}

		// Parse key = value
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("line %d: invalid syntax: %s", lineNum+1, line)
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Remove quotes from string values
		if len(value) >= 2 && ((value[0] == '"' && value[len(value)-1] == '"') ||
			(value[0] == '\'' && value[len(value)-1] == '\'')) {
			value = value[1 : len(value)-1]
		}

		// Apply value to config
		if err := applyConfigValue(cfg, key, value); err != nil {
			return fmt.Errorf("line %d: %w", lineNum+1, err)
		}
	}

	return nil
}

// applyConfigValue applies a key-value pair to the configuration.
func applyConfigValue(cfg *Config, key, value string) error {
	switch key {
	case "port", "binary_port":
		// Accept both "port" and "binary_port" for backward compatibility
		// (binary_port was used when text protocol existed)
		port, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid port value: %s", value)
		}
		cfg.Port = port
	case "replication_port":
		port, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid replication_port value: %s", value)
		}
		cfg.ReplPort = port
	case "role":
		cfg.Role = value
	case "master_addr":
		cfg.MasterAddr = value
	case "db_path":
		cfg.DBPath = value
	case "data_dir":
		cfg.DataDir = value
	case "storage_engine":
		cfg.StorageEngine = value
	case "buffer_pool_size":
		size, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid buffer_pool_size value: %s", value)
		}
		cfg.BufferPoolSize = size
	case "checkpoint_secs":
		secs, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid checkpoint_secs value: %s", value)
		}
		cfg.CheckpointSecs = secs
	case "log_level":
		cfg.LogLevel = value
	case "log_json":
		cfg.LogJSON = strings.ToLower(value) == "true" || value == "1"
	case "encryption_enabled":
		cfg.EncryptionEnabled = strings.ToLower(value) == "true" || value == "1"

	// Cluster configuration
	case "cluster_port":
		port, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid cluster_port value: %s", value)
		}
		cfg.ClusterPort = port
	case "cluster_peers":
		// Parse as array: ["addr1", "addr2"] or comma-separated
		value = strings.TrimSpace(value)
		if strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]") {
			// Array format
			value = value[1 : len(value)-1]
		}
		peers := strings.Split(value, ",")
		cfg.ClusterPeers = make([]string, 0, len(peers))
		for _, peer := range peers {
			peer = strings.TrimSpace(peer)
			peer = strings.Trim(peer, "\"'")
			if peer != "" {
				cfg.ClusterPeers = append(cfg.ClusterPeers, peer)
			}
		}
	case "heartbeat_interval_ms":
		ms, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid heartbeat_interval_ms value: %s", value)
		}
		cfg.HeartbeatInterval = ms
	case "heartbeat_timeout_ms":
		ms, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid heartbeat_timeout_ms value: %s", value)
		}
		cfg.HeartbeatTimeout = ms
	case "election_timeout_ms":
		ms, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid election_timeout_ms value: %s", value)
		}
		cfg.ElectionTimeout = ms
	case "min_quorum":
		q, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid min_quorum value: %s", value)
		}
		cfg.MinQuorum = q
	case "enable_pre_vote":
		cfg.EnablePreVote = strings.ToLower(value) == "true" || value == "1"
	case "replication_mode":
		cfg.ReplicationMode = strings.ToLower(value)
	case "sync_timeout_ms":
		ms, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid sync_timeout_ms value: %s", value)
		}
		cfg.SyncTimeout = ms
	case "max_replication_lag_ms":
		ms, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid max_replication_lag_ms value: %s", value)
		}
		cfg.MaxReplicationLag = ms

	default:
		// Ignore unknown keys for forward compatibility
	}

	return nil
}

// String returns a string representation of the configuration.
func (c *Config) String() string {
	var sb strings.Builder
	sb.WriteString("FlyDB Configuration:\n")
	sb.WriteString(fmt.Sprintf("  Role:             %s\n", c.Role))
	sb.WriteString(fmt.Sprintf("  Port:             %d\n", c.Port))
	sb.WriteString(fmt.Sprintf("  Replication Port: %d\n", c.ReplPort))
	if c.Role == "cluster" {
		sb.WriteString(fmt.Sprintf("  Cluster Port:     %d\n", c.ClusterPort))
		sb.WriteString(fmt.Sprintf("  Cluster Peers:    %v\n", c.ClusterPeers))
		sb.WriteString(fmt.Sprintf("  Replication Mode: %s\n", c.ReplicationMode))
	}
	if c.MasterAddr != "" {
		sb.WriteString(fmt.Sprintf("  Master Address:   %s\n", c.MasterAddr))
	}
	sb.WriteString(fmt.Sprintf("  DB Path:          %s\n", c.DBPath))
	sb.WriteString(fmt.Sprintf("  Encryption:       %v\n", c.EncryptionEnabled))
	sb.WriteString(fmt.Sprintf("  Log Level:        %s\n", c.LogLevel))
	sb.WriteString(fmt.Sprintf("  Log JSON:         %v\n", c.LogJSON))
	if c.ConfigFile != "" {
		sb.WriteString(fmt.Sprintf("  Config File:      %s\n", c.ConfigFile))
	}
	return sb.String()
}

// IsEncryptionEnabled returns true if data-at-rest encryption is enabled.
func (c *Config) IsEncryptionEnabled() bool {
	return c.EncryptionEnabled
}

// ToTOML returns the configuration as a TOML string.
func (c *Config) ToTOML() string {
	var sb strings.Builder
	sb.WriteString("# FlyDB Configuration File\n")
	sb.WriteString("# Generated by FlyDB\n\n")
	sb.WriteString("# Server role: standalone, master, slave, or cluster\n")
	sb.WriteString(fmt.Sprintf("role = \"%s\"\n\n", c.Role))
	sb.WriteString("# Network ports\n")
	sb.WriteString("# Port is for client connections (binary protocol)\n")
	sb.WriteString(fmt.Sprintf("port = %d\n", c.Port))
	sb.WriteString(fmt.Sprintf("replication_port = %d\n", c.ReplPort))
	sb.WriteString(fmt.Sprintf("cluster_port = %d\n\n", c.ClusterPort))

	if c.MasterAddr != "" {
		sb.WriteString("# Master address for slave mode\n")
		sb.WriteString(fmt.Sprintf("master_addr = \"%s\"\n\n", c.MasterAddr))
	}

	// Cluster configuration
	if c.Role == "cluster" || len(c.ClusterPeers) > 0 {
		sb.WriteString("# Cluster configuration\n")
		sb.WriteString("# List of peer addresses (comma-separated or array format)\n")
		if len(c.ClusterPeers) > 0 {
			sb.WriteString("cluster_peers = [")
			for i, peer := range c.ClusterPeers {
				if i > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(fmt.Sprintf("\"%s\"", peer))
			}
			sb.WriteString("]\n")
		} else {
			sb.WriteString("# cluster_peers = [\"host1:9998\", \"host2:9998\"]\n")
		}
		sb.WriteString(fmt.Sprintf("heartbeat_interval_ms = %d\n", c.HeartbeatInterval))
		sb.WriteString(fmt.Sprintf("heartbeat_timeout_ms = %d\n", c.HeartbeatTimeout))
		sb.WriteString(fmt.Sprintf("election_timeout_ms = %d\n", c.ElectionTimeout))
		sb.WriteString(fmt.Sprintf("min_quorum = %d  # 0 = auto-calculate\n", c.MinQuorum))
		sb.WriteString(fmt.Sprintf("enable_pre_vote = %v\n\n", c.EnablePreVote))
	}

	// Replication configuration
	sb.WriteString("# Replication mode: async, semi_sync, or sync\n")
	sb.WriteString(fmt.Sprintf("replication_mode = \"%s\"\n", c.ReplicationMode))
	sb.WriteString(fmt.Sprintf("sync_timeout_ms = %d\n", c.SyncTimeout))
	sb.WriteString(fmt.Sprintf("max_replication_lag_ms = %d\n\n", c.MaxReplicationLag))

	// Raft consensus configuration (01.26.13+)
	sb.WriteString("# Raft Consensus Configuration (01.26.13+)\n")
	sb.WriteString("# Enable Raft consensus for leader election (replaces legacy Bully algorithm)\n")
	sb.WriteString("# Raft provides stronger consistency guarantees and pre-vote protocol\n")
	sb.WriteString(fmt.Sprintf("enable_raft = %v\n", c.EnableRaft))
	sb.WriteString(fmt.Sprintf("raft_election_timeout_ms = %d\n", c.RaftElectionTimeout))
	sb.WriteString(fmt.Sprintf("raft_heartbeat_interval_ms = %d\n\n", c.RaftHeartbeatInterval))

	// Compression configuration (01.26.13+)
	sb.WriteString("# Compression Configuration (01.26.13+)\n")
	sb.WriteString("# Enable compression for WAL entries and replication traffic\n")
	sb.WriteString("# Reduces disk I/O and network bandwidth at the cost of CPU\n")
	sb.WriteString(fmt.Sprintf("enable_compression = %v\n", c.EnableCompression))
	sb.WriteString("# Compression algorithm: gzip, lz4, snappy, or zstd\n")
	sb.WriteString(fmt.Sprintf("compression_algorithm = \"%s\"\n", c.CompressionAlgorithm))
	sb.WriteString("# Minimum payload size in bytes to compress (smaller payloads skip compression)\n")
	sb.WriteString(fmt.Sprintf("compression_min_size = %d\n\n", c.CompressionMinSize))

	// Performance configuration (01.26.13+)
	sb.WriteString("# Performance Configuration (01.26.13+)\n")
	sb.WriteString("# Enable zero-copy buffer pooling for reduced memory allocations\n")
	sb.WriteString(fmt.Sprintf("enable_zero_copy = %v\n", c.EnableZeroCopy))
	sb.WriteString("# Buffer pool size in bytes for zero-copy operations (0 = auto-size)\n")
	sb.WriteString(fmt.Sprintf("buffer_pool_size_bytes = %d\n\n", c.BufferPoolSizeBytes))

	sb.WriteString("# Storage\n")
	sb.WriteString(fmt.Sprintf("data_dir = \"%s\"\n", c.DataDir))
	sb.WriteString(fmt.Sprintf("db_path = \"%s\"\n\n", c.DBPath))

	sb.WriteString("# Data-at-rest encryption (ENABLED BY DEFAULT for security)\n")
	sb.WriteString("# When enabled, you MUST set FLYDB_ENCRYPTION_PASSPHRASE environment variable\n")
	sb.WriteString("# To disable encryption, set encryption_enabled = false\n")
	sb.WriteString("# WARNING: Keep your passphrase safe - data cannot be recovered without it!\n")
	sb.WriteString(fmt.Sprintf("encryption_enabled = %v\n\n", c.EncryptionEnabled))

	sb.WriteString("# Logging\n")
	sb.WriteString(fmt.Sprintf("log_level = \"%s\"\n", c.LogLevel))
	sb.WriteString(fmt.Sprintf("log_json = %v\n", c.LogJSON))
	return sb.String()
}

// SaveToFile saves the configuration to a file.
func (c *Config) SaveToFile(path string) error {
	// Expand environment variables
	path = os.ExpandEnv(path)

	// Create directory if needed
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	// Write file
	if err := os.WriteFile(path, []byte(c.ToTOML()), 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// validate is an internal method that validates configuration.
func (m *Manager) validate(cfg *Config) error {
	return cfg.Validate()
}
