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
	port = 8888
	binary_port = 8889
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
  - FLYDB_PORT: Server port for text protocol
  - FLYDB_BINARY_PORT: Server port for binary protocol
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
	EnvPort                 = "FLYDB_PORT"
	EnvBinaryPort           = "FLYDB_BINARY_PORT"
	EnvReplPort             = "FLYDB_REPL_PORT"
	EnvRole                 = "FLYDB_ROLE"
	EnvMasterAddr           = "FLYDB_MASTER_ADDR"
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
	Port       int    `toml:"port" json:"port"`
	BinaryPort int    `toml:"binary_port" json:"binary_port"`
	ReplPort   int    `toml:"replication_port" json:"replication_port"`
	Role       string `toml:"role" json:"role"`
	MasterAddr string `toml:"master_addr" json:"master_addr"`

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
		Port:                 8888,
		BinaryPort:           8889,
		ReplPort:             9999,
		Role:                 "standalone",
		MasterAddr:           "",
		DBPath:               "flydb.fdb",
		DataDir:              GetDefaultDataDir(),
		StorageEngine:        "disk", // FlyDB uses disk storage exclusively
		BufferPoolSize:       0,      // 0 = auto-size based on available memory
		CheckpointSecs:       60,     // 1 minute
		DefaultDatabase:      "default",
		DefaultEncoding:      "UTF8",
		DefaultLocale:        "en_US",
		DefaultCollation:     "default",
		EncryptionEnabled:    true, // Encryption enabled by default for security
		EncryptionPassphrase: "",
		LogLevel:             "info",
		LogJSON:              false,
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
	if c.BinaryPort < 1 || c.BinaryPort > 65535 {
		errs = append(errs, fmt.Sprintf("invalid binary_port: %d (must be 1-65535)", c.BinaryPort))
	}
	if c.ReplPort < 1 || c.ReplPort > 65535 {
		errs = append(errs, fmt.Sprintf("invalid replication_port: %d (must be 1-65535)", c.ReplPort))
	}

	// Check for port conflicts
	if c.Port == c.BinaryPort {
		errs = append(errs, "port and binary_port cannot be the same")
	}
	if c.Role == "master" && (c.Port == c.ReplPort || c.BinaryPort == c.ReplPort) {
		errs = append(errs, "replication_port must be different from port and binary_port")
	}

	// Validate role
	switch c.Role {
	case "standalone", "master", "slave":
		// Valid roles
	default:
		errs = append(errs, fmt.Sprintf("invalid role: %s (must be standalone, master, or slave)", c.Role))
	}

	// Validate slave configuration
	if c.Role == "slave" && c.MasterAddr == "" {
		errs = append(errs, "master_addr is required for slave role")
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
	if v := os.Getenv(EnvBinaryPort); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.BinaryPort = port
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
	case "port":
		port, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid port value: %s", value)
		}
		cfg.Port = port
	case "binary_port":
		port, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid binary_port value: %s", value)
		}
		cfg.BinaryPort = port
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
	sb.WriteString(fmt.Sprintf("  Binary Port:      %d\n", c.BinaryPort))
	sb.WriteString(fmt.Sprintf("  Replication Port: %d\n", c.ReplPort))
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
	sb.WriteString(fmt.Sprintf("# Server role: standalone, master, or slave\n"))
	sb.WriteString(fmt.Sprintf("role = \"%s\"\n\n", c.Role))
	sb.WriteString(fmt.Sprintf("# Network ports\n"))
	sb.WriteString(fmt.Sprintf("port = %d\n", c.Port))
	sb.WriteString(fmt.Sprintf("binary_port = %d\n", c.BinaryPort))
	sb.WriteString(fmt.Sprintf("replication_port = %d\n\n", c.ReplPort))
	if c.MasterAddr != "" {
		sb.WriteString(fmt.Sprintf("# Master address for slave mode\n"))
		sb.WriteString(fmt.Sprintf("master_addr = \"%s\"\n\n", c.MasterAddr))
	}
	sb.WriteString(fmt.Sprintf("# Storage\n"))
	sb.WriteString(fmt.Sprintf("db_path = \"%s\"\n\n", c.DBPath))
	sb.WriteString(fmt.Sprintf("# Data-at-rest encryption (ENABLED BY DEFAULT for security)\n"))
	sb.WriteString(fmt.Sprintf("# When enabled, you MUST set FLYDB_ENCRYPTION_PASSPHRASE environment variable\n"))
	sb.WriteString(fmt.Sprintf("# To disable encryption, set encryption_enabled = false\n"))
	sb.WriteString(fmt.Sprintf("# WARNING: Keep your passphrase safe - data cannot be recovered without it!\n"))
	sb.WriteString(fmt.Sprintf("encryption_enabled = %v\n\n", c.EncryptionEnabled))
	sb.WriteString(fmt.Sprintf("# Logging\n"))
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
