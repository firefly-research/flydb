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

package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Port != 8889 {
		t.Errorf("Expected default port 8889, got %d", cfg.Port)
	}
	if cfg.ReplPort != 9999 {
		t.Errorf("Expected default replication port 9999, got %d", cfg.ReplPort)
	}
	if cfg.Role != "standalone" {
		t.Errorf("Expected default role 'standalone', got '%s'", cfg.Role)
	}
	if cfg.DBPath != "flydb.fdb" {
		t.Errorf("Expected default db_path 'flydb.fdb', got '%s'", cfg.DBPath)
	}
	// Encryption is enabled by default for security
	if cfg.EncryptionEnabled != true {
		t.Errorf("Expected default encryption_enabled true (security default), got %v", cfg.EncryptionEnabled)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("Expected default log_level 'info', got '%s'", cfg.LogLevel)
	}
	if cfg.LogJSON != false {
		t.Errorf("Expected default log_json false, got %v", cfg.LogJSON)
	}
}

// validTestConfig returns a valid config with all required fields set
func validTestConfig() *Config {
	cfg := DefaultConfig()
	cfg.DBPath = "test.fdb"
	return cfg
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{
			name:    "valid standalone config",
			cfg:     DefaultConfig(),
			wantErr: false,
		},
		{
			name: "invalid port - zero",
			cfg: func() *Config {
				cfg := validTestConfig()
				cfg.Port = 0
				return cfg
			}(),
			wantErr: true,
		},
		{
			name: "invalid port - too high",
			cfg: func() *Config {
				cfg := validTestConfig()
				cfg.Port = 70000
				return cfg
			}(),
			wantErr: true,
		},
		{
			name: "invalid role",
			cfg: func() *Config {
				cfg := validTestConfig()
				cfg.Role = "invalid"
				return cfg
			}(),
			wantErr: true,
		},
		{
			name: "invalid log level",
			cfg: func() *Config {
				cfg := validTestConfig()
				cfg.LogLevel = "invalid"
				return cfg
			}(),
			wantErr: true,
		},
		{
			name: "empty db_path",
			cfg: func() *Config {
				cfg := validTestConfig()
				cfg.DBPath = ""
				return cfg
			}(),
			wantErr: true,
		},
		{
			name: "valid cluster config",
			cfg: func() *Config {
				cfg := validTestConfig()
				cfg.Role = "cluster"
				cfg.ClusterPeers = []string{"node2:9998", "node3:9998"}
				return cfg
			}(),
			wantErr: false,
		},
		{
			name: "cluster without peers",
			cfg: func() *Config {
				cfg := validTestConfig()
				cfg.Role = "cluster"
				cfg.ClusterPeers = []string{}
				return cfg
			}(),
			wantErr: true,
		},
		{
			name: "invalid replication mode",
			cfg: func() *Config {
				cfg := validTestConfig()
				cfg.ReplicationMode = "invalid"
				return cfg
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLoadFromFile(t *testing.T) {
	// Create a temporary config file
	tmpDir, err := os.MkdirTemp("", "flydb_config_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configContent := `# Test configuration
role = "cluster"
port = 9000
replication_port = 9002
db_path = "/tmp/test.fdb"
log_level = "debug"
log_json = true
cluster_peers = ["node2:9998"]
`

	configPath := filepath.Join(tmpDir, "flydb.conf")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	mgr := NewManager()
	if err := mgr.LoadFromFile(configPath); err != nil {
		t.Fatalf("LoadFromFile failed: %v", err)
	}

	cfg := mgr.Get()

	if cfg.Role != "cluster" {
		t.Errorf("Expected role 'cluster', got '%s'", cfg.Role)
	}
	if cfg.Port != 9000 {
		t.Errorf("Expected port 9000, got %d", cfg.Port)
	}
	if cfg.ReplPort != 9002 {
		t.Errorf("Expected replication_port 9002, got %d", cfg.ReplPort)
	}
	if cfg.DBPath != "/tmp/test.fdb" {
		t.Errorf("Expected db_path '/tmp/test.fdb', got '%s'", cfg.DBPath)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("Expected log_level 'debug', got '%s'", cfg.LogLevel)
	}
	if cfg.LogJSON != true {
		t.Errorf("Expected log_json true, got %v", cfg.LogJSON)
	}
	if cfg.ConfigFile != configPath {
		t.Errorf("Expected ConfigFile '%s', got '%s'", configPath, cfg.ConfigFile)
	}
}

func TestLoadFromEnv(t *testing.T) {
	// Save original env vars
	origPort := os.Getenv(EnvPort)
	origRole := os.Getenv(EnvRole)
	origLogLevel := os.Getenv(EnvLogLevel)
	origLogJSON := os.Getenv(EnvLogJSON)
	origAdminPass := os.Getenv(EnvAdminPassword)

	// Restore env vars after test
	defer func() {
		os.Setenv(EnvPort, origPort)
		os.Setenv(EnvRole, origRole)
		os.Setenv(EnvLogLevel, origLogLevel)
		os.Setenv(EnvLogJSON, origLogJSON)
		os.Setenv(EnvAdminPassword, origAdminPass)
	}()

	// Set test env vars
	os.Setenv(EnvPort, "7777")
	os.Setenv(EnvRole, "cluster")
	os.Setenv(EnvLogLevel, "debug")
	os.Setenv(EnvLogJSON, "true")
	os.Setenv(EnvAdminPassword, "testpassword")

	mgr := NewManager()
	mgr.LoadFromEnv()

	cfg := mgr.Get()

	if cfg.Port != 7777 {
		t.Errorf("Expected port 7777 from env, got %d", cfg.Port)
	}
	if cfg.Role != "cluster" {
		t.Errorf("Expected role 'cluster' from env, got '%s'", cfg.Role)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("Expected log_level 'debug' from env, got '%s'", cfg.LogLevel)
	}
	if cfg.LogJSON != true {
		t.Errorf("Expected log_json true from env, got %v", cfg.LogJSON)
	}
	if cfg.AdminPassword != "testpassword" {
		t.Errorf("Expected admin_password 'testpassword' from env, got '%s'", cfg.AdminPassword)
	}
}

func TestConfigPrecedence(t *testing.T) {
	// Create a temporary config file
	tmpDir, err := os.MkdirTemp("", "flydb_config_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Config file sets port to 9000
	configContent := `port = 9000
role = "standalone"
db_path = "test.fdb"
log_level = "info"
`
	configPath := filepath.Join(tmpDir, "flydb.conf")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	// Save and set env var to override port to 7777
	origPort := os.Getenv(EnvPort)
	defer os.Setenv(EnvPort, origPort)
	os.Setenv(EnvPort, "7777")

	mgr := NewManager()
	if err := mgr.LoadFromFile(configPath); err != nil {
		t.Fatalf("LoadFromFile failed: %v", err)
	}
	mgr.LoadFromEnv()

	cfg := mgr.Get()

	// Env var should override file value
	if cfg.Port != 7777 {
		t.Errorf("Expected port 7777 (env override), got %d", cfg.Port)
	}
}

func TestToTOML(t *testing.T) {
	cfg := &Config{
		Port:         8889,
		ReplPort:     9999,
		Role:         "cluster",
		ClusterPeers: []string{"node2:9998"},
		DBPath:       "/var/lib/flydb/data.fdb",
		LogLevel:     "info",
		LogJSON:      false,
	}

	toml := cfg.ToTOML()

	// Check that key values are present
	if !contains(toml, "role = \"cluster\"") {
		t.Error("TOML output missing role")
	}
	if !contains(toml, "port = 8889") {
		t.Error("TOML output missing port")
	}
	if !contains(toml, "db_path = \"/var/lib/flydb/data.fdb\"") {
		t.Error("TOML output missing db_path")
	}
}

func TestSaveToFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flydb_config_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultConfig()
	cfg.Port = 7777
	cfg.Role = "cluster"
	cfg.ClusterPeers = []string{"node2:9998"}

	configPath := filepath.Join(tmpDir, "subdir", "flydb.conf")
	if err := cfg.SaveToFile(configPath); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Error("Config file was not created")
	}

	// Load it back and verify
	mgr := NewManager()
	if err := mgr.LoadFromFile(configPath); err != nil {
		t.Fatalf("Failed to load saved config: %v", err)
	}

	loaded := mgr.Get()
	if loaded.Port != 7777 {
		t.Errorf("Expected port 7777, got %d", loaded.Port)
	}
	if loaded.Role != "cluster" {
		t.Errorf("Expected role 'cluster', got '%s'", loaded.Role)
	}
}

func TestReload(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flydb_config_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Initial config
	configContent := `port = 9000
role = "standalone"
db_path = "test.fdb"
log_level = "info"
`
	configPath := filepath.Join(tmpDir, "flydb.conf")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	mgr := NewManager()
	if err := mgr.LoadFromFile(configPath); err != nil {
		t.Fatalf("LoadFromFile failed: %v", err)
	}

	cfg := mgr.Get()
	if cfg.Port != 9000 {
		t.Errorf("Expected initial port 9000, got %d", cfg.Port)
	}

	// Track reload callback
	reloadCalled := false
	mgr.OnReload(func(c *Config) {
		reloadCalled = true
	})

	// Update config file
	newContent := `port = 8000
role = "standalone"
db_path = "test.fdb"
log_level = "debug"
`
	if err := os.WriteFile(configPath, []byte(newContent), 0644); err != nil {
		t.Fatalf("Failed to update config file: %v", err)
	}

	// Reload
	if err := mgr.Reload(); err != nil {
		t.Fatalf("Reload failed: %v", err)
	}

	cfg = mgr.Get()
	if cfg.Port != 8000 {
		t.Errorf("Expected reloaded port 8000, got %d", cfg.Port)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("Expected reloaded log_level 'debug', got '%s'", cfg.LogLevel)
	}
	if !reloadCalled {
		t.Error("Reload callback was not called")
	}
}

func TestGlobalManager(t *testing.T) {
	mgr := Global()
	if mgr == nil {
		t.Error("Global() returned nil")
	}

	// Should return the same instance
	mgr2 := Global()
	if mgr != mgr2 {
		t.Error("Global() returned different instances")
	}
}

func TestConfigString(t *testing.T) {
	cfg := DefaultConfig()
	str := cfg.String()

	if !contains(str, "Role:") {
		t.Error("String() missing Role")
	}
	if !contains(str, "Port:") {
		t.Error("String() missing Port")
	}
	if !contains(str, "standalone") {
		t.Error("String() missing role value")
	}
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestEncryptionConfigFromFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flydb_config_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configContent := `role = "standalone"
port = 8889
replication_port = 9999
db_path = "test.fdb"
encryption_enabled = true
log_level = "info"
`
	configPath := filepath.Join(tmpDir, "flydb.conf")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	mgr := NewManager()
	if err := mgr.LoadFromFile(configPath); err != nil {
		t.Fatalf("LoadFromFile failed: %v", err)
	}

	cfg := mgr.Get()
	if !cfg.EncryptionEnabled {
		t.Error("Expected encryption_enabled true from file, got false")
	}
}

func TestEncryptionConfigFromEnv(t *testing.T) {
	// Save original env vars
	origEnabled := os.Getenv(EnvEncryptionEnabled)
	origPassphrase := os.Getenv(EnvEncryptionPassphrase)

	// Restore env vars after test
	defer func() {
		os.Setenv(EnvEncryptionEnabled, origEnabled)
		os.Setenv(EnvEncryptionPassphrase, origPassphrase)
	}()

	// Set test env vars
	os.Setenv(EnvEncryptionEnabled, "true")
	os.Setenv(EnvEncryptionPassphrase, "test-passphrase")

	mgr := NewManager()
	mgr.LoadFromEnv()

	cfg := mgr.Get()

	if !cfg.EncryptionEnabled {
		t.Error("Expected encryption_enabled true from env, got false")
	}
	if cfg.EncryptionPassphrase != "test-passphrase" {
		t.Errorf("Expected encryption_passphrase 'test-passphrase' from env, got '%s'", cfg.EncryptionPassphrase)
	}
}

func TestEncryptionConfigToTOML(t *testing.T) {
	cfg := &Config{
		Port:              8889,
		ReplPort:          9999,
		Role:              "standalone",
		DBPath:            "test.fdb",
		EncryptionEnabled: true,
		LogLevel:          "info",
		LogJSON:           false,
	}

	toml := cfg.ToTOML()

	if !contains(toml, "encryption_enabled = true") {
		t.Error("TOML output missing encryption_enabled")
	}
}

func TestIsEncryptionEnabled(t *testing.T) {
	cfg := DefaultConfig()
	// Encryption is now enabled by default
	if !cfg.IsEncryptionEnabled() {
		t.Error("Expected IsEncryptionEnabled() to return true for default config (encryption enabled by default)")
	}

	cfg.EncryptionEnabled = false
	if cfg.IsEncryptionEnabled() {
		t.Error("Expected IsEncryptionEnabled() to return false when disabled")
	}
}
