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

package wizard

import (
	"os"
	"path/filepath"
	"testing"

	"flydb/internal/config"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Port != "8889" {
		t.Errorf("Expected default port '8889', got '%s'", cfg.Port)
	}
	if cfg.ReplPort != "9999" {
		t.Errorf("Expected default replication port '9999', got '%s'", cfg.ReplPort)
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

func TestFromConfig(t *testing.T) {
	cfg := &config.Config{
		Port:              9000,
		ReplPort:          9002,
		Role:              "cluster",
		ClusterPeers:      []string{"localhost:9999"},
		DBPath:            "/tmp/test.fdb",
		EncryptionEnabled: true,
		LogLevel:          "debug",
		LogJSON:           true,
		ConfigFile:        "/etc/flydb/flydb.conf",
	}

	wizardCfg := FromConfig(cfg)

	if wizardCfg.Port != "9000" {
		t.Errorf("Expected port '9000', got '%s'", wizardCfg.Port)
	}
	if wizardCfg.ReplPort != "9002" {
		t.Errorf("Expected replication port '9002', got '%s'", wizardCfg.ReplPort)
	}
	if wizardCfg.Role != "cluster" {
		t.Errorf("Expected role 'cluster', got '%s'", wizardCfg.Role)
	}
	if len(wizardCfg.ClusterPeers) != 1 || wizardCfg.ClusterPeers[0] != "localhost:9999" {
		t.Errorf("Expected cluster peers '[localhost:9999]', got '%v'", wizardCfg.ClusterPeers)
	}
	if wizardCfg.DBPath != "/tmp/test.fdb" {
		t.Errorf("Expected db_path '/tmp/test.fdb', got '%s'", wizardCfg.DBPath)
	}
	if wizardCfg.EncryptionEnabled != true {
		t.Errorf("Expected encryption_enabled true, got %v", wizardCfg.EncryptionEnabled)
	}
	if wizardCfg.LogLevel != "debug" {
		t.Errorf("Expected log_level 'debug', got '%s'", wizardCfg.LogLevel)
	}
	if wizardCfg.LogJSON != true {
		t.Errorf("Expected log_json true, got %v", wizardCfg.LogJSON)
	}
	if wizardCfg.ConfigFile != "/etc/flydb/flydb.conf" {
		t.Errorf("Expected config_file '/etc/flydb/flydb.conf', got '%s'", wizardCfg.ConfigFile)
	}
}

func TestToConfig(t *testing.T) {
	wizardCfg := &Config{
		Port:              "9000",
		ReplPort:          "9002",
		Role:              "cluster",
		ClusterPeers:      []string{"node1:9999"},
		DBPath:            "/var/lib/flydb/data.fdb",
		EncryptionEnabled: true,
		LogLevel:          "warn",
		LogJSON:           true,
		ConfigFile:        "./flydb.conf",
	}

	cfg := wizardCfg.ToConfig()

	if cfg.Port != 9000 {
		t.Errorf("Expected port 9000, got %d", cfg.Port)
	}
	if cfg.ReplPort != 9002 {
		t.Errorf("Expected replication port 9002, got %d", cfg.ReplPort)
	}
	if cfg.Role != "cluster" {
		t.Errorf("Expected role 'cluster', got '%s'", cfg.Role)
	}
	if len(cfg.ClusterPeers) != 1 || cfg.ClusterPeers[0] != "node1:9999" {
		t.Errorf("Expected cluster peers '[node1:9999]', got '%v'", cfg.ClusterPeers)
	}
	if cfg.DBPath != "/var/lib/flydb/data.fdb" {
		t.Errorf("Expected db_path '/var/lib/flydb/data.fdb', got '%s'", cfg.DBPath)
	}
	if cfg.EncryptionEnabled != true {
		t.Errorf("Expected encryption_enabled true, got %v", cfg.EncryptionEnabled)
	}
	if cfg.LogLevel != "warn" {
		t.Errorf("Expected log_level 'warn', got '%s'", cfg.LogLevel)
	}
	if cfg.LogJSON != true {
		t.Errorf("Expected log_json true, got %v", cfg.LogJSON)
	}
	if cfg.ConfigFile != "./flydb.conf" {
		t.Errorf("Expected config_file './flydb.conf', got '%s'", cfg.ConfigFile)
	}
}

func TestValidatePort(t *testing.T) {
	tests := []struct {
		port  string
		valid bool
	}{
		{"8888", true},
		{"1", true},
		{"65535", true},
		{"0", false},
		{"65536", false},
		{"-1", false},
		{"abc", false},
		{"", false},
	}

	for _, tt := range tests {
		result := ValidatePort(tt.port)
		if result != tt.valid {
			t.Errorf("ValidatePort(%s) = %v, want %v", tt.port, result, tt.valid)
		}
	}
}

func TestSaveConfigToFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wizard_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	wizardCfg := &Config{
		Port:     "7777",
		ReplPort: "7779",
		Role:     "cluster",
		DBPath:   "/tmp/test.fdb",
		LogLevel: "debug",
		LogJSON:  true,
	}

	configPath := filepath.Join(tmpDir, "test.conf")
	err = SaveConfigToFile(wizardCfg, configPath)
	if err != nil {
		t.Fatalf("SaveConfigToFile failed: %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Error("Config file was not created")
	}

	// Load it back and verify
	mgr := config.NewManager()
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
	if loaded.LogLevel != "debug" {
		t.Errorf("Expected log_level 'debug', got '%s'", loaded.LogLevel)
	}
}

func TestLoadExistingConfig(t *testing.T) {
	// Create a temporary config file
	tmpDir, err := os.MkdirTemp("", "wizard_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configContent := `{
  "role": "cluster",
  "port": 9000,
  "replication_port": 9002,
  "db_path": "/tmp/test.fdb",
  "log_level": "debug"
}`

	configPath := filepath.Join(tmpDir, "flydb.json")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	// Use environment variable to specify config file path
	// This is more reliable than changing working directory
	oldEnv := os.Getenv(config.EnvConfigFile)
	os.Setenv(config.EnvConfigFile, configPath)
	defer os.Setenv(config.EnvConfigFile, oldEnv)

	cfg, path := LoadExistingConfig()
	if cfg == nil {
		t.Fatal("LoadExistingConfig returned nil config")
	}
	if path == "" {
		t.Fatal("LoadExistingConfig returned empty path")
	}

	if cfg.Port != 9000 {
		t.Errorf("Expected port 9000, got %d", cfg.Port)
	}
	if cfg.Role != "cluster" {
		t.Errorf("Expected role 'cluster', got '%s'", cfg.Role)
	}
}

func TestRoundTrip(t *testing.T) {
	// Test that FromConfig -> ToConfig preserves values
	original := &config.Config{
		Port:              8889,
		ReplPort:          9999,
		Role:              "standalone",
		DBPath:            "flydb.fdb",
		EncryptionEnabled: true,
		LogLevel:          "info",
		LogJSON:           false,
	}

	wizardCfg := FromConfig(original)
	result := wizardCfg.ToConfig()

	if result.Port != original.Port {
		t.Errorf("Port mismatch: got %d, want %d", result.Port, original.Port)
	}
	if result.ReplPort != original.ReplPort {
		t.Errorf("ReplPort mismatch: got %d, want %d", result.ReplPort, original.ReplPort)
	}
	if result.Role != original.Role {
		t.Errorf("Role mismatch: got %s, want %s", result.Role, original.Role)
	}
	if result.DBPath != original.DBPath {
		t.Errorf("DBPath mismatch: got %s, want %s", result.DBPath, original.DBPath)
	}
	if result.EncryptionEnabled != original.EncryptionEnabled {
		t.Errorf("EncryptionEnabled mismatch: got %v, want %v", result.EncryptionEnabled, original.EncryptionEnabled)
	}
	if result.LogLevel != original.LogLevel {
		t.Errorf("LogLevel mismatch: got %s, want %s", result.LogLevel, original.LogLevel)
	}
	if result.LogJSON != original.LogJSON {
		t.Errorf("LogJSON mismatch: got %v, want %v", result.LogJSON, original.LogJSON)
	}
}
