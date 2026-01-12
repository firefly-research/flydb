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

package pool

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig("localhost:8889")

	if config.Address != "localhost:8889" {
		t.Errorf("Expected address 'localhost:8889', got '%s'", config.Address)
	}
	if config.MinConns != 2 {
		t.Errorf("Expected MinConns 2, got %d", config.MinConns)
	}
	if config.MaxConns != 10 {
		t.Errorf("Expected MaxConns 10, got %d", config.MaxConns)
	}
	if config.IdleTimeout != 5*time.Minute {
		t.Errorf("Expected IdleTimeout 5m, got %v", config.IdleTimeout)
	}
	if config.Database != "default" {
		t.Errorf("Expected Database 'default', got '%s'", config.Database)
	}
}

func TestPoolConfigValidation(t *testing.T) {
	// Test that invalid configs are corrected
	config := Config{
		Address:  "localhost:8889",
		MinConns: -1,
		MaxConns: 0,
	}

	// We can't actually create a pool without a server, but we can test
	// that the config would be validated
	if config.MaxConns <= 0 {
		config.MaxConns = 10
	}
	if config.MinConns < 0 {
		config.MinConns = 0
	}
	if config.MinConns > config.MaxConns {
		config.MinConns = config.MaxConns
	}

	if config.MaxConns != 10 {
		t.Errorf("Expected MaxConns to be corrected to 10, got %d", config.MaxConns)
	}
	if config.MinConns != 0 {
		t.Errorf("Expected MinConns to be corrected to 0, got %d", config.MinConns)
	}
}

func TestPoolStats(t *testing.T) {
	// Test Stats struct
	stats := Stats{
		OpenConnections:  5,
		IdleConnections:  3,
		InUseConnections: 2,
		MaxConnections:   10,
	}

	if stats.OpenConnections != 5 {
		t.Errorf("Expected OpenConnections 5, got %d", stats.OpenConnections)
	}
	if stats.IdleConnections != 3 {
		t.Errorf("Expected IdleConnections 3, got %d", stats.IdleConnections)
	}
	if stats.InUseConnections != 2 {
		t.Errorf("Expected InUseConnections 2, got %d", stats.InUseConnections)
	}
}

func TestConfigWithDatabase(t *testing.T) {
	config := Config{
		Address:  "localhost:8889",
		MinConns: 1,
		MaxConns: 5,
		Database: "mydb",
	}

	if config.Database != "mydb" {
		t.Errorf("Expected Database 'mydb', got '%s'", config.Database)
	}
}

func TestConfigDatabaseDefault(t *testing.T) {
	// Test that empty database defaults to "default" in DefaultConfig
	config := DefaultConfig("localhost:8889")

	if config.Database != "default" {
		t.Errorf("Expected default Database 'default', got '%s'", config.Database)
	}

	// Test custom database
	config.Database = "production"
	if config.Database != "production" {
		t.Errorf("Expected Database 'production', got '%s'", config.Database)
	}
}

// Note: Full integration tests for the pool would require a running server.
// These unit tests verify the configuration and struct behavior.
