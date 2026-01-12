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

package sdk

import (
	"testing"
)

func TestNewSession(t *testing.T) {
	session := NewSession("testuser", "testdb")

	if session.Username != "testuser" {
		t.Errorf("Expected username 'testuser', got '%s'", session.Username)
	}
	if session.Database != "testdb" {
		t.Errorf("Expected database 'testdb', got '%s'", session.Database)
	}
	if session.State != SessionStateActive {
		t.Errorf("Expected state Active, got %v", session.State)
	}
	if !session.AutoCommit {
		t.Error("Expected AutoCommit to be true by default")
	}
	if session.IsolationLevel != IsolationReadCommitted {
		t.Errorf("Expected IsolationLevel ReadCommitted, got %v", session.IsolationLevel)
	}
}

func TestSessionSetGetDatabase(t *testing.T) {
	session := NewSession("user", "default")

	if session.GetDatabase() != "default" {
		t.Errorf("Expected initial database 'default', got '%s'", session.GetDatabase())
	}

	session.SetDatabase("newdb")

	if session.GetDatabase() != "newdb" {
		t.Errorf("Expected database 'newdb', got '%s'", session.GetDatabase())
	}
}

func TestNewConnectionConfig(t *testing.T) {
	config := NewConnectionConfig()

	if config.Host != "localhost" {
		t.Errorf("Expected host 'localhost', got '%s'", config.Host)
	}
	if config.Port != 5433 {
		t.Errorf("Expected port 5433, got %d", config.Port)
	}
	if config.Database != "default" {
		t.Errorf("Expected database 'default', got '%s'", config.Database)
	}
	if config.ConnectTimeout != 30 {
		t.Errorf("Expected connect timeout 30, got %d", config.ConnectTimeout)
	}
	if !config.AutoCommit {
		t.Error("Expected AutoCommit to be true by default")
	}
}

func TestParseODBCConnectionString(t *testing.T) {
	tests := []struct {
		name     string
		connStr  string
		expected *ConnectionConfig
	}{
		{
			name:    "basic ODBC",
			connStr: "Server=myhost;Port=5433;Database=mydb;Uid=user;Pwd=pass",
			expected: &ConnectionConfig{
				Host:     "myhost",
				Port:     5433,
				Database: "mydb",
				Username: "user",
				Password: "pass",
			},
		},
		{
			name:    "ODBC with driver",
			connStr: "Driver={FlyDB};Server=localhost;Port=8889;Database=testdb;Uid=admin;Pwd=secret",
			expected: &ConnectionConfig{
				Host:     "localhost",
				Port:     8889,
				Database: "testdb",
				Username: "admin",
				Password: "secret",
			},
		},
		{
			name:    "ODBC with Data Source",
			connStr: "Data Source=192.168.1.1;Initial Catalog=production;User Id=app;Password=apppass",
			expected: &ConnectionConfig{
				Host:     "192.168.1.1",
				Port:     5433, // default
				Database: "production",
				Username: "app",
				Password: "apppass",
			},
		},
		{
			name:    "ODBC with options",
			connStr: "Server=host;Database=db;Uid=u;Pwd=p;ReadOnly=true;AutoCommit=false",
			expected: &ConnectionConfig{
				Host:       "host",
				Database:   "db",
				Username:   "u",
				Password:   "p",
				ReadOnly:   true,
				AutoCommit: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := ParseConnectionString(tt.connStr)
			if err != nil {
				t.Fatalf("ParseConnectionString failed: %v", err)
			}

			if config.Host != tt.expected.Host {
				t.Errorf("Host mismatch: expected '%s', got '%s'", tt.expected.Host, config.Host)
			}
			if tt.expected.Port != 0 && config.Port != tt.expected.Port {
				t.Errorf("Port mismatch: expected %d, got %d", tt.expected.Port, config.Port)
			}
			if config.Database != tt.expected.Database {
				t.Errorf("Database mismatch: expected '%s', got '%s'", tt.expected.Database, config.Database)
			}
			if config.Username != tt.expected.Username {
				t.Errorf("Username mismatch: expected '%s', got '%s'", tt.expected.Username, config.Username)
			}
			if config.Password != tt.expected.Password {
				t.Errorf("Password mismatch: expected '%s', got '%s'", tt.expected.Password, config.Password)
			}
		})
	}
}

func TestParseJDBCConnectionString(t *testing.T) {
	tests := []struct {
		name     string
		connStr  string
		expected *ConnectionConfig
	}{
		{
			name:    "basic JDBC",
			connStr: "jdbc:flydb://myhost:5433/mydb?user=testuser&password=testpass",
			expected: &ConnectionConfig{
				Host:     "myhost",
				Port:     5433,
				Database: "mydb",
				Username: "testuser",
				Password: "testpass",
			},
		},
		{
			name:    "JDBC without port",
			connStr: "jdbc:flydb://localhost/testdb?user=admin&password=secret",
			expected: &ConnectionConfig{
				Host:     "localhost",
				Port:     5433, // default
				Database: "testdb",
				Username: "admin",
				Password: "secret",
			},
		},
		{
			name:    "JDBC with options",
			connStr: "jdbc:flydb://host:8889/db?user=u&password=p&readonly=true&autocommit=false",
			expected: &ConnectionConfig{
				Host:       "host",
				Port:       8889,
				Database:   "db",
				Username:   "u",
				Password:   "p",
				ReadOnly:   true,
				AutoCommit: false,
			},
		},
		{
			name:    "JDBC minimal",
			connStr: "jdbc:flydb://server/database",
			expected: &ConnectionConfig{
				Host:     "server",
				Port:     5433,
				Database: "database",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := ParseConnectionString(tt.connStr)
			if err != nil {
				t.Fatalf("ParseConnectionString failed: %v", err)
			}

			if config.Host != tt.expected.Host {
				t.Errorf("Host mismatch: expected '%s', got '%s'", tt.expected.Host, config.Host)
			}
			if config.Port != tt.expected.Port {
				t.Errorf("Port mismatch: expected %d, got %d", tt.expected.Port, config.Port)
			}
			if config.Database != tt.expected.Database {
				t.Errorf("Database mismatch: expected '%s', got '%s'", tt.expected.Database, config.Database)
			}
			if config.Username != tt.expected.Username {
				t.Errorf("Username mismatch: expected '%s', got '%s'", tt.expected.Username, config.Username)
			}
			if config.Password != tt.expected.Password {
				t.Errorf("Password mismatch: expected '%s', got '%s'", tt.expected.Password, config.Password)
			}
			if config.ReadOnly != tt.expected.ReadOnly {
				t.Errorf("ReadOnly mismatch: expected %v, got %v", tt.expected.ReadOnly, config.ReadOnly)
			}
		})
	}
}

func TestIsolationLevelString(t *testing.T) {
	tests := []struct {
		level    IsolationLevel
		expected string
	}{
		{IsolationReadUncommitted, "READ UNCOMMITTED"},
		{IsolationReadCommitted, "READ COMMITTED"},
		{IsolationRepeatableRead, "REPEATABLE READ"},
		{IsolationSerializable, "SERIALIZABLE"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if tt.level.String() != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, tt.level.String())
			}
		})
	}
}

func TestIsolationLevelODBCValue(t *testing.T) {
	tests := []struct {
		level    IsolationLevel
		expected int
	}{
		{IsolationReadUncommitted, 1},
		{IsolationReadCommitted, 2},
		{IsolationRepeatableRead, 4},
		{IsolationSerializable, 8},
	}

	for _, tt := range tests {
		if tt.level.ODBCValue() != tt.expected {
			t.Errorf("IsolationLevel %v: expected ODBC value %d, got %d", tt.level, tt.expected, tt.level.ODBCValue())
		}
	}
}

func TestIsolationLevelJDBCValue(t *testing.T) {
	tests := []struct {
		level    IsolationLevel
		expected int
	}{
		{IsolationReadUncommitted, 1},
		{IsolationReadCommitted, 2},
		{IsolationRepeatableRead, 4},
		{IsolationSerializable, 8},
	}

	for _, tt := range tests {
		if tt.level.JDBCValue() != tt.expected {
			t.Errorf("IsolationLevel %v: expected JDBC value %d, got %d", tt.level, tt.expected, tt.level.JDBCValue())
		}
	}
}

func TestSessionClose(t *testing.T) {
	session := NewSession("user", "db")

	// Add some cursors and prepared statements
	cursor := NewCursor(session.ID)
	session.AddCursor(cursor)

	stmt := NewPreparedStatement(session.ID, "SELECT * FROM users WHERE id = $1")
	session.AddPreparedStatement(stmt)

	if len(session.Cursors) != 1 {
		t.Errorf("Expected 1 cursor, got %d", len(session.Cursors))
	}
	if len(session.PreparedStmts) != 1 {
		t.Errorf("Expected 1 prepared statement, got %d", len(session.PreparedStmts))
	}

	err := session.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if session.State != SessionStateClosed {
		t.Errorf("Expected state Closed, got %v", session.State)
	}
	if len(session.Cursors) != 0 {
		t.Errorf("Expected 0 cursors after close, got %d", len(session.Cursors))
	}
	if len(session.PreparedStmts) != 0 {
		t.Errorf("Expected 0 prepared statements after close, got %d", len(session.PreparedStmts))
	}
}
