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

package server

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"flydb/internal/auth"
	"flydb/internal/storage"
)

// testAdminPassword is the password used for the admin user in tests.
const testAdminPassword = "admin"

func setupTestServer(t *testing.T) (*Server, string, func()) {
	tmpDir, err := os.MkdirTemp("", "flydb_server_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	kv, err := storage.NewKVStore(tmpDir + "/test.wal")
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create KVStore: %v", err)
	}

	// Initialize admin user for tests
	authMgr := auth.NewAuthManager(kv)
	if err := authMgr.InitializeAdmin(testAdminPassword); err != nil {
		kv.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to initialize admin user: %v", err)
	}

	// Use a random available port
	port := findAvailablePort(t)
	addr := fmt.Sprintf(":%d", port)
	srv := NewServerWithStore(addr, kv)

	cleanup := func() {
		kv.Close()
		os.RemoveAll(tmpDir)
	}

	return srv, addr, cleanup
}

func findAvailablePort(t *testing.T) int {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Failed to find available port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

func TestServerPing(t *testing.T) {
	srv, addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Start server in background
	go srv.Start()
	time.Sleep(100 * time.Millisecond) // Wait for server to start

	// Connect to server
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Send PING
	fmt.Fprintln(conn, "PING")

	// Read response
	scanner := bufio.NewScanner(conn)
	if scanner.Scan() {
		response := scanner.Text()
		if response != "PONG" {
			t.Errorf("Expected 'PONG', got '%s'", response)
		}
	} else {
		t.Error("No response received")
	}
}

func TestServerAuth(t *testing.T) {
	srv, addr, cleanup := setupTestServer(t)
	defer cleanup()

	go srv.Start()
	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	scanner := bufio.NewScanner(conn)

	// Test admin authentication
	fmt.Fprintln(conn, "AUTH admin admin")
	if scanner.Scan() {
		response := scanner.Text()
		if !strings.Contains(response, "AUTH OK") {
			t.Errorf("Expected 'AUTH OK', got '%s'", response)
		}
	}

	// Test failed authentication
	fmt.Fprintln(conn, "AUTH baduser badpass")
	if scanner.Scan() {
		response := scanner.Text()
		if !strings.Contains(response, "ERROR") {
			t.Errorf("Expected error for bad auth, got '%s'", response)
		}
	}
}

func TestServerSQL(t *testing.T) {
	srv, addr, cleanup := setupTestServer(t)
	defer cleanup()

	go srv.Start()
	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	scanner := bufio.NewScanner(conn)

	// Authenticate as admin
	fmt.Fprintln(conn, "AUTH admin admin")
	scanner.Scan() // consume auth response

	// Create a table
	fmt.Fprintln(conn, "SQL CREATE TABLE users (id INT, name TEXT)")
	if scanner.Scan() {
		response := scanner.Text()
		if strings.Contains(response, "ERROR") {
			t.Errorf("CREATE TABLE failed: %s", response)
		}
	}

	// Insert a row
	fmt.Fprintln(conn, "SQL INSERT INTO users VALUES (1, 'Alice')")
	if scanner.Scan() {
		response := scanner.Text()
		if strings.Contains(response, "ERROR") {
			t.Errorf("INSERT failed: %s", response)
		}
	}

	// Select the row
	fmt.Fprintln(conn, "SQL SELECT name FROM users WHERE id = 1")
	if scanner.Scan() {
		response := scanner.Text()
		if !strings.Contains(response, "Alice") {
			t.Errorf("Expected 'Alice' in response, got '%s'", response)
		}
	}
}

