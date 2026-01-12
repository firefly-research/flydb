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
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"flydb/internal/auth"
	"flydb/internal/protocol"
	"flydb/internal/storage"
)

// testAdminPassword is the password used for the admin user in tests.
const testAdminPassword = "admin"

func setupTestServer(t *testing.T) (*Server, string, func()) {
	tmpDir, err := os.MkdirTemp("", "flydb_server_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	config := storage.StorageConfig{
		DataDir:            tmpDir,
		BufferPoolSize:     256,
		CheckpointInterval: 0,
	}
	store, err := storage.NewStorageEngine(config)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create storage engine: %v", err)
	}

	// Initialize admin user for tests
	authMgr := auth.NewAuthManager(store)
	if err := authMgr.InitializeAdmin(testAdminPassword); err != nil {
		store.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to initialize admin user: %v", err)
	}

	// Use a random available port
	port := findAvailablePort(t)
	addr := fmt.Sprintf(":%d", port)
	srv := NewServerWithStore(addr, store)

	cleanup := func() {
		srv.Stop()
		store.Close()
		time.Sleep(10 * time.Millisecond)
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

// sendBinaryMessage sends a binary protocol message to the connection.
func sendBinaryMessage(conn net.Conn, msgType protocol.MessageType, payload []byte) error {
	return protocol.WriteMessage(conn, msgType, payload)
}

// readBinaryMessage reads a binary protocol message from the connection.
func readBinaryMessage(conn net.Conn) (*protocol.Message, error) {
	return protocol.ReadMessage(conn)
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

	// Set read timeout
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Send PING using binary protocol
	if err := sendBinaryMessage(conn, protocol.MsgPing, nil); err != nil {
		t.Fatalf("Failed to send PING: %v", err)
	}

	// Read response
	msg, err := readBinaryMessage(conn)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	if msg.Header.Type != protocol.MsgPong {
		t.Errorf("Expected PONG message type, got %v", msg.Header.Type)
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

	// Set read timeout
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Test admin authentication
	authMsg := &protocol.AuthMessage{
		Username: "admin",
		Password: testAdminPassword,
	}
	payload, err := authMsg.Encode()
	if err != nil {
		t.Fatalf("Failed to encode auth message: %v", err)
	}

	if err := sendBinaryMessage(conn, protocol.MsgAuth, payload); err != nil {
		t.Fatalf("Failed to send AUTH: %v", err)
	}

	msg, err := readBinaryMessage(conn)
	if err != nil {
		t.Fatalf("Failed to read auth response: %v", err)
	}

	if msg.Header.Type != protocol.MsgAuthResult {
		t.Errorf("Expected AuthResult message type, got %v", msg.Header.Type)
	}

	authResult, err := protocol.DecodeAuthResultMessage(msg.Payload)
	if err != nil {
		t.Fatalf("Failed to decode auth result: %v", err)
	}

	if !authResult.Success {
		t.Errorf("Expected successful authentication, got failure: %s", authResult.Message)
	}
}

func TestServerAuthFailure(t *testing.T) {
	srv, addr, cleanup := setupTestServer(t)
	defer cleanup()

	go srv.Start()
	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Set read timeout
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Test failed authentication
	authMsg := &protocol.AuthMessage{
		Username: "baduser",
		Password: "badpass",
	}
	payload, err := authMsg.Encode()
	if err != nil {
		t.Fatalf("Failed to encode auth message: %v", err)
	}

	if err := sendBinaryMessage(conn, protocol.MsgAuth, payload); err != nil {
		t.Fatalf("Failed to send AUTH: %v", err)
	}

	msg, err := readBinaryMessage(conn)
	if err != nil {
		t.Fatalf("Failed to read auth response: %v", err)
	}

	if msg.Header.Type != protocol.MsgAuthResult {
		t.Errorf("Expected AuthResult message type, got %v", msg.Header.Type)
	}

	authResult, err := protocol.DecodeAuthResultMessage(msg.Payload)
	if err != nil {
		t.Fatalf("Failed to decode auth result: %v", err)
	}

	if authResult.Success {
		t.Errorf("Expected authentication failure, got success")
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

	// Set read timeout
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Authenticate as admin
	authMsg := &protocol.AuthMessage{
		Username: "admin",
		Password: testAdminPassword,
	}
	payload, err := authMsg.Encode()
	if err != nil {
		t.Fatalf("Failed to encode auth message: %v", err)
	}

	if err := sendBinaryMessage(conn, protocol.MsgAuth, payload); err != nil {
		t.Fatalf("Failed to send AUTH: %v", err)
	}

	msg, err := readBinaryMessage(conn)
	if err != nil {
		t.Fatalf("Failed to read auth response: %v", err)
	}

	authResult, err := protocol.DecodeAuthResultMessage(msg.Payload)
	if err != nil {
		t.Fatalf("Failed to decode auth result: %v", err)
	}

	if !authResult.Success {
		t.Fatalf("Authentication failed: %s", authResult.Message)
	}

	// Create a table
	queryMsg := &protocol.QueryMessage{Query: "CREATE TABLE users (id INT, name TEXT)"}
	payload, err = queryMsg.Encode()
	if err != nil {
		t.Fatalf("Failed to encode query message: %v", err)
	}

	if err := sendBinaryMessage(conn, protocol.MsgQuery, payload); err != nil {
		t.Fatalf("Failed to send CREATE TABLE: %v", err)
	}

	msg, err = readBinaryMessage(conn)
	if err != nil {
		t.Fatalf("Failed to read CREATE TABLE response: %v", err)
	}

	if msg.Header.Type == protocol.MsgError {
		errMsg, _ := protocol.DecodeErrorMessage(msg.Payload)
		t.Fatalf("CREATE TABLE failed: %s", errMsg.Message)
	}

	// Insert a row
	queryMsg = &protocol.QueryMessage{Query: "INSERT INTO users VALUES (1, 'Alice')"}
	payload, err = queryMsg.Encode()
	if err != nil {
		t.Fatalf("Failed to encode query message: %v", err)
	}

	if err := sendBinaryMessage(conn, protocol.MsgQuery, payload); err != nil {
		t.Fatalf("Failed to send INSERT: %v", err)
	}

	msg, err = readBinaryMessage(conn)
	if err != nil {
		t.Fatalf("Failed to read INSERT response: %v", err)
	}

	if msg.Header.Type == protocol.MsgError {
		errMsg, _ := protocol.DecodeErrorMessage(msg.Payload)
		t.Fatalf("INSERT failed: %s", errMsg.Message)
	}

	// Select the row
	queryMsg = &protocol.QueryMessage{Query: "SELECT name FROM users WHERE id = 1"}
	payload, err = queryMsg.Encode()
	if err != nil {
		t.Fatalf("Failed to encode query message: %v", err)
	}

	if err := sendBinaryMessage(conn, protocol.MsgQuery, payload); err != nil {
		t.Fatalf("Failed to send SELECT: %v", err)
	}

	msg, err = readBinaryMessage(conn)
	if err != nil {
		t.Fatalf("Failed to read SELECT response: %v", err)
	}

	if msg.Header.Type == protocol.MsgError {
		errMsg, _ := protocol.DecodeErrorMessage(msg.Payload)
		t.Fatalf("SELECT failed: %s", errMsg.Message)
	}

	if msg.Header.Type != protocol.MsgQueryResult {
		t.Fatalf("Expected QueryResult message type, got %v", msg.Header.Type)
	}

	result, err := protocol.DecodeQueryResultMessage(msg.Payload)
	if err != nil {
		t.Fatalf("Failed to decode query result: %v", err)
	}

	if !result.Success {
		t.Fatalf("Query failed: %s", result.Message)
	}

	// The result is returned as a text message containing the query output
	// Check that the message contains "Alice"
	if !strings.Contains(result.Message, "Alice") {
		t.Errorf("Expected 'Alice' in response, got '%s'", result.Message)
	}
}

