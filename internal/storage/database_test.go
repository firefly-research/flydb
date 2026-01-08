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

package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDatabaseManagerCreate(t *testing.T) {
	// Create a temporary directory for the test
	tmpDir, err := os.MkdirTemp("", "flydb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a new DatabaseManager
	mgr, err := NewDatabaseManager(tmpDir, EncryptionConfig{})
	if err != nil {
		t.Fatalf("Failed to create DatabaseManager: %v", err)
	}
	defer mgr.Close()

	// Verify default database exists
	if !mgr.DatabaseExists(DefaultDatabaseName) {
		t.Error("Default database should exist")
	}

	// Create a new database
	err = mgr.CreateDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Verify the database exists
	if !mgr.DatabaseExists("testdb") {
		t.Error("testdb should exist after creation")
	}

	// Verify the database file was created
	dbPath := filepath.Join(tmpDir, "testdb.fdb")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Error("Database file should exist on disk")
	}
}

func TestDatabaseManagerList(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flydb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	mgr, err := NewDatabaseManager(tmpDir, EncryptionConfig{})
	if err != nil {
		t.Fatalf("Failed to create DatabaseManager: %v", err)
	}
	defer mgr.Close()

	// Create some databases
	mgr.CreateDatabase("db1")
	mgr.CreateDatabase("db2")
	mgr.CreateDatabase("db3")

	// List databases
	databases := mgr.ListDatabases()

	// Should have default + 3 created = 4 databases
	if len(databases) != 4 {
		t.Errorf("Expected 4 databases, got %d: %v", len(databases), databases)
	}

	// Verify all databases are in the list
	expected := map[string]bool{"default": true, "db1": true, "db2": true, "db3": true}
	for _, db := range databases {
		if !expected[db] {
			t.Errorf("Unexpected database in list: %s", db)
		}
		delete(expected, db)
	}
	if len(expected) > 0 {
		t.Errorf("Missing databases: %v", expected)
	}
}

func TestDatabaseManagerDrop(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flydb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	mgr, err := NewDatabaseManager(tmpDir, EncryptionConfig{})
	if err != nil {
		t.Fatalf("Failed to create DatabaseManager: %v", err)
	}
	defer mgr.Close()

	// Create a database
	mgr.CreateDatabase("dropme")
	if !mgr.DatabaseExists("dropme") {
		t.Fatal("Database should exist after creation")
	}

	// Drop the database
	err = mgr.DropDatabase("dropme")
	if err != nil {
		t.Fatalf("Failed to drop database: %v", err)
	}

	// Verify it no longer exists
	if mgr.DatabaseExists("dropme") {
		t.Error("Database should not exist after drop")
	}

	// Verify the file was deleted
	dbPath := filepath.Join(tmpDir, "dropme.fdb")
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Error("Database file should be deleted from disk")
	}
}

func TestDatabaseManagerCannotDropDefault(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flydb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	mgr, err := NewDatabaseManager(tmpDir, EncryptionConfig{})
	if err != nil {
		t.Fatalf("Failed to create DatabaseManager: %v", err)
	}
	defer mgr.Close()

	// Try to drop the default database
	err = mgr.DropDatabase(DefaultDatabaseName)
	if err == nil {
		t.Error("Should not be able to drop the default database")
	}
}

