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

	// Should have _system + default + 3 created = 5 databases
	if len(databases) != 5 {
		t.Errorf("Expected 5 databases, got %d: %v", len(databases), databases)
	}

	// Verify all databases are in the list
	expected := map[string]bool{SystemDatabaseName: true, "default": true, "db1": true, "db2": true, "db3": true}
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

func TestDatabaseManagerWithOptions(t *testing.T) {
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

	// Create database with options
	opts := CreateDatabaseOptions{
		Owner:       "testuser",
		Encoding:    EncodingUTF8,
		Locale:      "de_DE",
		Collation:   CollationUnicode,
		Description: "Test database",
	}
	err = mgr.CreateDatabaseWithOptions("mydb", opts)
	if err != nil {
		t.Fatalf("Failed to create database with options: %v", err)
	}

	// Get the database and verify metadata
	db, err := mgr.GetDatabase("mydb")
	if err != nil {
		t.Fatalf("Failed to get database: %v", err)
	}

	if db.Metadata.Owner != "testuser" {
		t.Errorf("Expected owner 'testuser', got '%s'", db.Metadata.Owner)
	}
	if db.Metadata.Encoding != EncodingUTF8 {
		t.Errorf("Expected encoding UTF8, got '%s'", db.Metadata.Encoding)
	}
	if db.Metadata.Locale != "de_DE" {
		t.Errorf("Expected locale 'de_DE', got '%s'", db.Metadata.Locale)
	}
	if db.Metadata.Collation != CollationUnicode {
		t.Errorf("Expected collation 'unicode', got '%s'", db.Metadata.Collation)
	}
	if db.Metadata.Description != "Test database" {
		t.Errorf("Expected description 'Test database', got '%s'", db.Metadata.Description)
	}
}

func TestDatabaseMetadataPersistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flydb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create database with metadata
	mgr1, err := NewDatabaseManager(tmpDir, EncryptionConfig{})
	if err != nil {
		t.Fatalf("Failed to create DatabaseManager: %v", err)
	}

	opts := CreateDatabaseOptions{
		Owner:    "alice",
		Encoding: EncodingLatin1,
		Locale:   "fr_FR",
	}
	err = mgr1.CreateDatabaseWithOptions("persistent", opts)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	mgr1.Close()

	// Reopen and verify metadata persisted
	mgr2, err := NewDatabaseManager(tmpDir, EncryptionConfig{})
	if err != nil {
		t.Fatalf("Failed to reopen DatabaseManager: %v", err)
	}
	defer mgr2.Close()

	db, err := mgr2.GetDatabase("persistent")
	if err != nil {
		t.Fatalf("Failed to get database: %v", err)
	}

	if db.Metadata.Owner != "alice" {
		t.Errorf("Expected owner 'alice', got '%s'", db.Metadata.Owner)
	}
	if db.Metadata.Encoding != EncodingLatin1 {
		t.Errorf("Expected encoding LATIN1, got '%s'", db.Metadata.Encoding)
	}
	if db.Metadata.Locale != "fr_FR" {
		t.Errorf("Expected locale 'fr_FR', got '%s'", db.Metadata.Locale)
	}
}

func TestDatabaseHelperMethods(t *testing.T) {
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

	opts := CreateDatabaseOptions{
		Encoding:  EncodingUTF16,
		Locale:    "ja_JP",
		Collation: CollationBinary,
	}
	mgr.CreateDatabaseWithOptions("helpers", opts)

	db, _ := mgr.GetDatabase("helpers")

	if db.GetEncoding() != EncodingUTF16 {
		t.Errorf("GetEncoding() returned wrong value")
	}
	if db.GetLocale() != "ja_JP" {
		t.Errorf("GetLocale() returned wrong value")
	}
	if db.GetCollation() != CollationBinary {
		t.Errorf("GetCollation() returned wrong value")
	}
	if db.IsReadOnly() != false {
		t.Errorf("IsReadOnly() should return false by default")
	}
}
