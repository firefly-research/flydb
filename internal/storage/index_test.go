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
	"encoding/json"
	"os"
	"testing"
)

func setupIndexTest(t *testing.T) (*IndexManager, *KVStore, func()) {
	tmpDir, err := os.MkdirTemp("", "flydb_index_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	store, err := NewKVStore(tmpDir + "/test.wal")
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create KVStore: %v", err)
	}

	indexMgr := NewIndexManager(store)

	cleanup := func() {
		store.Close()
		os.RemoveAll(tmpDir)
	}

	return indexMgr, store, cleanup
}

func TestIndexManagerCreateIndex(t *testing.T) {
	indexMgr, _, cleanup := setupIndexTest(t)
	defer cleanup()

	err := indexMgr.CreateIndex("users", "email")
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	if !indexMgr.HasIndex("users", "email") {
		t.Error("Expected index to exist")
	}
}

func TestIndexManagerLookup(t *testing.T) {
	indexMgr, store, cleanup := setupIndexTest(t)
	defer cleanup()

	// Insert a row
	row := map[string]interface{}{"id": "1", "email": "alice@example.com"}
	rowData, _ := json.Marshal(row)
	store.Put("row:users:1", rowData)

	// Create index
	indexMgr.CreateIndex("users", "email")

	// Lookup by email
	rowKey, found := indexMgr.Lookup("users", "email", "alice@example.com")
	if !found {
		t.Fatal("Expected to find row by email")
	}
	if rowKey != "row:users:1" {
		t.Errorf("Expected 'row:users:1', got '%s'", rowKey)
	}
}

func TestIndexManagerOnInsert(t *testing.T) {
	indexMgr, _, cleanup := setupIndexTest(t)
	defer cleanup()

	// Create index first
	indexMgr.CreateIndex("users", "email")

	// Insert a row
	row := map[string]interface{}{"id": "1", "email": "bob@example.com"}
	indexMgr.OnInsert("users", "row:users:1", row)

	// Lookup should find the new row
	rowKey, found := indexMgr.Lookup("users", "email", "bob@example.com")
	if !found {
		t.Fatal("Expected to find row after OnInsert")
	}
	if rowKey != "row:users:1" {
		t.Errorf("Expected 'row:users:1', got '%s'", rowKey)
	}
}

func TestIndexManagerOnDelete(t *testing.T) {
	indexMgr, _, cleanup := setupIndexTest(t)
	defer cleanup()

	// Create index and insert row
	indexMgr.CreateIndex("users", "email")
	row := map[string]interface{}{"id": "1", "email": "charlie@example.com"}
	indexMgr.OnInsert("users", "row:users:1", row)

	// Delete the row
	indexMgr.OnDelete("users", "row:users:1", row)

	// Lookup should not find the row
	_, found := indexMgr.Lookup("users", "email", "charlie@example.com")
	if found {
		t.Error("Expected row to be removed from index after OnDelete")
	}
}

func TestIndexManagerDropIndex(t *testing.T) {
	indexMgr, _, cleanup := setupIndexTest(t)
	defer cleanup()

	// Create and then drop index
	indexMgr.CreateIndex("users", "email")
	if !indexMgr.HasIndex("users", "email") {
		t.Fatal("Index should exist after creation")
	}

	err := indexMgr.DropIndex("users", "email")
	if err != nil {
		t.Fatalf("DropIndex failed: %v", err)
	}

	if indexMgr.HasIndex("users", "email") {
		t.Error("Index should not exist after drop")
	}
}

func TestIndexManagerGetIndexedColumns(t *testing.T) {
	indexMgr, _, cleanup := setupIndexTest(t)
	defer cleanup()

	// Create multiple indexes
	indexMgr.CreateIndex("users", "email")
	indexMgr.CreateIndex("users", "name")

	columns := indexMgr.GetIndexedColumns("users")
	if len(columns) != 2 {
		t.Errorf("Expected 2 indexed columns, got %d", len(columns))
	}
}

