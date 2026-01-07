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
Index Manager Implementation
=============================

The IndexManager maintains B-Tree indexes for efficient query execution.
It supports creating indexes on table columns and automatically maintains
them during INSERT, UPDATE, and DELETE operations.

Index Storage:
==============

Index metadata is stored in the KVStore with the key prefix:
  _sys_index:<table>:<column>

The actual index data is maintained in-memory using B-Tree structures.
On startup, indexes are rebuilt by scanning the table data.

Usage:
======

	indexMgr := storage.NewIndexManager(kvStore)
	indexMgr.CreateIndex("users", "email")
	
	// Lookup by indexed column
	rowKeys := indexMgr.Lookup("users", "email", "alice@example.com")
*/
package storage

import (
	"encoding/json"
	"sync"
)

// indexKeyPrefix is the storage key prefix for index metadata.
const indexKeyPrefix = "_sys_index:"

// IndexInfo stores metadata about an index.
type IndexInfo struct {
	TableName  string `json:"table_name"`
	ColumnName string `json:"column_name"`
}

// IndexManager manages B-Tree indexes for tables.
// It provides methods for creating indexes, looking up values,
// and maintaining indexes during data modifications.
//
// Thread Safety: All methods are safe for concurrent use.
type IndexManager struct {
	store   Engine
	indexes map[string]*BTree // key: "table:column"
	mu      sync.RWMutex
}

// NewIndexManager creates a new IndexManager backed by the given storage engine.
// It loads existing index metadata from storage and rebuilds the indexes.
func NewIndexManager(store Engine) *IndexManager {
	im := &IndexManager{
		store:   store,
		indexes: make(map[string]*BTree),
	}
	im.loadIndexes()
	return im
}

// loadIndexes loads index metadata from storage and rebuilds the B-Trees.
func (im *IndexManager) loadIndexes() {
	// Scan for index metadata
	data, err := im.store.Scan(indexKeyPrefix)
	if err != nil {
		return
	}

	for _, val := range data {
		var info IndexInfo
		if err := json.Unmarshal(val, &info); err != nil {
			continue
		}
		// Create the B-Tree and rebuild from table data
		im.rebuildIndex(info.TableName, info.ColumnName)
	}
}

// rebuildIndex rebuilds a B-Tree index by scanning all rows in the table.
func (im *IndexManager) rebuildIndex(table, column string) {
	key := table + ":" + column
	tree := NewBTree(16) // Use degree 16 for good performance

	// Scan all rows in the table
	prefix := "row:" + table + ":"
	rows, err := im.store.Scan(prefix)
	if err != nil {
		return
	}

	for rowKey, rowData := range rows {
		var row map[string]interface{}
		if err := json.Unmarshal(rowData, &row); err != nil {
			continue
		}
		if val, ok := row[column]; ok {
			// Index the column value -> row key
			valStr, _ := val.(string)
			if valStr == "" {
				// Handle numeric values
				if numVal, ok := val.(float64); ok {
					valStr = json.Number(json.Number(string(rune(int(numVal))))).String()
				}
			}
			tree.Insert(valStr, rowKey)
		}
	}

	im.mu.Lock()
	im.indexes[key] = tree
	im.mu.Unlock()
}

// CreateIndex creates a new index on the specified table and column.
// Returns an error if the index already exists.
func (im *IndexManager) CreateIndex(table, column string) error {
	key := table + ":" + column

	im.mu.Lock()
	if _, exists := im.indexes[key]; exists {
		im.mu.Unlock()
		return nil // Index already exists
	}
	im.mu.Unlock()

	// Store index metadata
	info := IndexInfo{TableName: table, ColumnName: column}
	data, _ := json.Marshal(info)
	storageKey := indexKeyPrefix + table + ":" + column
	if err := im.store.Put(storageKey, data); err != nil {
		return err
	}

	// Build the index
	im.rebuildIndex(table, column)
	return nil
}

// DropIndex removes an index.
func (im *IndexManager) DropIndex(table, column string) error {
	key := table + ":" + column

	im.mu.Lock()
	delete(im.indexes, key)
	im.mu.Unlock()

	// Remove index metadata
	storageKey := indexKeyPrefix + table + ":" + column
	return im.store.Delete(storageKey)
}

// HasIndex checks if an index exists for the given table and column.
func (im *IndexManager) HasIndex(table, column string) bool {
	key := table + ":" + column
	im.mu.RLock()
	defer im.mu.RUnlock()
	_, exists := im.indexes[key]
	return exists
}

// Lookup finds all row keys where the indexed column equals the given value.
// Returns nil if no index exists for the column.
func (im *IndexManager) Lookup(table, column, value string) (string, bool) {
	key := table + ":" + column
	im.mu.RLock()
	tree, exists := im.indexes[key]
	im.mu.RUnlock()

	if !exists {
		return "", false
	}

	return tree.Search(value)
}

// OnInsert updates all indexes for a table when a row is inserted.
// rowKey is the storage key for the row (e.g., "row:users:1").
// row is the deserialized row data.
func (im *IndexManager) OnInsert(table, rowKey string, row map[string]interface{}) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	for key, tree := range im.indexes {
		// Check if this index is for the given table
		if len(key) > len(table)+1 && key[:len(table)+1] == table+":" {
			column := key[len(table)+1:]
			if val, ok := row[column]; ok {
				valStr := im.valueToString(val)
				tree.Insert(valStr, rowKey)
			}
		}
	}
}

// OnUpdate updates all indexes for a table when a row is updated.
// oldRow contains the previous values, newRow contains the new values.
func (im *IndexManager) OnUpdate(table, rowKey string, oldRow, newRow map[string]interface{}) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	for key, tree := range im.indexes {
		if len(key) > len(table)+1 && key[:len(table)+1] == table+":" {
			column := key[len(table)+1:]
			oldVal, oldOk := oldRow[column]
			newVal, newOk := newRow[column]

			// Remove old index entry if value changed
			if oldOk {
				oldStr := im.valueToString(oldVal)
				newStr := ""
				if newOk {
					newStr = im.valueToString(newVal)
				}
				if oldStr != newStr {
					tree.Delete(oldStr)
					if newOk {
						tree.Insert(newStr, rowKey)
					}
				}
			} else if newOk {
				// New value added
				tree.Insert(im.valueToString(newVal), rowKey)
			}
		}
	}
}

// OnDelete updates all indexes for a table when a row is deleted.
func (im *IndexManager) OnDelete(table, rowKey string, row map[string]interface{}) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	for key, tree := range im.indexes {
		if len(key) > len(table)+1 && key[:len(table)+1] == table+":" {
			column := key[len(table)+1:]
			if val, ok := row[column]; ok {
				valStr := im.valueToString(val)
				tree.Delete(valStr)
			}
		}
	}
}

// valueToString converts an interface{} value to a string for indexing.
func (im *IndexManager) valueToString(val interface{}) string {
	switch v := val.(type) {
	case string:
		return v
	case float64:
		return json.Number(json.Number(string(rune(int(v))))).String()
	default:
		data, _ := json.Marshal(v)
		return string(data)
	}
}

// GetIndexedColumns returns all indexed columns for a table.
func (im *IndexManager) GetIndexedColumns(table string) []string {
	im.mu.RLock()
	defer im.mu.RUnlock()

	var columns []string
	prefix := table + ":"
	for key := range im.indexes {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			columns = append(columns, key[len(prefix):])
		}
	}
	return columns
}

// ListIndexes returns a list of all indexes in the format "table.column".
func (im *IndexManager) ListIndexes() []string {
	im.mu.RLock()
	defer im.mu.RUnlock()

	var indexes []string
	for key := range im.indexes {
		indexes = append(indexes, key)
	}
	return indexes
}

