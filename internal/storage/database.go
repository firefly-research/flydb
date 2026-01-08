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
Package storage provides multi-database support for FlyDB.

Database Manager Overview:
==========================

The DatabaseManager provides MySQL-like multi-database functionality,
allowing users to create, drop, and switch between multiple databases.
Each database is stored in a separate .fdb file within the data directory.

Architecture:
=============

	┌─────────────────────────────────────────────────────┐
	│                  DatabaseManager                     │
	├─────────────────────────────────────────────────────┤
	│  dataDir: /var/lib/flydb/                           │
	│  databases: map[string]*Database                    │
	│  encConfig: EncryptionConfig                        │
	└─────────────────────────────────────────────────────┘
	                         │
	         ┌───────────────┼───────────────┐
	         ▼               ▼               ▼
	┌─────────────┐  ┌─────────────┐  ┌─────────────┐
	│  default    │  │   mydb      │  │  testdb     │
	│  .fdb       │  │   .fdb      │  │  .fdb       │
	└─────────────┘  └─────────────┘  └─────────────┘

Usage:
======

	mgr, err := storage.NewDatabaseManager("/var/lib/flydb", encConfig)
	if err != nil {
	    log.Fatal(err)
	}
	defer mgr.Close()

	// Create a new database
	err = mgr.CreateDatabase("mydb")

	// Get a database
	db, err := mgr.GetDatabase("mydb")

	// List all databases
	databases := mgr.ListDatabases()

	// Drop a database
	err = mgr.DropDatabase("mydb")
*/
package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// DefaultDatabaseName is the name of the default database.
const DefaultDatabaseName = "default"

// Reserved database names that cannot be created or dropped by users.
var reservedDatabaseNames = map[string]bool{
	"_system":     true,
	"information": true,
	"mysql":       true,
	"sys":         true,
}

// Database represents a single database instance with its own storage.
type Database struct {
	Name      string    // Database name
	Path      string    // Path to the .fdb file
	Store     *KVStore  // The underlying key-value store
	CreatedAt time.Time // When the database was created
}

// Close closes the database and releases resources.
func (d *Database) Close() error {
	if d.Store != nil {
		return d.Store.Close()
	}
	return nil
}

// DatabaseManager manages multiple databases within a data directory.
type DatabaseManager struct {
	dataDir   string               // Base directory for all database files
	databases map[string]*Database // Loaded databases (lazy-loaded)
	encConfig EncryptionConfig     // Encryption configuration
	mu        sync.RWMutex         // Protects databases map
}

// NewDatabaseManager creates a new DatabaseManager with the specified data directory.
// If the data directory doesn't exist, it will be created.
// The default database is automatically created if it doesn't exist.
func NewDatabaseManager(dataDir string, encConfig EncryptionConfig) (*DatabaseManager, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory '%s': %w", dataDir, err)
	}

	mgr := &DatabaseManager{
		dataDir:   dataDir,
		databases: make(map[string]*Database),
		encConfig: encConfig,
	}

	// Ensure default database exists
	if err := mgr.ensureDefaultDatabase(); err != nil {
		return nil, err
	}

	return mgr, nil
}

// ensureDefaultDatabase creates the default database if it doesn't exist.
func (m *DatabaseManager) ensureDefaultDatabase() error {
	defaultPath := m.getDatabasePath(DefaultDatabaseName)
	if _, err := os.Stat(defaultPath); os.IsNotExist(err) {
		// Create the default database
		return m.CreateDatabase(DefaultDatabaseName)
	}
	return nil
}

// getDatabasePath returns the file path for a database.
func (m *DatabaseManager) getDatabasePath(name string) string {
	return filepath.Join(m.dataDir, name+".fdb")
}

// validateDatabaseName checks if a database name is valid.
func (m *DatabaseManager) validateDatabaseName(name string) error {
	if name == "" {
		return errors.New("database name cannot be empty")
	}
	if len(name) > 64 {
		return errors.New("database name cannot exceed 64 characters")
	}
	// Check for invalid characters
	for _, ch := range name {
		if !isValidDatabaseNameChar(ch) {
			return fmt.Errorf("invalid character '%c' in database name", ch)
		}
	}
	// Check for reserved names
	if reservedDatabaseNames[strings.ToLower(name)] {
		return fmt.Errorf("'%s' is a reserved database name", name)
	}
	return nil
}

