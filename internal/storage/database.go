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
	│                  DatabaseManager                    │
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
	"encoding/json"
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

// System database for global data (users, permissions, etc.)
const SystemDatabaseName = "_system"

// Key prefix for database metadata
const databaseMetadataKey = "_sys_db_meta"

// Reserved database names that cannot be created or dropped by users.
var reservedDatabaseNames = map[string]bool{
	"_system":     true,
	"information": true,
	"flydb":       true,
	"sys":         true,
}

// Supported character encodings for databases.
type CharacterEncoding string

const (
	EncodingUTF8    CharacterEncoding = "UTF8"
	EncodingLatin1  CharacterEncoding = "LATIN1"
	EncodingASCII   CharacterEncoding = "ASCII"
	EncodingUTF16   CharacterEncoding = "UTF16"
	EncodingDefault CharacterEncoding = EncodingUTF8
)

// Supported collations for string comparison.
type Collation string

const (
	CollationDefault         Collation = "default"
	CollationBinary          Collation = "binary"
	CollationCaseInsensitive Collation = "nocase"
	CollationUnicode         Collation = "unicode"
)

// DatabaseMetadata stores configuration and metadata for a database.
// This is persisted within the database's KVStore.
type DatabaseMetadata struct {
	Name        string            `json:"name"`
	Owner       string            `json:"owner"`        // User who created the database
	Encoding    CharacterEncoding `json:"encoding"`     // Character encoding (UTF8, LATIN1, etc.)
	Collation   Collation         `json:"collation"`    // Default collation for string comparison
	Locale      string            `json:"locale"`       // Locale for sorting (e.g., "en_US", "de_DE")
	CreatedAt   time.Time         `json:"created_at"`   // When the database was created
	UpdatedAt   time.Time         `json:"updated_at"`   // Last metadata update
	Description string            `json:"description"`  // Optional description
	ReadOnly    bool              `json:"read_only"`    // If true, database is read-only
	MaxSize     int64             `json:"max_size"`     // Maximum size in bytes (0 = unlimited)
	Properties  map[string]string `json:"properties"`   // Custom properties
}

// DefaultDatabaseMetadata returns metadata with default values.
func DefaultDatabaseMetadata(name string) *DatabaseMetadata {
	now := time.Now()
	return &DatabaseMetadata{
		Name:       name,
		Owner:      "admin",
		Encoding:   EncodingDefault,
		Collation:  CollationDefault,
		Locale:     "en_US",
		CreatedAt:  now,
		UpdatedAt:  now,
		ReadOnly:   false,
		MaxSize:    0,
		Properties: make(map[string]string),
	}
}

// Database represents a single database instance with its own storage.
type Database struct {
	Name      string            // Database name
	Path      string            // Path to the .fdb file
	Store     *KVStore          // The underlying key-value store
	Metadata  *DatabaseMetadata // Database configuration and metadata
	CreatedAt time.Time         // When the database was created (from metadata)
}

// Close closes the database and releases resources.
func (d *Database) Close() error {
	if d.Store != nil {
		return d.Store.Close()
	}
	return nil
}

// LoadMetadata loads the database metadata from storage.
func (d *Database) LoadMetadata() error {
	if d.Store == nil {
		return errors.New("database store is nil")
	}

	data, err := d.Store.Get(databaseMetadataKey)
	if err != nil {
		// No metadata found, use defaults
		d.Metadata = DefaultDatabaseMetadata(d.Name)
		return nil
	}

	var meta DatabaseMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return fmt.Errorf("failed to parse database metadata: %w", err)
	}

	d.Metadata = &meta
	d.CreatedAt = meta.CreatedAt
	return nil
}

// SaveMetadata persists the database metadata to storage.
func (d *Database) SaveMetadata() error {
	if d.Store == nil {
		return errors.New("database store is nil")
	}
	if d.Metadata == nil {
		return errors.New("database metadata is nil")
	}

	d.Metadata.UpdatedAt = time.Now()
	data, err := json.Marshal(d.Metadata)
	if err != nil {
		return fmt.Errorf("failed to serialize database metadata: %w", err)
	}

	return d.Store.Put(databaseMetadataKey, data)
}

// GetEncoding returns the database's character encoding.
func (d *Database) GetEncoding() CharacterEncoding {
	if d.Metadata != nil {
		return d.Metadata.Encoding
	}
	return EncodingDefault
}

// GetCollation returns the database's default collation.
func (d *Database) GetCollation() Collation {
	if d.Metadata != nil {
		return d.Metadata.Collation
	}
	return CollationDefault
}

// GetLocale returns the database's locale setting.
func (d *Database) GetLocale() string {
	if d.Metadata != nil && d.Metadata.Locale != "" {
		return d.Metadata.Locale
	}
	return "en_US"
}

// IsReadOnly returns whether the database is read-only.
func (d *Database) IsReadOnly() bool {
	if d.Metadata != nil {
		return d.Metadata.ReadOnly
	}
	return false
}

// GetOwner returns the database owner.
func (d *Database) GetOwner() string {
	if d.Metadata != nil && d.Metadata.Owner != "" {
		return d.Metadata.Owner
	}
	return "admin"
}

// GetTableCount returns the number of tables in the database.
func (d *Database) GetTableCount() int {
	if d.Store == nil {
		return 0
	}
	// Scan for table schema keys
	tables, err := d.Store.Scan("_schema:")
	if err != nil {
		return 0
	}
	return len(tables)
}

// GetSize returns the approximate size of the database file in bytes.
func (d *Database) GetSize() int64 {
	info, err := os.Stat(d.Path)
	if err != nil {
		return 0
	}
	return info.Size()
}

// GetCreatedAt returns when the database was created.
func (d *Database) GetCreatedAt() time.Time {
	if d.Metadata != nil && !d.Metadata.CreatedAt.IsZero() {
		return d.Metadata.CreatedAt
	}
	return d.CreatedAt
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

// ensureDefaultDatabase creates the default and system databases if they don't exist.
func (m *DatabaseManager) ensureDefaultDatabase() error {
	// Create system database first (for global users, permissions, etc.)
	systemPath := m.getDatabasePath(SystemDatabaseName)
	if _, err := os.Stat(systemPath); os.IsNotExist(err) {
		if err := m.createSystemDatabase(); err != nil {
			return fmt.Errorf("failed to create system database: %w", err)
		}
	}

	// Create default database
	defaultPath := m.getDatabasePath(DefaultDatabaseName)
	if _, err := os.Stat(defaultPath); os.IsNotExist(err) {
		return m.CreateDatabase(DefaultDatabaseName)
	}
	return nil
}

// createSystemDatabase creates the internal system database for global objects.
// This database stores users, roles, and global permissions.
func (m *DatabaseManager) createSystemDatabase() error {
	dbPath := m.getDatabasePath(SystemDatabaseName)

	var store *KVStore
	var err error
	if m.encConfig.Enabled {
		store, err = NewKVStoreWithEncryption(dbPath, m.encConfig)
	} else {
		store, err = NewKVStore(dbPath)
	}
	if err != nil {
		return err
	}

	now := time.Now()
	meta := &DatabaseMetadata{
		Name:        SystemDatabaseName,
		Owner:       "system",
		Encoding:    EncodingDefault,
		Collation:   CollationDefault,
		Locale:      "en_US",
		Description: "FlyDB system database for global objects",
		CreatedAt:   now,
		UpdatedAt:   now,
		Properties:  make(map[string]string),
	}

	db := &Database{
		Name:      SystemDatabaseName,
		Path:      dbPath,
		Store:     store,
		Metadata:  meta,
		CreatedAt: now,
	}

	if err := db.SaveMetadata(); err != nil {
		store.Close()
		os.Remove(dbPath)
		return err
	}

	m.databases[SystemDatabaseName] = db
	return nil
}

// getDatabasePath returns the file path for a database.
func (m *DatabaseManager) getDatabasePath(name string) string {
	return filepath.Join(m.dataDir, name+".fdb")
}

// isValidDatabaseNameChar checks if a character is valid in a database name.
func isValidDatabaseNameChar(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z') ||
		(ch >= '0' && ch <= '9') ||
		ch == '_'
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

// CreateDatabaseOptions contains options for creating a new database.
type CreateDatabaseOptions struct {
	Owner       string            // Owner of the database (default: "admin")
	Encoding    CharacterEncoding // Character encoding (default: UTF8)
	Collation   Collation         // Default collation (default: "default")
	Locale      string            // Locale for sorting (default: "en_US")
	Description string            // Optional description
}

// DefaultCreateDatabaseOptions returns default options for database creation.
func DefaultCreateDatabaseOptions() CreateDatabaseOptions {
	return CreateDatabaseOptions{
		Owner:     "admin",
		Encoding:  EncodingDefault,
		Collation: CollationDefault,
		Locale:    "en_US",
	}
}

// CreateDatabase creates a new database with the given name.
// Returns an error if the database already exists or the name is invalid.
func (m *DatabaseManager) CreateDatabase(name string) error {
	return m.CreateDatabaseWithOptions(name, DefaultCreateDatabaseOptions())
}

// CreateDatabaseWithOptions creates a new database with the given name and options.
// Returns an error if the database already exists or the name is invalid.
func (m *DatabaseManager) CreateDatabaseWithOptions(name string, opts CreateDatabaseOptions) error {
	if err := m.validateDatabaseName(name); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if database already exists
	dbPath := m.getDatabasePath(name)
	if _, err := os.Stat(dbPath); err == nil {
		return fmt.Errorf("database '%s' already exists", name)
	}

	// Create the database file by opening a new KVStore
	var store *KVStore
	var err error
	if m.encConfig.Enabled {
		store, err = NewKVStoreWithEncryption(dbPath, m.encConfig)
	} else {
		store, err = NewKVStore(dbPath)
	}
	if err != nil {
		return fmt.Errorf("failed to create database '%s': %w", name, err)
	}

	// Create metadata
	now := time.Now()
	meta := &DatabaseMetadata{
		Name:        name,
		Owner:       opts.Owner,
		Encoding:    opts.Encoding,
		Collation:   opts.Collation,
		Locale:      opts.Locale,
		Description: opts.Description,
		CreatedAt:   now,
		UpdatedAt:   now,
		Properties:  make(map[string]string),
	}

	// Create the database object
	db := &Database{
		Name:      name,
		Path:      dbPath,
		Store:     store,
		Metadata:  meta,
		CreatedAt: now,
	}

	// Save metadata to the database
	if err := db.SaveMetadata(); err != nil {
		store.Close()
		os.Remove(dbPath)
		return fmt.Errorf("failed to save database metadata: %w", err)
	}

	// Store in the map
	m.databases[name] = db

	return nil
}

// CreateDatabaseIfNotExists creates a database if it doesn't already exist.
// Returns nil if the database already exists (no error).
func (m *DatabaseManager) CreateDatabaseIfNotExists(name string) error {
	return m.CreateDatabaseIfNotExistsWithOptions(name, DefaultCreateDatabaseOptions())
}

// CreateDatabaseIfNotExistsWithOptions creates a database with options if it doesn't already exist.
// Returns nil if the database already exists (no error).
func (m *DatabaseManager) CreateDatabaseIfNotExistsWithOptions(name string, opts CreateDatabaseOptions) error {
	if err := m.validateDatabaseName(name); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if database already exists
	dbPath := m.getDatabasePath(name)
	if _, err := os.Stat(dbPath); err == nil {
		return nil // Already exists, not an error
	}

	// Create the database file
	var store *KVStore
	var err error
	if m.encConfig.Enabled {
		store, err = NewKVStoreWithEncryption(dbPath, m.encConfig)
	} else {
		store, err = NewKVStore(dbPath)
	}
	if err != nil {
		return fmt.Errorf("failed to create database '%s': %w", name, err)
	}

	// Create metadata
	now := time.Now()
	meta := &DatabaseMetadata{
		Name:        name,
		Owner:       opts.Owner,
		Encoding:    opts.Encoding,
		Collation:   opts.Collation,
		Locale:      opts.Locale,
		Description: opts.Description,
		CreatedAt:   now,
		UpdatedAt:   now,
		Properties:  make(map[string]string),
	}

	db := &Database{
		Name:      name,
		Path:      dbPath,
		Store:     store,
		Metadata:  meta,
		CreatedAt: now,
	}

	// Save metadata
	if err := db.SaveMetadata(); err != nil {
		store.Close()
		os.Remove(dbPath)
		return fmt.Errorf("failed to save database metadata: %w", err)
	}

	m.databases[name] = db

	return nil
}

// DropDatabase removes a database and deletes its file.
// Returns an error if the database doesn't exist or is the default database.
func (m *DatabaseManager) DropDatabase(name string) error {
	if name == DefaultDatabaseName {
		return errors.New("cannot drop the default database")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	dbPath := m.getDatabasePath(name)

	// Check if database exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("database '%s' does not exist", name)
	}

	// Close the database if it's loaded
	if db, ok := m.databases[name]; ok {
		if err := db.Close(); err != nil {
			return fmt.Errorf("failed to close database '%s': %w", name, err)
		}
		delete(m.databases, name)
	}

	// Delete the database file
	if err := os.Remove(dbPath); err != nil {
		return fmt.Errorf("failed to delete database file '%s': %w", name, err)
	}

	return nil
}

// DropDatabaseIfExists removes a database if it exists.
// Returns nil if the database doesn't exist (no error).
func (m *DatabaseManager) DropDatabaseIfExists(name string) error {
	if name == DefaultDatabaseName {
		return errors.New("cannot drop the default database")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	dbPath := m.getDatabasePath(name)

	// Check if database exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil // Doesn't exist, not an error
	}

	// Close the database if it's loaded
	if db, ok := m.databases[name]; ok {
		if err := db.Close(); err != nil {
			return fmt.Errorf("failed to close database '%s': %w", name, err)
		}
		delete(m.databases, name)
	}

	// Delete the database file
	if err := os.Remove(dbPath); err != nil {
		return fmt.Errorf("failed to delete database file '%s': %w", name, err)
	}

	return nil
}

// GetDatabase returns a database by name, loading it if necessary.
// Returns an error if the database doesn't exist.
func (m *DatabaseManager) GetDatabase(name string) (*Database, error) {
	m.mu.RLock()
	if db, ok := m.databases[name]; ok {
		m.mu.RUnlock()
		return db, nil
	}
	m.mu.RUnlock()

	// Need to load the database
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if db, ok := m.databases[name]; ok {
		return db, nil
	}

	dbPath := m.getDatabasePath(name)

	// Check if database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("database '%s' does not exist", name)
	}

	// Load the database
	var store *KVStore
	var err error
	if m.encConfig.Enabled {
		store, err = NewKVStoreWithEncryption(dbPath, m.encConfig)
	} else {
		store, err = NewKVStore(dbPath)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to load database '%s': %w", name, err)
	}

	db := &Database{
		Name:  name,
		Path:  dbPath,
		Store: store,
	}

	// Load metadata from the database
	if err := db.LoadMetadata(); err != nil {
		// Log warning but continue - metadata is optional
		db.Metadata = DefaultDatabaseMetadata(name)
	}
	db.CreatedAt = db.Metadata.CreatedAt

	m.databases[name] = db

	return db, nil
}

// GetDatabaseMetadata returns metadata for a database without fully loading it.
func (m *DatabaseManager) GetDatabaseMetadata(name string) (*DatabaseMetadata, error) {
	db, err := m.GetDatabase(name)
	if err != nil {
		return nil, err
	}
	return db.Metadata, nil
}

// UpdateDatabaseMetadata updates the metadata for a database.
func (m *DatabaseManager) UpdateDatabaseMetadata(name string, meta *DatabaseMetadata) error {
	db, err := m.GetDatabase(name)
	if err != nil {
		return err
	}

	db.Metadata = meta
	return db.SaveMetadata()
}

// DatabaseExists checks if a database exists.
func (m *DatabaseManager) DatabaseExists(name string) bool {
	dbPath := m.getDatabasePath(name)
	_, err := os.Stat(dbPath)
	return err == nil
}

// ListDatabases returns a sorted list of all database names.
func (m *DatabaseManager) ListDatabases() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Scan the data directory for .fdb files
	entries, err := os.ReadDir(m.dataDir)
	if err != nil {
		return []string{}
	}

	var databases []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".fdb") {
			dbName := strings.TrimSuffix(name, ".fdb")
			databases = append(databases, dbName)
		}
	}

	sort.Strings(databases)
	return databases
}

// Close closes all loaded databases and releases resources.
func (m *DatabaseManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for name, db := range m.databases {
		if err := db.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close database '%s': %w", name, err)
		}
	}
	m.databases = make(map[string]*Database)

	return lastErr
}

// GetDataDir returns the data directory path.
func (m *DatabaseManager) GetDataDir() string {
	return m.dataDir
}

// GetDefaultDatabase returns the default database.
func (m *DatabaseManager) GetDefaultDatabase() (*Database, error) {
	return m.GetDatabase(DefaultDatabaseName)
}

// GetSystemDatabase returns the system database for global objects.
// The system database stores users, roles, and global permissions.
func (m *DatabaseManager) GetSystemDatabase() (*Database, error) {
	return m.GetDatabase(SystemDatabaseName)
}

// DataDirectoryHasData checks if the data directory contains any database files.
// This is used to determine if this is a first-time setup or an existing installation.
func DataDirectoryHasData(dataDir string) bool {
	// Check if directory exists
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return false
	}

	// Check for any .fdb files (database files)
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return false
	}

	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".fdb") {
			return true
		}
	}

	return false
}
