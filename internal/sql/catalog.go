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
Package sql contains the Catalog component for schema management.

Catalog Overview:
=================

The Catalog is FlyDB's schema registry. It maintains metadata about all
tables in the database, including their names and column definitions.

The Catalog serves several purposes:
 1. Schema validation during INSERT (column count matching)
 2. Column name resolution during SELECT
 3. Table existence checking for all operations
 4. Schema persistence across server restarts

Storage Strategy:
=================

Table schemas are stored in the same KVStore as application data,
using a reserved key prefix:

	Key:   schema:<table_name>
	Value: JSON-encoded TableSchema

Example:

	Key:   schema:users
	Value: {"Name":"users","Columns":[{"Name":"id","Type":"INT"},{"Name":"name","Type":"TEXT"}]}

Caching Strategy:
=================

The Catalog uses a write-through cache:
  - All schemas are loaded into memory on startup
  - New schemas are written to both memory and storage
  - Reads check memory first, then fall back to storage

This provides fast schema lookups while ensuring durability.

Thread Safety:
==============

The current implementation is NOT thread-safe. In a production system,
you would add a sync.RWMutex to protect the Tables map.

For this educational implementation, thread safety is handled at the
server level (one request at a time per connection).
*/
package sql

import (
	"encoding/json"
	"time"

	"flydb/internal/storage"

	ferrors "flydb/internal/errors"
)

// schemaKeyPrefix is the storage key prefix for table schemas.
// All schema keys follow the format: schema:<table_name>
const schemaKeyPrefix = "schema:"

// viewKeyPrefix is the storage key prefix for view definitions.
// All view keys follow the format: view:<view_name>
const viewKeyPrefix = "view:"

// ViewDefinition represents a stored view in the catalog.
// A view is a virtual table based on a SELECT query.
type ViewDefinition struct {
	Name     string `json:"name"`      // The view name
	QuerySQL string `json:"query_sql"` // The original SQL query string
}

// Catalog manages table schemas for the database.
// It provides methods for creating tables and retrieving schema information.
//
// The Catalog maintains an in-memory cache of schemas for fast lookups,
// backed by persistent storage for durability.
type Catalog struct {
	// Tables is the in-memory cache of table schemas.
	// Key: table name, Value: TableSchema
	Tables map[string]TableSchema

	// Procedures is the in-memory cache of stored procedures.
	// Key: procedure name, Value: StoredProcedure
	Procedures map[string]StoredProcedure

	// Views is the in-memory cache of view definitions.
	// Key: view name, Value: ViewDefinition
	Views map[string]ViewDefinition

	// store is the underlying storage engine for persistence.
	store storage.Engine
}

// TableSchema defines the structure of a database table.
// It contains the table name, column definitions, constraints, and auto-increment state.
//
// The column order is significant - INSERT statements must provide
// values in the same order as the columns are defined.
type TableSchema struct {
	Name        string            // The table name (unique identifier)
	Columns     []ColumnDef       // Ordered list of column definitions
	Constraints []TableConstraint // Table-level constraints (composite keys, etc.)
	AutoIncSeq  map[string]int64  // Auto-increment sequence values per column
	CreatedAt   time.Time         `json:"created_at,omitempty"`  // When the table was created
	ModifiedAt  time.Time         `json:"modified_at,omitempty"` // When the table was last modified
	Owner       string            `json:"owner,omitempty"`       // User who created the table
}

// GetPrimaryKeyColumns returns the names of all primary key columns.
func (s TableSchema) GetPrimaryKeyColumns() []string {
	var pkCols []string

	// Check column-level primary keys
	for _, col := range s.Columns {
		if col.IsPrimaryKey() {
			pkCols = append(pkCols, col.Name)
		}
	}

	// Check table-level primary key constraints
	for _, constraint := range s.Constraints {
		if constraint.Type == ConstraintPrimaryKey {
			pkCols = append(pkCols, constraint.Columns...)
		}
	}

	return pkCols
}

// ForeignKeyInfo contains information about a foreign key constraint including referential actions.
type ForeignKeyInfo struct {
	Column    string            // Column in this table
	RefTable  string            // Referenced table name
	RefColumn string            // Referenced column name
	OnDelete  ReferentialAction // Action on DELETE of referenced row
	OnUpdate  ReferentialAction // Action on UPDATE of referenced row's key
}

// GetForeignKeys returns all foreign key constraints for the table.
func (s TableSchema) GetForeignKeys() []ForeignKeyInfo {
	var fks []ForeignKeyInfo

	// Check column-level foreign keys
	for _, col := range s.Columns {
		if fk := col.GetForeignKey(); fk != nil {
			fks = append(fks, ForeignKeyInfo{
				Column:    col.Name,
				RefTable:  fk.Table,
				RefColumn: fk.Column,
				OnDelete:  fk.OnDelete,
				OnUpdate:  fk.OnUpdate,
			})
		}
	}

	// Check table-level foreign key constraints
	for _, constraint := range s.Constraints {
		if constraint.Type == ConstraintForeignKey && constraint.ForeignKey != nil {
			for _, col := range constraint.Columns {
				fks = append(fks, ForeignKeyInfo{
					Column:    col,
					RefTable:  constraint.ForeignKey.Table,
					RefColumn: constraint.ForeignKey.Column,
					OnDelete:  constraint.ForeignKey.OnDelete,
					OnUpdate:  constraint.ForeignKey.OnUpdate,
				})
			}
		}
	}

	return fks
}

// GetAutoIncrementColumns returns the names of all auto-increment columns.
func (s TableSchema) GetAutoIncrementColumns() []string {
	var cols []string
	for _, col := range s.Columns {
		if col.IsAutoIncrement() {
			cols = append(cols, col.Name)
		}
	}
	return cols
}

// GetColumnIndex returns the index of a column by name, or -1 if not found.
func (s TableSchema) GetColumnIndex(name string) int {
	for i, col := range s.Columns {
		if col.Name == name {
			return i
		}
	}
	return -1
}

// NewCatalog creates a new Catalog and loads existing schemas from storage.
// This function is called during server startup to restore the schema state.
//
// The initialization process:
//  1. Create an empty in-memory cache
//  2. Scan storage for all schema entries
//  3. Deserialize and cache each schema
//
// Parameters:
//   - store: The storage engine for persistence
//
// Returns a fully initialized Catalog with all existing schemas loaded.
func NewCatalog(store storage.Engine) *Catalog {
	c := &Catalog{
		Tables:     make(map[string]TableSchema),
		Procedures: make(map[string]StoredProcedure),
		Views:      make(map[string]ViewDefinition),
		store:      store,
	}
	// Load existing schemas from storage into memory.
	c.load()
	return c
}

// load reads all schema definitions from the storage engine.
// It scans for all keys with the "schema:" prefix and deserializes
// each schema into the in-memory cache.
//
// This method is called during Catalog initialization to restore
// the schema state after a server restart.
//
// Errors during loading are silently ignored - this allows the server
// to start even if some schemas are corrupted. In production, you might
// want to log these errors or fail fast.
func (c *Catalog) load() {
	// Scan for all keys with the schema prefix.
	dataMap, err := c.store.Scan(schemaKeyPrefix)
	if err != nil {
		// Storage error - start with empty catalog.
		// In production, consider logging this error.
		return
	}

	// Deserialize each schema and add to the cache.
	for _, v := range dataMap {
		var schema TableSchema
		if err := json.Unmarshal(v, &schema); err == nil {
			c.Tables[schema.Name] = schema
		}
		// Silently skip schemas that fail to deserialize.
		// In production, consider logging these errors.
	}

	// Load stored procedures
	procMap, err := c.store.Scan("procedure:")
	if err != nil {
		return
	}
	for _, v := range procMap {
		var proc StoredProcedure
		if err := json.Unmarshal(v, &proc); err == nil {
			c.Procedures[proc.Name] = proc
		}
	}

	// Load views
	viewMap, err := c.store.Scan(viewKeyPrefix)
	if err != nil {
		return
	}
	for _, v := range viewMap {
		var view ViewDefinition
		if err := json.Unmarshal(v, &view); err == nil {
			c.Views[view.Name] = view
		}
	}
}

// CreateTable creates a new table schema and persists it to storage.
// The schema is added to both the in-memory cache and the storage engine.
//
// Parameters:
//   - name: The table name (must be unique)
//   - cols: The column definitions for the table
//
// Returns an error if:
//   - A table with the same name already exists
//   - The schema cannot be serialized
//   - The storage write fails
//
// Example:
//
//	err := catalog.CreateTable("users", []ColumnDef{
//	    {Name: "id", Type: "INT"},
//	    {Name: "name", Type: "TEXT"},
//	})
func (c *Catalog) CreateTable(name string, cols []ColumnDef) error {
	return c.CreateTableWithConstraints(name, cols, nil)
}

// CreateTableWithConstraints creates a new table schema with table-level constraints.
// The schema is added to both the in-memory cache and the storage engine.
//
// Parameters:
//   - name: The table name (must be unique)
//   - cols: The column definitions for the table
//   - constraints: Table-level constraints (composite keys, etc.)
//
// Returns an error if:
//   - A table with the same name already exists
//   - The schema cannot be serialized
//   - The storage write fails
func (c *Catalog) CreateTableWithConstraints(name string, cols []ColumnDef, constraints []TableConstraint) error {
	// Check if the table already exists.
	if _, exists := c.Tables[name]; exists {
		return ferrors.TableAlreadyExists("")
	}

	// Initialize auto-increment sequences for applicable columns
	autoIncSeq := make(map[string]int64)
	for _, col := range cols {
		if col.IsAutoIncrement() {
			autoIncSeq[col.Name] = 0
		}
	}

	// Create the schema and add to the in-memory cache.
	now := time.Now()
	schema := TableSchema{
		Name:        name,
		Columns:     cols,
		Constraints: constraints,
		AutoIncSeq:  autoIncSeq,
		CreatedAt:   now,
		ModifiedAt:  now,
	}
	c.Tables[name] = schema

	// Serialize the schema to JSON for storage.
	data, err := json.Marshal(schema)
	if err != nil {
		// Rollback the in-memory change on serialization failure.
		delete(c.Tables, name)
		return err
	}

	// Persist the schema to storage.
	if err := c.store.Put(schemaKeyPrefix+name, data); err != nil {
		// Rollback the in-memory change on storage failure.
		delete(c.Tables, name)
		return err
	}

	return nil
}

// GetNextAutoIncrement returns the next auto-increment value for a column and updates the sequence.
func (c *Catalog) GetNextAutoIncrement(tableName, columnName string) (int64, error) {
	schema, ok := c.Tables[tableName]
	if !ok {
		return 0, ferrors.TableNotFound("")
	}

	if schema.AutoIncSeq == nil {
		schema.AutoIncSeq = make(map[string]int64)
	}

	// Increment and get the next value
	schema.AutoIncSeq[columnName]++
	nextVal := schema.AutoIncSeq[columnName]

	// Update the in-memory cache
	c.Tables[tableName] = schema

	// Persist the updated schema
	data, err := json.Marshal(schema)
	if err != nil {
		return nextVal, nil // Return the value even if persistence fails
	}
	c.store.Put(schemaKeyPrefix+tableName, data)

	return nextVal, nil
}

// UpdateAutoIncrement updates the auto-increment sequence if the provided value is higher.
// This is used when inserting explicit values to ensure the sequence stays ahead.
func (c *Catalog) UpdateAutoIncrement(tableName, columnName string, value int64) error {
	schema, ok := c.Tables[tableName]
	if !ok {
		return ferrors.TableNotFound("")
	}

	if schema.AutoIncSeq == nil {
		schema.AutoIncSeq = make(map[string]int64)
	}

	// Only update if the new value is higher
	if value > schema.AutoIncSeq[columnName] {
		schema.AutoIncSeq[columnName] = value
		c.Tables[tableName] = schema

		// Persist the updated schema
		data, err := json.Marshal(schema)
		if err != nil {
			return nil // Ignore persistence errors
		}
		c.store.Put(schemaKeyPrefix+tableName, data)
	}

	return nil
}

// GetTable retrieves a table schema by name.
// It first checks the in-memory cache, then falls back to storage.
//
// The fallback to storage handles the case where a schema was created
// by another process or loaded after the Catalog was initialized.
//
// Parameters:
//   - name: The table name to look up
//
// Returns:
//   - schema: The TableSchema if found
//   - ok: true if the table exists, false otherwise
//
// Example:
//
//	schema, ok := catalog.GetTable("users")
//	if !ok {
//	    return ferrors.TableNotFound("")
//	}
func (c *Catalog) GetTable(name string) (TableSchema, bool) {
	// Check the in-memory cache first (fast path).
	if t, ok := c.Tables[name]; ok {
		return t, true
	}

	// Fall back to storage (slow path).
	// This handles schemas created by other processes or after initialization.
	val, err := c.store.Get(schemaKeyPrefix + name)
	if err == nil {
		var schema TableSchema
		if err := json.Unmarshal(val, &schema); err == nil {
			// Cache the schema for future lookups.
			c.Tables[name] = schema
			return schema, true
		}
	}

	// Table not found in cache or storage.
	return TableSchema{}, false
}

// CreateProcedure creates a new stored procedure and persists it to storage.
func (c *Catalog) CreateProcedure(proc StoredProcedure) error {
	if _, exists := c.Procedures[proc.Name]; exists {
		return ferrors.ProcedureAlreadyExists(proc.Name)
	}

	// Persist to storage
	data, err := json.Marshal(proc)
	if err != nil {
		return ferrors.InternalError("failed to serialize procedure").WithCause(err)
	}
	if err := c.store.Put("procedure:"+proc.Name, data); err != nil {
		return ferrors.NewStorageError("failed to store procedure").WithCause(err)
	}

	// Add to cache
	c.Procedures[proc.Name] = proc
	return nil
}

// GetProcedure retrieves a stored procedure by name.
func (c *Catalog) GetProcedure(name string) (StoredProcedure, bool) {
	proc, ok := c.Procedures[name]
	return proc, ok
}

// DropProcedure removes a stored procedure.
func (c *Catalog) DropProcedure(name string) error {
	if _, exists := c.Procedures[name]; !exists {
		return ferrors.ProcedureNotFound(name)
	}

	// Remove from storage
	if err := c.store.Delete("procedure:" + name); err != nil {
		return ferrors.NewStorageError("failed to delete procedure").WithCause(err)
	}

	// Remove from cache
	delete(c.Procedures, name)
	return nil
}

// AddColumn adds a new column to an existing table.
// The column is added at the end of the column list.
//
// Parameters:
//   - tableName: The name of the table to modify
//   - col: The column definition to add
//
// Returns an error if:
//   - The table does not exist
//   - A column with the same name already exists
//   - The schema cannot be persisted
func (c *Catalog) AddColumn(tableName string, col ColumnDef) error {
	schema, ok := c.Tables[tableName]
	if !ok {
		return ferrors.TableNotFound(tableName)
	}

	// Check if column already exists
	for _, existingCol := range schema.Columns {
		if existingCol.Name == col.Name {
			return ferrors.ColumnAlreadyExists(col.Name)
		}
	}

	// Add the new column
	schema.Columns = append(schema.Columns, col)

	// Initialize auto-increment if applicable
	if col.IsAutoIncrement() {
		if schema.AutoIncSeq == nil {
			schema.AutoIncSeq = make(map[string]int64)
		}
		schema.AutoIncSeq[col.Name] = 0
	}

	// Update modification time
	schema.ModifiedAt = time.Now()

	// Update the cache
	c.Tables[tableName] = schema

	// Persist the updated schema
	data, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	return c.store.Put(schemaKeyPrefix+tableName, data)
}

// DropColumn removes a column from an existing table.
//
// Parameters:
//   - tableName: The name of the table to modify
//   - columnName: The name of the column to remove
//
// Returns an error if:
//   - The table does not exist
//   - The column does not exist
//   - The column is a primary key
//   - The schema cannot be persisted
func (c *Catalog) DropColumn(tableName, columnName string) error {
	schema, ok := c.Tables[tableName]
	if !ok {
		return ferrors.TableNotFound(tableName)
	}

	// Find and remove the column
	found := false
	newColumns := make([]ColumnDef, 0, len(schema.Columns)-1)
	for _, col := range schema.Columns {
		if col.Name == columnName {
			// Check if it's a primary key
			if col.IsPrimaryKey() {
				return ferrors.ConstraintViolation("PRIMARY KEY", "cannot drop primary key column: "+columnName)
			}
			found = true
			continue
		}
		newColumns = append(newColumns, col)
	}

	if !found {
		return ferrors.ColumnNotFound(columnName, tableName)
	}

	schema.Columns = newColumns

	// Remove auto-increment sequence if applicable
	if schema.AutoIncSeq != nil {
		delete(schema.AutoIncSeq, columnName)
	}

	// Update modification time
	schema.ModifiedAt = time.Now()

	// Update the cache
	c.Tables[tableName] = schema

	// Persist the updated schema
	data, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	return c.store.Put(schemaKeyPrefix+tableName, data)
}

// RenameColumn renames a column in an existing table.
//
// Parameters:
//   - tableName: The name of the table to modify
//   - oldName: The current name of the column
//   - newName: The new name for the column
//
// Returns an error if:
//   - The table does not exist
//   - The old column does not exist
//   - A column with the new name already exists
//   - The schema cannot be persisted
func (c *Catalog) RenameColumn(tableName, oldName, newName string) error {
	schema, ok := c.Tables[tableName]
	if !ok {
		return ferrors.TableNotFound(tableName)
	}

	// Check if new name already exists
	for _, col := range schema.Columns {
		if col.Name == newName {
			return ferrors.ColumnAlreadyExists(newName)
		}
	}

	// Find and rename the column
	found := false
	for i, col := range schema.Columns {
		if col.Name == oldName {
			schema.Columns[i].Name = newName
			found = true
			break
		}
	}

	if !found {
		return ferrors.ColumnNotFound(oldName, tableName)
	}

	// Update auto-increment sequence key if applicable
	if schema.AutoIncSeq != nil {
		if seq, ok := schema.AutoIncSeq[oldName]; ok {
			delete(schema.AutoIncSeq, oldName)
			schema.AutoIncSeq[newName] = seq
		}
	}

	// Update modification time
	schema.ModifiedAt = time.Now()

	// Update the cache
	c.Tables[tableName] = schema

	// Persist the updated schema
	data, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	return c.store.Put(schemaKeyPrefix+tableName, data)
}

// ModifyColumn changes the type and/or constraints of an existing column.
//
// Parameters:
//   - tableName: The name of the table to modify
//   - columnName: The name of the column to modify
//   - newType: The new type for the column
//   - newConstraints: The new constraints for the column (optional)
//
// Returns an error if:
//   - The table does not exist
//   - The column does not exist
//   - The schema cannot be persisted
func (c *Catalog) ModifyColumn(tableName, columnName, newType string, newConstraints []ColumnConstraint) error {
	schema, ok := c.Tables[tableName]
	if !ok {
		return ferrors.TableNotFound(tableName)
	}

	// Find and modify the column
	found := false
	for i, col := range schema.Columns {
		if col.Name == columnName {
			schema.Columns[i].Type = newType
			if newConstraints != nil {
				schema.Columns[i].Constraints = newConstraints
			}
			found = true

			// Update auto-increment sequence if applicable
			if schema.Columns[i].IsAutoIncrement() {
				if schema.AutoIncSeq == nil {
					schema.AutoIncSeq = make(map[string]int64)
				}
				if _, exists := schema.AutoIncSeq[columnName]; !exists {
					schema.AutoIncSeq[columnName] = 0
				}
			}
			break
		}
	}

	if !found {
		return ferrors.ColumnNotFound(columnName, tableName)
	}

	// Update modification time
	schema.ModifiedAt = time.Now()

	// Update the cache
	c.Tables[tableName] = schema

	// Persist the updated schema
	data, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	return c.store.Put(schemaKeyPrefix+tableName, data)
}

// AddConstraint adds a table-level constraint to an existing table.
//
// Parameters:
//   - tableName: The name of the table to modify
//   - constraint: The constraint to add
//
// Returns an error if:
//   - The table does not exist
//   - A constraint with the same name already exists
//   - The schema cannot be persisted
func (c *Catalog) AddConstraint(tableName string, constraint TableConstraint) error {
	schema, ok := c.Tables[tableName]
	if !ok {
		return ferrors.TableNotFound(tableName)
	}

	// Check if a constraint with this name already exists
	if constraint.Name != "" {
		for _, existing := range schema.Constraints {
			if existing.Name == constraint.Name {
				return ferrors.ConstraintAlreadyExists(constraint.Name)
			}
		}
	}

	// Validate constraint columns exist in the table
	for _, colName := range constraint.Columns {
		found := false
		for _, col := range schema.Columns {
			if col.Name == colName {
				found = true
				break
			}
		}
		if !found {
			return ferrors.ColumnNotFound(colName, tableName)
		}
	}

	// Add the constraint
	schema.Constraints = append(schema.Constraints, constraint)
	schema.ModifiedAt = time.Now()

	// Update the cache
	c.Tables[tableName] = schema

	// Persist the updated schema
	data, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	return c.store.Put(schemaKeyPrefix+tableName, data)
}

// DropConstraint removes a table-level constraint from an existing table.
//
// Parameters:
//   - tableName: The name of the table to modify
//   - constraintName: The name of the constraint to drop
//
// Returns an error if:
//   - The table does not exist
//   - The constraint does not exist
//   - The schema cannot be persisted
func (c *Catalog) DropConstraint(tableName, constraintName string) error {
	schema, ok := c.Tables[tableName]
	if !ok {
		return ferrors.TableNotFound(tableName)
	}

	// Find and remove the constraint
	found := false
	newConstraints := make([]TableConstraint, 0, len(schema.Constraints))
	for _, constraint := range schema.Constraints {
		if constraint.Name == constraintName {
			found = true
			continue
		}
		newConstraints = append(newConstraints, constraint)
	}

	if !found {
		return ferrors.ConstraintNotFound(constraintName)
	}

	schema.Constraints = newConstraints
	schema.ModifiedAt = time.Now()

	// Update the cache
	c.Tables[tableName] = schema

	// Persist the updated schema
	data, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	return c.store.Put(schemaKeyPrefix+tableName, data)
}

// DropTable removes a table from the catalog.
//
// Parameters:
//   - tableName: The name of the table to drop
//
// Returns an error if:
//   - The table does not exist
//   - The table cannot be removed from storage
func (c *Catalog) DropTable(tableName string) error {
	if _, ok := c.Tables[tableName]; !ok {
		return ferrors.TableNotFound(tableName)
	}

	// Remove from storage
	if err := c.store.Delete(schemaKeyPrefix + tableName); err != nil {
		return err
	}

	// Remove from cache
	delete(c.Tables, tableName)
	return nil
}

// CreateView creates a new view and persists it to storage.
// A view is a virtual table based on a SELECT query.
//
// Parameters:
//   - name: The view name (must be unique among views and tables)
//   - querySQL: The SQL query string that defines the view
//
// Returns an error if:
//   - A view or table with the same name already exists
//   - The view cannot be serialized
//   - The storage write fails
func (c *Catalog) CreateView(name, querySQL string) error {
	// Check if a view with this name already exists
	if _, exists := c.Views[name]; exists {
		return ferrors.ViewAlreadyExists(name)
	}

	// Check if a table with this name already exists
	if _, exists := c.Tables[name]; exists {
		return ferrors.TableAlreadyExists(name)
	}

	view := ViewDefinition{
		Name:     name,
		QuerySQL: querySQL,
	}

	// Persist to storage
	data, err := json.Marshal(view)
	if err != nil {
		return ferrors.InternalError("failed to serialize view").WithCause(err)
	}
	if err := c.store.Put(viewKeyPrefix+name, data); err != nil {
		return ferrors.NewStorageError("failed to store view").WithCause(err)
	}

	// Add to cache
	c.Views[name] = view
	return nil
}

// GetView retrieves a view definition by name.
//
// Parameters:
//   - name: The view name to look up
//
// Returns:
//   - The ViewDefinition if found
//   - A boolean indicating whether the view exists
func (c *Catalog) GetView(name string) (ViewDefinition, bool) {
	view, ok := c.Views[name]
	return view, ok
}

// DropView removes a view from the catalog.
//
// Parameters:
//   - name: The name of the view to drop
//
// Returns an error if:
//   - The view does not exist
//   - The view cannot be removed from storage
func (c *Catalog) DropView(name string) error {
	if _, ok := c.Views[name]; !ok {
		return ferrors.ViewNotFound(name)
	}

	// Remove from storage
	if err := c.store.Delete(viewKeyPrefix + name); err != nil {
		return ferrors.NewStorageError("failed to delete view").WithCause(err)
	}

	// Remove from cache
	delete(c.Views, name)
	return nil
}
