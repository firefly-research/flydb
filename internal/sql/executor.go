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
Package sql contains the Executor component for SQL statement execution.

Executor Overview:
==================

The Executor is the heart of FlyDB's query processing. It takes parsed
AST nodes and executes them against the storage engine, handling:

  - Permission checks (table-level access control)
  - Row-Level Security (RLS) filtering
  - Data manipulation (INSERT, UPDATE, DELETE)
  - Query execution (SELECT with JOIN, ORDER BY, LIMIT)
  - Schema management (CREATE TABLE)
  - User management (CREATE USER, GRANT)

Execution Flow:
===============

	SQL String → Lexer → Parser → AST → Executor → Result
	                                        ↓
	                                   Storage Engine

The Executor receives an AST node from the Parser and:
 1. Validates permissions for the current user
 2. Applies RLS filters if configured
 3. Executes the operation against storage
 4. Returns the result as a string

Permission Model:
=================

FlyDB implements a simple permission model:

  - Admin users (username "admin") have full access
  - Anonymous users (empty username) have full access (for bootstrap)
  - Regular users need explicit GRANT for each table

Row-Level Security (RLS):
=========================

RLS restricts which rows a user can see/modify based on a condition.
When a GRANT includes a WHERE clause, that condition is applied to
all queries by that user on that table.

Example:

	GRANT SELECT ON orders WHERE user_id = 'alice' TO alice

Now when alice queries orders, she only sees rows where user_id = 'alice'.

Storage Key Conventions:
========================

The Executor uses these key prefixes in the storage engine:

	row:<table>:<id>   - Table row data (JSON)
	seq:<table>        - Auto-increment sequence for table
	schema:<table>     - Table schema (managed by Catalog)
	user:<username>    - User credentials (managed by AuthManager)
	perm:<user>:<table> - Permissions (managed by AuthManager)

Reactive Notifications:
=======================

The Executor supports reactive notifications via the OnInsert callback.
When a row is inserted, the callback is invoked with the table name
and JSON representation of the row. This enables the WATCH feature.
*/
package sql

import (
	"encoding/json"
	"errors"
	"flydb/internal/auth"
	"flydb/internal/storage"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Executor executes SQL statements against the storage engine.
// It coordinates between the Catalog (schema), AuthManager (permissions),
// and Storage Engine (data) to process SQL operations.
//
// The Executor maintains a user context that is used for permission
// checks and RLS filtering. This context should be set before each
// query using SetUser().
type Executor struct {
	// store is the underlying storage engine for data operations.
	store storage.Engine

	// kvStore is the concrete KVStore for transaction support.
	kvStore *storage.KVStore

	// catalog manages table schemas.
	catalog *Catalog

	// auth handles user authentication and permissions.
	auth *auth.AuthManager

	// indexMgr manages B-Tree indexes for efficient lookups.
	indexMgr *storage.IndexManager

	// currentUser is the username for the current execution context.
	// Empty string or "admin" grants full access.
	currentUser string

	// tx is the current active transaction (nil if not in a transaction).
	tx *storage.Transaction

	// OnInsert is a callback invoked after each successful INSERT.
	// It receives the table name and JSON representation of the inserted row.
	// This enables reactive features like WATCH subscriptions.
	OnInsert func(tableName string, rowJSON string)

	// preparedStmts manages prepared statements for this executor.
	preparedStmts *PreparedStatementManager

	// triggerMgr manages database triggers.
	triggerMgr *TriggerManager
}

// aggState holds the accumulator state for aggregate function computation.
// It is used by computeAggregates and computeGroupedAggregates.
type aggState struct {
	count int
	sum   float64
	min   *float64
	max   *float64
	// For non-numeric MIN/MAX
	minStr *string
	maxStr *string
}

// NewExecutor creates a new Executor with the given storage and auth manager.
// It initializes a new Catalog to manage table schemas.
//
// Parameters:
//   - store: The storage engine for data operations
//   - auth: The authentication manager for permission checks
//
// Returns a fully initialized Executor ready to process statements.
func NewExecutor(store storage.Engine, auth *auth.AuthManager) *Executor {
	// Try to get the concrete KVStore for transaction support
	kvStore, _ := store.(*storage.KVStore)

	exec := &Executor{
		store:   store,
		kvStore: kvStore,
		catalog: NewCatalog(store),
		auth:    auth,
	}

	// Initialize index manager
	exec.indexMgr = storage.NewIndexManager(store)

	// Initialize prepared statement manager
	exec.preparedStmts = NewPreparedStatementManager(exec)

	// Initialize trigger manager
	exec.triggerMgr = NewTriggerManager(store)

	return exec
}

// SetUser sets the current execution context user.
// This should be called before executing statements to ensure
// proper permission checks and RLS filtering.
//
// Special values:
//   - "" (empty): Full access (anonymous/bootstrap mode)
//   - "admin": Full access (administrator)
//   - Other: Regular user, subject to GRANT permissions
//
// Parameters:
//   - user: The username for the execution context
func (e *Executor) SetUser(user string) {
	e.currentUser = user
}

// Execute runs the given SQL statement and returns the result as a string.
// This is the main entry point for statement execution.
//
// The execution process:
//  1. Determine the statement type via type switch
//  2. Check permissions for the operation
//  3. Delegate to the appropriate handler method
//  4. Return the result or error
//
// Parameters:
//   - stmt: The parsed AST node to execute
//
// Returns:
//   - result: A string representation of the result (e.g., "INSERT OK", query results)
//   - error: Any error that occurred during execution
//
// Supported statement types:
//   - CreateTableStmt: Creates a new table (admin only)
//   - CreateUserStmt: Creates a new user (admin only)
//   - GrantStmt: Grants permissions (admin only)
//   - InsertStmt: Inserts a row (requires table access)
//   - UpdateStmt: Updates rows (requires table access)
//   - DeleteStmt: Deletes rows (requires table access)
//   - SelectStmt: Queries data (requires table access)
func (e *Executor) Execute(stmt Statement) (string, error) {
	switch s := stmt.(type) {
	case *CreateTableStmt:
		// CREATE TABLE requires admin privileges.
		// This prevents regular users from modifying the schema.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeCreate(s)

	case *CreateUserStmt:
		// CREATE USER requires admin privileges.
		// Only admins can create new database users.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeCreateUser(s)

	case *GrantStmt:
		// GRANT requires admin privileges.
		// Only admins can assign permissions to users.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeGrant(s)

	case *InsertStmt:
		// INSERT requires access to the target table.
		if err := e.checkAccess(s.TableName); err != nil {
			return "", err
		}
		return e.executeInsert(s)

	case *UpdateStmt:
		// UPDATE requires access to the target table.
		// RLS is applied during execution to filter which rows can be updated.
		if err := e.checkAccess(s.TableName); err != nil {
			return "", err
		}
		return e.executeUpdate(s)

	case *DeleteStmt:
		// DELETE requires access to the target table.
		// RLS is applied during execution to filter which rows can be deleted.
		if err := e.checkAccess(s.TableName); err != nil {
			return "", err
		}
		return e.executeDelete(s)

	case *SelectStmt:
		// SELECT requires access to the primary table.
		if err := e.checkAccess(s.TableName); err != nil {
			return "", err
		}
		// If there's a JOIN, also check access to the joined table.
		if s.Join != nil {
			if err := e.checkAccess(s.Join.TableName); err != nil {
				return "", err
			}
		}
		return e.executeSelect(s)

	case *BeginStmt:
		return e.executeBegin()

	case *CommitStmt:
		return e.executeCommit()

	case *RollbackStmt:
		return e.executeRollback()

	case *CreateIndexStmt:
		// CREATE INDEX requires admin privileges.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeCreateIndex(s)

	case *PrepareStmt:
		return e.executePrepare(s)

	case *ExecuteStmt:
		return e.executeExecute(s)

	case *DeallocateStmt:
		return e.executeDeallocate(s)

	case *IntrospectStmt:
		// INTROSPECT requires admin privileges for security.
		// Only admins can view database metadata.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeIntrospect(s)

	case *UnionStmt:
		return e.executeUnion(s)

	case *CreateProcedureStmt:
		// CREATE PROCEDURE requires admin privileges.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeCreateProcedure(s)

	case *CallStmt:
		return e.executeCall(s)

	case *DropProcedureStmt:
		// DROP PROCEDURE requires admin privileges.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeDropProcedure(s)

	case *AlterTableStmt:
		// ALTER TABLE requires admin privileges.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeAlterTable(s)

	case *CreateViewStmt:
		// CREATE VIEW requires admin privileges.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeCreateView(s)

	case *DropViewStmt:
		// DROP VIEW requires admin privileges.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeDropView(s)

	case *CreateTriggerStmt:
		// CREATE TRIGGER requires admin privileges.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeCreateTrigger(s)

	case *DropTriggerStmt:
		// DROP TRIGGER requires admin privileges.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeDropTrigger(s)
	}

	return "", errors.New("unknown statement")
}

// checkAccess verifies if the current user has access to the specified table.
// Admin users and anonymous users (empty username) have full access.
// Regular users need an explicit GRANT for the table.
//
// Parameters:
//   - table: The table name to check access for
//
// Returns an error if access is denied, nil otherwise.
func (e *Executor) checkAccess(table string) error {
	// Admin and anonymous users have full access.
	if e.currentUser == "" || e.currentUser == "admin" {
		return nil
	}

	// Check if the user has been granted access to this table.
	allowed, _ := e.auth.CheckPermission(e.currentUser, table)
	if !allowed {
		return errors.New("permission denied for table " + table)
	}
	return nil
}

// executeCreateUser handles CREATE USER statements.
// It delegates to the AuthManager to create the user account.
//
// Parameters:
//   - stmt: The parsed CREATE USER statement
//
// Returns "CREATE USER OK" on success, or an error.
func (e *Executor) executeCreateUser(stmt *CreateUserStmt) (string, error) {
	err := e.auth.CreateUser(stmt.Username, stmt.Password)
	if err != nil {
		return "", err
	}
	return "CREATE USER OK", nil
}

// executeGrant handles GRANT statements.
// It delegates to the AuthManager to record the permission.
//
// If the GRANT includes a WHERE clause, it sets up Row-Level Security
// for the user on that table.
//
// Parameters:
//   - stmt: The parsed GRANT statement
//
// Returns "GRANT OK" on success, or an error.
func (e *Executor) executeGrant(stmt *GrantStmt) (string, error) {
	// Extract RLS condition if present.
	var rlsCol, rlsVal string
	if stmt.Where != nil {
		rlsCol = stmt.Where.Column
		rlsVal = stmt.Where.Value
	}

	// Record the permission in the AuthManager.
	err := e.auth.Grant(stmt.Username, stmt.TableName, rlsCol, rlsVal)
	if err != nil {
		return "", err
	}
	return "GRANT OK", nil
}

// executeCreate handles CREATE TABLE statements.
// It delegates to the Catalog to create the table schema.
//
// Parameters:
//   - stmt: The parsed CREATE TABLE statement
//
// Returns "CREATE TABLE OK" on success, or an error.
func (e *Executor) executeCreate(stmt *CreateTableStmt) (string, error) {
	// Validate column types before creating the table
	for _, col := range stmt.Columns {
		if !IsValidType(col.Type) {
			return "", fmt.Errorf("invalid column type: %s (valid types: INT, TEXT, BOOLEAN, FLOAT, TIMESTAMP, DATE, BLOB, UUID, JSONB, SERIAL, BIGINT, DECIMAL, NUMERIC, TIME, VARCHAR)", col.Type)
		}
	}

	// Validate foreign key references
	for _, col := range stmt.Columns {
		if fk := col.GetForeignKey(); fk != nil {
			refTable, ok := e.catalog.GetTable(fk.Table)
			if !ok {
				return "", fmt.Errorf("foreign key references non-existent table: %s", fk.Table)
			}
			// Verify the referenced column exists
			found := false
			for _, refCol := range refTable.Columns {
				if refCol.Name == fk.Column {
					found = true
					break
				}
			}
			if !found {
				return "", fmt.Errorf("foreign key references non-existent column: %s.%s", fk.Table, fk.Column)
			}
		}
	}

	// Validate table-level foreign key constraints
	for _, constraint := range stmt.Constraints {
		if constraint.Type == ConstraintForeignKey && constraint.ForeignKey != nil {
			refTable, ok := e.catalog.GetTable(constraint.ForeignKey.Table)
			if !ok {
				return "", fmt.Errorf("foreign key references non-existent table: %s", constraint.ForeignKey.Table)
			}
			// Verify the referenced column exists
			found := false
			for _, refCol := range refTable.Columns {
				if refCol.Name == constraint.ForeignKey.Column {
					found = true
					break
				}
			}
			if !found {
				return "", fmt.Errorf("foreign key references non-existent column: %s.%s", constraint.ForeignKey.Table, constraint.ForeignKey.Column)
			}
		}
	}

	err := e.catalog.CreateTableWithConstraints(stmt.TableName, stmt.Columns, stmt.Constraints)
	if err != nil {
		return "", err
	}
	return "CREATE TABLE OK", nil
}

// executeInsert handles INSERT statements.
// It validates the data against the schema, generates a row ID,
// and stores the row in the storage engine.
//
// The row is stored as JSON with the key format: row:<table>:<id>
// The ID is auto-generated using a sequence stored at seq:<table>
//
// After a successful insert, the OnInsert callback is invoked
// to notify subscribers (for the WATCH feature).
//
// Parameters:
//   - stmt: The parsed INSERT statement
//
// Returns "INSERT 1" on success, or an error.
func (e *Executor) executeInsert(stmt *InsertStmt) (string, error) {
	// Validate that the table exists.
	table, ok := e.catalog.GetTable(stmt.TableName)
	if !ok {
		return "", errors.New("table not found")
	}

	// Handle auto-increment columns - allow fewer values if auto-increment columns can fill the gap
	autoIncCols := table.GetAutoIncrementColumns()
	expectedCols := len(table.Columns) - len(autoIncCols)

	// Validate that the number of values matches (either all columns or non-auto-increment columns)
	if len(stmt.Values) != len(table.Columns) && len(stmt.Values) != expectedCols {
		return "", errors.New("column count mismatch")
	}

	// Build the row with proper handling of auto-increment and default values
	normalizedValues := make([]string, len(table.Columns))
	valueIdx := 0

	for i, col := range table.Columns {
		var value string

		// Handle auto-increment columns
		if col.IsAutoIncrement() {
			if len(stmt.Values) == len(table.Columns) {
				// Explicit value provided
				value = stmt.Values[valueIdx]
				valueIdx++
				// Update the sequence if the explicit value is higher
				if intVal, err := parseIntValue(value); err == nil {
					e.catalog.UpdateAutoIncrement(stmt.TableName, col.Name, intVal)
				}
			} else {
				// Generate auto-increment value
				nextVal, err := e.catalog.GetNextAutoIncrement(stmt.TableName, col.Name)
				if err != nil {
					return "", fmt.Errorf("failed to generate auto-increment value for %s: %v", col.Name, err)
				}
				value = fmt.Sprintf("%d", nextVal)
			}
		} else {
			if valueIdx < len(stmt.Values) {
				value = stmt.Values[valueIdx]
				valueIdx++
			} else {
				// Check for default value
				if defaultVal, hasDefault := col.GetDefaultValue(); hasDefault {
					value = defaultVal
				} else {
					return "", fmt.Errorf("no value provided for column %s", col.Name)
				}
			}
		}

		// Check NOT NULL constraint
		if col.IsNotNull() && (value == "" || value == "NULL") {
			return "", fmt.Errorf("column %s cannot be NULL", col.Name)
		}

		// Validate the value against the column type
		if value != "" && value != "NULL" {
			if err := ValidateValue(col.Type, value); err != nil {
				return "", fmt.Errorf("column %s: %v", col.Name, err)
			}
			// Normalize the value for consistent storage
			normalized, err := NormalizeValue(col.Type, value)
			if err != nil {
				return "", fmt.Errorf("column %s: %v", col.Name, err)
			}
			normalizedValues[i] = normalized
		} else {
			normalizedValues[i] = value
		}
	}

	// Check UNIQUE and PRIMARY KEY constraints
	if err := e.checkUniqueConstraints(table, normalizedValues, ""); err != nil {
		return "", err
	}

	// Check FOREIGN KEY constraints
	if err := e.checkForeignKeyConstraints(table, normalizedValues); err != nil {
		return "", err
	}

	// Check CHECK constraints
	if err := e.checkCheckConstraints(table, normalizedValues); err != nil {
		return "", err
	}

	// Execute BEFORE INSERT triggers
	if err := e.executeTriggers(stmt.TableName, TriggerTimingBefore, TriggerEventInsert); err != nil {
		return "", fmt.Errorf("BEFORE INSERT trigger failed: %v", err)
	}

	// Generate a unique row ID using an auto-increment sequence.
	// The sequence is stored at key "seq:<table>".
	seqKey := "seq:" + table.Name
	var seq int
	seqVal, err := e.store.Get(seqKey)
	if err == nil {
		json.Unmarshal(seqVal, &seq)
	}
	seq++

	// Persist the updated sequence.
	seqBytes, _ := json.Marshal(seq)
	e.store.Put(seqKey, seqBytes)

	// Create the row key: row:<table>:<id>
	rowKey := fmt.Sprintf("row:%s:%d", table.Name, seq)

	// Build the row as a map of column names to values.
	row := make(map[string]interface{})
	for i, col := range table.Columns {
		row[col.Name] = normalizedValues[i]
	}

	// Serialize the row to JSON and store it.
	data, err := json.Marshal(row)
	if err != nil {
		return "", err
	}
	err = e.store.Put(rowKey, data)
	if err != nil {
		return "", err
	}

	// Update indexes for the new row.
	if e.indexMgr != nil {
		e.indexMgr.OnInsert(table.Name, rowKey, row)
	}

	// Invoke the OnInsert callback for reactive notifications.
	// This enables the WATCH feature to notify subscribers.
	if e.OnInsert != nil {
		e.OnInsert(table.Name, string(data))
	}

	// Execute AFTER INSERT triggers
	if err := e.executeTriggers(stmt.TableName, TriggerTimingAfter, TriggerEventInsert); err != nil {
		return "", fmt.Errorf("AFTER INSERT trigger failed: %v", err)
	}

	return "INSERT 1", nil
}

// parseIntValue parses a string value as an int64.
func parseIntValue(value string) (int64, error) {
	var intVal int64
	_, err := fmt.Sscanf(value, "%d", &intVal)
	return intVal, err
}

// checkUniqueConstraints verifies that UNIQUE and PRIMARY KEY constraints are satisfied.
// excludeRowKey is used during UPDATE to exclude the current row from the check.
func (e *Executor) checkUniqueConstraints(table TableSchema, values []string, excludeRowKey string) error {
	// Get all existing rows
	prefix := "row:" + table.Name + ":"
	rows, err := e.store.Scan(prefix)
	if err != nil {
		return err
	}

	// Check each column with UNIQUE or PRIMARY KEY constraint
	for i, col := range table.Columns {
		if col.IsUnique() || col.IsPrimaryKey() {
			newValue := values[i]
			if newValue == "" || newValue == "NULL" {
				continue // NULL values don't violate uniqueness
			}

			for rowKey, rowData := range rows {
				if excludeRowKey != "" && rowKey == excludeRowKey {
					continue // Skip the row being updated
				}

				var row map[string]interface{}
				if err := json.Unmarshal(rowData, &row); err != nil {
					continue
				}

				if existingVal, ok := row[col.Name]; ok {
					if fmt.Sprintf("%v", existingVal) == newValue {
						if col.IsPrimaryKey() {
							return fmt.Errorf("duplicate primary key value for column %s: %s", col.Name, newValue)
						}
						return fmt.Errorf("duplicate value for unique column %s: %s", col.Name, newValue)
					}
				}
			}
		}
	}

	// Check table-level unique constraints (composite unique keys)
	for _, constraint := range table.Constraints {
		if constraint.Type == ConstraintUnique || constraint.Type == ConstraintPrimaryKey {
			// Build the composite key value for the new row
			var newKeyParts []string
			for _, colName := range constraint.Columns {
				idx := table.GetColumnIndex(colName)
				if idx >= 0 && idx < len(values) {
					newKeyParts = append(newKeyParts, values[idx])
				}
			}

			// Check against existing rows
			for rowKey, rowData := range rows {
				if excludeRowKey != "" && rowKey == excludeRowKey {
					continue
				}

				var row map[string]interface{}
				if err := json.Unmarshal(rowData, &row); err != nil {
					continue
				}

				// Build the composite key value for the existing row
				var existingKeyParts []string
				allMatch := true
				for _, colName := range constraint.Columns {
					if existingVal, ok := row[colName]; ok {
						existingKeyParts = append(existingKeyParts, fmt.Sprintf("%v", existingVal))
					} else {
						allMatch = false
						break
					}
				}

				if allMatch && len(existingKeyParts) == len(newKeyParts) {
					match := true
					for j := range newKeyParts {
						if newKeyParts[j] != existingKeyParts[j] {
							match = false
							break
						}
					}
					if match {
						if constraint.Type == ConstraintPrimaryKey {
							return fmt.Errorf("duplicate primary key value for columns %v", constraint.Columns)
						}
						return fmt.Errorf("duplicate value for unique constraint on columns %v", constraint.Columns)
					}
				}
			}
		}
	}

	return nil
}

// checkForeignKeyConstraints verifies that all foreign key references exist.
func (e *Executor) checkForeignKeyConstraints(table TableSchema, values []string) error {
	fks := table.GetForeignKeys()

	for _, fk := range fks {
		// Get the value for the foreign key column
		idx := table.GetColumnIndex(fk.Column)
		if idx < 0 || idx >= len(values) {
			continue
		}

		fkValue := values[idx]
		if fkValue == "" || fkValue == "NULL" {
			continue // NULL foreign keys are allowed
		}

		// Check if the referenced value exists
		refTable, ok := e.catalog.GetTable(fk.RefTable)
		if !ok {
			return fmt.Errorf("foreign key references non-existent table: %s", fk.RefTable)
		}

		// Scan the referenced table for the value
		prefix := "row:" + fk.RefTable + ":"
		rows, err := e.store.Scan(prefix)
		if err != nil {
			return err
		}

		found := false
		for _, rowData := range rows {
			var row map[string]interface{}
			if err := json.Unmarshal(rowData, &row); err != nil {
				continue
			}

			if existingVal, ok := row[fk.RefColumn]; ok {
				if fmt.Sprintf("%v", existingVal) == fkValue {
					found = true
					break
				}
			}
		}

		if !found {
			return fmt.Errorf("foreign key constraint violation: value %s not found in %s.%s", fkValue, refTable.Name, fk.RefColumn)
		}
	}

	return nil
}

// checkCheckConstraints validates CHECK constraints for all columns.
func (e *Executor) checkCheckConstraints(table TableSchema, values []string) error {
	for i, col := range table.Columns {
		checkExpr := col.GetCheckConstraint()
		if checkExpr == nil {
			continue
		}

		if i >= len(values) {
			continue
		}

		value := values[i]
		if value == "" || value == "NULL" {
			continue // NULL values bypass CHECK constraints
		}

		if !e.evaluateCheckExpr(checkExpr, value) {
			return fmt.Errorf("CHECK constraint violation on column %s", col.Name)
		}
	}

	return nil
}

// evaluateCheckExpr evaluates a CHECK expression against a value.
func (e *Executor) evaluateCheckExpr(expr *CheckExpr, value string) bool {
	result := false

	switch expr.Operator {
	case "=":
		result = value == expr.Value
	case "<>":
		result = value != expr.Value
	case "<":
		result = compareValues(value, expr.Value) < 0
	case ">":
		result = compareValues(value, expr.Value) > 0
	case "<=":
		result = compareValues(value, expr.Value) <= 0
	case ">=":
		result = compareValues(value, expr.Value) >= 0
	case "IN":
		for _, v := range expr.Values {
			if value == v {
				result = true
				break
			}
		}
	case "BETWEEN":
		result = compareValues(value, expr.MinValue) >= 0 && compareValues(value, expr.MaxValue) <= 0
	default:
		return false
	}

	// Handle AND condition
	if expr.And != nil {
		result = result && e.evaluateCheckExpr(expr.And, value)
	}

	// Handle OR condition
	if expr.Or != nil {
		result = result || e.evaluateCheckExpr(expr.Or, value)
	}

	return result
}

// executeUpdate handles UPDATE statements.
// It scans all rows in the table, applies WHERE and RLS filters,
// and updates matching rows.
//
// The update process:
//  1. Scan all rows with the table prefix
//  2. Filter by WHERE clause (if present)
//  3. Filter by RLS condition (if user has RLS)
//  4. Apply the column updates
//  5. Save the modified row
//
// Parameters:
//   - stmt: The parsed UPDATE statement
//
// Returns "UPDATE <count>" where count is the number of rows updated.
func (e *Executor) executeUpdate(stmt *UpdateStmt) (string, error) {
	// Get table schema for type validation
	table, ok := e.catalog.GetTable(stmt.TableName)
	if !ok {
		return "", errors.New("table not found")
	}

	// Build a map of column names to types for validation
	colTypes := make(map[string]string)
	for _, col := range table.Columns {
		colTypes[col.Name] = col.Type
	}

	// Validate and normalize update values
	normalizedUpdates := make(map[string]string)
	for col, val := range stmt.Updates {
		colType, exists := colTypes[col]
		if !exists {
			return "", fmt.Errorf("column not found: %s", col)
		}
		if err := ValidateValue(colType, val); err != nil {
			return "", fmt.Errorf("column %s: %v", col, err)
		}
		normalized, err := NormalizeValue(colType, val)
		if err != nil {
			return "", fmt.Errorf("column %s: %v", col, err)
		}
		normalizedUpdates[col] = normalized
	}

	// Execute BEFORE UPDATE triggers
	if err := e.executeTriggers(stmt.TableName, TriggerTimingBefore, TriggerEventUpdate); err != nil {
		return "", fmt.Errorf("BEFORE UPDATE trigger failed: %v", err)
	}

	// Scan all rows in the table.
	prefix := "row:" + stmt.TableName + ":"
	rows, err := e.store.Scan(prefix)
	if err != nil {
		return "", err
	}

	count := 0
	for key, val := range rows {
		// Deserialize the row.
		var row map[string]interface{}
		json.Unmarshal(val, &row)

		// Apply WHERE filter.
		if stmt.Where != nil {
			colVal, exists := row[stmt.Where.Column]
			if !exists {
				continue
			}
			if fmt.Sprintf("%v", colVal) != stmt.Where.Value {
				continue
			}
		}

		// Apply RLS filter for non-admin users.
		if e.currentUser != "" && e.currentUser != "admin" {
			allowed, authRLS := e.auth.CheckPermission(e.currentUser, stmt.TableName)
			if !allowed {
				continue
			}
			if authRLS != nil {
				colVal, exists := row[authRLS.Column]
				if !exists || fmt.Sprintf("%v", colVal) != authRLS.Value {
					continue
				}
			}
		}

		// Save old row for index update
		oldRow := make(map[string]interface{})
		for k, v := range row {
			oldRow[k] = v
		}

		// Apply the column updates with normalized values.
		for col, newVal := range normalizedUpdates {
			row[col] = newVal
		}

		// Check NOT NULL constraints for updated columns
		for colName, newVal := range normalizedUpdates {
			for _, col := range table.Columns {
				if col.Name == colName && col.IsNotNull() {
					if newVal == "" || newVal == "NULL" {
						return "", fmt.Errorf("column %s cannot be NULL", colName)
					}
				}
			}
		}

		// Build values array for constraint checking
		newValues := make([]string, len(table.Columns))
		for i, col := range table.Columns {
			if val, ok := row[col.Name]; ok {
				newValues[i] = fmt.Sprintf("%v", val)
			}
		}

		// Check UNIQUE and PRIMARY KEY constraints (excluding current row)
		if err := e.checkUniqueConstraints(table, newValues, key); err != nil {
			return "", err
		}

		// Check FOREIGN KEY constraints
		if err := e.checkForeignKeyConstraints(table, newValues); err != nil {
			return "", err
		}

		// Check CHECK constraints for updated columns
		for colName, newVal := range normalizedUpdates {
			for _, col := range table.Columns {
				if col.Name == colName {
					checkExpr := col.GetCheckConstraint()
					if checkExpr != nil && newVal != "" && newVal != "NULL" {
						if !e.evaluateCheckExpr(checkExpr, newVal) {
							return "", fmt.Errorf("CHECK constraint violation on column %s", colName)
						}
					}
				}
			}
		}

		// Save the updated row.
		newData, _ := json.Marshal(row)
		e.store.Put(key, newData)

		// Update indexes
		if e.indexMgr != nil {
			e.indexMgr.OnUpdate(stmt.TableName, key, oldRow, row)
		}

		count++
	}

	// Execute AFTER UPDATE triggers
	if err := e.executeTriggers(stmt.TableName, TriggerTimingAfter, TriggerEventUpdate); err != nil {
		return "", fmt.Errorf("AFTER UPDATE trigger failed: %v", err)
	}

	return fmt.Sprintf("UPDATE %d", count), nil
}

// executeDelete handles DELETE statements.
// It scans all rows in the table, applies WHERE and RLS filters,
// and deletes matching rows.
//
// The delete process:
//  1. Scan all rows with the table prefix
//  2. Filter by WHERE clause (if present)
//  3. Filter by RLS condition (if user has RLS)
//  4. Delete matching rows from storage
//
// Parameters:
//   - stmt: The parsed DELETE statement
//
// Returns "DELETE <count>" where count is the number of rows deleted.
func (e *Executor) executeDelete(stmt *DeleteStmt) (string, error) {
	// Execute BEFORE DELETE triggers
	if err := e.executeTriggers(stmt.TableName, TriggerTimingBefore, TriggerEventDelete); err != nil {
		return "", fmt.Errorf("BEFORE DELETE trigger failed: %v", err)
	}

	// Scan all rows in the table.
	prefix := "row:" + stmt.TableName + ":"
	rows, err := e.store.Scan(prefix)
	if err != nil {
		return "", err
	}

	count := 0
	for key, val := range rows {
		// Deserialize the row.
		var row map[string]interface{}
		json.Unmarshal(val, &row)

		// Apply WHERE filter.
		if stmt.Where != nil {
			colVal, exists := row[stmt.Where.Column]
			if !exists {
				continue
			}
			if fmt.Sprintf("%v", colVal) != stmt.Where.Value {
				continue
			}
		}

		// Apply RLS filter for non-admin users.
		if e.currentUser != "" && e.currentUser != "admin" {
			allowed, authRLS := e.auth.CheckPermission(e.currentUser, stmt.TableName)
			if !allowed {
				continue
			}
			if authRLS != nil {
				colVal, exists := row[authRLS.Column]
				if !exists || fmt.Sprintf("%v", colVal) != authRLS.Value {
					continue
				}
			}
		}

		// Check for foreign key references from other tables
		if err := e.checkForeignKeyReferences(stmt.TableName, row); err != nil {
			return "", err
		}

		// Update indexes before deleting
		if e.indexMgr != nil {
			e.indexMgr.OnDelete(stmt.TableName, key, row)
		}

		// Delete the row from storage.
		e.store.Delete(key)
		count++
	}

	// Execute AFTER DELETE triggers
	if err := e.executeTriggers(stmt.TableName, TriggerTimingAfter, TriggerEventDelete); err != nil {
		return "", fmt.Errorf("AFTER DELETE trigger failed: %v", err)
	}

	return fmt.Sprintf("DELETE %d", count), nil
}

// checkForeignKeyReferences checks if any other table has a foreign key reference to this row.
// This prevents deleting rows that are still referenced by other tables.
func (e *Executor) checkForeignKeyReferences(tableName string, row map[string]interface{}) error {
	// Get all tables and check their foreign keys
	for tblName, schema := range e.catalog.Tables {
		if tblName == tableName {
			continue // Skip self-references for now
		}

		fks := schema.GetForeignKeys()
		for _, fk := range fks {
			if fk.RefTable != tableName {
				continue
			}

			// Get the value being deleted
			refValue, ok := row[fk.RefColumn]
			if !ok {
				continue
			}
			refValueStr := fmt.Sprintf("%v", refValue)

			// Check if any row in the referencing table has this value
			prefix := "row:" + tblName + ":"
			rows, err := e.store.Scan(prefix)
			if err != nil {
				continue
			}

			for _, rowData := range rows {
				var refRow map[string]interface{}
				if err := json.Unmarshal(rowData, &refRow); err != nil {
					continue
				}

				if fkValue, ok := refRow[fk.Column]; ok {
					if fmt.Sprintf("%v", fkValue) == refValueStr {
						return fmt.Errorf("cannot delete: row is referenced by %s.%s", tblName, fk.Column)
					}
				}
			}
		}
	}

	return nil
}

// executeSelect handles SELECT statements.
// It supports WHERE filtering, JOINs, ORDER BY, and LIMIT.
//
// The query execution process:
//  1. Validate that the table exists
//  2. Load RLS condition for the current user
//  3. Scan all rows from the primary table
//  4. If JOIN is present, perform a Nested Loop Join
//  5. Apply WHERE and RLS filters
//  6. Project the requested columns
//  7. Sort results if ORDER BY is present
//  8. Apply LIMIT if present
//
// Join Algorithm:
// FlyDB uses a simple Nested Loop Join, which iterates through all
// combinations of rows from both tables and filters by the ON condition.
// This is O(n*m) complexity but simple to implement.
//
// Parameters:
//   - stmt: The parsed SELECT statement
//
// Returns the query results as newline-separated CSV rows.
func (e *Executor) executeSelect(stmt *SelectStmt) (string, error) {
	// Check if the table is actually a view
	if view, ok := e.catalog.GetView(stmt.TableName); ok {
		return e.executeViewQuery(stmt, view)
	}

	// Validate that the primary table exists.
	table, ok := e.catalog.GetTable(stmt.TableName)
	if !ok {
		return "", errors.New("table not found")
	}

	// Expand "*" to all columns from the table schema.
	// This allows SELECT * FROM table to work correctly.
	if len(stmt.Columns) == 1 && stmt.Columns[0] == "*" {
		stmt.Columns = make([]string, len(table.Columns))
		for i, col := range table.Columns {
			stmt.Columns[i] = col.Name
		}
	}

	// Load RLS condition for the current user.
	// This will be applied to filter rows during processing.
	var rls *Condition
	if e.currentUser != "" && e.currentUser != "admin" {
		allowed, authRLS := e.auth.CheckPermission(e.currentUser, stmt.TableName)
		if !allowed {
			return "", errors.New("permission denied")
		}
		if authRLS != nil {
			rls = &Condition{Column: authRLS.Column, Value: authRLS.Value}
		}
	}

	// Try to use an index for the WHERE clause if available.
	// This provides O(log N) lookup instead of O(N) full table scan.
	var rows map[string][]byte
	var err error

	if stmt.Where != nil && e.indexMgr != nil && e.indexMgr.HasIndex(stmt.TableName, stmt.Where.Column) {
		// Use index lookup for O(log N) performance
		rowKey, found := e.indexMgr.Lookup(stmt.TableName, stmt.Where.Column, stmt.Where.Value)
		if found {
			rows = make(map[string][]byte)
			val, err := e.store.Get(rowKey)
			if err == nil {
				rows[rowKey] = val
			}
		} else {
			rows = make(map[string][]byte) // Empty result
		}
	} else {
		// Fall back to full table scan
		prefix := "row:" + stmt.TableName + ":"
		rows, err = e.store.Scan(prefix)
		if err != nil {
			return "", err
		}
	}

	var result []string

	// If there's a JOIN, load the join table data.
	var joinRows map[string][]byte
	if stmt.Join != nil {
		_, ok := e.catalog.GetTable(stmt.Join.TableName)
		if !ok {
			return "", errors.New("join table not found")
		}
		p := "row:" + stmt.Join.TableName + ":"
		joinRows, err = e.store.Scan(p)
		if err != nil {
			return "", err
		}
	}

	// Process each row from the primary table.
	for _, val := range rows {
		var row map[string]interface{}
		json.Unmarshal(val, &row)

		// Create a flattened row with both simple and qualified column names.
		// This allows queries to use either "name" or "users.name".
		flatRow := make(map[string]interface{})
		for k, v := range row {
			flatRow[k] = v                    // Simple access: "name"
			flatRow[stmt.TableName+"."+k] = v // Qualified access: "users.name"
		}

		if stmt.Join != nil {
			// Nested Loop Join: iterate through all rows in the join table.
			for _, jVal := range joinRows {
				var jRow map[string]interface{}
				json.Unmarshal(jVal, &jRow)

				// Combine columns from both tables.
				combinedRow := make(map[string]interface{})
				for k, v := range flatRow {
					combinedRow[k] = v
				}
				for k, v := range jRow {
					combinedRow[k] = v
					combinedRow[stmt.Join.TableName+"."+k] = v
				}

				// Check the JOIN ON condition.
				// The condition is stored as Column = Value, where both
				// may be column references (e.g., "users.id = orders.user_id").
				leftVal, ok1 := combinedRow[stmt.Join.On.Column]
				rightVal, ok2 := combinedRow[stmt.Join.On.Value]

				// If the right side isn't a column, treat it as a literal.
				if !ok2 {
					rightVal = stmt.Join.On.Value
				}

				// If the values match, process the combined row.
				if ok1 && fmt.Sprintf("%v", leftVal) == fmt.Sprintf("%v", rightVal) {
					e.processRow(combinedRow, stmt, &result, rls)
				}
			}
		} else {
			// No JOIN - process the row directly.
			e.processRow(flatRow, stmt, &result, rls)
		}
	}

	// Apply ORDER BY if present.
	// Note: This is a simplified implementation that sorts the formatted
	// result strings. A production implementation would sort the row
	// structs before formatting.
	if stmt.OrderBy != nil {
		col := stmt.OrderBy.Column
		desc := stmt.OrderBy.Direction == "DESC"

		sort.Slice(result, func(i, j int) bool {
			// Suppress unused variable warning.
			// In a real implementation, we would extract the column value
			// from each row and compare those values.
			_ = col

			// Extract the first value from each CSV row for comparison.
			v1 := strings.TrimSpace(result[i])
			v2 := strings.TrimSpace(result[j])

			// Try to compare as integers.
			var i1, i2 int
			_, err1 := fmt.Sscanf(v1, "%d", &i1)
			_, err2 := fmt.Sscanf(v2, "%d", &i2)

			if err1 == nil && err2 == nil {
				if desc {
					return i1 > i2
				}
				return i1 < i2
			}

			// Fallback to string comparison.
			if desc {
				return v1 > v2
			}
			return v1 < v2
		})
	}

	// Apply DISTINCT if present - remove duplicate rows
	if stmt.Distinct {
		seen := make(map[string]bool)
		uniqueResult := make([]string, 0, len(result))
		for _, row := range result {
			if !seen[row] {
				seen[row] = true
				uniqueResult = append(uniqueResult, row)
			}
		}
		result = uniqueResult
	}

	// Apply LIMIT if present.
	if stmt.Limit > 0 && len(result) > stmt.Limit {
		result = result[:stmt.Limit]
	}

	// If there are aggregate functions, compute them
	if len(stmt.Aggregates) > 0 {
		// If GROUP BY is present, compute aggregates per group
		if len(stmt.GroupBy) > 0 {
			return e.computeGroupedAggregates(stmt, rows, rls)
		}
		return e.computeAggregates(stmt, rows, rls)
	}

	// Build the result with row count information.
	// Format: data rows followed by a summary line with row count.
	rowCount := len(result)
	if rowCount == 0 {
		return fmt.Sprintf("(0 rows)"), nil
	}
	return fmt.Sprintf("%s\n(%d rows)", strings.Join(result, "\n"), rowCount), nil
}

// executeUnion executes a UNION statement by combining results from multiple SELECTs.
// UNION removes duplicates by default, UNION ALL keeps all rows.
func (e *Executor) executeUnion(stmt *UnionStmt) (string, error) {
	// Execute the left SELECT
	leftResult, err := e.executeSelect(stmt.Left)
	if err != nil {
		return "", fmt.Errorf("error executing left side of UNION: %v", err)
	}

	// Execute the right SELECT
	rightResult, err := e.executeSelect(stmt.Right)
	if err != nil {
		return "", fmt.Errorf("error executing right side of UNION: %v", err)
	}

	// Parse the results to extract rows
	leftRows := extractResultRows(leftResult)
	rightRows := extractResultRows(rightResult)

	// Combine the results
	var combinedRows []string
	if stmt.All {
		// UNION ALL: keep all rows including duplicates
		combinedRows = append(leftRows, rightRows...)
	} else {
		// UNION: remove duplicates
		seen := make(map[string]bool)
		for _, row := range leftRows {
			if !seen[row] {
				seen[row] = true
				combinedRows = append(combinedRows, row)
			}
		}
		for _, row := range rightRows {
			if !seen[row] {
				seen[row] = true
				combinedRows = append(combinedRows, row)
			}
		}
	}

	// Handle chained UNIONs
	if stmt.NextUnion != nil {
		// Create a temporary result and recursively process
		tempResult := formatUnionResult(combinedRows)
		nextResult, err := e.executeUnion(stmt.NextUnion)
		if err != nil {
			return "", err
		}
		nextRows := extractResultRows(nextResult)
		if stmt.NextUnion.All {
			combinedRows = append(combinedRows, nextRows...)
		} else {
			seen := make(map[string]bool)
			for _, row := range combinedRows {
				seen[row] = true
			}
			for _, row := range nextRows {
				if !seen[row] {
					seen[row] = true
					combinedRows = append(combinedRows, row)
				}
			}
		}
		_ = tempResult // suppress unused warning
	}

	return formatUnionResult(combinedRows), nil
}

// extractResultRows extracts data rows from a SELECT result string.
// It removes the row count line at the end.
func extractResultRows(result string) []string {
	var rows []string
	lines := strings.Split(result, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		// Skip the row count line
		if strings.HasPrefix(line, "(") && strings.HasSuffix(line, " rows)") {
			continue
		}
		if line == "" {
			continue
		}
		rows = append(rows, line)
	}
	return rows
}

// formatUnionResult formats combined rows into a result string with row count.
func formatUnionResult(rows []string) string {
	rowCount := len(rows)
	if rowCount == 0 {
		return "(0 rows)"
	}
	return fmt.Sprintf("%s\n(%d rows)", strings.Join(rows, "\n"), rowCount)
}

// processRow applies WHERE and RLS filters to a row, then projects
// the requested columns and adds the result to the output.
//
// This helper function is used by executeSelect to process each row
// (or combined row in the case of JOINs).
//
// Parameters:
//   - row: The row data as a map of column names to values
//   - stmt: The SELECT statement (for WHERE and column list)
//   - result: Pointer to the result slice to append to
//   - rls: Optional RLS condition to apply
func (e *Executor) processRow(row map[string]interface{}, stmt *SelectStmt, result *[]string, rls *Condition) {
	// Apply extended WHERE filter with subquery support.
	if stmt.WhereExt != nil {
		if !e.evaluateWhereClause(stmt.WhereExt, row) {
			return
		}
	} else if stmt.Where != nil {
		// Fallback to simple WHERE for backward compatibility
		colVal, exists := row[stmt.Where.Column]
		if !exists {
			return
		}
		if fmt.Sprintf("%v", colVal) != stmt.Where.Value {
			return
		}
	}

	// Apply RLS filter.
	if rls != nil {
		colVal, exists := row[rls.Column]
		if !exists {
			return
		}
		if fmt.Sprintf("%v", colVal) != rls.Value {
			return
		}
	}

	// Project the requested columns.
	// Build a CSV row with the values of the selected columns.
	var outRow []string
	for _, col := range stmt.Columns {
		if v, ok := row[col]; ok {
			outRow = append(outRow, fmt.Sprintf("%v", v))
		}
	}

	// Add the formatted row to the result.
	*result = append(*result, strings.Join(outRow, ", "))
}

// evaluateWhereClause evaluates an extended WHERE clause against a row.
// It supports subqueries with IN, NOT IN, and EXISTS operators.
func (e *Executor) evaluateWhereClause(where *WhereClause, row map[string]interface{}) bool {
	// Handle EXISTS operator
	if where.Operator == "EXISTS" {
		if where.Subquery != nil {
			result, err := e.executeSelect(where.Subquery)
			if err != nil {
				return false
			}
			// EXISTS returns true if the subquery returns any rows
			return !strings.Contains(result, "(0 rows)")
		}
		return false
	}

	// Get the column value from the row
	colVal, exists := row[where.Column]
	if !exists {
		return false
	}
	colStr := fmt.Sprintf("%v", colVal)

	switch where.Operator {
	case "=":
		return colStr == where.Value
	case "<":
		return compareValues(colStr, where.Value) < 0
	case ">":
		return compareValues(colStr, where.Value) > 0
	case "<=":
		return compareValues(colStr, where.Value) <= 0
	case ">=":
		return compareValues(colStr, where.Value) >= 0
	case "IN", "NOT IN":
		var valuesToCheck []string

		if where.IsSubquery && where.Subquery != nil {
			// Execute the subquery and extract values
			result, err := e.executeSelect(where.Subquery)
			if err != nil {
				return false
			}
			// Parse the result to extract values
			valuesToCheck = extractSubqueryValues(result)
		} else {
			valuesToCheck = where.Values
		}

		// Check if the column value is in the list
		found := false
		for _, v := range valuesToCheck {
			if colStr == v {
				found = true
				break
			}
		}

		if where.Operator == "IN" {
			return found
		}
		return !found // NOT IN
	}

	return false
}

// compareValues compares two values, trying numeric comparison first.
func compareValues(a, b string) int {
	// Try to compare as integers
	var ai, bi int
	_, err1 := fmt.Sscanf(a, "%d", &ai)
	_, err2 := fmt.Sscanf(b, "%d", &bi)
	if err1 == nil && err2 == nil {
		if ai < bi {
			return -1
		} else if ai > bi {
			return 1
		}
		return 0
	}

	// Try to compare as floats
	var af, bf float64
	_, err1 = fmt.Sscanf(a, "%f", &af)
	_, err2 = fmt.Sscanf(b, "%f", &bf)
	if err1 == nil && err2 == nil {
		if af < bf {
			return -1
		} else if af > bf {
			return 1
		}
		return 0
	}

	// Fall back to string comparison
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

// extractSubqueryValues extracts values from a SELECT result.
// It parses the result format and returns the first column values.
func extractSubqueryValues(result string) []string {
	var values []string
	lines := strings.Split(result, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		// Skip the row count line
		if strings.HasPrefix(line, "(") && strings.HasSuffix(line, " rows)") {
			continue
		}
		if line == "" {
			continue
		}
		// Extract the first column value (before any comma)
		parts := strings.SplitN(line, ",", 2)
		if len(parts) > 0 {
			values = append(values, strings.TrimSpace(parts[0]))
		}
	}
	return values
}

// executeBegin starts a new transaction.
// Returns an error if a transaction is already active.
func (e *Executor) executeBegin() (string, error) {
	if e.tx != nil && e.tx.IsActive() {
		return "", errors.New("transaction already in progress")
	}

	if e.kvStore == nil {
		return "", errors.New("transactions not supported with this storage engine")
	}

	e.tx = storage.NewTransaction(e.kvStore)
	return "BEGIN", nil
}

// executeCommit commits the current transaction.
// Returns an error if no transaction is active.
func (e *Executor) executeCommit() (string, error) {
	if e.tx == nil || !e.tx.IsActive() {
		return "", errors.New("no transaction in progress")
	}

	err := e.tx.Commit()
	if err != nil {
		e.tx = nil
		return "", err
	}

	e.tx = nil
	return "COMMIT", nil
}

// executeRollback aborts the current transaction.
// Returns an error if no transaction is active.
func (e *Executor) executeRollback() (string, error) {
	if e.tx == nil || !e.tx.IsActive() {
		return "", errors.New("no transaction in progress")
	}

	err := e.tx.Rollback()
	e.tx = nil
	if err != nil {
		return "", err
	}

	return "ROLLBACK", nil
}

// executeCreateIndex creates a new index on a table column.
func (e *Executor) executeCreateIndex(stmt *CreateIndexStmt) (string, error) {
	// Verify the table exists
	_, ok := e.catalog.GetTable(stmt.TableName)
	if !ok {
		return "", errors.New("table not found: " + stmt.TableName)
	}

	// Create the index
	err := e.indexMgr.CreateIndex(stmt.TableName, stmt.ColumnName)
	if err != nil {
		return "", err
	}

	return "CREATE INDEX OK", nil
}

// InTransaction returns true if a transaction is currently active.
func (e *Executor) InTransaction() bool {
	return e.tx != nil && e.tx.IsActive()
}

// GetTransaction returns the current active transaction, or nil if none.
func (e *Executor) GetTransaction() *storage.Transaction {
	return e.tx
}

// SetTransaction sets the current transaction (used by server for per-connection transactions).
func (e *Executor) SetTransaction(tx *storage.Transaction) {
	e.tx = tx
}

// executePrepare handles PREPARE statements.
// It compiles the query and stores it for later execution.
//
// Parameters:
//   - stmt: The parsed PREPARE statement
//
// Returns "PREPARE OK" on success, or an error.
func (e *Executor) executePrepare(stmt *PrepareStmt) (string, error) {
	err := e.preparedStmts.Prepare(stmt.Name, stmt.Query)
	if err != nil {
		return "", err
	}
	return "PREPARE OK", nil
}

// executeExecute handles EXECUTE statements.
// It runs a previously prepared statement with the given parameters.
//
// Parameters:
//   - stmt: The parsed EXECUTE statement
//
// Returns the result of the executed query, or an error.
func (e *Executor) executeExecute(stmt *ExecuteStmt) (string, error) {
	return e.preparedStmts.ExecuteWithStringParams(stmt.Name, stmt.Params)
}

// executeDeallocate handles DEALLOCATE statements.
// It removes a prepared statement from memory.
//
// Parameters:
//   - stmt: The parsed DEALLOCATE statement
//
// Returns "DEALLOCATE OK" on success, or an error.
func (e *Executor) executeDeallocate(stmt *DeallocateStmt) (string, error) {
	err := e.preparedStmts.Deallocate(stmt.Name)
	if err != nil {
		return "", err
	}
	return "DEALLOCATE OK", nil
}

// GetPreparedStatementManager returns the prepared statement manager.
func (e *Executor) GetPreparedStatementManager() *PreparedStatementManager {
	return e.preparedStmts
}


// computeAggregates computes aggregate function results for a SELECT statement.
// It processes all matching rows and computes COUNT, SUM, AVG, MIN, MAX.
//
// Parameters:
//   - stmt: The SELECT statement with aggregate expressions
//   - rows: The raw row data from the table
//   - rls: Optional RLS condition to apply
//
// Returns a single row with the aggregate results.
func (e *Executor) computeAggregates(stmt *SelectStmt, rows map[string][]byte, rls *Condition) (string, error) {
	// Initialize aggregate accumulators
	states := make(map[int]*aggState)
	for i := range stmt.Aggregates {
		states[i] = &aggState{}
	}

	// Process each row
	for _, val := range rows {
		var row map[string]interface{}
		if err := json.Unmarshal(val, &row); err != nil {
			continue
		}

		// Apply WHERE filter
		if stmt.Where != nil {
			colVal, exists := row[stmt.Where.Column]
			if !exists {
				continue
			}
			if fmt.Sprintf("%v", colVal) != stmt.Where.Value {
				continue
			}
		}

		// Apply RLS filter
		if rls != nil {
			colVal, exists := row[rls.Column]
			if !exists {
				continue
			}
			if fmt.Sprintf("%v", colVal) != rls.Value {
				continue
			}
		}

		// Update each aggregate
		for i, agg := range stmt.Aggregates {
			state := states[i]

			if agg.Function == "COUNT" {
				if agg.Column == "*" {
					state.count++
				} else if _, exists := row[agg.Column]; exists {
					state.count++
				}
				continue
			}

			// For SUM, AVG, MIN, MAX we need the column value
			colVal, exists := row[agg.Column]
			if !exists {
				continue
			}

			// Try to parse as number
			strVal := fmt.Sprintf("%v", colVal)
			numVal, err := strconv.ParseFloat(strVal, 64)

			switch agg.Function {
			case "SUM":
				if err == nil {
					state.sum += numVal
					state.count++
				}
			case "AVG":
				if err == nil {
					state.sum += numVal
					state.count++
				}
			case "MIN":
				if err == nil {
					if state.min == nil || numVal < *state.min {
						state.min = &numVal
					}
				} else {
					if state.minStr == nil || strVal < *state.minStr {
						state.minStr = &strVal
					}
				}
				state.count++
			case "MAX":
				if err == nil {
					if state.max == nil || numVal > *state.max {
						state.max = &numVal
					}
				} else {
					if state.maxStr == nil || strVal > *state.maxStr {
						state.maxStr = &strVal
					}
				}
				state.count++
			}
		}
	}

	// Build result row
	var results []string
	for i, agg := range stmt.Aggregates {
		state := states[i]

		switch agg.Function {
		case "COUNT":
			results = append(results, fmt.Sprintf("%d", state.count))
		case "SUM":
			results = append(results, fmt.Sprintf("%.2f", state.sum))
		case "AVG":
			if state.count > 0 {
				results = append(results, fmt.Sprintf("%.2f", state.sum/float64(state.count)))
			} else {
				results = append(results, "NULL")
			}
		case "MIN":
			if state.min != nil {
				results = append(results, fmt.Sprintf("%.2f", *state.min))
			} else if state.minStr != nil {
				results = append(results, *state.minStr)
			} else {
				results = append(results, "NULL")
			}
		case "MAX":
			if state.max != nil {
				results = append(results, fmt.Sprintf("%.2f", *state.max))
			} else if state.maxStr != nil {
				results = append(results, *state.maxStr)
			} else {
				results = append(results, "NULL")
			}
		}
	}

	return fmt.Sprintf("%s\n(1 row)", strings.Join(results, ", ")), nil
}

// computeGroupedAggregates computes aggregate function results grouped by specified columns.
// It processes all matching rows, groups them, and computes COUNT, SUM, AVG, MIN, MAX per group.
//
// Parameters:
//   - stmt: The SELECT statement with GROUP BY and aggregate expressions
//   - rows: The raw row data from the table
//   - rls: Optional RLS condition to apply
//
// Returns multiple rows with group key columns and aggregate results.
func (e *Executor) computeGroupedAggregates(stmt *SelectStmt, rows map[string][]byte, rls *Condition) (string, error) {
	// Group rows by the GROUP BY columns
	type groupData struct {
		keyValues []string                 // Values of GROUP BY columns
		rows      []map[string]interface{} // Rows in this group
	}

	groups := make(map[string]*groupData)

	// Process each row and assign to groups
	for _, val := range rows {
		var row map[string]interface{}
		if err := json.Unmarshal(val, &row); err != nil {
			continue
		}

		// Apply WHERE filter
		if stmt.Where != nil {
			colVal, exists := row[stmt.Where.Column]
			if !exists {
				continue
			}
			if fmt.Sprintf("%v", colVal) != stmt.Where.Value {
				continue
			}
		}

		// Apply RLS filter
		if rls != nil {
			colVal, exists := row[rls.Column]
			if !exists {
				continue
			}
			if fmt.Sprintf("%v", colVal) != rls.Value {
				continue
			}
		}

		// Build group key from GROUP BY columns
		var keyParts []string
		for _, col := range stmt.GroupBy {
			if v, exists := row[col]; exists {
				keyParts = append(keyParts, fmt.Sprintf("%v", v))
			} else {
				keyParts = append(keyParts, "NULL")
			}
		}
		groupKey := strings.Join(keyParts, "|")

		if groups[groupKey] == nil {
			groups[groupKey] = &groupData{
				keyValues: keyParts,
				rows:      []map[string]interface{}{},
			}
		}
		groups[groupKey].rows = append(groups[groupKey].rows, row)
	}

	// Compute aggregates for each group
	var resultRows []string

	for _, group := range groups {
		// Initialize aggregate accumulators for this group
		states := make(map[int]*aggState)
		for i := range stmt.Aggregates {
			states[i] = &aggState{}
		}

		// Process each row in the group
		for _, row := range group.rows {
			for i, agg := range stmt.Aggregates {
				state := states[i]

				if agg.Function == "COUNT" {
					if agg.Column == "*" {
						state.count++
					} else if _, exists := row[agg.Column]; exists {
						state.count++
					}
					continue
				}

				// For SUM, AVG, MIN, MAX we need the column value
				colVal, exists := row[agg.Column]
				if !exists {
					continue
				}

				strVal := fmt.Sprintf("%v", colVal)
				numVal, err := strconv.ParseFloat(strVal, 64)

				switch agg.Function {
				case "SUM":
					if err == nil {
						state.sum += numVal
						state.count++
					}
				case "AVG":
					if err == nil {
						state.sum += numVal
						state.count++
					}
				case "MIN":
					if err == nil {
						if state.min == nil || numVal < *state.min {
							state.min = &numVal
						}
					} else {
						if state.minStr == nil || strVal < *state.minStr {
							state.minStr = &strVal
						}
					}
					state.count++
				case "MAX":
					if err == nil {
						if state.max == nil || numVal > *state.max {
							state.max = &numVal
						}
					} else {
						if state.maxStr == nil || strVal > *state.maxStr {
							state.maxStr = &strVal
						}
					}
					state.count++
				}
			}
		}

		// Apply HAVING filter if present
		if stmt.Having != nil {
			havingPassed := e.evaluateHaving(stmt.Having, states, stmt.Aggregates)
			if !havingPassed {
				continue
			}
		}

		// Build result row: group key columns + aggregate results
		var rowParts []string

		// Add GROUP BY column values
		rowParts = append(rowParts, group.keyValues...)

		// Add aggregate results
		for i, agg := range stmt.Aggregates {
			state := states[i]

			switch agg.Function {
			case "COUNT":
				rowParts = append(rowParts, fmt.Sprintf("%d", state.count))
			case "SUM":
				rowParts = append(rowParts, fmt.Sprintf("%.2f", state.sum))
			case "AVG":
				if state.count > 0 {
					rowParts = append(rowParts, fmt.Sprintf("%.2f", state.sum/float64(state.count)))
				} else {
					rowParts = append(rowParts, "NULL")
				}
			case "MIN":
				if state.min != nil {
					rowParts = append(rowParts, fmt.Sprintf("%.2f", *state.min))
				} else if state.minStr != nil {
					rowParts = append(rowParts, *state.minStr)
				} else {
					rowParts = append(rowParts, "NULL")
				}
			case "MAX":
				if state.max != nil {
					rowParts = append(rowParts, fmt.Sprintf("%.2f", *state.max))
				} else if state.maxStr != nil {
					rowParts = append(rowParts, *state.maxStr)
				} else {
					rowParts = append(rowParts, "NULL")
				}
			}
		}

		resultRows = append(resultRows, strings.Join(rowParts, ", "))
	}

	rowCount := len(resultRows)
	if rowCount == 0 {
		return "(0 rows)", nil
	}
	return fmt.Sprintf("%s\n(%d rows)", strings.Join(resultRows, "\n"), rowCount), nil
}

// evaluateHaving evaluates a HAVING clause against computed aggregate states.
// Returns true if the group passes the HAVING filter.
func (e *Executor) evaluateHaving(having *HavingClause, states map[int]*aggState, aggregates []*AggregateExpr) bool {
	// Find the matching aggregate in the states
	var aggValue float64
	found := false

	for i, agg := range aggregates {
		if agg.Function == having.Aggregate.Function && agg.Column == having.Aggregate.Column {
			state := states[i]
			switch agg.Function {
			case "COUNT":
				aggValue = float64(state.count)
				found = true
			case "SUM":
				aggValue = state.sum
				found = true
			case "AVG":
				if state.count > 0 {
					aggValue = state.sum / float64(state.count)
					found = true
				}
			case "MIN":
				if state.min != nil {
					aggValue = *state.min
					found = true
				}
			case "MAX":
				if state.max != nil {
					aggValue = *state.max
					found = true
				}
			}
			break
		}
	}

	// If the HAVING aggregate wasn't in the SELECT list, compute it separately
	if !found {
		// For simplicity, we'll compute it from the first matching state
		// In a real implementation, we'd need to track this separately
		return true
	}

	// Parse the comparison value
	compareValue, err := strconv.ParseFloat(having.Value, 64)
	if err != nil {
		return false
	}

	// Apply the comparison
	switch having.Operator {
	case "=":
		return aggValue == compareValue
	case "<":
		return aggValue < compareValue
	case ">":
		return aggValue > compareValue
	case "<=":
		return aggValue <= compareValue
	case ">=":
		return aggValue >= compareValue
	}

	return false
}

// executeIntrospect handles INTROSPECT statements for database metadata inspection.
// It returns information about database objects based on the target specified.
//
// Supported targets:
//   - USERS: List all database users with their usernames
//   - TABLES: List all tables with their column schemas
//   - TABLE <name>: Detailed info for a specific table
//   - INDEXES: List all indexes with their table and column information
//   - SERVER: Show server/daemon information
//   - STATUS: Show overall database status and statistics
//
// Parameters:
//   - stmt: The parsed INTROSPECT statement
//
// Returns formatted metadata information.
func (e *Executor) executeIntrospect(stmt *IntrospectStmt) (string, error) {
	switch stmt.Target {
	case "USERS":
		return e.introspectUsers()
	case "TABLES":
		return e.introspectTables()
	case "TABLE":
		return e.introspectTable(stmt.ObjectName)
	case "INDEXES":
		return e.introspectIndexes()
	case "SERVER":
		return e.introspectServer()
	case "STATUS":
		return e.introspectStatus()
	default:
		return "", fmt.Errorf("unknown introspect target: %s", stmt.Target)
	}
}

// introspectUsers returns information about all database users.
func (e *Executor) introspectUsers() (string, error) {
	// Scan for all user keys with the _sys_users: prefix
	users, err := e.store.Scan("_sys_users:")
	if err != nil {
		return "", err
	}

	if len(users) == 0 {
		return "(0 rows)", nil
	}

	var results []string
	for key := range users {
		// Extract username from key format: _sys_users:<username>
		username := strings.TrimPrefix(key, "_sys_users:")
		results = append(results, username)
	}

	// Sort for consistent output
	sort.Strings(results)

	return fmt.Sprintf("%s\n(%d rows)", strings.Join(results, "\n"), len(results)), nil
}

// introspectServer returns information about the FlyDB server/daemon.
func (e *Executor) introspectServer() (string, error) {
	var results []string

	results = append(results, "╔══════════════════════════════════════╗")
	results = append(results, "║         FlyDB Server Info            ║")
	results = append(results, "╠══════════════════════════════════════╣")
	results = append(results, "║  Server:     FlyDB Daemon            ║")
	results = append(results, "║  Version:    01.26.1                 ║")
	results = append(results, "║  Status:     Running                 ║")
	results = append(results, "║  Storage:    WAL-backed              ║")
	results = append(results, "║  Protocol:   Binary + Text           ║")
	results = append(results, "╚══════════════════════════════════════╝")

	return strings.Join(results, "\n"), nil
}

// introspectTables returns information about all tables and their schemas.
func (e *Executor) introspectTables() (string, error) {
	if len(e.catalog.Tables) == 0 {
		return "(0 rows)", nil
	}

	var results []string
	// Get sorted table names for consistent output
	var tableNames []string
	for name := range e.catalog.Tables {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	for _, tableName := range tableNames {
		table := e.catalog.Tables[tableName]
		// Build column info
		var colInfos []string
		for _, col := range table.Columns {
			colInfos = append(colInfos, fmt.Sprintf("%s %s", col.Name, col.Type))
		}
		// Format: table_name, (col1 TYPE, col2 TYPE, ...)
		result := fmt.Sprintf("%s, (%s)", tableName, strings.Join(colInfos, ", "))
		results = append(results, result)
	}

	return fmt.Sprintf("%s\n(%d rows)", strings.Join(results, "\n"), len(results)), nil
}

// introspectIndexes returns information about all indexes.
func (e *Executor) introspectIndexes() (string, error) {
	if e.indexMgr == nil {
		return "(0 rows)", nil
	}

	indexes := e.indexMgr.ListIndexes()
	if len(indexes) == 0 {
		return "(0 rows)", nil
	}

	var results []string
	for _, idx := range indexes {
		results = append(results, idx)
	}

	// Sort for consistent output
	sort.Strings(results)

	return fmt.Sprintf("%s\n(%d rows)", strings.Join(results, "\n"), len(results)), nil
}

// introspectTable returns detailed information about a specific table.
func (e *Executor) introspectTable(tableName string) (string, error) {
	table, ok := e.catalog.GetTable(tableName)
	if !ok {
		return "", fmt.Errorf("table not found: %s", tableName)
	}

	var results []string

	// Table name
	results = append(results, fmt.Sprintf("Table: %s", table.Name))

	// Column information with constraints
	results = append(results, "Columns:")
	for i, col := range table.Columns {
		colInfo := fmt.Sprintf("  %d. %s (%s)", i+1, col.Name, col.Type)

		// Add constraint information
		var constraints []string
		if col.IsPrimaryKey() {
			constraints = append(constraints, "PRIMARY KEY")
		}
		if col.IsNotNull() && !col.IsPrimaryKey() {
			constraints = append(constraints, "NOT NULL")
		}
		if col.IsUnique() && !col.IsPrimaryKey() {
			constraints = append(constraints, "UNIQUE")
		}
		if col.IsAutoIncrement() {
			constraints = append(constraints, "AUTO_INCREMENT")
		}
		if fk := col.GetForeignKey(); fk != nil {
			constraints = append(constraints, fmt.Sprintf("REFERENCES %s(%s)", fk.Table, fk.Column))
		}
		if defaultVal, hasDefault := col.GetDefaultValue(); hasDefault {
			constraints = append(constraints, fmt.Sprintf("DEFAULT %s", defaultVal))
		}

		if len(constraints) > 0 {
			colInfo += " [" + strings.Join(constraints, ", ") + "]"
		}
		results = append(results, colInfo)
	}

	// Primary key information
	pkCols := table.GetPrimaryKeyColumns()
	if len(pkCols) > 0 {
		results = append(results, fmt.Sprintf("Primary Key: %s", strings.Join(pkCols, ", ")))
	} else {
		results = append(results, "Primary Key: none")
	}

	// Foreign key information
	fks := table.GetForeignKeys()
	if len(fks) > 0 {
		results = append(results, "Foreign Keys:")
		for _, fk := range fks {
			results = append(results, fmt.Sprintf("  %s -> %s(%s)", fk.Column, fk.RefTable, fk.RefColumn))
		}
	}

	// Table-level constraints
	if len(table.Constraints) > 0 {
		results = append(results, "Table Constraints:")
		for _, constraint := range table.Constraints {
			constraintInfo := fmt.Sprintf("  %s", constraint.Type)
			if constraint.Name != "" {
				constraintInfo = fmt.Sprintf("  %s (%s)", constraint.Name, constraint.Type)
			}
			if len(constraint.Columns) > 0 {
				constraintInfo += fmt.Sprintf(" on (%s)", strings.Join(constraint.Columns, ", "))
			}
			if constraint.ForeignKey != nil {
				constraintInfo += fmt.Sprintf(" -> %s(%s)", constraint.ForeignKey.Table, constraint.ForeignKey.Column)
			}
			results = append(results, constraintInfo)
		}
	}

	// Auto-increment sequences
	autoIncCols := table.GetAutoIncrementColumns()
	if len(autoIncCols) > 0 {
		results = append(results, "Auto-increment columns:")
		for _, colName := range autoIncCols {
			if table.AutoIncSeq != nil {
				if seq, ok := table.AutoIncSeq[colName]; ok {
					results = append(results, fmt.Sprintf("  %s: next value = %d", colName, seq+1))
				}
			}
		}
	}

	// Row count - scan for all rows in this table
	rowPrefix := "row:" + tableName + ":"
	rows, err := e.store.Scan(rowPrefix)
	rowCount := 0
	if err == nil {
		rowCount = len(rows)
	}
	results = append(results, fmt.Sprintf("Row count: %d", rowCount))

	// Indexes on this table
	if e.indexMgr != nil {
		indexes := e.indexMgr.ListIndexes()
		var tableIndexes []string
		for _, idx := range indexes {
			// Index format is "table:column"
			if strings.HasPrefix(idx, tableName+":") {
				column := strings.TrimPrefix(idx, tableName+":")
				tableIndexes = append(tableIndexes, column)
			}
		}
		if len(tableIndexes) > 0 {
			sort.Strings(tableIndexes)
			results = append(results, fmt.Sprintf("Indexes: %s", strings.Join(tableIndexes, ", ")))
		} else {
			results = append(results, "Indexes: none")
		}
	} else {
		results = append(results, "Indexes: none")
	}

	// Calculate approximate storage size
	storageSize := 0
	for _, rowData := range rows {
		storageSize += len(rowData)
	}
	results = append(results, fmt.Sprintf("Storage size: %d bytes", storageSize))

	return strings.Join(results, "\n"), nil
}

// introspectStatus returns comprehensive database status and statistics.
// This replaces the old INTROSPECT DATABASES and INTROSPECT DATABASE commands.
func (e *Executor) introspectStatus() (string, error) {
	var results []string

	// Header
	results = append(results, "╔══════════════════════════════════════════════════════╗")
	results = append(results, "║              FlyDB Database Status                   ║")
	results = append(results, "╠══════════════════════════════════════════════════════╣")

	// Table count
	tableCount := len(e.catalog.Tables)
	results = append(results, fmt.Sprintf("║  Tables:        %-36d ║", tableCount))

	// List table names
	if tableCount > 0 {
		var tableNames []string
		for name := range e.catalog.Tables {
			tableNames = append(tableNames, name)
		}
		sort.Strings(tableNames)
		tableList := strings.Join(tableNames, ", ")
		if len(tableList) > 36 {
			tableList = tableList[:33] + "..."
		}
		results = append(results, fmt.Sprintf("║  Table list:    %-36s ║", tableList))
	}

	// User count
	users, err := e.store.Scan("_sys_users:")
	userCount := 0
	if err == nil {
		userCount = len(users)
	}
	results = append(results, fmt.Sprintf("║  Users:         %-36d ║", userCount))

	// Index count
	indexCount := 0
	if e.indexMgr != nil {
		indexes := e.indexMgr.ListIndexes()
		indexCount = len(indexes)
	}
	results = append(results, fmt.Sprintf("║  Indexes:       %-36d ║", indexCount))

	// Total row count across all tables
	totalRows := 0
	totalStorageSize := 0
	for tableName := range e.catalog.Tables {
		rowPrefix := "row:" + tableName + ":"
		rows, err := e.store.Scan(rowPrefix)
		if err == nil {
			totalRows += len(rows)
			for _, rowData := range rows {
				totalStorageSize += len(rowData)
			}
		}
	}
	results = append(results, fmt.Sprintf("║  Total rows:    %-36d ║", totalRows))
	results = append(results, fmt.Sprintf("║  Data size:     %-33d B ║", totalStorageSize))

	// WAL size if available
	if kvStore, ok := e.store.(*storage.KVStore); ok {
		if wal := kvStore.WAL(); wal != nil {
			if walSize, err := wal.Size(); err == nil {
				results = append(results, fmt.Sprintf("║  WAL size:      %-33d B ║", walSize))
			}
		}
	}

	results = append(results, "╠══════════════════════════════════════════════════════╣")
	results = append(results, "║  Storage:       WAL-backed                           ║")
	results = append(results, "║  Status:        Active                               ║")
	results = append(results, "╚══════════════════════════════════════════════════════╝")

	return strings.Join(results, "\n"), nil
}

// executeCreateProcedure creates a new stored procedure.
func (e *Executor) executeCreateProcedure(stmt *CreateProcedureStmt) (string, error) {
	proc := StoredProcedure{
		Name:       stmt.Name,
		Parameters: stmt.Parameters,
		BodySQL:    stmt.BodySQL,
	}

	if err := e.catalog.CreateProcedure(proc); err != nil {
		return "", err
	}

	return "CREATE PROCEDURE OK", nil
}

// executeCall executes a stored procedure.
func (e *Executor) executeCall(stmt *CallStmt) (string, error) {
	proc, ok := e.catalog.GetProcedure(stmt.ProcedureName)
	if !ok {
		return "", fmt.Errorf("procedure not found: %s", stmt.ProcedureName)
	}

	// Validate argument count
	if len(stmt.Arguments) != len(proc.Parameters) {
		return "", fmt.Errorf("procedure %s expects %d arguments, got %d",
			stmt.ProcedureName, len(proc.Parameters), len(stmt.Arguments))
	}

	// Execute each statement in the procedure body
	var results []string
	for _, sqlStr := range proc.BodySQL {
		// Substitute parameters ($1, $2, etc.) with actual values
		execSQL := sqlStr
		for i, arg := range stmt.Arguments {
			placeholder := fmt.Sprintf("$%d", i+1)
			execSQL = strings.ReplaceAll(execSQL, placeholder, arg)
		}

		// Parse and execute the statement
		lexer := NewLexer(execSQL)
		parser := NewParser(lexer)
		parsedStmt, err := parser.Parse()
		if err != nil {
			return "", fmt.Errorf("error parsing procedure statement: %v", err)
		}

		result, err := e.Execute(parsedStmt)
		if err != nil {
			return "", fmt.Errorf("error executing procedure statement: %v", err)
		}
		results = append(results, result)
	}

	if len(results) == 0 {
		return "CALL OK", nil
	}
	return strings.Join(results, "\n"), nil
}

// executeDropProcedure removes a stored procedure.
func (e *Executor) executeDropProcedure(stmt *DropProcedureStmt) (string, error) {
	if err := e.catalog.DropProcedure(stmt.Name); err != nil {
		return "", err
	}
	return "DROP PROCEDURE OK", nil
}

// executeAlterTable handles ALTER TABLE statements.
// It modifies the structure of an existing table.
//
// Parameters:
//   - stmt: The parsed ALTER TABLE statement
//
// Returns "ALTER TABLE OK" on success, or an error.
func (e *Executor) executeAlterTable(stmt *AlterTableStmt) (string, error) {
	// Verify the table exists
	table, ok := e.catalog.GetTable(stmt.TableName)
	if !ok {
		return "", errors.New("table not found: " + stmt.TableName)
	}

	switch stmt.Action {
	case AlterActionAddColumn:
		if stmt.ColumnDef == nil {
			return "", errors.New("column definition required for ADD COLUMN")
		}

		// Validate the column type
		if !IsValidType(stmt.ColumnDef.Type) {
			return "", fmt.Errorf("invalid column type: %s", stmt.ColumnDef.Type)
		}

		// Validate foreign key references if present
		if fk := stmt.ColumnDef.GetForeignKey(); fk != nil {
			refTable, ok := e.catalog.GetTable(fk.Table)
			if !ok {
				return "", fmt.Errorf("foreign key references non-existent table: %s", fk.Table)
			}
			found := false
			for _, refCol := range refTable.Columns {
				if refCol.Name == fk.Column {
					found = true
					break
				}
			}
			if !found {
				return "", fmt.Errorf("foreign key references non-existent column: %s.%s", fk.Table, fk.Column)
			}
		}

		// Add the column to the schema
		if err := e.catalog.AddColumn(stmt.TableName, *stmt.ColumnDef); err != nil {
			return "", err
		}

		// Update existing rows to include the new column with default value
		prefix := "row:" + stmt.TableName + ":"
		rows, err := e.store.Scan(prefix)
		if err != nil {
			return "", err
		}

		defaultVal := ""
		if dv, hasDefault := stmt.ColumnDef.GetDefaultValue(); hasDefault {
			defaultVal = dv
		}

		for key, val := range rows {
			var row map[string]interface{}
			if err := json.Unmarshal(val, &row); err != nil {
				continue
			}
			row[stmt.ColumnDef.Name] = defaultVal
			newData, _ := json.Marshal(row)
			e.store.Put(key, newData)
		}

		return "ALTER TABLE OK", nil

	case AlterActionDropColumn:
		if stmt.ColumnName == "" {
			return "", errors.New("column name required for DROP COLUMN")
		}

		// Drop the column from the schema
		if err := e.catalog.DropColumn(stmt.TableName, stmt.ColumnName); err != nil {
			return "", err
		}

		// Update existing rows to remove the column
		prefix := "row:" + stmt.TableName + ":"
		rows, err := e.store.Scan(prefix)
		if err != nil {
			return "", err
		}

		for key, val := range rows {
			var row map[string]interface{}
			if err := json.Unmarshal(val, &row); err != nil {
				continue
			}
			delete(row, stmt.ColumnName)
			newData, _ := json.Marshal(row)
			e.store.Put(key, newData)
		}

		return "ALTER TABLE OK", nil

	case AlterActionRenameColumn:
		if stmt.ColumnName == "" || stmt.NewColumnName == "" {
			return "", errors.New("old and new column names required for RENAME COLUMN")
		}

		// Rename the column in the schema
		if err := e.catalog.RenameColumn(stmt.TableName, stmt.ColumnName, stmt.NewColumnName); err != nil {
			return "", err
		}

		// Update existing rows to rename the column
		prefix := "row:" + stmt.TableName + ":"
		rows, err := e.store.Scan(prefix)
		if err != nil {
			return "", err
		}

		for key, val := range rows {
			var row map[string]interface{}
			if err := json.Unmarshal(val, &row); err != nil {
				continue
			}
			if v, ok := row[stmt.ColumnName]; ok {
				row[stmt.NewColumnName] = v
				delete(row, stmt.ColumnName)
				newData, _ := json.Marshal(row)
				e.store.Put(key, newData)
			}
		}

		return "ALTER TABLE OK", nil

	case AlterActionModifyColumn:
		if stmt.ColumnName == "" || stmt.NewColumnType == "" {
			return "", errors.New("column name and new type required for MODIFY COLUMN")
		}

		// Validate the new column type
		if !IsValidType(stmt.NewColumnType) {
			return "", fmt.Errorf("invalid column type: %s", stmt.NewColumnType)
		}

		// Get the current column definition
		var currentCol *ColumnDef
		for _, col := range table.Columns {
			if col.Name == stmt.ColumnName {
				currentCol = &col
				break
			}
		}
		if currentCol == nil {
			return "", errors.New("column not found: " + stmt.ColumnName)
		}

		// Determine new constraints
		var newConstraints []ColumnConstraint
		if stmt.ColumnDef != nil && len(stmt.ColumnDef.Constraints) > 0 {
			newConstraints = stmt.ColumnDef.Constraints
		} else {
			newConstraints = currentCol.Constraints
		}

		// Modify the column in the schema
		if err := e.catalog.ModifyColumn(stmt.TableName, stmt.ColumnName, stmt.NewColumnType, newConstraints); err != nil {
			return "", err
		}

		return "ALTER TABLE OK", nil

	case AlterActionAddConstraint:
		// TODO: Implement ADD CONSTRAINT
		return "", errors.New("ADD CONSTRAINT not yet implemented")

	case AlterActionDropConstraint:
		// TODO: Implement DROP CONSTRAINT
		return "", errors.New("DROP CONSTRAINT not yet implemented")

	default:
		return "", fmt.Errorf("unknown ALTER TABLE action: %s", stmt.Action)
	}
}

// executeCreateView creates a new view in the catalog.
// A view is a virtual table based on a SELECT query.
//
// Parameters:
//   - stmt: The parsed CREATE VIEW statement
//
// Returns "CREATE VIEW OK" on success, or an error.
func (e *Executor) executeCreateView(stmt *CreateViewStmt) (string, error) {
	// Validate that the underlying table(s) exist
	if stmt.Query.TableName != "" {
		if _, ok := e.catalog.GetTable(stmt.Query.TableName); !ok {
			// Check if it's a view
			if _, ok := e.catalog.GetView(stmt.Query.TableName); !ok {
				return "", fmt.Errorf("table or view not found: %s", stmt.Query.TableName)
			}
		}
	}

	// Reconstruct the SQL query from the AST for storage
	querySQL := reconstructSelectSQL(stmt.Query)

	// Create the view in the catalog
	if err := e.catalog.CreateView(stmt.ViewName, querySQL); err != nil {
		return "", err
	}

	return "CREATE VIEW OK", nil
}

// executeDropView removes a view from the catalog.
//
// Parameters:
//   - stmt: The parsed DROP VIEW statement
//
// Returns "DROP VIEW OK" on success, or an error.
func (e *Executor) executeDropView(stmt *DropViewStmt) (string, error) {
	if err := e.catalog.DropView(stmt.ViewName); err != nil {
		return "", err
	}
	return "DROP VIEW OK", nil
}

// executeCreateTrigger creates a new trigger on a table.
//
// Parameters:
//   - stmt: The parsed CREATE TRIGGER statement
//
// Returns "CREATE TRIGGER OK" on success, or an error.
func (e *Executor) executeCreateTrigger(stmt *CreateTriggerStmt) (string, error) {
	// Validate that the table exists
	if _, ok := e.catalog.GetTable(stmt.TableName); !ok {
		return "", fmt.Errorf("table not found: %s", stmt.TableName)
	}

	// Create the trigger
	trigger := &Trigger{
		Name:      stmt.TriggerName,
		Timing:    stmt.Timing,
		Event:     stmt.Event,
		TableName: stmt.TableName,
		ActionSQL: stmt.ActionSQL,
	}

	if err := e.triggerMgr.CreateTrigger(trigger); err != nil {
		return "", err
	}

	return "CREATE TRIGGER OK", nil
}

// executeDropTrigger removes a trigger from a table.
//
// Parameters:
//   - stmt: The parsed DROP TRIGGER statement
//
// Returns "DROP TRIGGER OK" on success, or an error.
func (e *Executor) executeDropTrigger(stmt *DropTriggerStmt) (string, error) {
	if err := e.triggerMgr.DropTrigger(stmt.TableName, stmt.TriggerName); err != nil {
		return "", err
	}
	return "DROP TRIGGER OK", nil
}

// executeTriggers executes all triggers for a table with the specified timing and event.
// It parses and executes the action SQL for each matching trigger.
//
// Parameters:
//   - tableName: The table the operation is being performed on
//   - timing: BEFORE or AFTER
//   - event: INSERT, UPDATE, or DELETE
//
// Returns an error if any trigger fails to execute.
func (e *Executor) executeTriggers(tableName string, timing TriggerTiming, event TriggerEvent) error {
	triggers := e.triggerMgr.GetTriggers(tableName, timing, event)

	for _, trigger := range triggers {
		// Parse and execute the trigger's action SQL
		lexer := NewLexer(trigger.ActionSQL)
		parser := NewParser(lexer)
		stmt, err := parser.Parse()
		if err != nil {
			return fmt.Errorf("trigger %s: failed to parse action SQL: %v", trigger.Name, err)
		}

		_, err = e.Execute(stmt)
		if err != nil {
			return fmt.Errorf("trigger %s: failed to execute action: %v", trigger.Name, err)
		}
	}

	return nil
}

// reconstructSelectSQL reconstructs a SQL query string from a SelectStmt AST.
// This is used to store view definitions.
func reconstructSelectSQL(stmt *SelectStmt) string {
	var sql string

	// SELECT clause
	sql = "SELECT "
	if stmt.Distinct {
		sql += "DISTINCT "
	}
	if len(stmt.Columns) == 0 {
		sql += "*"
	} else {
		sql += joinStrings(stmt.Columns, ", ")
	}

	// FROM clause
	if stmt.TableName != "" {
		sql += " FROM " + stmt.TableName
	}

	// JOIN clause
	if stmt.Join != nil {
		sql += " JOIN " + stmt.Join.TableName
		if stmt.Join.On != nil {
			sql += " ON " + stmt.Join.On.Column + " = " + stmt.Join.On.Value
		}
	}

	// WHERE clause
	if stmt.Where != nil {
		sql += " WHERE " + stmt.Where.Column + " = " + stmt.Where.Value
	}

	// GROUP BY clause
	if len(stmt.GroupBy) > 0 {
		sql += " GROUP BY " + joinStrings(stmt.GroupBy, ", ")
	}

	// HAVING clause
	if stmt.Having != nil && stmt.Having.Aggregate != nil {
		sql += " HAVING " + stmt.Having.Aggregate.Function + "(" + stmt.Having.Aggregate.Column + ") " + stmt.Having.Operator + " " + stmt.Having.Value
	}

	// ORDER BY clause
	if stmt.OrderBy != nil {
		sql += " ORDER BY " + stmt.OrderBy.Column
		if stmt.OrderBy.Direction != "" {
			sql += " " + stmt.OrderBy.Direction
		}
	}

	// LIMIT clause
	if stmt.Limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", stmt.Limit)
	}

	return sql
}

// joinStrings joins a slice of strings with a separator.
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}

// executeViewQuery executes a SELECT query against a view.
// It parses the view's stored query, applies any additional WHERE conditions
// from the outer query, and executes the combined query.
//
// Parameters:
//   - outerStmt: The SELECT statement that references the view
//   - view: The view definition containing the stored query
//
// Returns the query results as newline-separated CSV rows.
func (e *Executor) executeViewQuery(outerStmt *SelectStmt, view ViewDefinition) (string, error) {
	// Parse the view's stored query
	lexer := NewLexer(view.QuerySQL)
	parser := NewParser(lexer)
	parsedStmt, err := parser.Parse()
	if err != nil {
		return "", fmt.Errorf("failed to parse view query: %v", err)
	}

	viewQuery, ok := parsedStmt.(*SelectStmt)
	if !ok {
		return "", errors.New("view query is not a SELECT statement")
	}

	// Apply column selection from outer query
	// If outer query selects specific columns, use those
	// If outer query selects *, use view's columns
	if len(outerStmt.Columns) > 0 && !(len(outerStmt.Columns) == 1 && outerStmt.Columns[0] == "*") {
		viewQuery.Columns = outerStmt.Columns
	}

	// Merge WHERE conditions from outer query with view's WHERE
	// Note: For simplicity, we only support simple conditions in views
	// The outer query's WHERE takes precedence if both are specified
	if outerStmt.Where != nil {
		viewQuery.Where = outerStmt.Where
	}

	// Apply ORDER BY from outer query if specified
	if outerStmt.OrderBy != nil {
		viewQuery.OrderBy = outerStmt.OrderBy
	}

	// Apply LIMIT from outer query if specified
	if outerStmt.Limit > 0 {
		viewQuery.Limit = outerStmt.Limit
	}

	// Execute the modified view query
	return e.executeSelect(viewQuery)
}