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
	"flydb/internal/banner"
	"flydb/internal/storage"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
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

	// collator provides string comparison based on database collation settings.
	collator storage.Collator

	// encoder provides encoding validation based on database encoding settings.
	encoder storage.Encoder
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
	// For GROUP_CONCAT/STRING_AGG
	concatValues []string
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
		store:    store,
		kvStore:  kvStore,
		catalog:  NewCatalog(store),
		auth:     auth,
		collator: storage.GetCollator(storage.CollationDefault, "en_US"),
		encoder:  storage.GetEncoder(storage.EncodingDefault),
	}

	// Initialize index manager
	exec.indexMgr = storage.NewIndexManager(store)

	// Initialize prepared statement manager
	exec.preparedStmts = NewPreparedStatementManager(exec)

	// Initialize trigger manager
	exec.triggerMgr = NewTriggerManager(store)

	return exec
}

// SetCollation sets the collation for string comparisons.
func (e *Executor) SetCollation(collation storage.Collation, locale string) {
	e.collator = storage.GetCollator(collation, locale)
}

// SetEncoding sets the encoding for data validation.
func (e *Executor) SetEncoding(encoding storage.CharacterEncoding) {
	e.encoder = storage.GetEncoder(encoding)
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

	case *RevokeStmt:
		// REVOKE requires admin privileges.
		// Only admins can remove permissions from users.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeRevoke(s)

	case *CreateRoleStmt:
		// CREATE ROLE requires admin privileges.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeCreateRole(s)

	case *DropRoleStmt:
		// DROP ROLE requires admin privileges.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeDropRole(s)

	case *GrantRoleStmt:
		// GRANT role requires admin privileges.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeGrantRole(s)

	case *RevokeRoleStmt:
		// REVOKE role requires admin privileges.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeRevokeRole(s)

	case *DropUserStmt:
		// DROP USER requires admin privileges.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeDropUser(s)

	case *AlterUserStmt:
		// ALTER USER requires admin privileges.
		// Only admins can modify user accounts.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeAlterUser(s)

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
		return e.executeRollback(s)

	case *SavepointStmt:
		return e.executeSavepoint(s)

	case *ReleaseSavepointStmt:
		return e.executeReleaseSavepoint(s)

	case *CreateIndexStmt:
		// CREATE INDEX requires admin privileges.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeCreateIndex(s)

	case *DropIndexStmt:
		// DROP INDEX requires admin privileges.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeDropIndex(s)

	case *PrepareStmt:
		return e.executePrepare(s)

	case *ExecuteStmt:
		return e.executeExecute(s)

	case *DeallocateStmt:
		return e.executeDeallocate(s)

	case *InspectStmt:
		// INSPECT requires admin privileges for security.
		// Only admins can view database metadata.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeInspect(s)

	case *UnionStmt:
		return e.executeUnion(s)

	case *IntersectStmt:
		return e.executeIntersect(s)

	case *ExceptStmt:
		return e.executeExcept(s)

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

	case *DropTableStmt:
		// DROP TABLE requires admin privileges.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeDropTable(s)

	case *TruncateTableStmt:
		// TRUNCATE TABLE requires admin privileges.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeTruncateTable(s)

	case *CreateDatabaseStmt:
		// CREATE DATABASE requires admin privileges.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeCreateDatabase(s)

	case *DropDatabaseStmt:
		// DROP DATABASE requires admin privileges.
		if e.currentUser != "" && e.currentUser != "admin" {
			return "", errors.New("permission denied")
		}
		return e.executeDropDatabase(s)

	case *UseDatabaseStmt:
		// USE DATABASE is handled at the server level, not executor level.
		// This case returns an error because USE should be intercepted by the server
		// before reaching the executor.
		return "", errors.New("USE statement must be handled by the server")
	}

	return "", errors.New("unknown statement")
}

// checkAccess verifies if the current user has access to the specified table.
// Admin users and anonymous users (empty username) have full access.
// Regular users need an explicit GRANT or appropriate role for the table.
//
// This method uses the RBAC system to check:
// 1. Direct user privileges (legacy)
// 2. Role-based privileges
// 3. Role inheritance
//
// Parameters:
//   - table: The table name to check access for
//
// Returns an error if access is denied, nil otherwise.
func (e *Executor) checkAccess(table string) error {
	return e.checkAccessWithPrivilege(table, auth.PrivilegeSelect)
}

// checkAccessWithPrivilege verifies if the current user has a specific privilege on a table.
// This is the main authorization check that integrates with the RBAC system.
//
// Parameters:
//   - table: The table name to check access for
//   - privilege: The specific privilege required (SELECT, INSERT, UPDATE, DELETE, etc.)
//
// Returns an error if access is denied, nil otherwise.
func (e *Executor) checkAccessWithPrivilege(table string, privilege auth.PrivilegeType) error {
	// Admin and anonymous users have full access.
	if e.currentUser == "" || e.currentUser == "admin" {
		return nil
	}

	// Use RBAC to check privilege
	// Note: Database context is managed at the server level, so we use "*" for database-agnostic checks
	check := e.auth.CheckPrivilege(e.currentUser, privilege, "*", table)
	if !check.Allowed {
		return fmt.Errorf("permission denied: %s on table %s", privilege, table)
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

// executeRevoke handles REVOKE statements.
// It delegates to the AuthManager to remove the permission.
//
// Parameters:
//   - stmt: The parsed REVOKE statement
//
// Returns "REVOKE OK" on success, or an error.
func (e *Executor) executeRevoke(stmt *RevokeStmt) (string, error) {
	err := e.auth.Revoke(stmt.Username, stmt.TableName)
	if err != nil {
		return "", err
	}
	return "REVOKE OK", nil
}

// executeCreateRole handles CREATE ROLE statements.
// It delegates to the AuthManager to create the role.
//
// Parameters:
//   - stmt: The parsed CREATE ROLE statement
//
// Returns "CREATE ROLE OK" on success, or an error.
func (e *Executor) executeCreateRole(stmt *CreateRoleStmt) (string, error) {
	err := e.auth.CreateRole(stmt.RoleName, stmt.Description, e.currentUser)
	if err != nil {
		return "", err
	}
	return "CREATE ROLE OK", nil
}

// executeDropRole handles DROP ROLE statements.
// It delegates to the AuthManager to delete the role.
//
// Parameters:
//   - stmt: The parsed DROP ROLE statement
//
// Returns "DROP ROLE OK" on success, or an error.
func (e *Executor) executeDropRole(stmt *DropRoleStmt) (string, error) {
	err := e.auth.DropRole(stmt.RoleName)
	if err != nil {
		if stmt.IfExists {
			return "DROP ROLE OK", nil
		}
		return "", err
	}
	return "DROP ROLE OK", nil
}

// executeGrantRole handles GRANT role TO user statements.
// It delegates to the AuthManager to assign the role.
//
// Parameters:
//   - stmt: The parsed GRANT role statement
//
// Returns "GRANT ROLE OK" on success, or an error.
func (e *Executor) executeGrantRole(stmt *GrantRoleStmt) (string, error) {
	err := e.auth.GrantRoleToUser(stmt.Username, stmt.RoleName, stmt.Database, e.currentUser)
	if err != nil {
		return "", err
	}
	return "GRANT ROLE OK", nil
}

// executeRevokeRole handles REVOKE role FROM user statements.
// It delegates to the AuthManager to remove the role assignment.
//
// Parameters:
//   - stmt: The parsed REVOKE role statement
//
// Returns "REVOKE ROLE OK" on success, or an error.
func (e *Executor) executeRevokeRole(stmt *RevokeRoleStmt) (string, error) {
	err := e.auth.RevokeRoleFromUser(stmt.Username, stmt.RoleName, stmt.Database)
	if err != nil {
		return "", err
	}
	return "REVOKE ROLE OK", nil
}

// executeDropUser handles DROP USER statements.
// It delegates to the AuthManager to delete the user.
//
// Parameters:
//   - stmt: The parsed DROP USER statement
//
// Returns "DROP USER OK" on success, or an error.
func (e *Executor) executeDropUser(stmt *DropUserStmt) (string, error) {
	err := e.auth.DropUser(stmt.Username)
	if err != nil {
		if stmt.IfExists {
			return "DROP USER OK", nil
		}
		return "", err
	}
	return "DROP USER OK", nil
}

// executeAlterUser handles ALTER USER statements.
// It delegates to the AuthManager to update the user's password.
//
// Parameters:
//   - stmt: The parsed ALTER USER statement
//
// Returns "ALTER USER OK" on success, or an error.
func (e *Executor) executeAlterUser(stmt *AlterUserStmt) (string, error) {
	err := e.auth.AlterUser(stmt.Username, stmt.NewPassword)
	if err != nil {
		return "", err
	}
	return "ALTER USER OK", nil
}

// executeCreate handles CREATE TABLE statements.
// It delegates to the Catalog to create the table schema.
//
// Parameters:
//   - stmt: The parsed CREATE TABLE statement
//
// Returns "CREATE TABLE OK" on success, or an error.
func (e *Executor) executeCreate(stmt *CreateTableStmt) (string, error) {
	// Check if table already exists
	if _, exists := e.catalog.GetTable(stmt.TableName); exists {
		if stmt.IfNotExists {
			// IF NOT EXISTS specified, silently succeed
			return "CREATE TABLE OK", nil
		}
		return "", fmt.Errorf("table exists")
	}

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
// Returns "INSERT <count>" on success, or an error.
func (e *Executor) executeInsert(stmt *InsertStmt) (string, error) {
	// Validate that the table exists.
	table, ok := e.catalog.GetTable(stmt.TableName)
	if !ok {
		return "", errors.New("table not found")
	}

	// Determine which rows to insert
	rowsToInsert := stmt.MultiValues
	if len(rowsToInsert) == 0 && len(stmt.Values) > 0 {
		// Backward compatibility: use Values if MultiValues is empty
		rowsToInsert = [][]string{stmt.Values}
	}

	if len(rowsToInsert) == 0 {
		return "", errors.New("no values to insert")
	}

	insertedCount := 0

	for _, rowValues := range rowsToInsert {
		normalizedValues, conflictRowKey, err := e.prepareInsertRow(table, stmt.Columns, rowValues)
		if err != nil {
			// Check if this is a unique constraint violation and we have ON CONFLICT
			if stmt.OnConflict != nil && conflictRowKey != "" {
				if stmt.OnConflict.DoNothing {
					// Skip this row
					continue
				} else if stmt.OnConflict.DoUpdate {
					// Update the conflicting row
					if err := e.updateConflictingRow(table, conflictRowKey, stmt.OnConflict.Updates); err != nil {
						return "", err
					}
					insertedCount++
					continue
				}
			}
			return "", err
		}

		// Insert the row
		if err := e.insertSingleRow(table, stmt.TableName, normalizedValues); err != nil {
			return "", err
		}
		insertedCount++
	}

	return fmt.Sprintf("INSERT %d", insertedCount), nil
}

// prepareInsertRow prepares a single row for insertion, handling column mapping and validation.
// Returns the normalized values, a conflicting row key (if any), and an error.
func (e *Executor) prepareInsertRow(table TableSchema, columns []string, values []string) ([]string, string, error) {
	normalizedValues := make([]string, len(table.Columns))

	// Build a map of column name to value index if columns are specified
	var columnValueMap map[string]string
	if len(columns) > 0 {
		if len(columns) != len(values) {
			return nil, "", errors.New("column count does not match value count")
		}
		columnValueMap = make(map[string]string)
		for i, col := range columns {
			columnValueMap[col] = values[i]
		}
	}

	for i, col := range table.Columns {
		var value string

		if columnValueMap != nil {
			// Column list specified - look up value by column name
			if v, ok := columnValueMap[col.Name]; ok {
				value = v
			} else if col.IsAutoIncrement() {
				// Generate auto-increment value
				nextVal, err := e.catalog.GetNextAutoIncrement(table.Name, col.Name)
				if err != nil {
					return nil, "", fmt.Errorf("failed to generate auto-increment value for %s: %v", col.Name, err)
				}
				value = fmt.Sprintf("%d", nextVal)
			} else if defaultVal, hasDefault := col.GetDefaultValue(); hasDefault {
				value = defaultVal
			} else if col.IsNotNull() {
				return nil, "", fmt.Errorf("no value provided for NOT NULL column %s", col.Name)
			} else {
				value = "NULL"
			}
		} else {
			// No column list - use positional values
			autoIncCols := table.GetAutoIncrementColumns()
			expectedCols := len(table.Columns) - len(autoIncCols)

			if len(values) != len(table.Columns) && len(values) != expectedCols {
				return nil, "", errors.New("column count mismatch")
			}

			valueIdx := 0
			for j := 0; j < i; j++ {
				if !table.Columns[j].IsAutoIncrement() || len(values) == len(table.Columns) {
					valueIdx++
				}
			}

			if col.IsAutoIncrement() {
				if len(values) == len(table.Columns) {
					value = values[i]
					if intVal, err := parseIntValue(value); err == nil {
						e.catalog.UpdateAutoIncrement(table.Name, col.Name, intVal)
					}
				} else {
					nextVal, err := e.catalog.GetNextAutoIncrement(table.Name, col.Name)
					if err != nil {
						return nil, "", fmt.Errorf("failed to generate auto-increment value for %s: %v", col.Name, err)
					}
					value = fmt.Sprintf("%d", nextVal)
				}
			} else {
				if valueIdx < len(values) {
					value = values[valueIdx]
				} else if defaultVal, hasDefault := col.GetDefaultValue(); hasDefault {
					value = defaultVal
				} else {
					return nil, "", fmt.Errorf("no value provided for column %s", col.Name)
				}
			}
		}

		// Check NOT NULL constraint
		if col.IsNotNull() && (value == "" || value == "NULL") {
			return nil, "", fmt.Errorf("column %s cannot be NULL", col.Name)
		}

		// Validate and normalize the value
		if value != "" && value != "NULL" {
			if err := ValidateValue(col.Type, value); err != nil {
				return nil, "", fmt.Errorf("column %s: %v", col.Name, err)
			}

			// Validate encoding for TEXT/VARCHAR columns
			if e.encoder != nil && isTextType(col.Type) {
				if err := e.encoder.Validate(value); err != nil {
					return nil, "", fmt.Errorf("column %s: encoding error: %v", col.Name, err)
				}
			}

			normalized, err := NormalizeValue(col.Type, value)
			if err != nil {
				return nil, "", fmt.Errorf("column %s: %v", col.Name, err)
			}
			normalizedValues[i] = normalized
		} else {
			normalizedValues[i] = value
		}
	}

	// Check UNIQUE and PRIMARY KEY constraints, returning conflict info
	conflictRowKey, err := e.checkUniqueConstraintsWithConflict(table, normalizedValues, "")
	if err != nil {
		return nil, conflictRowKey, err
	}

	// Check FOREIGN KEY constraints
	if err := e.checkForeignKeyConstraints(table, normalizedValues); err != nil {
		return nil, "", err
	}

	// Check CHECK constraints
	if err := e.checkCheckConstraints(table, normalizedValues); err != nil {
		return nil, "", err
	}

	return normalizedValues, "", nil
}

// insertSingleRow inserts a single prepared row into the table.
func (e *Executor) insertSingleRow(table TableSchema, tableName string, normalizedValues []string) error {
	// Execute BEFORE INSERT triggers
	if err := e.executeTriggers(tableName, TriggerTimingBefore, TriggerEventInsert); err != nil {
		return fmt.Errorf("BEFORE INSERT trigger failed: %v", err)
	}

	// Generate a unique row ID
	seqKey := "seq:" + table.Name
	var seq int
	seqVal, err := e.store.Get(seqKey)
	if err == nil {
		json.Unmarshal(seqVal, &seq)
	}
	seq++

	seqBytes, _ := json.Marshal(seq)
	e.store.Put(seqKey, seqBytes)

	rowKey := fmt.Sprintf("row:%s:%d", table.Name, seq)

	// Build the row
	row := make(map[string]interface{})
	for i, col := range table.Columns {
		row[col.Name] = normalizedValues[i]
	}

	// Store the row
	data, err := json.Marshal(row)
	if err != nil {
		return err
	}
	if err := e.store.Put(rowKey, data); err != nil {
		return err
	}

	// Update indexes
	if e.indexMgr != nil {
		e.indexMgr.OnInsert(table.Name, rowKey, row)
	}

	// Invoke OnInsert callback
	if e.OnInsert != nil {
		e.OnInsert(table.Name, string(data))
	}

	// Execute AFTER INSERT triggers
	if err := e.executeTriggers(tableName, TriggerTimingAfter, TriggerEventInsert); err != nil {
		return fmt.Errorf("AFTER INSERT trigger failed: %v", err)
	}

	return nil
}

// checkUniqueConstraintsWithConflict checks unique constraints and returns the conflicting row key.
func (e *Executor) checkUniqueConstraintsWithConflict(table TableSchema, values []string, excludeRowKey string) (string, error) {
	prefix := "row:" + table.Name + ":"
	rows, err := e.store.Scan(prefix)
	if err != nil {
		return "", err
	}

	for i, col := range table.Columns {
		if col.IsUnique() || col.IsPrimaryKey() {
			newValue := values[i]
			if newValue == "" || newValue == "NULL" {
				continue
			}

			for rowKey, rowData := range rows {
				if excludeRowKey != "" && rowKey == excludeRowKey {
					continue
				}

				var row map[string]interface{}
				if err := json.Unmarshal(rowData, &row); err != nil {
					continue
				}

				if existingVal, ok := row[col.Name]; ok {
					if fmt.Sprintf("%v", existingVal) == newValue {
						if col.IsPrimaryKey() {
							return rowKey, fmt.Errorf("duplicate primary key value for column %s: %s", col.Name, newValue)
						}
						return rowKey, fmt.Errorf("duplicate value for unique column %s: %s", col.Name, newValue)
					}
				}
			}
		}
	}

	return "", nil
}

// updateConflictingRow updates a row that caused a conflict during INSERT.
func (e *Executor) updateConflictingRow(table TableSchema, rowKey string, updates map[string]string) error {
	// Get the existing row
	rowData, err := e.store.Get(rowKey)
	if err != nil {
		return err
	}

	var row map[string]interface{}
	if err := json.Unmarshal(rowData, &row); err != nil {
		return err
	}

	// Apply updates
	for col, val := range updates {
		row[col] = val
	}

	// Store the updated row
	data, err := json.Marshal(row)
	if err != nil {
		return err
	}

	return e.store.Put(rowKey, data)
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

		// Validate encoding for TEXT/VARCHAR columns
		if e.encoder != nil && isTextType(colType) {
			if err := e.encoder.Validate(val); err != nil {
				return "", fmt.Errorf("column %s: encoding error: %v", col, err)
			}
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
			// Handle different join types
			matched := false

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
				leftVal, ok1 := combinedRow[stmt.Join.On.Column]
				rightVal, ok2 := combinedRow[stmt.Join.On.Value]

				// If the right side isn't a column, treat it as a literal.
				if !ok2 {
					rightVal = stmt.Join.On.Value
				}

				// If the values match, process the combined row.
				if ok1 && fmt.Sprintf("%v", leftVal) == fmt.Sprintf("%v", rightVal) {
					e.processRow(combinedRow, stmt, &result, rls)
					matched = true
				}
			}

			// For LEFT JOIN: if no match found, include left row with NULLs for right table
			if !matched && (stmt.Join.JoinType == JoinTypeLeft || stmt.Join.JoinType == JoinTypeFull) {
				// Create a row with NULLs for the join table columns
				combinedRow := make(map[string]interface{})
				for k, v := range flatRow {
					combinedRow[k] = v
				}
				// Add NULL values for join table columns
				if joinTable, ok := e.catalog.GetTable(stmt.Join.TableName); ok {
					for _, col := range joinTable.Columns {
						combinedRow[col.Name] = "NULL"
						combinedRow[stmt.Join.TableName+"."+col.Name] = "NULL"
					}
				}
				e.processRow(combinedRow, stmt, &result, rls)
			}
		} else {
			// No JOIN - process the row directly.
			e.processRow(flatRow, stmt, &result, rls)
		}
	}

	// For RIGHT JOIN and FULL JOIN: include unmatched rows from the right table
	if stmt.Join != nil && (stmt.Join.JoinType == JoinTypeRight || stmt.Join.JoinType == JoinTypeFull) {
		// Track which right rows were matched
		for _, jVal := range joinRows {
			var jRow map[string]interface{}
			json.Unmarshal(jVal, &jRow)

			// Check if this right row matched any left row
			matched := false
			for _, val := range rows {
				var row map[string]interface{}
				json.Unmarshal(val, &row)

				// Check join condition
				leftVal, ok1 := row[stmt.Join.On.Column]
				if !ok1 {
					leftVal, ok1 = row[strings.TrimPrefix(stmt.Join.On.Column, stmt.TableName+".")]
				}
				rightVal, ok2 := jRow[stmt.Join.On.Value]
				if !ok2 {
					rightVal, ok2 = jRow[strings.TrimPrefix(stmt.Join.On.Value, stmt.Join.TableName+".")]
				}

				if ok1 && ok2 && fmt.Sprintf("%v", leftVal) == fmt.Sprintf("%v", rightVal) {
					matched = true
					break
				}
			}

			// If not matched, include with NULLs for left table
			if !matched {
				combinedRow := make(map[string]interface{})
				// Add NULL values for left table columns
				if leftTable, ok := e.catalog.GetTable(stmt.TableName); ok {
					for _, col := range leftTable.Columns {
						combinedRow[col.Name] = "NULL"
						combinedRow[stmt.TableName+"."+col.Name] = "NULL"
					}
				}
				// Add right table values
				for k, v := range jRow {
					combinedRow[k] = v
					combinedRow[stmt.Join.TableName+"."+k] = v
				}
				e.processRow(combinedRow, stmt, &result, rls)
			}
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

	// Apply OFFSET if present.
	if stmt.Offset > 0 && len(result) > stmt.Offset {
		result = result[stmt.Offset:]
	} else if stmt.Offset > 0 {
		result = nil // OFFSET exceeds result count
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
	// Format: header row, data rows, then a summary line with row count.
	rowCount := len(result)

	// Build header row from column names and function expressions
	headers := make([]string, 0, len(stmt.Columns)+len(stmt.Functions))
	headers = append(headers, stmt.Columns...)
	for _, fn := range stmt.Functions {
		if fn.Alias != "" {
			headers = append(headers, fn.Alias)
		} else {
			// Build function name like "upper(name)"
			headers = append(headers, fmt.Sprintf("%s(%s)", strings.ToLower(fn.Function), strings.Join(fn.Arguments, ", ")))
		}
	}
	header := strings.Join(headers, ", ")

	if rowCount == 0 {
		return fmt.Sprintf("%s\n(0 rows)", header), nil
	}
	return fmt.Sprintf("%s\n%s\n(%d rows)", header, strings.Join(result, "\n"), rowCount), nil
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

	// Extract header from left result (use left side's column names)
	header := extractResultHeader(leftResult)

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
		tempResult := formatUnionResultWithHeader(header, combinedRows)
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

	return formatUnionResultWithHeader(header, combinedRows), nil
}

// executeIntersect executes an INTERSECT statement by returning only rows that appear in both SELECTs.
// INTERSECT removes duplicates by default, INTERSECT ALL keeps duplicates.
func (e *Executor) executeIntersect(stmt *IntersectStmt) (string, error) {
	// Execute the left SELECT
	leftResult, err := e.executeSelect(stmt.Left)
	if err != nil {
		return "", fmt.Errorf("error executing left side of INTERSECT: %v", err)
	}

	// Execute the right SELECT
	rightResult, err := e.executeSelect(stmt.Right)
	if err != nil {
		return "", fmt.Errorf("error executing right side of INTERSECT: %v", err)
	}

	// Extract header from left result
	header := extractResultHeader(leftResult)

	// Parse the results to extract rows
	leftRows := extractResultRows(leftResult)
	rightRows := extractResultRows(rightResult)

	// Build a set of right rows for efficient lookup
	rightSet := make(map[string]int)
	for _, row := range rightRows {
		rightSet[row]++
	}

	// Find intersection
	var intersectRows []string
	if stmt.All {
		// INTERSECT ALL: keep duplicates up to the minimum count in both sets
		leftCount := make(map[string]int)
		for _, row := range leftRows {
			leftCount[row]++
		}
		for row, lc := range leftCount {
			if rc, ok := rightSet[row]; ok {
				// Add the minimum of left and right counts
				count := lc
				if rc < count {
					count = rc
				}
				for i := 0; i < count; i++ {
					intersectRows = append(intersectRows, row)
				}
			}
		}
	} else {
		// INTERSECT: remove duplicates, only include rows in both sets
		seen := make(map[string]bool)
		for _, row := range leftRows {
			if !seen[row] && rightSet[row] > 0 {
				seen[row] = true
				intersectRows = append(intersectRows, row)
			}
		}
	}

	return formatUnionResultWithHeader(header, intersectRows), nil
}

// executeExcept executes an EXCEPT statement by returning rows from left that are not in right.
// EXCEPT removes duplicates by default, EXCEPT ALL keeps duplicates.
func (e *Executor) executeExcept(stmt *ExceptStmt) (string, error) {
	// Execute the left SELECT
	leftResult, err := e.executeSelect(stmt.Left)
	if err != nil {
		return "", fmt.Errorf("error executing left side of EXCEPT: %v", err)
	}

	// Execute the right SELECT
	rightResult, err := e.executeSelect(stmt.Right)
	if err != nil {
		return "", fmt.Errorf("error executing right side of EXCEPT: %v", err)
	}

	// Extract header from left result
	header := extractResultHeader(leftResult)

	// Parse the results to extract rows
	leftRows := extractResultRows(leftResult)
	rightRows := extractResultRows(rightResult)

	// Build a set of right rows for efficient lookup
	rightSet := make(map[string]int)
	for _, row := range rightRows {
		rightSet[row]++
	}

	// Find difference (left - right)
	var exceptRows []string
	if stmt.All {
		// EXCEPT ALL: for each row in left, subtract the count in right
		leftCount := make(map[string]int)
		for _, row := range leftRows {
			leftCount[row]++
		}
		for row, lc := range leftCount {
			rc := rightSet[row]
			remaining := lc - rc
			if remaining > 0 {
				for i := 0; i < remaining; i++ {
					exceptRows = append(exceptRows, row)
				}
			}
		}
	} else {
		// EXCEPT: remove duplicates, only include rows in left but not in right
		seen := make(map[string]bool)
		for _, row := range leftRows {
			if !seen[row] && rightSet[row] == 0 {
				seen[row] = true
				exceptRows = append(exceptRows, row)
			}
		}
	}

	return formatUnionResultWithHeader(header, exceptRows), nil
}

// extractResultRows extracts data rows from a SELECT result string.
// It removes the header row (first line) and the row count line at the end.
func extractResultRows(result string) []string {
	var rows []string
	lines := strings.Split(result, "\n")
	isFirstLine := true
	for _, line := range lines {
		line = strings.TrimSpace(line)
		// Skip the header row (first non-empty line)
		if isFirstLine && line != "" {
			isFirstLine = false
			continue
		}
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

// extractResultHeader extracts the header row from a SELECT result string.
func extractResultHeader(result string) string {
	lines := strings.Split(result, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			return line
		}
	}
	return ""
}

// formatUnionResult formats combined rows into a result string with row count.
// Deprecated: Use formatUnionResultWithHeader instead.
func formatUnionResult(rows []string) string {
	rowCount := len(rows)
	if rowCount == 0 {
		return "(0 rows)"
	}
	return fmt.Sprintf("%s\n(%d rows)", strings.Join(rows, "\n"), rowCount)
}

// formatUnionResultWithHeader formats combined rows into a result string with header and row count.
func formatUnionResultWithHeader(header string, rows []string) string {
	rowCount := len(rows)
	if rowCount == 0 {
		return fmt.Sprintf("%s\n(0 rows)", header)
	}
	return fmt.Sprintf("%s\n%s\n(%d rows)", header, strings.Join(rows, "\n"), rowCount)
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

	// Evaluate scalar functions and add their results
	for _, fn := range stmt.Functions {
		result := e.evaluateScalarFunction(fn, row)
		outRow = append(outRow, result)
	}

	// Add the formatted row to the result.
	*result = append(*result, strings.Join(outRow, ", "))
}

// evaluateScalarFunction evaluates a scalar function against a row.
func (e *Executor) evaluateScalarFunction(fn *FunctionExpr, row map[string]interface{}) string {
	// Resolve arguments - can be column names or literals
	args := make([]string, len(fn.Arguments))
	for i, arg := range fn.Arguments {
		// Check if it's a string literal (starts and ends with ')
		if len(arg) >= 2 && arg[0] == '\'' && arg[len(arg)-1] == '\'' {
			args[i] = arg[1 : len(arg)-1] // Remove quotes
		} else if v, ok := row[arg]; ok {
			// It's a column name
			args[i] = fmt.Sprintf("%v", v)
		} else {
			// It's a literal value (number, NULL, etc.)
			args[i] = arg
		}
	}

	switch fn.Function {
	// String functions
	case "UPPER":
		if len(args) >= 1 {
			return strings.ToUpper(args[0])
		}
	case "LOWER":
		if len(args) >= 1 {
			return strings.ToLower(args[0])
		}
	case "LENGTH", "LEN":
		if len(args) >= 1 {
			return fmt.Sprintf("%d", len(args[0]))
		}
	case "CONCAT":
		return strings.Join(args, "")
	case "SUBSTRING", "SUBSTR":
		if len(args) >= 2 {
			str := args[0]
			start, _ := strconv.Atoi(args[1])
			if start < 1 {
				start = 1
			}
			start-- // Convert to 0-based index
			if start >= len(str) {
				return ""
			}
			if len(args) >= 3 {
				length, _ := strconv.Atoi(args[2])
				end := start + length
				if end > len(str) {
					end = len(str)
				}
				return str[start:end]
			}
			return str[start:]
		}
	case "TRIM":
		if len(args) >= 1 {
			return strings.TrimSpace(args[0])
		}
	case "LTRIM":
		if len(args) >= 1 {
			return strings.TrimLeft(args[0], " \t\n\r")
		}
	case "RTRIM":
		if len(args) >= 1 {
			return strings.TrimRight(args[0], " \t\n\r")
		}
	case "REPLACE":
		if len(args) >= 3 {
			return strings.ReplaceAll(args[0], args[1], args[2])
		}
	case "LEFT":
		if len(args) >= 2 {
			str := args[0]
			n, _ := strconv.Atoi(args[1])
			if n > len(str) {
				n = len(str)
			}
			if n < 0 {
				n = 0
			}
			return str[:n]
		}
	case "RIGHT":
		if len(args) >= 2 {
			str := args[0]
			n, _ := strconv.Atoi(args[1])
			if n > len(str) {
				n = len(str)
			}
			if n < 0 {
				n = 0
			}
			return str[len(str)-n:]
		}
	case "REVERSE":
		if len(args) >= 1 {
			runes := []rune(args[0])
			for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
				runes[i], runes[j] = runes[j], runes[i]
			}
			return string(runes)
		}
	case "REPEAT":
		if len(args) >= 2 {
			n, _ := strconv.Atoi(args[1])
			return strings.Repeat(args[0], n)
		}

	// Numeric functions
	case "ABS":
		if len(args) >= 1 {
			n, err := strconv.ParseFloat(args[0], 64)
			if err == nil {
				if n < 0 {
					n = -n
				}
				return fmt.Sprintf("%g", n)
			}
		}
	case "ROUND":
		if len(args) >= 1 {
			n, err := strconv.ParseFloat(args[0], 64)
			if err == nil {
				decimals := 0
				if len(args) >= 2 {
					decimals, _ = strconv.Atoi(args[1])
				}
				multiplier := 1.0
				for i := 0; i < decimals; i++ {
					multiplier *= 10
				}
				rounded := float64(int(n*multiplier+0.5)) / multiplier
				if decimals > 0 {
					return fmt.Sprintf("%.*f", decimals, rounded)
				}
				return fmt.Sprintf("%g", rounded)
			}
		}
	case "CEIL", "CEILING":
		if len(args) >= 1 {
			n, err := strconv.ParseFloat(args[0], 64)
			if err == nil {
				return fmt.Sprintf("%g", float64(int(n)+1))
			}
		}
	case "FLOOR":
		if len(args) >= 1 {
			n, err := strconv.ParseFloat(args[0], 64)
			if err == nil {
				return fmt.Sprintf("%g", float64(int(n)))
			}
		}
	case "MOD":
		if len(args) >= 2 {
			a, err1 := strconv.Atoi(args[0])
			b, err2 := strconv.Atoi(args[1])
			if err1 == nil && err2 == nil && b != 0 {
				return fmt.Sprintf("%d", a%b)
			}
		}
	case "POWER", "POW":
		if len(args) >= 2 {
			base, err1 := strconv.ParseFloat(args[0], 64)
			exp, err2 := strconv.ParseFloat(args[1], 64)
			if err1 == nil && err2 == nil {
				result := 1.0
				for i := 0; i < int(exp); i++ {
					result *= base
				}
				return fmt.Sprintf("%g", result)
			}
		}
	case "SQRT":
		if len(args) >= 1 {
			n, err := strconv.ParseFloat(args[0], 64)
			if err == nil && n >= 0 {
				// Simple Newton's method for square root
				if n == 0 {
					return "0"
				}
				x := n
				for i := 0; i < 20; i++ {
					x = (x + n/x) / 2
				}
				return fmt.Sprintf("%g", x)
			}
		}

	// Date/Time functions
	case "NOW", "CURRENT_TIMESTAMP":
		return time.Now().Format("2006-01-02 15:04:05")
	case "CURRENT_DATE":
		return time.Now().Format("2006-01-02")
	case "CURRENT_TIME":
		return time.Now().Format("15:04:05")
	case "YEAR":
		if len(args) >= 1 {
			t, err := parseDateTime(args[0])
			if err == nil {
				return fmt.Sprintf("%d", t.Year())
			}
		}
	case "MONTH":
		if len(args) >= 1 {
			t, err := parseDateTime(args[0])
			if err == nil {
				return fmt.Sprintf("%d", t.Month())
			}
		}
	case "DAY":
		if len(args) >= 1 {
			t, err := parseDateTime(args[0])
			if err == nil {
				return fmt.Sprintf("%d", t.Day())
			}
		}
	case "HOUR":
		if len(args) >= 1 {
			t, err := parseDateTime(args[0])
			if err == nil {
				return fmt.Sprintf("%d", t.Hour())
			}
		}
	case "MINUTE":
		if len(args) >= 1 {
			t, err := parseDateTime(args[0])
			if err == nil {
				return fmt.Sprintf("%d", t.Minute())
			}
		}
	case "SECOND":
		if len(args) >= 1 {
			t, err := parseDateTime(args[0])
			if err == nil {
				return fmt.Sprintf("%d", t.Second())
			}
		}
	case "EXTRACT":
		// EXTRACT(part FROM date) - simplified: EXTRACT(part, date)
		if len(args) >= 2 {
			part := strings.ToUpper(args[0])
			t, err := parseDateTime(args[1])
			if err == nil {
				switch part {
				case "YEAR":
					return fmt.Sprintf("%d", t.Year())
				case "MONTH":
					return fmt.Sprintf("%d", t.Month())
				case "DAY":
					return fmt.Sprintf("%d", t.Day())
				case "HOUR":
					return fmt.Sprintf("%d", t.Hour())
				case "MINUTE":
					return fmt.Sprintf("%d", t.Minute())
				case "SECOND":
					return fmt.Sprintf("%d", t.Second())
				}
			}
		}
	case "DATE_ADD", "DATEADD":
		// DATE_ADD(date, interval, unit) - simplified
		if len(args) >= 3 {
			t, err := parseDateTime(args[0])
			if err == nil {
				interval, _ := strconv.Atoi(args[1])
				unit := strings.ToUpper(args[2])
				switch unit {
				case "DAY", "DAYS":
					t = t.AddDate(0, 0, interval)
				case "MONTH", "MONTHS":
					t = t.AddDate(0, interval, 0)
				case "YEAR", "YEARS":
					t = t.AddDate(interval, 0, 0)
				case "HOUR", "HOURS":
					t = t.Add(time.Duration(interval) * time.Hour)
				case "MINUTE", "MINUTES":
					t = t.Add(time.Duration(interval) * time.Minute)
				case "SECOND", "SECONDS":
					t = t.Add(time.Duration(interval) * time.Second)
				}
				return t.Format("2006-01-02 15:04:05")
			}
		}
	case "DATE_SUB":
		// DATE_SUB(date, interval, unit)
		if len(args) >= 3 {
			t, err := parseDateTime(args[0])
			if err == nil {
				interval, _ := strconv.Atoi(args[1])
				unit := strings.ToUpper(args[2])
				switch unit {
				case "DAY", "DAYS":
					t = t.AddDate(0, 0, -interval)
				case "MONTH", "MONTHS":
					t = t.AddDate(0, -interval, 0)
				case "YEAR", "YEARS":
					t = t.AddDate(-interval, 0, 0)
				case "HOUR", "HOURS":
					t = t.Add(time.Duration(-interval) * time.Hour)
				case "MINUTE", "MINUTES":
					t = t.Add(time.Duration(-interval) * time.Minute)
				case "SECOND", "SECONDS":
					t = t.Add(time.Duration(-interval) * time.Second)
				}
				return t.Format("2006-01-02 15:04:05")
			}
		}
	case "DATEDIFF":
		// DATEDIFF(date1, date2) - returns days between
		if len(args) >= 2 {
			t1, err1 := parseDateTime(args[0])
			t2, err2 := parseDateTime(args[1])
			if err1 == nil && err2 == nil {
				diff := t1.Sub(t2)
				return fmt.Sprintf("%d", int(diff.Hours()/24))
			}
		}

	// NULL handling functions
	case "COALESCE":
		for _, arg := range args {
			if arg != "NULL" && arg != "" {
				return arg
			}
		}
		return "NULL"
	case "NULLIF":
		if len(args) >= 2 {
			if args[0] == args[1] {
				return "NULL"
			}
			return args[0]
		}
	case "IFNULL", "NVL", "ISNULL":
		if len(args) >= 2 {
			if args[0] == "NULL" || args[0] == "" {
				return args[1]
			}
			return args[0]
		}

	// Type conversion functions
	case "CAST", "CONVERT":
		// CAST(value AS type) - simplified: CAST(value, type)
		if len(args) >= 2 {
			value := args[0]
			targetType := strings.ToUpper(args[1])

			switch targetType {
			case "INT", "INTEGER", "SMALLINT", "BIGINT":
				// Convert to integer
				if f, err := strconv.ParseFloat(value, 64); err == nil {
					return fmt.Sprintf("%d", int64(f))
				}
				return "0"
			case "FLOAT", "REAL", "DOUBLE", "DECIMAL", "NUMERIC":
				// Convert to float
				if f, err := strconv.ParseFloat(value, 64); err == nil {
					return fmt.Sprintf("%g", f)
				}
				return "0"
			case "TEXT", "VARCHAR", "CHAR", "STRING":
				// Already a string
				return value
			case "BOOLEAN", "BOOL":
				// Convert to boolean
				lower := strings.ToLower(value)
				if lower == "true" || lower == "1" || lower == "yes" || lower == "on" {
					return "true"
				}
				return "false"
			case "DATE":
				// Convert to date format
				if t, err := parseDateTime(value); err == nil {
					return t.Format("2006-01-02")
				}
				return value
			case "TIME":
				// Convert to time format
				if t, err := parseDateTime(value); err == nil {
					return t.Format("15:04:05")
				}
				return value
			case "TIMESTAMP", "DATETIME":
				// Convert to timestamp format
				if t, err := parseDateTime(value); err == nil {
					return t.Format("2006-01-02 15:04:05")
				}
				return value
			default:
				return value
			}
		}
	}

	return "NULL"
}

// evaluateWhereClause evaluates an extended WHERE clause against a row.
// It supports subqueries with IN, NOT IN, EXISTS, IS NULL, and IS NOT NULL operators.
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

	// Handle IS NULL and IS NOT NULL
	if where.Operator == "IS NULL" || where.Operator == "IS NOT NULL" {
		colVal, exists := row[where.Column]
		isNull := !exists || colVal == nil || fmt.Sprintf("%v", colVal) == "" || fmt.Sprintf("%v", colVal) == "NULL"
		if where.Operator == "IS NULL" {
			return isNull
		}
		return !isNull
	}

	// Get the column value from the row
	colVal, exists := row[where.Column]
	if !exists {
		return false
	}
	colStr := fmt.Sprintf("%v", colVal)

	switch where.Operator {
	case "=":
		// Use collator for string equality comparison
		return e.stringsEqual(colStr, where.Value)
	case "<>", "!=":
		return !e.stringsEqual(colStr, where.Value)
	case "<":
		return compareValuesWithCollator(colStr, where.Value, e.collator) < 0
	case ">":
		return compareValuesWithCollator(colStr, where.Value, e.collator) > 0
	case "<=":
		return compareValuesWithCollator(colStr, where.Value, e.collator) <= 0
	case ">=":
		return compareValuesWithCollator(colStr, where.Value, e.collator) >= 0
	case "LIKE", "NOT LIKE":
		matched := matchLikePattern(colStr, where.Value)
		if where.Operator == "LIKE" {
			return matched
		}
		return !matched
	case "BETWEEN":
		// BETWEEN is inclusive on both ends
		return compareValuesWithCollator(colStr, where.BetweenLow, e.collator) >= 0 &&
			compareValuesWithCollator(colStr, where.BetweenHigh, e.collator) <= 0
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

		// Check if the column value is in the list using collator
		found := false
		for _, v := range valuesToCheck {
			if e.stringsEqual(colStr, v) {
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

// matchLikePattern matches a string against a SQL LIKE pattern.
// % matches any sequence of characters (including empty)
// _ matches any single character
func matchLikePattern(str, pattern string) bool {
	// Convert SQL LIKE pattern to a simple matching algorithm
	// This is a basic implementation that handles % and _
	return matchLikeHelper(str, pattern, 0, 0)
}

func matchLikeHelper(str, pattern string, si, pi int) bool {
	for pi < len(pattern) {
		if pattern[pi] == '%' {
			// % matches zero or more characters
			// Try matching the rest of the pattern at each position
			for si <= len(str) {
				if matchLikeHelper(str, pattern, si, pi+1) {
					return true
				}
				si++
			}
			return false
		} else if pattern[pi] == '_' {
			// _ matches exactly one character
			if si >= len(str) {
				return false
			}
			si++
			pi++
		} else {
			// Regular character - must match exactly (case-insensitive)
			if si >= len(str) {
				return false
			}
			if strings.ToLower(string(str[si])) != strings.ToLower(string(pattern[pi])) {
				return false
			}
			si++
			pi++
		}
	}
	return si == len(str)
}

// compareValues compares two values, trying numeric comparison first.
// This is a standalone function that uses default string comparison.
func compareValues(a, b string) int {
	return compareValuesWithCollator(a, b, nil)
}

// compareValuesWithCollator compares two values using the provided collator for strings.
func compareValuesWithCollator(a, b string, collator storage.Collator) int {
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

	// Fall back to string comparison using collator
	if collator != nil {
		return collator.Compare(a, b)
	}

	// Default string comparison
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

// compareStrings compares two strings using the executor's collator.
func (e *Executor) compareStrings(a, b string) int {
	if e.collator != nil {
		return e.collator.Compare(a, b)
	}
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

// stringsEqual checks if two strings are equal using the executor's collator.
func (e *Executor) stringsEqual(a, b string) bool {
	if e.collator != nil {
		return e.collator.Equal(a, b)
	}
	return a == b
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

// executeRollback aborts the current transaction or rolls back to a savepoint.
// Returns an error if no transaction is active.
func (e *Executor) executeRollback(stmt *RollbackStmt) (string, error) {
	if e.tx == nil || !e.tx.IsActive() {
		return "", errors.New("no transaction in progress")
	}

	// Check if rolling back to a savepoint
	if stmt.ToSavepoint != "" {
		err := e.tx.RollbackToSavepoint(stmt.ToSavepoint)
		if err != nil {
			return "", err
		}
		return "ROLLBACK TO SAVEPOINT " + stmt.ToSavepoint, nil
	}

	// Full rollback
	err := e.tx.Rollback()
	e.tx = nil
	if err != nil {
		return "", err
	}

	return "ROLLBACK", nil
}

// executeSavepoint creates a savepoint within the current transaction.
func (e *Executor) executeSavepoint(stmt *SavepointStmt) (string, error) {
	if e.tx == nil || !e.tx.IsActive() {
		return "", errors.New("no transaction in progress")
	}

	err := e.tx.CreateSavepoint(stmt.Name)
	if err != nil {
		return "", err
	}

	return "SAVEPOINT " + stmt.Name, nil
}

// executeReleaseSavepoint releases a savepoint.
func (e *Executor) executeReleaseSavepoint(stmt *ReleaseSavepointStmt) (string, error) {
	if e.tx == nil || !e.tx.IsActive() {
		return "", errors.New("no transaction in progress")
	}

	err := e.tx.ReleaseSavepoint(stmt.Name)
	if err != nil {
		return "", err
	}

	return "RELEASE SAVEPOINT " + stmt.Name, nil
}

// executeCreateIndex creates a new index on a table column.
func (e *Executor) executeCreateIndex(stmt *CreateIndexStmt) (string, error) {
	// Verify the table exists
	_, ok := e.catalog.GetTable(stmt.TableName)
	if !ok {
		return "", errors.New("table not found: " + stmt.TableName)
	}

	// Check if index already exists
	prefix := "_sys_index:" + stmt.TableName + ":" + stmt.ColumnName
	existing, err := e.store.Scan(prefix)
	if err == nil && len(existing) > 0 {
		if stmt.IfNotExists {
			// IF NOT EXISTS specified, silently succeed
			return "CREATE INDEX OK", nil
		}
		return "", errors.New("index already exists on column: " + stmt.ColumnName)
	}

	// Create the index
	err = e.indexMgr.CreateIndex(stmt.TableName, stmt.ColumnName)
	if err != nil {
		if stmt.IfNotExists && strings.Contains(err.Error(), "already exists") {
			return "CREATE INDEX OK", nil
		}
		return "", err
	}

	return "CREATE INDEX OK", nil
}

// executeDropIndex drops an index from a table column.
func (e *Executor) executeDropIndex(stmt *DropIndexStmt) (string, error) {
	// Verify the table exists
	_, ok := e.catalog.GetTable(stmt.TableName)
	if !ok {
		if stmt.IfExists {
			return "DROP INDEX OK", nil
		}
		return "", errors.New("table not found: " + stmt.TableName)
	}

	// The index name format is "table:column", but we need to find the column
	// from the index name. For now, we'll parse the index name to extract the column.
	// Convention: index name is typically "idx_<table>_<column>" or just the column name.
	// We'll look for an index that matches the pattern.

	// First, try to find the index by scanning all indexes for this table
	if e.indexMgr == nil {
		return "", errors.New("index manager not initialized")
	}

	// Drop the index - the IndexManager.DropIndex expects table and column
	// We need to extract the column from the index name
	// For simplicity, we'll assume the index name contains the column name
	// or we can scan for indexes on this table

	// Get all indexes and find the one matching this name
	prefix := "_sys_index:" + stmt.TableName + ":"
	indexes, err := e.store.Scan(prefix)
	if err != nil {
		return "", err
	}

	// Find the index with matching name pattern
	var columnToDelete string
	for key := range indexes {
		// Key format: _sys_index:table:column
		parts := strings.Split(key, ":")
		if len(parts) >= 3 {
			column := parts[2]
			// Check if index name matches (idx_table_column or just column)
			expectedName := "idx_" + stmt.TableName + "_" + column
			if stmt.IndexName == expectedName || stmt.IndexName == column {
				columnToDelete = column
				break
			}
		}
	}

	if columnToDelete == "" {
		if stmt.IfExists {
			return "DROP INDEX OK", nil
		}
		return "", errors.New("index not found: " + stmt.IndexName)
	}

	// Drop the index
	err = e.indexMgr.DropIndex(stmt.TableName, columnToDelete)
	if err != nil {
		return "", err
	}

	return "DROP INDEX OK", nil
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
			case "GROUP_CONCAT", "STRING_AGG":
				state.concatValues = append(state.concatValues, strVal)
				state.count++
			}
		}
	}

	// Build header row from aggregate expressions
	var headers []string
	for _, agg := range stmt.Aggregates {
		if agg.Alias != "" {
			headers = append(headers, agg.Alias)
		} else if agg.Column == "*" {
			headers = append(headers, strings.ToLower(agg.Function))
		} else {
			headers = append(headers, fmt.Sprintf("%s(%s)", strings.ToLower(agg.Function), agg.Column))
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
		case "GROUP_CONCAT", "STRING_AGG":
			if len(state.concatValues) > 0 {
				sep := agg.Separator
				if sep == "" {
					sep = ","
				}
				results = append(results, strings.Join(state.concatValues, sep))
			} else {
				results = append(results, "NULL")
			}
		}
	}

	header := strings.Join(headers, ", ")
	return fmt.Sprintf("%s\n%s\n(1 row)", header, strings.Join(results, ", ")), nil
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
				case "GROUP_CONCAT", "STRING_AGG":
					state.concatValues = append(state.concatValues, strVal)
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
			case "GROUP_CONCAT", "STRING_AGG":
				if len(state.concatValues) > 0 {
					sep := agg.Separator
					if sep == "" {
						sep = ","
					}
					rowParts = append(rowParts, strings.Join(state.concatValues, sep))
				} else {
					rowParts = append(rowParts, "NULL")
				}
			}
		}

		resultRows = append(resultRows, strings.Join(rowParts, ", "))
	}

	// Build header row: GROUP BY columns + aggregate expressions
	var headers []string

	// Add GROUP BY column names
	headers = append(headers, stmt.GroupBy...)

	// Add aggregate column names
	for _, agg := range stmt.Aggregates {
		if agg.Alias != "" {
			headers = append(headers, agg.Alias)
		} else if agg.Column == "*" {
			headers = append(headers, strings.ToLower(agg.Function))
		} else {
			headers = append(headers, fmt.Sprintf("%s(%s)", strings.ToLower(agg.Function), agg.Column))
		}
	}

	header := strings.Join(headers, ", ")
	rowCount := len(resultRows)
	if rowCount == 0 {
		return fmt.Sprintf("%s\n(0 rows)", header), nil
	}
	return fmt.Sprintf("%s\n%s\n(%d rows)", header, strings.Join(resultRows, "\n"), rowCount), nil
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

// executeInspect handles INSPECT statements for database metadata inspection.
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
//   - stmt: The parsed INSPECT statement
//
// Returns formatted metadata information.
func (e *Executor) executeInspect(stmt *InspectStmt) (string, error) {
	switch stmt.Target {
	case "USERS":
		return e.inspectUsers()
	case "TABLES":
		return e.inspectTables()
	case "TABLE":
		return e.inspectTable(stmt.ObjectName)
	case "INDEXES":
		return e.inspectIndexes()
	case "SERVER":
		return e.inspectServer()
	case "STATUS":
		return e.inspectStatus()
	case "DATABASES":
		return e.inspectDatabases()
	case "DATABASE":
		return e.inspectDatabase(stmt.ObjectName)
	case "ROLES":
		return e.inspectRoles()
	case "ROLE":
		return e.inspectRole(stmt.ObjectName)
	case "USER":
		return e.inspectUser(stmt.ObjectName)
	case "USER_ROLES":
		return e.inspectUserRoles(stmt.ObjectName)
	case "USER_PRIVILEGES":
		return e.inspectUserPrivileges(stmt.ObjectName)
	case "PRIVILEGES":
		return e.inspectPrivileges()
	default:
		return "", fmt.Errorf("unknown inspect target: %s", stmt.Target)
	}
}

// inspectUsers returns information about all database users.
func (e *Executor) inspectUsers() (string, error) {
	// Scan for all user keys with the _sys_users: prefix
	users, err := e.store.Scan("_sys_users:")
	if err != nil {
		return "", err
	}

	header := "username, role, created_at, last_login"
	if len(users) == 0 {
		return fmt.Sprintf("%s\n(0 rows)", header), nil
	}

	// Parse user data to get metadata
	type userInfo struct {
		username  string
		role      string
		createdAt string
		lastLogin string
	}
	var userInfos []userInfo

	for key, data := range users {
		username := strings.TrimPrefix(key, "_sys_users:")
		role := "user"

		// Parse user JSON to get metadata
		var userData struct {
			IsAdmin   bool   `json:"is_admin"`
			CreatedAt string `json:"created_at"`
			LastLogin string `json:"last_login"`
		}
		createdAt := "-"
		lastLogin := "-"
		if err := json.Unmarshal(data, &userData); err == nil {
			if userData.IsAdmin {
				role = "admin"
			}
			if userData.CreatedAt != "" {
				// Parse and format the timestamp
				if t, err := time.Parse(time.RFC3339, userData.CreatedAt); err == nil {
					createdAt = t.Format("2006-01-02 15:04:05")
				} else {
					createdAt = userData.CreatedAt
				}
			}
			if userData.LastLogin != "" {
				if t, err := time.Parse(time.RFC3339, userData.LastLogin); err == nil {
					lastLogin = t.Format("2006-01-02 15:04:05")
				} else {
					lastLogin = userData.LastLogin
				}
			}
		} else if username == "admin" {
			role = "admin"
		}

		userInfos = append(userInfos, userInfo{
			username:  username,
			role:      role,
			createdAt: createdAt,
			lastLogin: lastLogin,
		})
	}

	// Sort by username for consistent output
	sort.Slice(userInfos, func(i, j int) bool {
		return userInfos[i].username < userInfos[j].username
	})

	var results []string
	for _, u := range userInfos {
		results = append(results, fmt.Sprintf("%s, %s, %s, %s", u.username, u.role, u.createdAt, u.lastLogin))
	}

	return fmt.Sprintf("%s\n%s\n(%d rows)", header, strings.Join(results, "\n"), len(results)), nil
}

// inspectServer returns information about the FlyDB server/daemon.
// Returns plain key-value pairs that the CLI can format.
func (e *Executor) inspectServer() (string, error) {
	var results []string

	results = append(results, "Server: FlyDB Daemon")
	results = append(results, fmt.Sprintf("Version: %s", banner.Version))
	results = append(results, "Status: Running")
	results = append(results, "Storage: WAL-backed")
	results = append(results, "Protocol: Binary + Text")
	results = append(results, fmt.Sprintf("Tables: %d", len(e.catalog.Tables)))

	// Count indexes
	indexCount := 0
	if e.indexMgr != nil {
		indexCount = len(e.indexMgr.ListIndexes())
	}
	results = append(results, fmt.Sprintf("Indexes: %d", indexCount))

	// Count users
	users, err := e.store.Scan("_sys_users:")
	userCount := 0
	if err == nil {
		userCount = len(users)
	}
	results = append(results, fmt.Sprintf("Users: %d", userCount))

	return strings.Join(results, "\n"), nil
}

// inspectTables returns information about all tables and their schemas.
func (e *Executor) inspectTables() (string, error) {
	header := "table_name, type, columns, rows, indexes, size, created_at, modified_at"
	if len(e.catalog.Tables) == 0 {
		return fmt.Sprintf("%s\n(0 rows)", header), nil
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
		colCount := len(table.Columns)

		// Count rows and calculate size
		rowPrefix := "row:" + tableName + ":"
		rows, _ := e.store.Scan(rowPrefix)
		rowCount := len(rows)

		// Calculate approximate data size
		var dataSize int64
		for _, data := range rows {
			dataSize += int64(len(data))
		}

		// Count indexes
		indexCount := 0
		if e.indexMgr != nil {
			indexes := e.indexMgr.GetIndexedColumns(tableName)
			indexCount = len(indexes)
		}

		// Format size
		sizeStr := formatBytes(dataSize)

		// Format timestamps
		createdAt := "-"
		modifiedAt := "-"
		if !table.CreatedAt.IsZero() {
			createdAt = table.CreatedAt.Format("2006-01-02 15:04:05")
		}
		if !table.ModifiedAt.IsZero() {
			modifiedAt = table.ModifiedAt.Format("2006-01-02 15:04:05")
		}

		result := fmt.Sprintf("%s, BASE TABLE, %d, %d, %d, %s, %s, %s", tableName, colCount, rowCount, indexCount, sizeStr, createdAt, modifiedAt)
		results = append(results, result)
	}

	return fmt.Sprintf("%s\n%s\n(%d rows)", header, strings.Join(results, "\n"), len(results)), nil
}

// formatBytes formats a byte count into a human-readable string.
func formatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// inspectIndexes returns information about all indexes.
func (e *Executor) inspectIndexes() (string, error) {
	header := "index_name, table_name, column_name, type"
	if e.indexMgr == nil {
		return fmt.Sprintf("%s\n(0 rows)", header), nil
	}

	indexes := e.indexMgr.ListIndexes()
	if len(indexes) == 0 {
		return fmt.Sprintf("%s\n(0 rows)", header), nil
	}

	var results []string
	for _, idx := range indexes {
		// Index format is "table:column"
		parts := strings.SplitN(idx, ":", 2)
		if len(parts) == 2 {
			tableName := parts[0]
			columnName := parts[1]
			indexName := fmt.Sprintf("idx_%s_%s", tableName, columnName)
			results = append(results, fmt.Sprintf("%s, %s, %s, btree", indexName, tableName, columnName))
		}
	}

	// Sort for consistent output
	sort.Strings(results)

	return fmt.Sprintf("%s\n%s\n(%d rows)", header, strings.Join(results, "\n"), len(results)), nil
}

// inspectRoles returns information about all roles in the system.
func (e *Executor) inspectRoles() (string, error) {
	roles, err := e.auth.ListRoles()
	if err != nil {
		return "", err
	}

	header := "role_name, description, is_built_in, created_at, created_by"
	if len(roles) == 0 {
		return fmt.Sprintf("%s\n(0 rows)", header), nil
	}

	var results []string
	for _, role := range roles {
		createdAt := "-"
		if role.CreatedAt != "" {
			if t, err := time.Parse(time.RFC3339, role.CreatedAt); err == nil {
				createdAt = t.Format("2006-01-02 15:04:05")
			} else {
				createdAt = role.CreatedAt
			}
		}
		builtIn := "no"
		if role.IsBuiltIn {
			builtIn = "yes"
		}
		results = append(results, fmt.Sprintf("%s, %s, %s, %s, %s",
			role.Name, role.Description, builtIn, createdAt, role.CreatedBy))
	}

	sort.Strings(results)
	return fmt.Sprintf("%s\n%s\n(%d rows)", header, strings.Join(results, "\n"), len(results)), nil
}

// inspectRole returns detailed information about a specific role.
func (e *Executor) inspectRole(roleName string) (string, error) {
	role, err := e.auth.GetRole(roleName)
	if err != nil {
		return "", err
	}

	// Get role privileges
	privs, err := e.auth.GetRolePrivileges(roleName)
	if err != nil {
		return "", err
	}

	// Get users with this role
	users, err := e.auth.GetUsersWithRole(roleName)
	if err != nil {
		return "", err
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Role: %s\n", role.Name))
	sb.WriteString(fmt.Sprintf("Description: %s\n", role.Description))
	sb.WriteString(fmt.Sprintf("Built-in: %v\n", role.IsBuiltIn))
	sb.WriteString(fmt.Sprintf("Created: %s by %s\n", role.CreatedAt, role.CreatedBy))
	sb.WriteString("\nPrivileges:\n")

	if len(privs) == 0 {
		sb.WriteString("  (none)\n")
	} else {
		for _, p := range privs {
			privList := make([]string, len(p.Privileges))
			for i, priv := range p.Privileges {
				privList[i] = string(priv)
			}
			sb.WriteString(fmt.Sprintf("  %s on %s.%s.%s\n",
				strings.Join(privList, ", "), p.Database, p.TableName, p.ColumnName))
		}
	}

	sb.WriteString("\nAssigned to users:\n")
	if len(users) == 0 {
		sb.WriteString("  (none)\n")
	} else {
		for _, u := range users {
			scope := "global"
			if u.Database != "" && u.Database != "*" {
				scope = "database: " + u.Database
			}
			sb.WriteString(fmt.Sprintf("  %s (%s)\n", u.Username, scope))
		}
	}

	return sb.String(), nil
}

// inspectUser returns detailed information about a specific user.
func (e *Executor) inspectUser(username string) (string, error) {
	// Get user data
	userData, err := e.store.Get("_sys_users:" + username)
	if err != nil {
		return "", fmt.Errorf("user not found: %s", username)
	}

	var user struct {
		Username  string   `json:"username"`
		IsAdmin   bool     `json:"is_admin"`
		DefaultDB string   `json:"default_db"`
		Databases []string `json:"databases"`
		Roles     []string `json:"roles"`
		CreatedAt string   `json:"created_at"`
		LastLogin string   `json:"last_login"`
		Status    string   `json:"status"`
	}
	if err := json.Unmarshal(userData, &user); err != nil {
		return "", err
	}

	// Get user roles from RBAC
	userRoles, _ := e.auth.GetUserRoles(username)

	// Check if user has admin role (grants access to all databases)
	hasAdminRole := false
	for _, r := range userRoles {
		if r.RoleName == "admin" {
			hasAdminRole = true
			break
		}
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Username: %s\n", username))
	sb.WriteString(fmt.Sprintf("Admin: %v\n", user.IsAdmin))
	sb.WriteString(fmt.Sprintf("Status: %s\n", user.Status))
	sb.WriteString(fmt.Sprintf("Default Database: %s\n", user.DefaultDB))
	sb.WriteString(fmt.Sprintf("Created: %s\n", user.CreatedAt))
	sb.WriteString(fmt.Sprintf("Last Login: %s\n", user.LastLogin))

	sb.WriteString("\nAssigned Roles:\n")
	if len(userRoles) == 0 {
		sb.WriteString("  (none)\n")
	} else {
		for _, r := range userRoles {
			scope := "global"
			if r.Database != "" && r.Database != "*" {
				scope = "database: " + r.Database
			}
			sb.WriteString(fmt.Sprintf("  %s (%s)\n", r.RoleName, scope))
		}
	}

	sb.WriteString("\nEffective Database Access:\n")
	if hasAdminRole || user.IsAdmin {
		// Admin users have access to ALL databases through the admin role
		sb.WriteString("  * (all databases via admin role)\n")
	} else if len(user.Databases) == 0 {
		// Check for database-scoped roles
		dbAccess := make(map[string][]string) // database -> roles
		for _, r := range userRoles {
			if r.Database != "" && r.Database != "*" {
				dbAccess[r.Database] = append(dbAccess[r.Database], r.RoleName)
			}
		}
		if len(dbAccess) == 0 {
			sb.WriteString("  (none)\n")
		} else {
			for db, roles := range dbAccess {
				sb.WriteString(fmt.Sprintf("  %s (via %s)\n", db, strings.Join(roles, ", ")))
			}
		}
	} else {
		for _, db := range user.Databases {
			sb.WriteString(fmt.Sprintf("  %s (direct grant)\n", db))
		}
	}

	return sb.String(), nil
}

// inspectUserRoles returns the roles assigned to a specific user.
func (e *Executor) inspectUserRoles(username string) (string, error) {
	// Verify user exists
	if _, err := e.store.Get("_sys_users:" + username); err != nil {
		return "", fmt.Errorf("user not found: %s", username)
	}

	userRoles, err := e.auth.GetUserRoles(username)
	if err != nil {
		return "", err
	}

	header := "role_name, database, granted_at, granted_by"
	if len(userRoles) == 0 {
		return fmt.Sprintf("%s\n(0 rows)", header), nil
	}

	var results []string
	for _, r := range userRoles {
		db := r.Database
		if db == "" || db == "*" {
			db = "(global)"
		}
		grantedAt := "-"
		if r.GrantedAt != "" {
			if t, err := time.Parse(time.RFC3339, r.GrantedAt); err == nil {
				grantedAt = t.Format("2006-01-02 15:04:05")
			} else {
				grantedAt = r.GrantedAt
			}
		}
		results = append(results, fmt.Sprintf("%s, %s, %s, %s",
			r.RoleName, db, grantedAt, r.GrantedBy))
	}

	return fmt.Sprintf("%s\n%s\n(%d rows)", header, strings.Join(results, "\n"), len(results)), nil
}

// inspectUserPrivileges returns all effective privileges for a user.
func (e *Executor) inspectUserPrivileges(username string) (string, error) {
	// Verify user exists
	if _, err := e.store.Get("_sys_users:" + username); err != nil {
		return "", fmt.Errorf("user not found: %s", username)
	}

	// Get user roles
	userRoles, err := e.auth.GetUserRoles(username)
	if err != nil {
		return "", err
	}

	header := "privilege, object_type, database, table, source"
	var results []string

	// Collect privileges from all roles
	for _, ur := range userRoles {
		privs, err := e.auth.GetRolePrivileges(ur.RoleName)
		if err != nil {
			continue
		}
		for _, p := range privs {
			for _, priv := range p.Privileges {
				results = append(results, fmt.Sprintf("%s, %s, %s, %s, role:%s",
					priv, p.ObjectType, p.Database, p.TableName, ur.RoleName))
			}
		}
	}

	// Get direct permissions (legacy)
	directPrivs, _ := e.store.Scan("_sys_privs:" + username + ":")
	for key := range directPrivs {
		parts := strings.Split(key, ":")
		if len(parts) >= 3 {
			tableName := parts[2]
			results = append(results, fmt.Sprintf("SELECT, TABLE, *, %s, direct", tableName))
		}
	}

	if len(results) == 0 {
		return fmt.Sprintf("%s\n(0 rows)", header), nil
	}

	sort.Strings(results)
	return fmt.Sprintf("%s\n%s\n(%d rows)", header, strings.Join(results, "\n"), len(results)), nil
}

// inspectPrivileges returns all privileges in the system.
func (e *Executor) inspectPrivileges() (string, error) {
	// Scan all role privileges
	privData, err := e.store.Scan("_sys_role_privs:")
	if err != nil {
		return "", err
	}

	header := "role, privilege, object_type, database, table"
	if len(privData) == 0 {
		return fmt.Sprintf("%s\n(0 rows)", header), nil
	}

	var results []string
	for _, data := range privData {
		var priv auth.RolePrivilege
		if err := json.Unmarshal(data, &priv); err != nil {
			continue
		}
		for _, p := range priv.Privileges {
			results = append(results, fmt.Sprintf("%s, %s, %s, %s, %s",
				priv.RoleName, p, priv.ObjectType, priv.Database, priv.TableName))
		}
	}

	sort.Strings(results)
	return fmt.Sprintf("%s\n%s\n(%d rows)", header, strings.Join(results, "\n"), len(results)), nil
}

// inspectTable returns detailed information about a specific table.
func (e *Executor) inspectTable(tableName string) (string, error) {
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

	// Timestamps
	if !table.CreatedAt.IsZero() {
		results = append(results, fmt.Sprintf("Created: %s", table.CreatedAt.Format("2006-01-02 15:04:05")))
	}
	if !table.ModifiedAt.IsZero() {
		results = append(results, fmt.Sprintf("Last modified: %s", table.ModifiedAt.Format("2006-01-02 15:04:05")))
	}
	if table.Owner != "" {
		results = append(results, fmt.Sprintf("Owner: %s", table.Owner))
	}

	return strings.Join(results, "\n"), nil
}

// inspectStatus returns comprehensive database status and statistics.
// Returns plain key-value pairs that the CLI can format.
func (e *Executor) inspectStatus() (string, error) {
	var results []string

	// Table count
	tableCount := len(e.catalog.Tables)
	results = append(results, fmt.Sprintf("Tables: %d", tableCount))

	// List table names
	if tableCount > 0 {
		var tableNames []string
		for name := range e.catalog.Tables {
			tableNames = append(tableNames, name)
		}
		sort.Strings(tableNames)
		results = append(results, fmt.Sprintf("Table list: %s", strings.Join(tableNames, ", ")))
	}

	// User count
	users, err := e.store.Scan("_sys_users:")
	userCount := 0
	if err == nil {
		userCount = len(users)
	}
	results = append(results, fmt.Sprintf("Users: %d", userCount))

	// Index count
	indexCount := 0
	if e.indexMgr != nil {
		indexes := e.indexMgr.ListIndexes()
		indexCount = len(indexes)
	}
	results = append(results, fmt.Sprintf("Indexes: %d", indexCount))

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
	results = append(results, fmt.Sprintf("Total rows: %d", totalRows))
	results = append(results, fmt.Sprintf("Data size: %d bytes", totalStorageSize))

	// WAL size if available
	if kvStore, ok := e.store.(*storage.KVStore); ok {
		if wal := kvStore.WAL(); wal != nil {
			if walSize, err := wal.Size(); err == nil {
				results = append(results, fmt.Sprintf("WAL size: %d bytes", walSize))
			}
		}
	}

	results = append(results, "Storage: WAL-backed")
	results = append(results, "Status: Active")

	return strings.Join(results, "\n"), nil
}

// executeCreateProcedure creates a new stored procedure.
func (e *Executor) executeCreateProcedure(stmt *CreateProcedureStmt) (string, error) {
	// Check if procedure already exists
	if _, exists := e.catalog.GetProcedure(stmt.Name); exists {
		if stmt.OrReplace {
			// Drop the existing procedure first
			if err := e.catalog.DropProcedure(stmt.Name); err != nil {
				return "", err
			}
		} else if stmt.IfNotExists {
			// IF NOT EXISTS specified, silently succeed
			return "CREATE PROCEDURE OK", nil
		} else {
			return "", fmt.Errorf("procedure already exists: %s", stmt.Name)
		}
	}

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
	// Check if procedure exists
	if _, exists := e.catalog.GetProcedure(stmt.Name); !exists {
		if stmt.IfExists {
			return "DROP PROCEDURE OK", nil
		}
		return "", fmt.Errorf("procedure not found: %s", stmt.Name)
	}

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
	// Check if view already exists
	if _, exists := e.catalog.GetView(stmt.ViewName); exists {
		if stmt.OrReplace {
			// Drop the existing view first
			if err := e.catalog.DropView(stmt.ViewName); err != nil {
				return "", err
			}
		} else if stmt.IfNotExists {
			// IF NOT EXISTS specified, silently succeed
			return "CREATE VIEW OK", nil
		} else {
			return "", fmt.Errorf("view already exists: %s", stmt.ViewName)
		}
	}

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
	// Check if view exists
	if _, exists := e.catalog.GetView(stmt.ViewName); !exists {
		if stmt.IfExists {
			return "DROP VIEW OK", nil
		}
		return "", fmt.Errorf("view not found: %s", stmt.ViewName)
	}

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

	// Check if trigger already exists
	if e.triggerMgr.TriggerExists(stmt.TableName, stmt.TriggerName) {
		if stmt.OrReplace {
			// Drop the existing trigger first
			if err := e.triggerMgr.DropTrigger(stmt.TableName, stmt.TriggerName); err != nil {
				return "", err
			}
		} else if stmt.IfNotExists {
			// IF NOT EXISTS specified, silently succeed
			return "CREATE TRIGGER OK", nil
		} else {
			return "", fmt.Errorf("trigger already exists: %s", stmt.TriggerName)
		}
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
	// Check if trigger exists
	if !e.triggerMgr.TriggerExists(stmt.TableName, stmt.TriggerName) {
		if stmt.IfExists {
			return "DROP TRIGGER OK", nil
		}
		return "", fmt.Errorf("trigger not found: %s", stmt.TriggerName)
	}

	if err := e.triggerMgr.DropTrigger(stmt.TableName, stmt.TriggerName); err != nil {
		return "", err
	}
	return "DROP TRIGGER OK", nil
}

// executeDropTable removes a table and all its data from the database.
//
// The drop process:
//  1. Verify the table exists (unless IF EXISTS is specified)
//  2. Check for foreign key references from other tables
//  3. Delete all rows in the table
//  4. Drop all indexes for the table
//  5. Drop all triggers for the table
//  6. Remove the table schema from the catalog
//
// Parameters:
//   - stmt: The parsed DROP TABLE statement
//
// Returns "DROP TABLE OK" on success, or an error.
func (e *Executor) executeDropTable(stmt *DropTableStmt) (string, error) {
	// Check if table exists
	table, ok := e.catalog.GetTable(stmt.TableName)
	if !ok {
		if stmt.IfExists {
			return "DROP TABLE OK", nil
		}
		return "", errors.New("table not found: " + stmt.TableName)
	}

	// Check for foreign key references from other tables
	// We need to ensure no other table references this table
	for tableName, tableSchema := range e.catalog.Tables {
		if tableName == stmt.TableName {
			continue
		}
		for _, col := range tableSchema.Columns {
			for _, constraint := range col.Constraints {
				if constraint.Type == ConstraintForeignKey && constraint.ForeignKey != nil && constraint.ForeignKey.Table == stmt.TableName {
					return "", fmt.Errorf("cannot drop table %s: referenced by foreign key in table %s", stmt.TableName, tableName)
				}
			}
		}
	}

	// Delete all rows in the table
	prefix := "row:" + stmt.TableName + ":"
	rows, err := e.store.Scan(prefix)
	if err != nil {
		return "", fmt.Errorf("failed to scan table rows: %v", err)
	}

	for key, rowData := range rows {
		// Update indexes before deleting
		if e.indexMgr != nil {
			var row map[string]interface{}
			if err := json.Unmarshal(rowData, &row); err == nil {
				e.indexMgr.OnDelete(stmt.TableName, key, row)
			}
		}
		e.store.Delete(key)
	}

	// Delete the sequence counter for auto-increment columns
	for _, col := range table.Columns {
		if col.Type == "SERIAL" {
			seqKey := "seq:" + stmt.TableName + ":" + col.Name
			e.store.Delete(seqKey)
		}
	}

	// Drop all indexes for the table
	if e.indexMgr != nil {
		e.indexMgr.DropAllIndexesForTable(stmt.TableName)
	}

	// Drop all triggers for the table
	if e.triggerMgr != nil {
		e.triggerMgr.DropAllTriggersForTable(stmt.TableName)
	}

	// Remove the table schema from the catalog
	if err := e.catalog.DropTable(stmt.TableName); err != nil {
		return "", err
	}

	return "DROP TABLE OK", nil
}

// executeTruncateTable removes all rows from a table but keeps the table structure.
//
// The truncate process:
//  1. Verify the table exists
//  2. Check for foreign key references from other tables
//  3. Delete all rows in the table
//  4. Reset auto-increment sequences
//  5. Clear index entries (but keep index definitions)
//
// Parameters:
//   - stmt: The parsed TRUNCATE TABLE statement
//
// Returns "TRUNCATE TABLE OK" on success, or an error.
func (e *Executor) executeTruncateTable(stmt *TruncateTableStmt) (string, error) {
	// Check if table exists
	table, ok := e.catalog.GetTable(stmt.TableName)
	if !ok {
		return "", errors.New("table not found: " + stmt.TableName)
	}

	// Check for foreign key references from other tables
	for tableName, tableSchema := range e.catalog.Tables {
		if tableName == stmt.TableName {
			continue
		}
		for _, col := range tableSchema.Columns {
			for _, constraint := range col.Constraints {
				if constraint.Type == ConstraintForeignKey && constraint.ForeignKey != nil && constraint.ForeignKey.Table == stmt.TableName {
					// Check if the referencing table has any rows
					refPrefix := "row:" + tableName + ":"
					refRows, err := e.store.Scan(refPrefix)
					if err == nil && len(refRows) > 0 {
						return "", fmt.Errorf("cannot truncate table %s: referenced by foreign key in table %s with existing data", stmt.TableName, tableName)
					}
				}
			}
		}
	}

	// Delete all rows in the table
	prefix := "row:" + stmt.TableName + ":"
	rows, err := e.store.Scan(prefix)
	if err != nil {
		return "", fmt.Errorf("failed to scan table rows: %v", err)
	}

	for key, rowData := range rows {
		// Update indexes before deleting
		if e.indexMgr != nil {
			var row map[string]interface{}
			if err := json.Unmarshal(rowData, &row); err == nil {
				e.indexMgr.OnDelete(stmt.TableName, key, row)
			}
		}
		e.store.Delete(key)
	}

	// Reset auto-increment sequences
	for _, col := range table.Columns {
		if col.Type == "SERIAL" {
			seqKey := "seq:" + stmt.TableName + ":" + col.Name
			e.store.Delete(seqKey)
		}
	}

	return "TRUNCATE TABLE OK", nil
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

	// OFFSET clause
	if stmt.Offset > 0 {
		sql += fmt.Sprintf(" OFFSET %d", stmt.Offset)
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

	// Apply OFFSET from outer query if specified
	if outerStmt.Offset > 0 {
		viewQuery.Offset = outerStmt.Offset
	}

	// Execute the modified view query
	return e.executeSelect(viewQuery)
}

// parseDateTime parses a date/time string in various formats.
func parseDateTime(s string) (time.Time, error) {
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02",
		"15:04:05",
		"2006/01/02 15:04:05",
		"2006/01/02",
		"01/02/2006",
		"01-02-2006",
		time.RFC3339,
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse date/time: %s", s)
}


// =============================================================================
// Database Management Execution
// =============================================================================

// executeCreateDatabase creates a new database.
// Note: This is a stub that returns an error because database operations
// must be handled at the server level where the DatabaseManager is available.
func (e *Executor) executeCreateDatabase(stmt *CreateDatabaseStmt) (string, error) {
	// Database operations are handled at the server level
	return "", errors.New("CREATE DATABASE must be handled by the server")
}

// executeDropDatabase drops a database.
// Note: This is a stub that returns an error because database operations
// must be handled at the server level where the DatabaseManager is available.
func (e *Executor) executeDropDatabase(stmt *DropDatabaseStmt) (string, error) {
	// Database operations are handled at the server level
	return "", errors.New("DROP DATABASE must be handled by the server")
}

// inspectDatabases returns information about all databases.
// Note: This is a stub that returns an error because database operations
// must be handled at the server level where the DatabaseManager is available.
func (e *Executor) inspectDatabases() (string, error) {
	// Database operations are handled at the server level
	return "", errors.New("INSPECT DATABASES must be handled by the server")
}

// inspectDatabase returns information about a specific database.
// Note: This is a stub that returns an error because database operations
// must be handled at the server level where the DatabaseManager is available.
func (e *Executor) inspectDatabase(dbName string) (string, error) {
	// Database operations are handled at the server level
	return "", errors.New("INSPECT DATABASE must be handled by the server")
}

// isTextType returns true if the column type is a text/string type.
func isTextType(colType string) bool {
	upper := strings.ToUpper(colType)
	return strings.HasPrefix(upper, "TEXT") ||
		strings.HasPrefix(upper, "VARCHAR") ||
		strings.HasPrefix(upper, "CHAR") ||
		upper == "STRING"
}
