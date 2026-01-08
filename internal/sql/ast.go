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
Package sql provides the SQL processing pipeline for FlyDB.

Abstract Syntax Tree (AST) Overview:
====================================

The AST is the intermediate representation of SQL statements after parsing.
It represents the structure of a SQL statement as a tree of nodes, where
each node type corresponds to a specific SQL construct.

AST Design Pattern:
===================

FlyDB uses the Visitor pattern for AST processing:

  1. All statement types implement the Statement interface
  2. The statementNode() method is a marker (no implementation needed)
  3. The Executor uses type switches to handle each statement type

This design allows:
  - Type-safe handling of different statement types
  - Easy addition of new statement types
  - Clear separation between parsing and execution

Supported SQL Statements:
=========================

Data Definition Language (DDL):
  - CREATE TABLE: Define new tables with columns and types
  - CREATE USER: Create database user accounts

Data Manipulation Language (DML):
  - SELECT: Query data with optional WHERE, JOIN, ORDER BY, LIMIT
  - INSERT: Add new rows to tables
  - UPDATE: Modify existing rows
  - DELETE: Remove rows from tables

Data Control Language (DCL):
  - GRANT: Assign permissions to users with optional RLS

AST Node Hierarchy:
===================

	Statement (interface)
	├── CreateTableStmt
	├── CreateUserStmt
	├── GrantStmt
	├── InsertStmt
	├── UpdateStmt
	├── DeleteStmt
	└── SelectStmt
	    ├── JoinClause
	    ├── OrderByClause
	    └── Condition (WHERE)

Example AST:
============

For the SQL: SELECT name FROM users WHERE id = 1

	SelectStmt{
	    TableName: "users",
	    Columns:   []string{"name"},
	    Where:     &Condition{Column: "id", Value: "1"},
	}
*/
package sql

// Statement represents a SQL statement node in the Abstract Syntax Tree (AST).
// All concrete statement types must implement this interface.
//
// The statementNode() method is a marker method that serves two purposes:
//  1. It ensures only intended types can be used as statements
//  2. It enables compile-time type checking
//
// This pattern is common in Go AST implementations (see go/ast package).
type Statement interface {
	statementNode()
}

// CreateUserStmt represents a CREATE USER statement.
// It creates a new database user with the specified credentials.
//
// SQL Syntax:
//
//	CREATE USER <username> IDENTIFIED BY '<password>'
//
// Example:
//
//	CREATE USER alice IDENTIFIED BY 'secret123'
//
// The password is stored as provided (cleartext in this demo).
// Production systems should hash passwords before storage.
type CreateUserStmt struct {
	Username string // The unique username for the new user
	Password string // The user's password
}

// statementNode implements the Statement interface.
func (s CreateUserStmt) statementNode() {}

// GrantStmt represents a GRANT statement.
// It assigns permissions to a user for accessing a specific table,
// optionally with Row-Level Security (RLS) restrictions.
//
// SQL Syntax:
//
//	GRANT SELECT ON <table> [WHERE <column> = <value>] TO <user>
//
// Examples:
//
//	GRANT SELECT ON products TO alice
//	GRANT SELECT ON orders WHERE user_id = 'alice' TO alice
//
// The optional WHERE clause enables RLS, restricting the user
// to only see rows matching the condition.
type GrantStmt struct {
	TableName string     // The table to grant access to
	Username  string     // The user receiving the permission
	Where     *Condition // Optional RLS condition (nil = full access)
}

// statementNode implements the Statement interface.
func (s GrantStmt) statementNode() {}

// RevokeStmt represents a REVOKE statement.
// It removes permissions from a user for accessing a specific table.
//
// SQL Syntax:
//
//	REVOKE ON <table> FROM <user>
//
// Examples:
//
//	REVOKE ON products FROM alice
//	REVOKE ON orders FROM bob
//
// This removes all access rights for the user on the specified table,
// including any Row-Level Security conditions that were set.
type RevokeStmt struct {
	TableName string // The table to revoke access from
	Username  string // The user losing the permission
}

// statementNode implements the Statement interface.
func (s RevokeStmt) statementNode() {}

// CreateRoleStmt represents a CREATE ROLE statement.
// It creates a new database role for RBAC.
//
// SQL Syntax:
//
//	CREATE ROLE <role_name> [WITH DESCRIPTION '<description>']
//
// Examples:
//
//	CREATE ROLE analyst
//	CREATE ROLE data_scientist WITH DESCRIPTION 'Data science team role'
type CreateRoleStmt struct {
	RoleName    string // The unique role name
	Description string // Optional description
}

// statementNode implements the Statement interface.
func (s CreateRoleStmt) statementNode() {}

// DropRoleStmt represents a DROP ROLE statement.
// It removes a role and all its associated privileges.
//
// SQL Syntax:
//
//	DROP ROLE [IF EXISTS] <role_name>
//
// Examples:
//
//	DROP ROLE analyst
//	DROP ROLE IF EXISTS old_role
type DropRoleStmt struct {
	RoleName string // The role to drop
	IfExists bool   // Whether to suppress error if role doesn't exist
}

// statementNode implements the Statement interface.
func (s DropRoleStmt) statementNode() {}

// AlterRoleStmt represents an ALTER ROLE statement.
// It modifies an existing role's properties.
//
// SQL Syntax:
//
//	ALTER ROLE <role_name> SET DESCRIPTION '<description>'
//
// Examples:
//
//	ALTER ROLE analyst SET DESCRIPTION 'Updated description'
type AlterRoleStmt struct {
	RoleName       string // The role to modify
	NewDescription string // New description for the role
}

// statementNode implements the Statement interface.
func (s AlterRoleStmt) statementNode() {}

// GrantRoleStmt represents a GRANT role TO user statement.
// It assigns a role to a user.
//
// SQL Syntax:
//
//	GRANT <role_name> TO <username> [ON DATABASE <database>]
//
// Examples:
//
//	GRANT reader TO alice
//	GRANT writer TO bob ON DATABASE sales
type GrantRoleStmt struct {
	RoleName string // The role to grant
	Username string // The user receiving the role
	Database string // Optional database scope
}

// statementNode implements the Statement interface.
func (s GrantRoleStmt) statementNode() {}

// RevokeRoleStmt represents a REVOKE role FROM user statement.
// It removes a role assignment from a user.
//
// SQL Syntax:
//
//	REVOKE <role_name> FROM <username> [ON DATABASE <database>]
//
// Examples:
//
//	REVOKE reader FROM alice
//	REVOKE writer FROM bob ON DATABASE sales
type RevokeRoleStmt struct {
	RoleName string // The role to revoke
	Username string // The user losing the role
	Database string // Optional database scope
}

// statementNode implements the Statement interface.
func (s RevokeRoleStmt) statementNode() {}

// DropUserStmt represents a DROP USER statement.
// It removes a user account from the database.
//
// SQL Syntax:
//
//	DROP USER [IF EXISTS] <username>
//
// Examples:
//
//	DROP USER alice
//	DROP USER IF EXISTS old_user
type DropUserStmt struct {
	Username string // The user to drop
	IfExists bool   // Whether to suppress error if user doesn't exist
}

// statementNode implements the Statement interface.
func (s DropUserStmt) statementNode() {}

// AlterUserStmt represents an ALTER USER statement.
// It modifies an existing user's properties, such as their password.
//
// SQL Syntax:
//
//	ALTER USER <username> IDENTIFIED BY '<new_password>'
//
// Examples:
//
//	ALTER USER alice IDENTIFIED BY 'new_secret123'
//	ALTER USER admin IDENTIFIED BY 'new-admin-password'
//
// Currently supports changing the user's password.
type AlterUserStmt struct {
	Username    string // The username to modify
	NewPassword string // The new password for the user
}

// statementNode implements the Statement interface.
func (s AlterUserStmt) statementNode() {}

// CreateTableStmt represents a CREATE TABLE statement.
// It defines a new table with the specified columns, types, and constraints.
//
// SQL Syntax:
//
//	CREATE TABLE [IF NOT EXISTS] <name> (
//	    <col1> <type1> [constraints],
//	    <col2> <type2> [constraints],
//	    [table_constraints]
//	)
//
// Examples:
//
//	CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)
//	CREATE TABLE IF NOT EXISTS orders (id SERIAL PRIMARY KEY, user_id INT REFERENCES users(id), amount DECIMAL)
//
// Supported column types: INT, TEXT, SERIAL, and others defined in types.go
// Supported constraints: PRIMARY KEY, FOREIGN KEY/REFERENCES, NOT NULL, UNIQUE, AUTO_INCREMENT, DEFAULT
type CreateTableStmt struct {
	TableName   string            // The name of the new table
	IfNotExists bool              // If true, don't error if table already exists
	Columns     []ColumnDef       // Column definitions (name, type, and constraints)
	Constraints []TableConstraint // Table-level constraints (composite keys, etc.)
}

// statementNode implements the Statement interface.
func (s CreateTableStmt) statementNode() {}

// ConstraintType represents the type of column constraint.
type ConstraintType string

// Constraint type constants.
const (
	ConstraintPrimaryKey    ConstraintType = "PRIMARY KEY"
	ConstraintForeignKey    ConstraintType = "FOREIGN KEY"
	ConstraintNotNull       ConstraintType = "NOT NULL"
	ConstraintUnique        ConstraintType = "UNIQUE"
	ConstraintAutoIncrement ConstraintType = "AUTO_INCREMENT"
	ConstraintDefault       ConstraintType = "DEFAULT"
	ConstraintCheck         ConstraintType = "CHECK"
)

// ForeignKeyRef defines a foreign key reference to another table.
type ForeignKeyRef struct {
	Table  string // Referenced table name
	Column string // Referenced column name
}

// ColumnConstraint defines a constraint on a column.
type ColumnConstraint struct {
	Type         ConstraintType // The type of constraint
	ForeignKey   *ForeignKeyRef // For FOREIGN KEY: the referenced table and column
	DefaultValue string         // For DEFAULT: the default value
	CheckExpr    *CheckExpr     // For CHECK: the validation expression
}

// CheckExpr represents a CHECK constraint expression.
// It defines a validation condition that must be true for all rows.
//
// SQL Syntax:
//
//	CHECK (<column> <operator> <value>)
//	CHECK (<column> IN (<value1>, <value2>, ...))
//	CHECK (<column> BETWEEN <min> AND <max>)
//
// Examples:
//
//	CHECK (age >= 0)
//	CHECK (status IN ('active', 'inactive', 'pending'))
//	CHECK (price > 0 AND price < 10000)
type CheckExpr struct {
	Column   string   // Column being checked
	Operator string   // Comparison operator: =, <, >, <=, >=, <>, IN, BETWEEN
	Value    string   // Value for simple comparisons
	Values   []string // Values for IN clause
	MinValue string   // Min value for BETWEEN
	MaxValue string   // Max value for BETWEEN
	And      *CheckExpr // Optional AND condition
	Or       *CheckExpr // Optional OR condition
}

// ColumnDef defines a single column in a table schema.
// It specifies the column name, data type, and optional constraints.
//
// Supported Types:
//   - INT: Integer values (stored as strings internally)
//   - TEXT: String values
//   - SERIAL: Auto-incrementing integer (implies PRIMARY KEY)
//   - And other types defined in types.go
//
// Supported Constraints:
//   - PRIMARY KEY: Unique identifier for the row
//   - FOREIGN KEY: Reference to another table's primary key
//   - NOT NULL: Column cannot contain NULL values
//   - UNIQUE: Column values must be unique
//   - AUTO_INCREMENT: Automatically increment integer values
//   - DEFAULT: Default value when not specified
//
// Note: FlyDB stores all values as strings internally.
// Type information is used for validation and display purposes.
type ColumnDef struct {
	Name        string             // Column name (case-sensitive)
	Type        string             // Column type (INT, TEXT, SERIAL, etc.)
	Constraints []ColumnConstraint // Column constraints (PRIMARY KEY, NOT NULL, etc.)
}

// HasConstraint checks if the column has a specific constraint type.
func (c ColumnDef) HasConstraint(constraintType ConstraintType) bool {
	for _, constraint := range c.Constraints {
		if constraint.Type == constraintType {
			return true
		}
	}
	return false
}

// IsPrimaryKey returns true if this column is a primary key.
func (c ColumnDef) IsPrimaryKey() bool {
	return c.HasConstraint(ConstraintPrimaryKey)
}

// IsNotNull returns true if this column has a NOT NULL constraint.
func (c ColumnDef) IsNotNull() bool {
	return c.HasConstraint(ConstraintNotNull) || c.IsPrimaryKey()
}

// IsUnique returns true if this column has a UNIQUE constraint.
func (c ColumnDef) IsUnique() bool {
	return c.HasConstraint(ConstraintUnique) || c.IsPrimaryKey()
}

// IsAutoIncrement returns true if this column auto-increments.
func (c ColumnDef) IsAutoIncrement() bool {
	return c.HasConstraint(ConstraintAutoIncrement) || c.Type == "SERIAL"
}

// GetForeignKey returns the foreign key reference if this column has one.
func (c ColumnDef) GetForeignKey() *ForeignKeyRef {
	for _, constraint := range c.Constraints {
		if constraint.Type == ConstraintForeignKey && constraint.ForeignKey != nil {
			return constraint.ForeignKey
		}
	}
	return nil
}

// GetDefaultValue returns the default value if this column has one.
func (c ColumnDef) GetDefaultValue() (string, bool) {
	for _, constraint := range c.Constraints {
		if constraint.Type == ConstraintDefault {
			return constraint.DefaultValue, true
		}
	}
	return "", false
}

// GetCheckConstraint returns the CHECK constraint if this column has one.
func (c ColumnDef) GetCheckConstraint() *CheckExpr {
	for _, constraint := range c.Constraints {
		if constraint.Type == ConstraintCheck && constraint.CheckExpr != nil {
			return constraint.CheckExpr
		}
	}
	return nil
}

// TableConstraint defines a table-level constraint (e.g., composite primary key).
type TableConstraint struct {
	Name       string         // Optional constraint name
	Type       ConstraintType // The type of constraint
	Columns    []string       // Columns involved in the constraint
	ForeignKey *ForeignKeyRef // For FOREIGN KEY: the referenced table and column
}

// InsertStmt represents an INSERT INTO statement.
// It adds one or more rows to a table with the specified values.
//
// SQL Syntax:
//
//	INSERT INTO <table> VALUES (<val1>, <val2>, ...)
//	INSERT INTO <table> (<col1>, <col2>) VALUES (<val1>, <val2>)
//	INSERT INTO <table> VALUES (<val1>, <val2>), (<val3>, <val4>)
//	INSERT INTO <table> (<col1>, <col2>) VALUES (<val1>, <val2>) ON CONFLICT DO NOTHING
//	INSERT INTO <table> (<col1>, <col2>) VALUES (<val1>, <val2>) ON CONFLICT DO UPDATE SET <col>=<val>
//
// Examples:
//
//	INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')
//	INSERT INTO users (id, name) VALUES (1, 'Alice')
//	INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')
//
// When Columns is empty, values must match all columns in table order.
// When Columns is specified, only those columns receive values.
type InsertStmt struct {
	TableName    string              // The target table
	Columns      []string            // Optional: specific columns to insert into
	Values       []string            // Values for the first row (backward compatibility)
	MultiValues  [][]string          // Multiple rows of values
	OnConflict   *OnConflictClause   // Optional: ON CONFLICT handling for upsert
}

// OnConflictClause represents the ON CONFLICT clause for upsert operations.
type OnConflictClause struct {
	DoNothing bool              // If true, do nothing on conflict
	DoUpdate  bool              // If true, update on conflict
	Updates   map[string]string // Column-to-value mapping for updates
}

// statementNode implements the Statement interface.
func (s InsertStmt) statementNode() {}

// UpdateStmt represents an UPDATE statement.
// It modifies existing rows in a table that match the WHERE condition.
//
// SQL Syntax:
//
//	UPDATE <table> SET <col1>=<val1>, <col2>=<val2> [WHERE <col>=<val>]
//
// Example:
//
//	UPDATE products SET price=1200 WHERE id=1
//
// If no WHERE clause is provided, all rows are updated.
type UpdateStmt struct {
	TableName string            // The target table
	Updates   map[string]string // Column-to-value mapping for updates
	Where     *Condition        // Optional filter condition
}

// statementNode implements the Statement interface.
func (s UpdateStmt) statementNode() {}

// DeleteStmt represents a DELETE statement.
// It removes rows from a table that match the WHERE condition.
//
// SQL Syntax:
//
//	DELETE FROM <table> [WHERE <col>=<val>]
//
// Example:
//
//	DELETE FROM users WHERE id=5
//
// If no WHERE clause is provided, all rows are deleted.
type DeleteStmt struct {
	TableName string     // The target table
	Where     *Condition // Optional filter condition
}

// statementNode implements the Statement interface.
func (s DeleteStmt) statementNode() {}

// OrderByClause represents an ORDER BY clause in a SELECT statement.
// It specifies how to sort the result set.
//
// SQL Syntax:
//
//	ORDER BY <column> [ASC|DESC]
//
// Example:
//
//	SELECT * FROM products ORDER BY price DESC
//
// Default direction is ASC (ascending) if not specified.
type OrderByClause struct {
	Column    string // The column to sort by
	Direction string // Sort direction: "ASC" or "DESC"
}

// SelectStmt represents a SELECT statement.
// It queries data from one or more tables with optional filtering,
// joining, grouping, sorting, and limiting.
//
// SQL Syntax:
//
//	SELECT [DISTINCT] <columns> FROM <table>
//	  [JOIN <table2> ON <condition>]
//	  [WHERE <condition>]
//	  [GROUP BY <column1>, <column2>, ...]
//	  [HAVING <aggregate_condition>]
//	  [ORDER BY <column> [ASC|DESC]]
//	  [LIMIT <n>]
//
// Examples:
//
//	SELECT name, email FROM users
//	SELECT DISTINCT category FROM products
//	SELECT * FROM users WHERE id = 1
//	SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id
//	SELECT name FROM products ORDER BY price DESC LIMIT 10
//	SELECT COUNT(*), SUM(amount) FROM orders
//	SELECT category, COUNT(*) FROM products GROUP BY category
//	SELECT category, SUM(price) FROM products GROUP BY category HAVING SUM(price) > 100
type SelectStmt struct {
	TableName  string           // Primary table to query
	Columns    []string         // Columns to return (or "*" for all)
	Distinct   bool             // Whether to remove duplicate rows
	Aggregates []*AggregateExpr // Aggregate function expressions
	Functions  []*FunctionExpr  // Scalar function expressions
	Where      *Condition       // Optional simple filter condition (backward compat)
	WhereExt   *WhereClause     // Optional extended WHERE clause with subquery support
	Join       *JoinClause      // Optional JOIN clause
	GroupBy    []string         // Optional GROUP BY columns
	Having     *HavingClause    // Optional HAVING clause for filtering groups
	OrderBy    *OrderByClause   // Optional ORDER BY clause
	Limit      int              // Maximum rows to return (0 = unlimited)
	Offset     int              // Number of rows to skip (0 = none)
	Subquery   *SelectStmt      // Optional subquery for FROM clause
	FromAlias  string           // Alias for subquery or table
}

// statementNode implements the Statement interface.
func (s SelectStmt) statementNode() {}

// UnionStmt represents a UNION operation combining multiple SELECT statements.
// It combines the results of two or more SELECT queries into a single result set.
//
// SQL Syntax:
//
//	SELECT ... UNION [ALL] SELECT ...
//	SELECT ... UNION [ALL] SELECT ... UNION [ALL] SELECT ...
//
// Examples:
//
//	SELECT name FROM employees UNION SELECT name FROM contractors
//	SELECT id, name FROM table1 UNION ALL SELECT id, name FROM table2
//
// UNION removes duplicates by default. UNION ALL keeps all rows including duplicates.
type UnionStmt struct {
	Left     *SelectStmt // Left SELECT statement
	Right    *SelectStmt // Right SELECT statement
	All      bool        // If true, keep duplicates (UNION ALL)
	NextUnion *UnionStmt // For chaining multiple UNIONs
}

// statementNode implements the Statement interface.
func (u UnionStmt) statementNode() {}

// IntersectStmt represents an INTERSECT operation combining two SELECT statements.
// It returns only rows that appear in both result sets.
//
// SQL Syntax:
//
//	SELECT ... INTERSECT [ALL] SELECT ...
//
// Examples:
//
//	SELECT name FROM employees INTERSECT SELECT name FROM managers
//
// INTERSECT removes duplicates by default. INTERSECT ALL keeps duplicates.
type IntersectStmt struct {
	Left  *SelectStmt // Left SELECT statement
	Right *SelectStmt // Right SELECT statement
	All   bool        // If true, keep duplicates (INTERSECT ALL)
}

// statementNode implements the Statement interface.
func (i IntersectStmt) statementNode() {}

// ExceptStmt represents an EXCEPT operation combining two SELECT statements.
// It returns rows from the left query that are not in the right query.
//
// SQL Syntax:
//
//	SELECT ... EXCEPT [ALL] SELECT ...
//
// Examples:
//
//	SELECT name FROM employees EXCEPT SELECT name FROM managers
//
// EXCEPT removes duplicates by default. EXCEPT ALL keeps duplicates.
type ExceptStmt struct {
	Left  *SelectStmt // Left SELECT statement
	Right *SelectStmt // Right SELECT statement
	All   bool        // If true, keep duplicates (EXCEPT ALL)
}

// statementNode implements the Statement interface.
func (e ExceptStmt) statementNode() {}

// JoinType represents the type of JOIN operation.
type JoinType string

// JOIN type constants.
const (
	JoinTypeInner JoinType = "INNER"
	JoinTypeLeft  JoinType = "LEFT"
	JoinTypeRight JoinType = "RIGHT"
	JoinTypeFull  JoinType = "FULL"
)

// JoinClause represents a JOIN operation in a SELECT statement.
// It combines rows from two tables based on a matching condition.
//
// SQL Syntax:
//
//	[INNER] JOIN <table> ON <left_col> = <right_col>
//	LEFT [OUTER] JOIN <table> ON <left_col> = <right_col>
//	RIGHT [OUTER] JOIN <table> ON <left_col> = <right_col>
//	FULL [OUTER] JOIN <table> ON <left_col> = <right_col>
//
// Examples:
//
//	SELECT users.name, orders.amount
//	FROM users JOIN orders ON users.id = orders.user_id
//
//	SELECT users.name, orders.amount
//	FROM users LEFT JOIN orders ON users.id = orders.user_id
//
// FlyDB implements a Nested Loop Join algorithm, which iterates
// through all combinations of rows and filters by the ON condition.
type JoinClause struct {
	JoinType  JoinType   // Type of join: INNER, LEFT, RIGHT, FULL
	TableName string     // The table to join with
	On        *Condition // The join condition (left_col = right_col)
}

// Condition represents a simple equality condition.
// It is used in WHERE clauses, JOIN ON clauses, and RLS definitions.
//
// SQL Syntax:
//
//	<column> = <value>
//
// Examples:
//
//	WHERE id = 1
//	ON users.id = orders.user_id
//
// Note: FlyDB currently only supports equality conditions.
// Future versions may add support for other operators (<, >, LIKE, etc.).
type Condition struct {
	Column string // The column name (may include table prefix: "table.column")
	Value  string // The value to compare against (or column name for JOINs)
}

// WhereClause represents a WHERE clause that can contain subqueries.
// It extends the simple Condition to support IN, EXISTS, and other subquery operations.
//
// SQL Syntax:
//
//	WHERE <column> = <value>
//	WHERE <column> IN (SELECT ...)
//	WHERE EXISTS (SELECT ...)
//	WHERE <column> IN (<value1>, <value2>, ...)
//
// Examples:
//
//	WHERE id = 1
//	WHERE category IN (SELECT category FROM featured_products)
//	WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
type WhereClause struct {
	Column      string       // The column name for comparison
	Operator    string       // Comparison operator: =, <, >, <=, >=, IN, EXISTS, NOT IN, NOT EXISTS, LIKE, NOT LIKE, BETWEEN, IS NULL, IS NOT NULL
	Value       string       // Simple value for comparison
	Values      []string     // List of values for IN clause
	BetweenLow  string       // Low value for BETWEEN
	BetweenHigh string       // High value for BETWEEN
	Subquery    *SelectStmt  // Subquery for IN/EXISTS
	IsSubquery  bool         // True if this uses a subquery
	And         *WhereClause // Optional AND condition
	Or          *WhereClause // Optional OR condition
}

// HavingClause represents a HAVING clause for filtering grouped results.
// It applies a condition to aggregate function results after GROUP BY.
//
// SQL Syntax:
//
//	HAVING <aggregate_function>(<column>) <operator> <value>
//
// Examples:
//
//	HAVING COUNT(*) > 5
//	HAVING SUM(amount) > 1000
//	HAVING AVG(price) > 50
//
// The HAVING clause is evaluated after grouping and aggregation,
// unlike WHERE which filters rows before grouping.
type HavingClause struct {
	Aggregate *AggregateExpr // The aggregate function to evaluate
	Operator  string         // Comparison operator (>, <, =, >=, <=)
	Value     string         // The value to compare against
}

// BeginStmt represents a BEGIN statement to start a transaction.
//
// SQL Syntax:
//
//	BEGIN
//	BEGIN TRANSACTION
//
// After BEGIN, all subsequent statements are part of the transaction
// until COMMIT or ROLLBACK is executed.
type BeginStmt struct{}

// statementNode implements the Statement interface.
func (s BeginStmt) statementNode() {}

// CommitStmt represents a COMMIT statement to commit a transaction.
//
// SQL Syntax:
//
//	COMMIT
//
// COMMIT applies all changes made during the transaction to the database.
type CommitStmt struct{}

// statementNode implements the Statement interface.
func (s CommitStmt) statementNode() {}

// RollbackStmt represents a ROLLBACK statement to abort a transaction.
//
// SQL Syntax:
//
//	ROLLBACK
//	ROLLBACK TO [SAVEPOINT] <name>
//
// ROLLBACK discards all changes made during the transaction.
// ROLLBACK TO rolls back to a specific savepoint.
type RollbackStmt struct {
	ToSavepoint string // Optional: savepoint name to rollback to
}

// statementNode implements the Statement interface.
func (s RollbackStmt) statementNode() {}

// SavepointStmt represents a SAVEPOINT statement.
//
// SQL Syntax:
//
//	SAVEPOINT <name>
//
// Creates a savepoint within the current transaction.
type SavepointStmt struct {
	Name string // Savepoint name
}

// statementNode implements the Statement interface.
func (s SavepointStmt) statementNode() {}

// ReleaseSavepointStmt represents a RELEASE SAVEPOINT statement.
//
// SQL Syntax:
//
//	RELEASE [SAVEPOINT] <name>
//
// Releases (destroys) a savepoint.
type ReleaseSavepointStmt struct {
	Name string // Savepoint name
}

// statementNode implements the Statement interface.
func (s ReleaseSavepointStmt) statementNode() {}

// CreateIndexStmt represents a CREATE INDEX statement.
//
// SQL Syntax:
//
//	CREATE INDEX [IF NOT EXISTS] <name> ON <table> (<column>)
//
// Examples:
//
//	CREATE INDEX idx_users_email ON users (email)
//	CREATE INDEX IF NOT EXISTS idx_users_email ON users (email)
//
// Indexes improve query performance for WHERE clause lookups
// on the indexed column.
type CreateIndexStmt struct {
	IndexName   string // Name of the index
	TableName   string // Table to create the index on
	ColumnName  string // Column to index
	IfNotExists bool   // If true, don't error if index already exists
}

// statementNode implements the Statement interface.
func (s CreateIndexStmt) statementNode() {}

// DropIndexStmt represents a DROP INDEX statement.
//
// SQL Syntax:
//
//	DROP INDEX [IF EXISTS] <name> ON <table>
//
// Examples:
//
//	DROP INDEX idx_users_email ON users
//	DROP INDEX IF EXISTS idx_users_email ON users
//
// Removes an index from a table.
type DropIndexStmt struct {
	IndexName string // Name of the index to drop
	TableName string // Table the index is on
	IfExists  bool   // If true, don't error if index doesn't exist
}

// statementNode implements the Statement interface.
func (s DropIndexStmt) statementNode() {}

// PrepareStmt represents a PREPARE statement for prepared statements.
//
// SQL Syntax:
//
//	PREPARE <name> AS <query>
//
// Example:
//
//	PREPARE get_user AS SELECT * FROM users WHERE id = $1
//
// Parameters are specified using $1, $2, etc. placeholders.
// The prepared statement can be executed multiple times with
// different parameter values using EXECUTE.
type PrepareStmt struct {
	Name  string // Name of the prepared statement
	Query string // The SQL query with parameter placeholders
}

// statementNode implements the Statement interface.
func (s PrepareStmt) statementNode() {}

// ExecuteStmt represents an EXECUTE statement for prepared statements.
//
// SQL Syntax:
//
//	EXECUTE <name> [USING <param1>, <param2>, ...]
//
// Example:
//
//	EXECUTE get_user USING 42
//
// The parameters are substituted for $1, $2, etc. in the prepared query.
type ExecuteStmt struct {
	Name   string   // Name of the prepared statement to execute
	Params []string // Parameter values to substitute
}

// statementNode implements the Statement interface.
func (s ExecuteStmt) statementNode() {}

// DeallocateStmt represents a DEALLOCATE statement for prepared statements.
//
// SQL Syntax:
//
//	DEALLOCATE <name>
//
// Example:
//
//	DEALLOCATE get_user
//
// This removes the prepared statement from memory.
type DeallocateStmt struct {
	Name string // Name of the prepared statement to deallocate
}

// statementNode implements the Statement interface.
func (s DeallocateStmt) statementNode() {}

// InspectStmt represents an INSPECT statement for database metadata inspection.
// It allows users to query information about database objects.
//
// SQL Syntax:
//
//	INSPECT USERS              - List all database users
//	INSPECT DATABASES          - List database information
//	INSPECT DATABASE <name>    - Detailed info for a specific database
//	INSPECT TABLES             - List all tables with their schemas
//	INSPECT TABLE <name>       - Detailed info for a specific table
//	INSPECT INDEXES            - List all indexes
//
// Examples:
//
//	INSPECT USERS
//	INSPECT TABLES
//	INSPECT TABLE employees
//	INSPECT DATABASE flydb
type InspectStmt struct {
	Target     string // The target to inspect: USERS, DATABASES, DATABASE, TABLES, TABLE, INDEXES
	ObjectName string // Optional: specific object name for TABLE or DATABASE targets
}

// statementNode implements the Statement interface.
func (s InspectStmt) statementNode() {}

// AggregateExpr represents an aggregate function call in a SELECT statement.
// Aggregate functions compute a single result from a set of input values.
//
// SQL Syntax:
//
//	<function>(<column>)
//	<function>(*)
//
// Supported Functions:
//   - COUNT: Returns the number of rows (COUNT(*) or COUNT(column))
//   - SUM: Returns the sum of numeric values
//   - AVG: Returns the average of numeric values
//   - MIN: Returns the minimum value
//   - MAX: Returns the maximum value
//
// Examples:
//
//	SELECT COUNT(*) FROM users
//	SELECT SUM(amount) FROM orders
//	SELECT AVG(price), MIN(price), MAX(price) FROM products
type AggregateExpr struct {
	Function  string // The aggregate function name (COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT, STRING_AGG)
	Column    string // The column to aggregate (or "*" for COUNT(*))
	Alias     string // Optional alias for the result column
	Separator string // Separator for GROUP_CONCAT/STRING_AGG (default: ",")
}

// FunctionExpr represents a scalar function call in a SELECT statement.
// Scalar functions operate on individual values and return a single value.
//
// SQL Syntax:
//
//	<function>(<arg1>, <arg2>, ...)
//
// Supported String Functions:
//   - UPPER(str): Convert to uppercase
//   - LOWER(str): Convert to lowercase
//   - LENGTH(str): String length
//   - CONCAT(str1, str2, ...): Concatenate strings
//   - SUBSTRING(str, start, length): Extract substring
//   - TRIM(str): Remove leading/trailing whitespace
//   - REPLACE(str, from, to): Replace occurrences
//   - LEFT(str, n): Get leftmost n characters
//   - RIGHT(str, n): Get rightmost n characters
//
// Supported Numeric Functions:
//   - ABS(n): Absolute value
//   - ROUND(n, decimals): Round to decimals
//   - CEIL(n): Round up
//   - FLOOR(n): Round down
//   - MOD(n, m): Modulo
//   - POWER(n, m): Power
//   - SQRT(n): Square root
//
// Supported Date/Time Functions:
//   - NOW(): Current timestamp
//   - CURRENT_DATE: Current date
//   - CURRENT_TIME: Current time
//
// Supported NULL Functions:
//   - COALESCE(val1, val2, ...): First non-null value
//   - NULLIF(val1, val2): NULL if equal
//   - IFNULL(val, default): Default if null
//
// Examples:
//
//	SELECT UPPER(name) FROM users
//	SELECT CONCAT(first_name, ' ', last_name) FROM users
//	SELECT LENGTH(description) FROM products
type FunctionExpr struct {
	Function  string   // The function name
	Arguments []string // Arguments (column names or literals)
	Alias     string   // Optional alias for the result column
}

// CreateProcedureStmt represents a CREATE PROCEDURE statement.
// It defines a stored procedure with parameters and a body of SQL statements.
//
// SQL Syntax:
//
//	CREATE [OR REPLACE] PROCEDURE [IF NOT EXISTS] <name>([<param1> <type1>, ...])
//	BEGIN
//	    <statements>
//	END
//
// Examples:
//
//	CREATE PROCEDURE get_user(user_id INT)
//	BEGIN
//	    SELECT * FROM users WHERE id = $1;
//	END
//
//	CREATE PROCEDURE IF NOT EXISTS update_status(id INT, status TEXT)
//	BEGIN
//	    UPDATE orders SET status = $2 WHERE id = $1;
//	END
//
//	CREATE OR REPLACE PROCEDURE update_status(id INT, status TEXT)
//	BEGIN
//	    UPDATE orders SET status = $2 WHERE id = $1;
//	END
type CreateProcedureStmt struct {
	Name        string           // Procedure name
	IfNotExists bool             // If true, don't error if procedure already exists
	OrReplace   bool             // If true, replace existing procedure
	Parameters  []ProcedureParam // Input parameters
	Body        []Statement      // SQL statements in the procedure body
	BodySQL     []string         // Raw SQL strings for the body (for storage)
}

// statementNode implements the Statement interface.
func (s CreateProcedureStmt) statementNode() {}

// ProcedureParam represents a parameter in a stored procedure.
type ProcedureParam struct {
	Name string // Parameter name
	Type string // Parameter type (INT, TEXT, etc.)
}

// CallStmt represents a CALL statement to execute a stored procedure.
//
// SQL Syntax:
//
//	CALL <procedure_name>([<arg1>, <arg2>, ...])
//
// Examples:
//
//	CALL get_user(1)
//	CALL update_status(42, 'completed')
type CallStmt struct {
	ProcedureName string   // Name of the procedure to call
	Arguments     []string // Arguments to pass to the procedure
}

// statementNode implements the Statement interface.
func (s CallStmt) statementNode() {}

// DropProcedureStmt represents a DROP PROCEDURE statement.
//
// SQL Syntax:
//
//	DROP PROCEDURE [IF EXISTS] <name>
//
// Examples:
//
//	DROP PROCEDURE get_user
//	DROP PROCEDURE IF EXISTS get_user
type DropProcedureStmt struct {
	Name     string // Procedure name to drop
	IfExists bool   // If true, don't error if procedure doesn't exist
}

// statementNode implements the Statement interface.
func (s DropProcedureStmt) statementNode() {}

// StoredProcedure represents a stored procedure in the catalog.
type StoredProcedure struct {
	Name       string           // Procedure name
	Parameters []ProcedureParam // Input parameters
	BodySQL    []string         // SQL statements as strings
}

// CreateViewStmt represents a CREATE VIEW statement.
// It creates a virtual table based on a SELECT query.
//
// SQL Syntax:
//
//	CREATE [OR REPLACE] VIEW [IF NOT EXISTS] <view_name> AS SELECT ...
//
// Examples:
//
//	CREATE VIEW active_users AS SELECT * FROM users WHERE status = 'active'
//	CREATE VIEW IF NOT EXISTS order_summary AS SELECT user_id, COUNT(*) FROM orders GROUP BY user_id
//	CREATE OR REPLACE VIEW active_users AS SELECT * FROM users WHERE status = 'active'
type CreateViewStmt struct {
	ViewName    string      // The name of the view
	IfNotExists bool        // If true, don't error if view already exists
	OrReplace   bool        // If true, replace existing view
	Query       *SelectStmt // The SELECT query that defines the view
}

// statementNode implements the Statement interface.
func (s CreateViewStmt) statementNode() {}

// DropViewStmt represents a DROP VIEW statement.
// It removes a view from the database.
//
// SQL Syntax:
//
//	DROP VIEW [IF EXISTS] <view_name>
//
// Examples:
//
//	DROP VIEW active_users
//	DROP VIEW IF EXISTS active_users
type DropViewStmt struct {
	ViewName string // The name of the view to drop
	IfExists bool   // If true, don't error if view doesn't exist
}

// statementNode implements the Statement interface.
func (s DropViewStmt) statementNode() {}

// AlterTableAction represents the type of ALTER TABLE operation.
type AlterTableAction string

// ALTER TABLE action constants.
const (
	AlterActionAddColumn    AlterTableAction = "ADD COLUMN"
	AlterActionDropColumn   AlterTableAction = "DROP COLUMN"
	AlterActionRenameColumn AlterTableAction = "RENAME COLUMN"
	AlterActionModifyColumn AlterTableAction = "MODIFY COLUMN"
	AlterActionAddConstraint    AlterTableAction = "ADD CONSTRAINT"
	AlterActionDropConstraint   AlterTableAction = "DROP CONSTRAINT"
)

// AlterTableStmt represents an ALTER TABLE statement.
// It modifies the structure of an existing table.
//
// SQL Syntax:
//
//	ALTER TABLE <table_name> ADD COLUMN <column_def>
//	ALTER TABLE <table_name> DROP COLUMN <column_name>
//	ALTER TABLE <table_name> RENAME COLUMN <old_name> TO <new_name>
//	ALTER TABLE <table_name> MODIFY COLUMN <column_name> <new_type>
//	ALTER TABLE <table_name> ADD CONSTRAINT <constraint_def>
//	ALTER TABLE <table_name> DROP CONSTRAINT <constraint_name>
//
// Examples:
//
//	ALTER TABLE users ADD COLUMN email TEXT NOT NULL
//	ALTER TABLE users DROP COLUMN email
//	ALTER TABLE users RENAME COLUMN email TO email_address
//	ALTER TABLE users MODIFY COLUMN age BIGINT
type AlterTableStmt struct {
	TableName      string           // The table to alter
	Action         AlterTableAction // The type of alteration
	ColumnDef      *ColumnDef       // For ADD COLUMN and MODIFY COLUMN
	ColumnName     string           // For DROP COLUMN, RENAME COLUMN, MODIFY COLUMN
	NewColumnName  string           // For RENAME COLUMN
	NewColumnType  string           // For MODIFY COLUMN
	Constraint     *TableConstraint // For ADD CONSTRAINT
	ConstraintName string           // For DROP CONSTRAINT
}

// statementNode implements the Statement interface.
func (s AlterTableStmt) statementNode() {}

// TriggerEvent represents the type of event that fires a trigger.
type TriggerEvent string

// Trigger event constants.
const (
	TriggerEventInsert TriggerEvent = "INSERT"
	TriggerEventUpdate TriggerEvent = "UPDATE"
	TriggerEventDelete TriggerEvent = "DELETE"
)

// TriggerTiming represents when the trigger fires relative to the event.
type TriggerTiming string

// Trigger timing constants.
const (
	TriggerTimingBefore TriggerTiming = "BEFORE"
	TriggerTimingAfter  TriggerTiming = "AFTER"
)

// CreateTriggerStmt represents a CREATE TRIGGER statement.
// It defines an automatic action that executes in response to INSERT, UPDATE, or DELETE operations.
//
// SQL Syntax:
//
//	CREATE [OR REPLACE] TRIGGER [IF NOT EXISTS] <trigger_name>
//	  BEFORE|AFTER INSERT|UPDATE|DELETE ON <table_name>
//	  FOR EACH ROW
//	  EXECUTE <action_sql>
//
// Examples:
//
//	CREATE TRIGGER log_insert AFTER INSERT ON users FOR EACH ROW EXECUTE INSERT INTO audit_log VALUES ('insert', 'users')
//	CREATE TRIGGER IF NOT EXISTS validate_update BEFORE UPDATE ON products FOR EACH ROW EXECUTE SELECT validate_product()
//	CREATE OR REPLACE TRIGGER log_insert AFTER INSERT ON users FOR EACH ROW EXECUTE INSERT INTO audit_log VALUES ('insert', 'users')
//
// Triggers are executed automatically when the specified event occurs on the table.
// BEFORE triggers execute before the operation, AFTER triggers execute after.
type CreateTriggerStmt struct {
	TriggerName string        // The name of the trigger (unique per table)
	IfNotExists bool          // If true, don't error if trigger already exists
	OrReplace   bool          // If true, replace existing trigger
	Timing      TriggerTiming // BEFORE or AFTER
	Event       TriggerEvent  // INSERT, UPDATE, or DELETE
	TableName   string        // The table the trigger is attached to
	ActionSQL   string        // The SQL statement to execute when the trigger fires
}

// statementNode implements the Statement interface.
func (s CreateTriggerStmt) statementNode() {}

// DropTriggerStmt represents a DROP TRIGGER statement.
// It removes a trigger from a table.
//
// SQL Syntax:
//
//	DROP TRIGGER [IF EXISTS] <trigger_name> ON <table_name>
//
// Examples:
//
//	DROP TRIGGER log_insert ON users
//	DROP TRIGGER IF EXISTS log_insert ON users
type DropTriggerStmt struct {
	TriggerName string // The name of the trigger to drop
	TableName   string // The table the trigger is attached to
	IfExists    bool   // If true, don't error if trigger doesn't exist
}

// statementNode implements the Statement interface.
func (s DropTriggerStmt) statementNode() {}

// Trigger represents a stored trigger definition.
type Trigger struct {
	Name      string        // Trigger name
	Timing    TriggerTiming // BEFORE or AFTER
	Event     TriggerEvent  // INSERT, UPDATE, or DELETE
	TableName string        // The table the trigger is attached to
	ActionSQL string        // The SQL statement to execute
}

// DropTableStmt represents a DROP TABLE statement.
// It removes a table and all its data from the database.
//
// SQL Syntax:
//
//	DROP TABLE [IF EXISTS] <table_name>
//
// Example:
//
//	DROP TABLE users
//	DROP TABLE IF EXISTS temp_data
type DropTableStmt struct {
	TableName string // The name of the table to drop
	IfExists  bool   // If true, don't error if table doesn't exist
}

// statementNode implements the Statement interface.
func (s DropTableStmt) statementNode() {}

// TruncateTableStmt represents a TRUNCATE TABLE statement.
// It removes all rows from a table but keeps the table structure.
//
// SQL Syntax:
//
//	TRUNCATE TABLE <table_name>
//
// Example:
//
//	TRUNCATE TABLE logs
type TruncateTableStmt struct {
	TableName string // The name of the table to truncate
}

// statementNode implements the Statement interface.
func (s TruncateTableStmt) statementNode() {}


// =============================================================================
// Database Management Statements
// =============================================================================

// CreateDatabaseStmt represents a CREATE DATABASE statement.
// It creates a new database with the specified name.
//
// SQL Syntax:
//
//	CREATE DATABASE <database_name>
//	CREATE DATABASE IF NOT EXISTS <database_name>
//	CREATE DATABASE <database_name> WITH ENCODING 'UTF8' LOCALE 'en_US' COLLATION 'default'
//
// Example:
//
//	CREATE DATABASE myapp
//	CREATE DATABASE IF NOT EXISTS test_db
//	CREATE DATABASE myapp WITH ENCODING 'UTF8' LOCALE 'de_DE'
type CreateDatabaseStmt struct {
	DatabaseName string // The name of the new database
	IfNotExists  bool   // If true, don't error if database already exists
	Owner        string // Owner of the database (optional)
	Encoding     string // Character encoding (UTF8, LATIN1, etc.)
	Locale       string // Locale for sorting (e.g., "en_US", "de_DE")
	Collation    string // Default collation for string comparison
	Description  string // Optional description
}

// statementNode implements the Statement interface.
func (s CreateDatabaseStmt) statementNode() {}

// DropDatabaseStmt represents a DROP DATABASE statement.
// It removes a database and all its contents.
//
// SQL Syntax:
//
//	DROP DATABASE <database_name>
//	DROP DATABASE IF EXISTS <database_name>
//
// Example:
//
//	DROP DATABASE myapp
//	DROP DATABASE IF EXISTS test_db
//
// Warning: This operation is destructive and cannot be undone.
type DropDatabaseStmt struct {
	DatabaseName string // The name of the database to drop
	IfExists     bool   // If true, don't error if database doesn't exist
}

// statementNode implements the Statement interface.
func (s DropDatabaseStmt) statementNode() {}

// UseDatabaseStmt represents a USE statement.
// It switches the current database context for the connection.
//
// SQL Syntax:
//
//	USE <database_name>
//
// Example:
//
//	USE myapp
//	USE production
//
// After executing USE, all subsequent SQL statements will operate
// on the specified database until another USE statement is executed.
type UseDatabaseStmt struct {
	DatabaseName string // The name of the database to switch to
}

// statementNode implements the Statement interface.
func (s UseDatabaseStmt) statementNode() {}
