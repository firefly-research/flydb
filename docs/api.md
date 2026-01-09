# FlyDB API Reference

This document provides a complete reference for FlyDB's SQL syntax, protocol commands, and configuration options.

## Table of Contents

1. [Protocol Commands](#protocol-commands)
   - [Text Protocol Commands](#text-protocol-commands)
   - [Binary Protocol](#binary-protocol)
   - [CLI Commands](#cli-commands)
2. [SQL Statements](#sql-statements)
   - [Data Definition Language (DDL)](#data-definition-language-ddl)
   - [Data Manipulation Language (DML)](#data-manipulation-language-dml)
   - [Transactions](#transactions)
   - [Stored Procedures](#stored-procedures)
   - [Triggers](#triggers)
   - [User Management](#user-management)
   - [Prepared Statements](#prepared-statements)
   - [Database Management](#database-management)
   - [Database Inspection](#database-inspection)
3. [Data Types](#data-types)
4. [Operators and Functions](#operators-and-functions)
   - [Comparison Operators](#comparison-operators)
   - [Logical Operators](#logical-operators)
   - [Aggregate Functions](#aggregate-functions)
   - [String Functions](#string-functions)
   - [Numeric Functions](#numeric-functions)
   - [Date/Time Functions](#datetime-functions)
   - [NULL Handling Functions](#null-handling-functions)
   - [Type Conversion Functions](#type-conversion-functions)
5. [Configuration](#configuration)
   - [Interactive Wizard](#interactive-wizard)
   - [Server Configuration](#server-configuration)
   - [Operative Modes](#operative-modes)
   - [Installation Script](#installation-script)
   - [Uninstallation Script](#uninstallation-script)

---

## Protocol Commands

FlyDB supports two protocols: a text protocol (port 8888) and a binary protocol (port 8889).

### Text Protocol Commands

Connect using: `telnet localhost 8888` or `nc localhost 8888`

#### PING

Test server connectivity.

```
PING
→ PONG
```

#### AUTH

Authenticate with username and password.

```
AUTH <username> <password>
→ AUTH OK
→ ERROR: invalid credentials
```

#### SQL

Execute a SQL statement.

```
SQL <statement>
→ <result>
→ ERROR: <message>
```

#### WATCH

Subscribe to table change notifications.

```
WATCH <table>
→ WATCH OK

# Then receive events:
→ EVENT INSERT users 42
→ EVENT UPDATE users 42
→ EVENT DELETE users 42
```

### Binary Protocol

The binary protocol provides a complete wire protocol for developing external JDBC/ODBC drivers.

#### Message Frame Format

```
┌───────────┬─────────┬──────────┬───────────┬────────────┬─────────────────┐
│ Magic (1B)│ Ver (1B)│ Type (1B)│ Flags (1B)│ Length (4B)│ Payload (var)   │
│   0xFD    │   0x01  │          │           │ Big-endian │                 │
└───────────┴─────────┴──────────┴───────────┴────────────┴─────────────────┘
```

- **Magic Byte**: `0xFD` identifies FlyDB protocol
- **Version**: Protocol version (currently `0x01`)
- **Type**: Message type (see table below)
- **Flags**: Reserved for compression/encryption
- **Length**: Payload length in bytes (max 16 MB)

#### Message Types

| Type | Code | Direction | Description |
|------|------|-----------|-------------|
| Query | 0x01 | Request | Execute SQL query |
| QueryResult | 0x02 | Response | Query results |
| Error | 0x03 | Response | Error response |
| Prepare | 0x04 | Request | Prepare statement |
| PrepareResult | 0x05 | Response | Prepare confirmation |
| Execute | 0x06 | Request | Execute prepared statement |
| Deallocate | 0x07 | Request | Deallocate prepared statement |
| Auth | 0x08 | Request | Authentication |
| AuthResult | 0x09 | Response | Auth result |
| Ping | 0x0A | Request | Keepalive |
| Pong | 0x0B | Response | Keepalive response |
| CursorOpen | 0x10 | Request | Open server-side cursor |
| CursorFetch | 0x11 | Request | Fetch rows from cursor |
| CursorClose | 0x12 | Request | Close cursor |
| CursorResult | 0x14 | Response | Cursor operation result |
| GetTables | 0x20 | Request | Get table metadata |
| GetColumns | 0x21 | Request | Get column metadata |
| GetTypeInfo | 0x25 | Request | Get type information |
| MetadataResult | 0x26 | Response | Metadata query result |
| BeginTx | 0x30 | Request | Begin transaction |
| CommitTx | 0x31 | Request | Commit transaction |
| RollbackTx | 0x32 | Request | Rollback transaction |
| TxResult | 0x34 | Response | Transaction result |
| SetOption | 0x40 | Request | Set session option |
| GetOption | 0x41 | Request | Get session option |
| GetServerInfo | 0x42 | Request | Get server information |
| SessionResult | 0x43 | Response | Session operation result |

#### Connection Lifecycle

1. **Connect**: Open TCP connection to binary port (default 8889)
2. **Authenticate**: Send Auth message with username/password
3. **Execute**: Send Query, Prepare, Execute, or other messages
4. **Disconnect**: Close TCP connection

See [Driver Development Guide](driver-development.md) for complete protocol specification and code examples.

### CLI Commands

The `fsql` interactive client (flydb-shell) provides local commands (prefixed with `\`) that are processed locally without server communication. These commands are inspired by PostgreSQL's `psql` client.

#### Navigation and Help

| Command | Aliases | Description |
|---------|---------|-------------|
| `\q` | `\quit` | Exit the CLI |
| `\h` | `\help` | Display help information |
| `\clear` | - | Clear the screen |
| `\s` | `\status`, `\conninfo` | Show connection status and session settings |
| `\v` | `\version` | Show CLI version |

#### Session Settings

| Command | Description |
|---------|-------------|
| `\timing` | Toggle query execution time display |
| `\x` | Toggle expanded (vertical) output mode |
| `\o [file]` | Redirect output to file (no argument resets to stdout) |
| `\sql` | Enter SQL mode (all input treated as SQL) |
| `\normal` | Return to normal mode with auto-detection |

#### Shortcuts

| Command | Description | Equivalent |
|---------|-------------|------------|
| `\dt` | List all tables | `INSPECT TABLES` |
| `\du` | List all users | `INSPECT USERS` |
| `\di` | List all indexes | `INSPECT INDEXES` |
| `\db`, `\l` | List all databases | `INSPECT DATABASES` |
| `\c <db>`, `\connect <db>` | Switch to database | `USE <database>` |

#### Shell Integration

| Command | Description |
|---------|-------------|
| `\! <command>` | Execute a shell command |

#### Examples

```
flydb> \timing
Timing is on.

flydb> SELECT * FROM users
+----+-------+-------------------+
| id | name  | email             |
+----+-------+-------------------+
| 1  | Alice | alice@example.com |
+----+-------+-------------------+
(1 row)
Time: 1.234ms

flydb> \o results.txt
Output set to file: results.txt

flydb> SELECT * FROM users
flydb> \o
Output reset to stdout.

flydb> \! ls -la
total 16
drwxr-xr-x  4 user  staff  128 Jan  8 10:00 .
...

flydb> \dt
+-------+---------------------------+
| Table | Schema                    |
+-------+---------------------------+
| users | id INT, name TEXT, ...    |
+-------+---------------------------+

flydb> \s

  Connection Status
  ──────────────────────────────

    Status: ● Connected
    Server: localhost:8889
    Protocol: Binary
    Format: table

  Session Settings
  ──────────────────────────────

    Timing: on
    Expanded: off
    Output: stdout
```

#### Keyboard Shortcuts

| Shortcut | Description |
|----------|-------------|
| `Ctrl+C` | Cancel current input / interrupt |
| `Ctrl+D` | Exit the CLI (same as `\q`) |
| `Ctrl+R` | Reverse search through command history |
| `↑` / `↓` | Navigate command history |
| `Tab` | Auto-complete commands and SQL keywords |

#### Authentication

The CLI supports secure password entry for the AUTH command:

```
flydb> AUTH
Username: admin
Password: ********
AUTH OK (admin)

flydb> AUTH admin
Password: ********
AUTH OK (admin)
```

When using `AUTH username password` on the command line, a security warning is displayed and the password is masked in command history.

#### SQL Execution Modes

The CLI provides three ways to execute SQL commands:

**1. Auto-Detection (Default)**

Common SQL keywords are automatically recognized and executed:
- `SELECT`, `INSERT`, `UPDATE`, `DELETE`
- `CREATE`, `DROP`, `ALTER`, `TRUNCATE`
- `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`
- `PREPARE`, `EXECUTE`, `DEALLOCATE`
- `GRANT`, `REVOKE`, `CALL`, `INSPECT`, `WITH`

```
flydb> SELECT * FROM users
flydb> CREATE TABLE orders (id INT PRIMARY KEY)
```

**2. Explicit SQL Prefix**

Use the `SQL` prefix to execute any SQL statement that isn't auto-detected:

```
flydb> SQL SHOW TABLES
flydb> SQL EXPLAIN SELECT * FROM users
```

**3. SQL Mode**

Enter SQL mode where all input is treated as SQL (except local `\` commands):

```
flydb> \sql
Entering SQL mode. All input will be executed as SQL.

flydb[sql]> SHOW TABLES
flydb[sql]> any_command_here_is_sql
flydb[sql]> \normal
Returning to normal mode with SQL auto-detection.

flydb>
```

---

## SQL Statements

### SQL Comments

FlyDB supports both single-line and multi-line SQL comments:

```sql
-- This is a single-line comment
SELECT * FROM users; -- Comment at end of line

/* This is a
   multi-line comment */
SELECT * FROM orders;

/* Inline comment */ SELECT name /* column */ FROM users;
```

### Data Definition Language (DDL)

#### CREATE TABLE

```sql
CREATE TABLE [IF NOT EXISTS] table_name (
    column1 TYPE [constraints],
    column2 TYPE [constraints],
    ...
)
```

**Supported Constraints:**

| Constraint | Description | Example |
|------------|-------------|---------|
| `PRIMARY KEY` | Unique identifier for rows | `id INT PRIMARY KEY` |
| `NOT NULL` | Column cannot be NULL | `name TEXT NOT NULL` |
| `UNIQUE` | Values must be unique | `email TEXT UNIQUE` |
| `DEFAULT value` | Default value if not specified | `active BOOLEAN DEFAULT true` |
| `AUTO_INCREMENT` | Auto-incrementing integer | `id INT AUTO_INCREMENT` |
| `REFERENCES table(col)` | Foreign key constraint | `user_id INT REFERENCES users(id)` |
| `CHECK (condition)` | Value must satisfy condition | `age INT CHECK (age >= 0)` |

**Supported Types:** See [Data Types](#data-types) section for complete list.

**Examples:**
```sql
-- Basic table with primary key and constraints
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    email VARCHAR(255) UNIQUE,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP
)

-- Table with foreign key reference
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id),
    total DECIMAL,
    status TEXT DEFAULT 'pending',
    CHECK (total >= 0)
)

-- Table with auto-increment
CREATE TABLE logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    message TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

#### DROP TABLE

Removes a table and all its data from the database.

```sql
DROP TABLE [IF EXISTS] table_name
```

**Examples:**
```sql
-- Drop a table
DROP TABLE users

-- Drop a table only if it exists (no error if table doesn't exist)
DROP TABLE IF EXISTS temp_data
```

**Notes:**
- Requires admin privileges
- Cannot drop a table that is referenced by foreign keys in other tables
- Automatically removes all indexes and triggers associated with the table
- Resets any auto-increment sequences for the table

#### TRUNCATE TABLE

Removes all rows from a table but keeps the table structure intact.

```sql
TRUNCATE TABLE table_name
```

**Example:**
```sql
TRUNCATE TABLE logs
```

**Notes:**
- Requires admin privileges
- Faster than `DELETE FROM table_name` for removing all rows
- Resets auto-increment sequences
- Cannot truncate a table that is referenced by foreign keys with existing data in other tables

#### CREATE INDEX

```sql
CREATE INDEX [IF NOT EXISTS] index_name ON table_name (column_name)
```

**Examples:**
```sql
-- Create an index
CREATE INDEX idx_users_email ON users (email)

-- Create an index only if it doesn't already exist
CREATE INDEX IF NOT EXISTS idx_users_email ON users (email)
```

#### DROP INDEX

```sql
DROP INDEX [IF EXISTS] index_name ON table_name
```

**Examples:**
```sql
-- Drop an index
DROP INDEX idx_users_email ON users

-- Drop an index only if it exists (no error if index doesn't exist)
DROP INDEX IF EXISTS idx_users_email ON users
```

#### CREATE VIEW

```sql
CREATE [OR REPLACE] VIEW [IF NOT EXISTS] view_name AS
SELECT columns FROM table [WHERE condition]
```

**Examples:**
```sql
-- Create a view
CREATE VIEW active_users AS
SELECT id, name, email FROM users WHERE active = true

-- Create a view only if it doesn't already exist
CREATE VIEW IF NOT EXISTS active_users AS
SELECT id, name, email FROM users WHERE active = true

-- Replace an existing view or create a new one
CREATE OR REPLACE VIEW active_users AS
SELECT id, name, email, created_at FROM users WHERE active = true
```

Views can be queried like tables:
```sql
SELECT * FROM active_users
```

#### DROP VIEW

```sql
DROP VIEW [IF EXISTS] view_name
```

**Examples:**
```sql
-- Drop a view
DROP VIEW active_users

-- Drop a view only if it exists (no error if view doesn't exist)
DROP VIEW IF EXISTS active_users
```

#### ALTER TABLE

Modify the structure of an existing table.

```sql
-- Add a new column
ALTER TABLE table_name ADD COLUMN column_name TYPE [constraints]

-- Drop a column
ALTER TABLE table_name DROP COLUMN column_name

-- Rename a column
ALTER TABLE table_name RENAME COLUMN old_name TO new_name

-- Modify a column type
ALTER TABLE table_name MODIFY COLUMN column_name NEW_TYPE
```

**Examples:**
```sql
ALTER TABLE users ADD COLUMN phone TEXT
ALTER TABLE users DROP COLUMN phone
ALTER TABLE users RENAME COLUMN email TO email_address
ALTER TABLE users MODIFY COLUMN age BIGINT
```

### Data Manipulation Language (DML)

#### SELECT

```sql
SELECT [DISTINCT] column1, column2, ...
FROM table_name
[WHERE condition]
[ORDER BY column [ASC|DESC]]
[LIMIT n]
[OFFSET m]
[GROUP BY column1, column2, ...]
[HAVING condition]
```

**Examples:**
```sql
-- Basic select
SELECT * FROM users

-- With DISTINCT (removes duplicate rows)
SELECT DISTINCT status FROM orders

-- With WHERE clause
SELECT name, email FROM users WHERE active = true

-- With ORDER BY and LIMIT
SELECT * FROM users ORDER BY created_at DESC LIMIT 10

-- With GROUP BY and aggregate
SELECT status, COUNT(*) FROM orders GROUP BY status

-- With HAVING
SELECT user_id, SUM(total) as sum FROM orders
GROUP BY user_id HAVING SUM(total) > 100
```

#### JOINs

```sql
SELECT columns
FROM table1
[INNER|LEFT|RIGHT|FULL] JOIN table2 ON table1.col = table2.col
[WHERE condition]
```

**Examples:**
```sql
-- Inner join
SELECT users.name, orders.total
FROM users
INNER JOIN orders ON users.id = orders.user_id

-- Left join
SELECT users.name, orders.total
FROM users
LEFT JOIN orders ON users.id = orders.user_id

-- Multiple joins
SELECT u.name, o.total, p.name as product
FROM users u
INNER JOIN orders o ON u.id = o.user_id
INNER JOIN products p ON o.product_id = p.id
```

#### UNION

Combines results from two SELECT statements, removing duplicates by default.

```sql
SELECT columns FROM table1
UNION [ALL]
SELECT columns FROM table2
```

**Examples:**
```sql
-- UNION removes duplicates
SELECT name FROM customers
UNION
SELECT name FROM suppliers

-- UNION ALL keeps all rows including duplicates
SELECT name FROM customers
UNION ALL
SELECT name FROM suppliers
```

#### INTERSECT

Returns only rows that appear in both result sets.

```sql
SELECT columns FROM table1
INTERSECT [ALL]
SELECT columns FROM table2
```

**Examples:**
```sql
-- Find names that exist in both tables
SELECT name FROM employees
INTERSECT
SELECT name FROM managers

-- INTERSECT ALL keeps duplicates up to the minimum count
SELECT name FROM employees
INTERSECT ALL
SELECT name FROM managers
```

#### EXCEPT

Returns rows from the first query that are not in the second query.

```sql
SELECT columns FROM table1
EXCEPT [ALL]
SELECT columns FROM table2
```

**Examples:**
```sql
-- Find employees who are not managers
SELECT name FROM employees
EXCEPT
SELECT name FROM managers

-- EXCEPT ALL subtracts counts
SELECT name FROM employees
EXCEPT ALL
SELECT name FROM managers
```

#### Subqueries

Subqueries can be used in WHERE clauses and FROM clauses.

```sql
-- IN subquery
SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)

-- NOT IN subquery
SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM inactive_users)

-- EXISTS subquery
SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)

-- In FROM clause (derived table)
SELECT * FROM (SELECT user_id, SUM(total) as sum FROM orders GROUP BY user_id) AS subq
WHERE sum > 500
```

#### INSERT

```sql
INSERT INTO table_name [(column1, column2, ...)]
VALUES (value1, value2, ...)
```

**Examples:**
```sql
-- Insert with all columns
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', true)

-- Insert with specific columns
INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')

-- Insert multiple rows
INSERT INTO users (name, email) VALUES
    ('Charlie', 'charlie@example.com'),
    ('Diana', 'diana@example.com')
```

#### UPDATE

```sql
UPDATE table_name
SET column1 = value1, column2 = value2, ...
[WHERE condition]
```

**Examples:**
```sql
-- Update single row
UPDATE users SET email = 'newemail@example.com' WHERE id = 1

-- Update multiple columns
UPDATE users SET active = false, updated_at = '2024-01-15' WHERE id = 1

-- Update multiple rows
UPDATE orders SET status = 'shipped' WHERE status = 'pending'
```

#### DELETE

```sql
DELETE FROM table_name
[WHERE condition]
```

**Examples:**
```sql
-- Delete single row
DELETE FROM users WHERE id = 1

-- Delete multiple rows
DELETE FROM orders WHERE status = 'cancelled'

-- Delete all rows (use with caution!)
DELETE FROM temp_data
```

### Transactions

#### BEGIN / COMMIT / ROLLBACK

```sql
BEGIN
-- statements
COMMIT

-- or
BEGIN
-- statements
ROLLBACK
```

**Example:**
```sql
BEGIN
INSERT INTO accounts (id, balance) VALUES (1, 1000)
UPDATE accounts SET balance = balance - 100 WHERE id = 1
UPDATE accounts SET balance = balance + 100 WHERE id = 2
COMMIT
```

#### SAVEPOINT

Create a savepoint within a transaction to allow partial rollback.

```sql
SAVEPOINT savepoint_name
```

#### RELEASE SAVEPOINT

Remove a savepoint (the changes remain).

```sql
RELEASE SAVEPOINT savepoint_name
```

#### ROLLBACK TO SAVEPOINT

Roll back to a specific savepoint without aborting the entire transaction.

```sql
ROLLBACK TO SAVEPOINT savepoint_name
```

**Example with Savepoints:**
```sql
BEGIN
INSERT INTO orders (id, total) VALUES (1, 100)
SAVEPOINT order_created

INSERT INTO order_items (order_id, product_id) VALUES (1, 10)
-- Oops, wrong product! Roll back just the item insert
ROLLBACK TO SAVEPOINT order_created

INSERT INTO order_items (order_id, product_id) VALUES (1, 20)
COMMIT
```

### Stored Procedures

#### CREATE PROCEDURE

```sql
CREATE [OR REPLACE] PROCEDURE [IF NOT EXISTS] procedure_name AS
BEGIN
    statement1;
    statement2;
END
```

**Examples:**
```sql
-- Create a procedure
CREATE PROCEDURE deactivate_old_users AS
BEGIN
    UPDATE users SET active = false WHERE last_login < '2023-01-01';
    DELETE FROM sessions WHERE user_id IN (SELECT id FROM users WHERE active = false);
END

-- Create a procedure only if it doesn't already exist
CREATE PROCEDURE IF NOT EXISTS deactivate_old_users AS
BEGIN
    UPDATE users SET active = false WHERE last_login < '2023-01-01';
END

-- Replace an existing procedure or create a new one
CREATE OR REPLACE PROCEDURE deactivate_old_users AS
BEGIN
    UPDATE users SET active = false WHERE last_login < '2024-01-01';
END
```

#### CALL

Execute a stored procedure.

```sql
CALL procedure_name
```

**Example:**
```sql
CALL deactivate_old_users
```

#### DROP PROCEDURE

Remove a stored procedure.

```sql
DROP PROCEDURE [IF EXISTS] procedure_name
```

**Examples:**
```sql
-- Drop a procedure
DROP PROCEDURE deactivate_old_users

-- Drop a procedure only if it exists (no error if procedure doesn't exist)
DROP PROCEDURE IF EXISTS deactivate_old_users
```

### Triggers

Triggers are automatic actions that execute in response to INSERT, UPDATE, or DELETE operations on tables.

#### CREATE TRIGGER

```sql
CREATE [OR REPLACE] TRIGGER [IF NOT EXISTS] trigger_name
  BEFORE|AFTER INSERT|UPDATE|DELETE ON table_name
  FOR EACH ROW
  EXECUTE action_sql
```

**Examples:**
```sql
-- Log all inserts to an audit table
CREATE TRIGGER log_insert AFTER INSERT ON users FOR EACH ROW EXECUTE INSERT INTO audit_log VALUES ( 'insert' , 'users' )

-- Validate before update
CREATE TRIGGER validate_update BEFORE UPDATE ON products FOR EACH ROW EXECUTE INSERT INTO validation_log VALUES ( 'validating' )

-- Log deletions
CREATE TRIGGER log_delete AFTER DELETE ON orders FOR EACH ROW EXECUTE INSERT INTO audit_log VALUES ( 'delete' , 'orders' )

-- Create a trigger only if it doesn't already exist
CREATE TRIGGER IF NOT EXISTS log_insert AFTER INSERT ON users FOR EACH ROW EXECUTE INSERT INTO audit_log VALUES ( 'insert' , 'users' )

-- Replace an existing trigger or create a new one
CREATE OR REPLACE TRIGGER log_insert AFTER INSERT ON users FOR EACH ROW EXECUTE INSERT INTO audit_log VALUES ( 'new_insert' , 'users' )
```

**Timing:**
- `BEFORE`: Trigger executes before the operation
- `AFTER`: Trigger executes after the operation

**Events:**
- `INSERT`: Fires on INSERT operations
- `UPDATE`: Fires on UPDATE operations
- `DELETE`: Fires on DELETE operations

#### DROP TRIGGER

```sql
DROP TRIGGER [IF EXISTS] trigger_name ON table_name
```

**Examples:**
```sql
-- Drop a trigger
DROP TRIGGER log_insert ON users

-- Drop a trigger only if it exists (no error if trigger doesn't exist)
DROP TRIGGER IF EXISTS log_insert ON users
```

### User Management

#### CREATE USER

Create a new database user with the specified credentials.

```sql
CREATE USER username IDENTIFIED BY 'password'
```

**Example:**
```sql
CREATE USER alice IDENTIFIED BY 'secret123'
```

#### ALTER USER

Modify an existing user's password.

```sql
ALTER USER username IDENTIFIED BY 'new_password'
```

**Example:**
```sql
ALTER USER alice IDENTIFIED BY 'new_secret456'
```

#### GRANT

Grant table access to a user, optionally with Row-Level Security (RLS).

```sql
GRANT [SELECT] ON table [WHERE column = value] TO user
```

**Examples:**
```sql
-- Grant full access to a table
GRANT SELECT ON users TO readonly_user

-- Grant access with Row-Level Security (RLS)
-- User can only see rows where the condition matches
GRANT SELECT ON orders WHERE user_id = 'alice' TO alice
```

**Note:** FlyDB uses a simplified permission model where GRANT provides full table access. Row-Level Security is applied via the optional WHERE clause in the GRANT statement.

#### REVOKE

Remove table access from a user.

```sql
REVOKE ON table FROM user
```

**Example:**
```sql
-- Remove alice's access to the orders table
REVOKE ON orders FROM alice
```

**Note:** REVOKE removes all access rights for the user on the specified table, including any Row-Level Security conditions that were set.

### Prepared Statements

#### PREPARE

Create a prepared statement with parameter placeholders.

```sql
PREPARE stmt_name AS SELECT * FROM users WHERE id = $1
```

**Note:** Use `$1`, `$2`, etc. for parameter placeholders.

#### EXECUTE

Execute a prepared statement with parameter values.

```sql
EXECUTE stmt_name (value1, value2, ...)
```

#### DEALLOCATE

Remove a prepared statement.

```sql
DEALLOCATE stmt_name
```

**Example:**
```sql
-- Create a prepared statement
PREPARE get_user AS SELECT * FROM users WHERE id = $1

-- Execute it multiple times with different values
EXECUTE get_user (42)
EXECUTE get_user (123)

-- Clean up when done
DEALLOCATE get_user
```

### Database Management

FlyDB supports multiple isolated databases within a single server instance.

#### CREATE DATABASE

Create a new database with optional settings.

```sql
CREATE DATABASE [IF NOT EXISTS] database_name
  [WITH ENCODING 'encoding']
  [LOCALE 'locale']
  [COLLATION 'collation']
  [OWNER 'username']
```

**Options:**

| Option | Values | Default | Description |
|--------|--------|---------|-------------|
| ENCODING | UTF8, LATIN1, ASCII, UTF16 | UTF8 | Character encoding |
| LOCALE | en_US, de_DE, fr_FR, etc. | en_US | Locale for sorting |
| COLLATION | default, binary, nocase, unicode | default | String comparison rules |
| OWNER | username | current user | Database owner |

**Examples:**
```sql
-- Create a simple database
CREATE DATABASE analytics

-- Create with custom settings
CREATE DATABASE reports WITH ENCODING 'UTF8' LOCALE 'de_DE' COLLATION 'unicode' OWNER 'alice'

-- Create only if it doesn't exist
CREATE DATABASE IF NOT EXISTS staging
```

#### DROP DATABASE

Remove a database and all its data.

```sql
DROP DATABASE [IF EXISTS] database_name
```

**Examples:**
```sql
-- Drop a database
DROP DATABASE staging

-- Drop only if it exists (no error if database doesn't exist)
DROP DATABASE IF EXISTS temp_db
```

**Notes:**
- Cannot drop the default database
- Cannot drop a database that has active connections
- Requires admin privileges

#### USE

Switch to a different database for the current connection.

```sql
USE database_name
```

**Example:**
```sql
USE analytics
-- All subsequent queries run against the analytics database
```

### Database Inspection

The INSPECT command provides metadata about database objects. Requires admin privileges.

#### INSPECT DATABASES

List all databases with their metadata.

```sql
INSPECT DATABASES
```

**Example output:**
```
+------------+-------+----------+--------+-----------+
| name       | owner | encoding | locale | collation |
+------------+-------+----------+--------+-----------+
| default    | admin | UTF8     | en_US  | default   |
| analytics  | alice | UTF8     | de_DE  | unicode   |
+------------+-------+----------+--------+-----------+
(2 rows)
```

#### INSPECT USERS

List all database users.

```sql
INSPECT USERS
```

#### INSPECT USER

Get detailed information about a specific user, including their roles and effective database access.

```sql
INSPECT USER username
```

**Example output for admin user:**
```
Username: admin
Admin: true
Status: active
Default Database:
Created: 2026-01-08T20:02:40+01:00
Last Login: 2026-01-08T20:03:40+01:00

Assigned Roles:
  admin (global)

Effective Database Access:
  * (all databases via admin role)
```

**Example output for regular user:**
```
Username: alice
Admin: false
Status: active
Default Database: analytics
Created: 2026-01-08T20:05:00+01:00
Last Login: 2026-01-08T20:10:00+01:00

Assigned Roles:
  reader (database: analytics)
  writer (database: sales)

Effective Database Access:
  analytics (via reader)
  sales (via writer)
```

#### INSPECT USER ROLES

Get the roles assigned to a specific user.

```sql
INSPECT USER username ROLES
```

#### INSPECT USER PRIVILEGES

Get the effective privileges for a specific user.

```sql
INSPECT USER username PRIVILEGES
```

#### INSPECT ROLES

List all roles in the system.

```sql
INSPECT ROLES
```

#### INSPECT ROLE

Get detailed information about a specific role.

```sql
INSPECT ROLE role_name
```

#### INSPECT PRIVILEGES

List all privileges in the system.

```sql
INSPECT PRIVILEGES
```

#### INSPECT TABLES

List all tables with their column schemas.

```sql
INSPECT TABLES
```

#### INSPECT TABLE

Get detailed information about a specific table.

```sql
INSPECT TABLE table_name
```

#### INSPECT INDEXES

List all indexes in the database.

```sql
INSPECT INDEXES
```

#### INSPECT SERVER

Show server information (version, status, etc.).

```sql
INSPECT SERVER
```

#### INSPECT STATUS

Show overall database status and statistics.

```sql
INSPECT STATUS
```

**Examples:**
```sql
INSPECT USERS
-- Returns: username
--          admin
--          alice
--          bob
--          (3 rows)

INSPECT TABLE users
-- Returns detailed schema information for the users table
```

---

## Data Types

### Numeric Types

| Type | Aliases | Description | Example |
|------|---------|-------------|---------|
| `INT` | `INTEGER` | 32-bit signed integer | `42`, `-100` |
| `BIGINT` | - | 64-bit signed integer | `9223372036854775807` |
| `SMALLINT` | `TINYINT` | 16-bit signed integer | `32767` |
| `SERIAL` | - | Auto-incrementing integer | (auto-generated) |
| `FLOAT` | - | 64-bit floating point | `3.14`, `-0.001` |
| `DOUBLE` | - | 64-bit floating point | `3.14159265359` |
| `REAL` | - | 64-bit floating point | `2.718` |
| `DECIMAL` | `NUMERIC` | Arbitrary precision decimal | `123.456` |
| `MONEY` | - | Currency values | `$1234.56`, `€99.99` |

### String Types

| Type | Aliases | Description | Example |
|------|---------|-------------|---------|
| `TEXT` | - | Variable-length string (unlimited) | `'Hello World'` |
| `VARCHAR(n)` | - | String with max length n | `'Hello'` |
| `CHAR(n)` | `CHARACTER` | Fixed-length string | `'ABC'` |
| `CLOB` | - | Character large object | `'Long text...'` |
| `NCHAR(n)` | - | Fixed-length Unicode string | `'日本語'` |
| `NVARCHAR(n)` | - | Variable-length Unicode string | `'日本語'` |
| `NTEXT` | - | Unicode text (unlimited) | `'日本語テキスト'` |

### Boolean Type

| Type | Aliases | Description | Example |
|------|---------|-------------|---------|
| `BOOLEAN` | `BOOL` | True or false | `true`, `false`, `1`, `0` |

### Date/Time Types

| Type | Aliases | Description | Example |
|------|---------|-------------|---------|
| `TIMESTAMP` | - | Date and time with timezone | `'2024-01-15T10:30:00Z'` |
| `DATETIME` | - | Date and time | `'2024-01-15 10:30:00'` |
| `DATE` | - | Date only (YYYY-MM-DD) | `'2024-01-15'` |
| `TIME` | - | Time only (HH:MM:SS) | `'10:30:00'` |
| `INTERVAL` | - | Time interval/duration | `'1 day'`, `'2 hours'` |

### Binary Types

| Type | Aliases | Description | Example |
|------|---------|-------------|---------|
| `BLOB` | - | Binary large object (base64 encoded) | `'SGVsbG8='` |
| `BYTEA` | - | Binary data (base64 encoded) | `'SGVsbG8='` |
| `BINARY` | - | Fixed-length binary (base64 encoded) | `'SGVsbG8='` |
| `VARBINARY` | - | Variable-length binary (base64 encoded) | `'SGVsbG8='` |

### Structured Types

| Type | Aliases | Description | Example |
|------|---------|-------------|---------|
| `UUID` | - | Universally unique identifier (RFC 4122) | `'550e8400-e29b-41d4-a716-446655440000'` |
| `JSONB` | `JSON` | Binary JSON for structured data | `'{"key": "value"}'` |

---

## Operators and Functions

### Comparison Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equal | `WHERE id = 1` |
| `<>` or `!=` | Not equal | `WHERE status <> 'deleted'` |
| `<` | Less than | `WHERE age < 18` |
| `>` | Greater than | `WHERE total > 100` |
| `<=` | Less than or equal | `WHERE quantity <= 10` |
| `>=` | Greater than or equal | `WHERE rating >= 4.0` |
| `LIKE` | Pattern matching | `WHERE name LIKE 'A%'` |
| `IN` | Value in list | `WHERE status IN ('active', 'pending')` |
| `BETWEEN` | Range check | `WHERE age BETWEEN 18 AND 65` |
| `IS NULL` | Null check | `WHERE email IS NULL` |
| `IS NOT NULL` | Not null check | `WHERE email IS NOT NULL` |

### Logical Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `AND` | Logical AND | `WHERE active = true AND age > 18` |
| `OR` | Logical OR | `WHERE status = 'admin' OR status = 'moderator'` |
| `NOT` | Logical NOT | `WHERE NOT deleted` |

### Arithmetic Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `+` | Addition | `SELECT price + tax FROM products` |
| `-` | Subtraction | `SELECT total - discount FROM orders` |
| `*` | Multiplication | `SELECT quantity * price FROM items` |
| `/` | Division | `SELECT total / count FROM stats` |
| `%` | Modulo (remainder) | `SELECT id % 10 FROM users` |

### String Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `\|\|` | String concatenation | `SELECT first_name \|\| ' ' \|\| last_name FROM users` |

### Aggregate Functions

| Function | Description | Example |
|----------|-------------|---------|
| `COUNT(*)` | Count rows | `SELECT COUNT(*) FROM users` |
| `COUNT(col)` | Count non-null values | `SELECT COUNT(email) FROM users` |
| `SUM(col)` | Sum of values | `SELECT SUM(total) FROM orders` |
| `AVG(col)` | Average of values | `SELECT AVG(age) FROM users` |
| `MIN(col)` | Minimum value | `SELECT MIN(price) FROM products` |
| `MAX(col)` | Maximum value | `SELECT MAX(created_at) FROM users` |
| `GROUP_CONCAT(col, sep)` | Concatenate values with separator | `SELECT GROUP_CONCAT(name, ', ') FROM users` |
| `STRING_AGG(col, sep)` | Concatenate values with separator (alias) | `SELECT STRING_AGG(tag, ';') FROM tags` |

### String Functions

| Function | Description | Example |
|----------|-------------|---------|
| `UPPER(str)` | Convert to uppercase | `SELECT UPPER(name) FROM users` |
| `LOWER(str)` | Convert to lowercase | `SELECT LOWER(email) FROM users` |
| `LENGTH(str)` / `LEN(str)` | String length | `SELECT LENGTH(name) FROM users` |
| `CONCAT(str1, str2, ...)` | Concatenate strings | `SELECT CONCAT(first, ' ', last) FROM users` |
| `SUBSTRING(str, start, len)` / `SUBSTR` | Extract substring | `SELECT SUBSTRING(name, 1, 3) FROM users` |
| `TRIM(str)` | Remove leading/trailing whitespace | `SELECT TRIM(name) FROM users` |
| `LTRIM(str)` | Remove leading whitespace | `SELECT LTRIM(name) FROM users` |
| `RTRIM(str)` | Remove trailing whitespace | `SELECT RTRIM(name) FROM users` |
| `REPLACE(str, from, to)` | Replace occurrences | `SELECT REPLACE(name, 'old', 'new') FROM users` |
| `LEFT(str, n)` | Get leftmost n characters | `SELECT LEFT(name, 5) FROM users` |
| `RIGHT(str, n)` | Get rightmost n characters | `SELECT RIGHT(name, 5) FROM users` |
| `REVERSE(str)` | Reverse string | `SELECT REVERSE(name) FROM users` |
| `REPEAT(str, n)` | Repeat string n times | `SELECT REPEAT('*', 5) FROM users` |

### Numeric Functions

| Function | Description | Example |
|----------|-------------|---------|
| `ABS(n)` | Absolute value | `SELECT ABS(balance) FROM accounts` |
| `ROUND(n, decimals)` | Round to decimals | `SELECT ROUND(price, 2) FROM products` |
| `CEIL(n)` / `CEILING(n)` | Round up | `SELECT CEIL(rating) FROM reviews` |
| `FLOOR(n)` | Round down | `SELECT FLOOR(rating) FROM reviews` |
| `MOD(n, m)` | Modulo (remainder) | `SELECT MOD(id, 10) FROM users` |
| `POWER(n, m)` / `POW(n, m)` | Power | `SELECT POWER(2, 8) FROM dual` |
| `SQRT(n)` | Square root | `SELECT SQRT(area) FROM shapes` |

### Date/Time Functions

| Function | Description | Example |
|----------|-------------|---------|
| `NOW()` | Current timestamp | `SELECT NOW()` |
| `CURRENT_DATE` | Current date | `SELECT CURRENT_DATE` |
| `CURRENT_TIME` | Current time | `SELECT CURRENT_TIME` |
| `CURRENT_TIMESTAMP` | Current timestamp | `SELECT CURRENT_TIMESTAMP` |
| `DATE_ADD(date, interval)` / `DATEADD` | Add to date | `SELECT DATE_ADD(created_at, 7)` |
| `DATE_SUB(date, interval)` | Subtract from date | `SELECT DATE_SUB(created_at, 7)` |
| `DATEDIFF(date1, date2)` | Days between dates | `SELECT DATEDIFF(end_date, start_date)` |
| `EXTRACT(part FROM date)` | Extract date part | `SELECT EXTRACT(YEAR FROM created_at)` |
| `YEAR(date)` | Extract year | `SELECT YEAR(created_at) FROM users` |
| `MONTH(date)` | Extract month | `SELECT MONTH(created_at) FROM users` |
| `DAY(date)` | Extract day | `SELECT DAY(created_at) FROM users` |
| `HOUR(time)` | Extract hour | `SELECT HOUR(created_at) FROM users` |
| `MINUTE(time)` | Extract minute | `SELECT MINUTE(created_at) FROM users` |
| `SECOND(time)` | Extract second | `SELECT SECOND(created_at) FROM users` |

### NULL Handling Functions

| Function | Description | Example |
|----------|-------------|---------|
| `COALESCE(val1, val2, ...)` | First non-null value | `SELECT COALESCE(nickname, name) FROM users` |
| `NULLIF(val1, val2)` | NULL if values equal | `SELECT NULLIF(status, 'unknown') FROM users` |
| `IFNULL(val, default)` / `NVL` / `ISNULL` | Default if null | `SELECT IFNULL(email, 'N/A') FROM users` |

### Type Conversion Functions

| Function | Description | Example |
|----------|-------------|---------|
| `CAST(val AS type)` | Convert to type | `SELECT CAST(id AS TEXT) FROM users` |
| `CONVERT(val, type)` | Convert to type | `SELECT CONVERT(price, INT) FROM products` |

---

## Configuration

### Interactive Wizard

When FlyDB is started without any command-line arguments, an interactive configuration wizard is launched:

```bash
./flydb
```

The wizard guides you through:

1. **Operative Mode Selection**
   - **Standalone**: Single server for development or small deployments
   - **Master**: Leader node that accepts writes and replicates to slaves
   - **Slave**: Follower node that receives replication from master

2. **Network Configuration**
   - Text protocol port (default: 8888)
   - Binary protocol port (default: 8889)
   - Replication port (master only, default: 9999)

3. **Storage Configuration**
   - Database file path (default: flydb.fdb)

4. **Logging Configuration**
   - Log level (debug, info, warn, error)
   - JSON logging option

5. **Configuration Summary and Confirmation**

The wizard displays sensible defaults in brackets. Press Enter to accept defaults.

### Server Configuration

FlyDB is configured via command-line flags:

| Flag | Default | Description |
|------|---------|-------------|
| `-port` | `8888` | Text protocol port |
| `-binary-port` | `8889` | Binary protocol port |
| `-repl-port` | `9999` | Replication port (master only) |
| `-db` | `flydb.fdb` | WAL file path |
| `-role` | `master` | Server role: `standalone`, `master`, or `slave` |
| `-master` | - | Master address for slave mode (host:port) |
| `-log-level` | `info` | Log level: debug, info, warn, error |
| `-log-json` | `false` | Enable JSON log output |

### Operative Modes

| Mode | Description | Replication |
|------|-------------|-------------|
| `standalone` | Single server mode | None |
| `master` | Leader node | Accepts slaves on repl-port |
| `slave` | Follower node | Connects to master |

### Encryption Configuration

**⚠️ BREAKING CHANGE: Encryption is enabled by default starting from version 01.26.1.**

FlyDB encrypts all WAL data on disk using AES-256-GCM. When encryption is enabled (the default), you **must** provide a passphrase.

#### Environment Variables for Encryption

| Variable | Default | Description |
|----------|---------|-------------|
| `FLYDB_ENCRYPTION_ENABLED` | `true` | Enable/disable data-at-rest encryption |
| `FLYDB_ENCRYPTION_PASSPHRASE` | - | **Required** when encryption enabled - passphrase for key derivation |

#### Setting the Passphrase

There are two ways to provide the encryption passphrase:

**Option 1: Environment Variable (Recommended for Production)**

```bash
# Set the passphrase (required when encryption is enabled)
export FLYDB_ENCRYPTION_PASSPHRASE="your-secure-passphrase"
./flydb -role standalone
```

**Option 2: Interactive Wizard**

When running FlyDB without arguments, the setup wizard will prompt for a passphrase:

```bash
./flydb
# Wizard prompts:
#   Step 4: Data-at-Rest Encryption
#   Enable encryption? (y/n) [y]: y
#   Encryption passphrase: ********
```

The wizard automatically detects if `FLYDB_ENCRYPTION_PASSPHRASE` is already set and uses it.

#### Disabling Encryption

To disable encryption, set `encryption_enabled = false` in your config file:

```toml
# flydb.conf
encryption_enabled = false
```

Or via environment variable:

```bash
export FLYDB_ENCRYPTION_ENABLED=false
./flydb -role standalone
```

#### Migration from Previous Versions

If upgrading from a version where encryption was disabled by default:

| Option | Action |
|--------|--------|
| Enable encryption | Set `FLYDB_ENCRYPTION_PASSPHRASE` environment variable |
| Keep encryption disabled | Add `encryption_enabled = false` to config file |

> ⚠️ **WARNING**: Keep your passphrase safe! Data cannot be recovered without it.

### Example Startup

```bash
# Interactive wizard (recommended for first-time setup)
./flydb

# Standalone mode with encryption (default)
export FLYDB_ENCRYPTION_PASSPHRASE="my-secure-passphrase"
./flydb -role standalone -db dev.fdb

# Standalone mode without encryption
export FLYDB_ENCRYPTION_ENABLED=false
./flydb -role standalone -db dev.fdb

# Master with replication
export FLYDB_ENCRYPTION_PASSPHRASE="my-secure-passphrase"
./flydb -port 8888 -repl-port 9999 -role master -db master.fdb

# Slave connecting to master
export FLYDB_ENCRYPTION_PASSPHRASE="my-secure-passphrase"
./flydb -port 8889 -role slave -master localhost:9999 -db slave.fdb

# With debug logging
./flydb -role standalone -log-level debug

# With JSON logging (for log aggregation)
./flydb -role standalone -log-json
```

### Installation Script

FlyDB includes an installation script that supports both remote installation and local source builds.

#### Quick Install (Remote)

Install FlyDB with a single command:

```bash
curl -sSL https://get.flydb.dev | bash
```

With options:

```bash
# Install with custom prefix
curl -sSL https://get.flydb.dev | bash -s -- --prefix ~/.local --yes

# Install specific version
curl -sSL https://get.flydb.dev | bash -s -- --version 01.26.0 --yes

# Install without system service
curl -sSL https://get.flydb.dev | bash -s -- --no-service --yes
```

#### Install from Source

```bash
# Clone and install interactively
git clone https://github.com/firefly-software/flydb.git
cd flydb
./install.sh

# Non-interactive installation with defaults
./install.sh --yes

# Install to custom prefix
./install.sh --prefix /opt/flydb

# System-wide installation
sudo ./install.sh --prefix /usr/local

# Force build from source (skip binary download)
./install.sh --from-source --yes

# Force download binaries (skip source build)
./install.sh --from-binary --yes
```

#### Installation Options

| Option | Description |
|--------|-------------|
| `--prefix <path>` | Installation prefix (default: `~/.local` or `/usr/local` with sudo) |
| `--version <ver>` | Install specific version |
| `--from-source` | Force building from source (requires Go 1.21+) |
| `--from-binary` | Force downloading pre-built binaries |
| `--yes` | Non-interactive mode, accept all defaults |
| `--no-service` | Skip system service installation |
| `--no-config` | Skip configuration file creation |
| `--help` | Show all available options |

The script:
- Auto-detects whether to build from source or download pre-built binaries
- Installs `flydb` daemon and `flydb-shell` client (with `fsql` symlink)
- Optionally sets up system service (systemd on Linux, launchd on macOS)
- Creates default configuration files
- Works on Linux and macOS

### Uninstallation Script

To remove FlyDB from your system:

```bash
# Interactive uninstallation
./uninstall.sh

# Preview what would be removed
./uninstall.sh --dry-run

# Non-interactive uninstallation
./uninstall.sh --yes

# Also remove data directories (WARNING: deletes databases!)
./uninstall.sh --remove-data --yes
```

The script automatically detects and removes:
- FlyDB binaries (`flydb`, `flydb-shell`, `fsql`)
- System service files
- Configuration directories (use `--no-config` to preserve)
- Data directories (only with `--remove-data` flag)

---

## See Also

- [Architecture Overview](architecture.md) - High-level system design
- [Implementation Details](implementation.md) - Technical deep-dives
- [Design Decisions](design-decisions.md) - Rationale and trade-offs

