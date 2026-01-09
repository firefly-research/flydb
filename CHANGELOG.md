# Changelog

All notable changes to FlyDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [01.26.9] - 2026-01-09

### Production-Ready Cluster Mode with Automatic Failover

This release introduces a fully integrated cluster mode with automatic leader election, enhanced configuration management, and an improved setup wizard. FlyDB now supports four operational modes: standalone, master, slave, and cluster.

### Added

#### Cluster Mode (`role = "cluster"`)
- **Automatic Leader Election**: Enhanced Bully algorithm with term-based elections and split-brain prevention
- **Quorum-Based Decisions**: Configurable minimum quorum for cluster consensus
- **Pre-Vote Protocol**: Optional pre-vote to prevent disruptions from partitioned nodes
- **Health Monitoring**: Automatic detection of unhealthy nodes with configurable thresholds
- **Cluster Events**: Real-time event notifications for leader changes, node joins/leaves, and quorum status
- **Dynamic Membership**: Nodes can join and leave the cluster without downtime

#### Enhanced Configuration
- **New Environment Variables**:
  - `FLYDB_CLUSTER_PORT` - Port for cluster communication (default: 9998)
  - `FLYDB_CLUSTER_PEERS` - Comma-separated list of peer addresses
  - `FLYDB_REPLICATION_MODE` - Replication mode: async, semi_sync, or sync
  - `FLYDB_HEARTBEAT_INTERVAL_MS` - Heartbeat interval in milliseconds
  - `FLYDB_HEARTBEAT_TIMEOUT_MS` - Heartbeat timeout in milliseconds
  - `FLYDB_ELECTION_TIMEOUT_MS` - Election timeout in milliseconds
  - `FLYDB_MIN_QUORUM` - Minimum quorum size for cluster decisions
- **New Configuration Options**:
  - `cluster_port` - Port for cluster communication
  - `cluster_peers` - List of peer node addresses
  - `replication_mode` - async, semi_sync, or sync
  - `heartbeat_interval_ms`, `heartbeat_timeout_ms`, `election_timeout_ms`
  - `min_quorum`, `enable_pre_vote`, `sync_timeout_ms`, `max_replication_lag`

#### Enhanced Setup Wizard
- **Cluster Role Option**: New "Cluster" option in the role selection menu
- **Peer Configuration**: Interactive configuration of cluster peer addresses
- **Replication Mode Selection**: Choose between async, semi_sync, or sync replication
- **Advanced Cluster Settings**: Configure heartbeat intervals, election timeouts, quorum size, and pre-vote

#### Install Script Enhancements
- **Updated Configuration Template**: Default config file now includes all cluster options
- **Cluster Setup Instructions**: Post-install instructions include cluster setup examples
- **Help Text Updates**: Documentation of all server roles and cluster configuration options

### Changed
- **Role Validation**: Configuration validation now accepts `cluster` as a valid role
- **Cluster Role Requirements**: Cluster mode requires at least one peer to be configured
- **Replication Mode Validation**: Validates replication mode is one of: async, semi_sync, sync

### Example

```bash
# Start a 3-node cluster
# Node 1:
flydb -role cluster -cluster-port 9998 -cluster-peers node2:9998,node3:9998

# Node 2:
flydb -role cluster -cluster-port 9998 -cluster-peers node1:9998,node3:9998

# Node 3:
flydb -role cluster -cluster-port 9998 -cluster-peers node1:9998,node2:9998
```

Or using environment variables:
```bash
FLYDB_ROLE=cluster \
FLYDB_CLUSTER_PORT=9998 \
FLYDB_CLUSTER_PEERS=node2:9998,node3:9998 \
FLYDB_REPLICATION_MODE=semi_sync \
flydb
```

---

## [01.26.8] - 2026-01-08

### Unified Disk-Based Storage Engine

This release consolidates FlyDB's storage layer into a single, unified disk-based storage engine with intelligent memory caching. This is a major architectural improvement that provides better performance, scalability, and simpler configuration.

### Added

#### Disk Storage Engine
- **Page-Based Storage**: 8KB pages with slotted page layout for efficient record storage
- **LRU-K Buffer Pool**: Intelligent page caching with frequency-based eviction
- **Auto-Sized Buffer Pool**: Automatically sizes based on available system memory (25% of RAM)
- **Heap File Management**: Efficient page allocation with free space tracking
- **Checkpoint System**: Periodic full database snapshots for faster recovery

#### Storage Configuration
- **`buffer_pool_size`**: Configure buffer pool size in pages (0 = auto-size)
- **`checkpoint_secs`**: Configure checkpoint interval in seconds (default: 60)

### Changed
- **BREAKING: Removed memory engine option** - FlyDB now uses disk storage exclusively
- **BREAKING: Database directory structure changed** - Each database is now a directory containing `data.db` and `wal.fdb`
- `storage_engine` configuration option is now deprecated (ignored if set)
- Default buffer pool auto-sizes based on available memory instead of fixed 1024 pages
- Improved startup time with checkpoint-based recovery

### Removed
- `KVStore` in-memory storage implementation (replaced by disk engine)
- `storage_engine` configuration option (disk is now the only option)

### Migration Notes
- Existing databases using the memory engine will need to be migrated
- The new directory structure requires data migration for existing installations
- Set `buffer_pool_size = 0` for automatic memory-based sizing (recommended)

---

## [01.26.7] - 2026-01-08

### Enhanced Multi-line Editing in fsql CLI

This release brings professional-grade multi-line editing to the fsql CLI, matching the experience of PostgreSQL's psql, MySQL CLI, and other industry-standard database shells.

### Added

#### Semicolon-Based Statement Termination
- **SQL statements now require a semicolon (`;`) to execute** - This is the standard behavior in professional database CLIs
- **Automatic multi-line continuation**: When a SQL statement doesn't end with `;`, the prompt changes to `->` and waits for more input
- **Explicit continuation with backslash**: Lines ending with `\` continue on the next line (legacy behavior preserved)
- **Smart command detection**: Non-SQL commands (`PING`, `AUTH`, `USE`) execute immediately without requiring semicolons

#### Multi-line Input Features
- **Continuation prompt**: Clear visual indicator (`->`) when in multi-line mode
- **Cancel with Ctrl+C**: Press Ctrl+C to cancel multi-line input and return to normal prompt
- **History support**: Complete multi-line statements are saved to history as a single entry
- **Works in both modes**: Full support in both readline mode and simple REPL (piped input)

### Changed
- **BREAKING: Renamed `INTROSPECT` to `INSPECT`** - Shorter, cleaner command name for database inspection
- CLI now waits for semicolon before executing SQL statements
- Help documentation updated with multi-line editing section
- Improved user experience for writing complex, multi-line SQL queries

### Example

```
flydb:default> SELECT id, name, email
        -> FROM users
        -> WHERE id > 0
        -> ORDER BY name;
+----+-------+-------------------+
| id | name  | email             |
+----+-------+-------------------+
| 1  | Alice | alice@example.com |
| 2  | Bob   | bob@example.com   |
+----+-------+-------------------+
(2 rows)
```

---

## [01.26.6] - 2026-01-08

### Role-Based Access Control (RBAC)

This release introduces a comprehensive Role-Based Access Control system for managing user permissions.

### Added

#### RBAC Core Features
- **Role Management**: `CREATE ROLE`, `DROP ROLE` commands for managing roles
- **Role Assignment**: `GRANT ROLE <role> TO <user>`, `REVOKE ROLE <role> FROM <user>`
- **Database-Scoped Roles**: Roles can be assigned globally or per-database with `ON DATABASE <db>`
- **Built-in Roles**: Four predefined roles initialized on server startup:
  - `admin`: Full administrative privileges on all objects
  - `reader`: SELECT privilege on all tables
  - `writer`: INSERT, UPDATE, DELETE privileges on all tables
  - `owner`: Full privileges on owned objects (database-scoped)
- **Privilege Checking**: Authorization checks now consider role-based privileges
- **Role Descriptions**: Optional descriptions with `CREATE ROLE <name> WITH DESCRIPTION '<desc>'`

#### RBAC Inspection
- `INSPECT ROLES`: List all roles with description, built-in status, creation info
- `INSPECT ROLE <name>`: Detailed role info including privileges and assigned users
- `INSPECT USER <name>`: Detailed user info including assigned roles
- `INSPECT USER <name> ROLES`: List roles assigned to a specific user
- `INSPECT USER <name> PRIVILEGES`: List all effective privileges for a user
- `INSPECT PRIVILEGES`: List all privileges in the system

#### Parser Enhancements
- Added `ROLE`, `ROLES`, `PRIVILEGES`, `DESCRIPTION`, `WITH` as SQL keywords
- Support for `GRANT ROLE` and `REVOKE ROLE` syntax with optional `ROLE` keyword
- Support for `ON DATABASE` clause for database-scoped role assignments

### Changed
- Server now initializes built-in roles on startup (idempotent operation)
- Authorization checks now cascade through role hierarchy
- DROP ROLE prevents deletion of built-in roles
- Admin user now automatically receives the `admin` RBAC role on creation
- **CLI Authentication Required**: Users must now authenticate with `AUTH` before executing SQL commands
- CLI prompt shows `[not authenticated]` until user logs in
- Empty username (anonymous) access is now denied - authentication is mandatory
- Legacy `IsAdmin` flag deprecated in favor of RBAC roles

### Security Improvements
- Removed anonymous user access - all users must authenticate
- RBAC is now the primary authorization mechanism (legacy permissions as fallback)
- CLI blocks all SQL commands until successful authentication
- Clearer separation between authentication (who you are) and authorization (what you can do)

### Fixed
- **Admin User RBAC Role**: Admin user now correctly receives the `admin` RBAC role
- **INSPECT USER**: Fixed `INSPECT USER <name>` to use system database (was returning "user not found")
- **INSPECT USER Database Access**: Now shows "Effective Database Access" instead of "Direct Database Access"
  - Admin users correctly show `* (all databases via admin role)` instead of `(none)`
  - Regular users show database access derived from their assigned roles
- **INSPECT ROLE**: Fixed `INSPECT ROLE <name>` to correctly show assigned users
- **INSPECT USER ROLES/PRIVILEGES**: Fixed to use system database for RBAC data
- **Role Initialization Order**: Built-in roles are now initialized before admin user creation
- **Upgrade Path**: Added `EnsureAdminHasRole()` to fix existing admin users from older versions

### Encryption Improvements
- **Auto-generate passphrase on first run**: If no passphrase is provided and this is a first-time setup, a secure passphrase is automatically generated and displayed prominently
- **Wizard passphrase handling**: When using existing config with encryption enabled:
  - First-time setup: Auto-generates passphrase and displays it with save instructions
  - Existing data: Prompts for passphrase or shows error with instructions
  - Environment variable: Automatically loads from `FLYDB_ENCRYPTION_PASSPHRASE` if set
- **Clear error on wrong passphrase**: If the passphrase is incorrect, FlyDB now fails with a clear error message instead of cryptic decryption errors
- **Fail-fast for existing data**: If existing encrypted data is found but no passphrase is provided, FlyDB fails immediately with instructions
- Added `DataDirectoryHasData()` helper to detect first-time vs. existing installations
- Added `IsEncryptionError()` helper to detect encryption-related failures

---

## [01.26.1] - 2026-01-06

### Initial Public Release

This is the first public release of FlyDB, a lightweight SQL database written in Go.

### Added

#### Core Database Features
- SQL query support: `CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`
- `DROP TABLE [IF EXISTS]` to remove tables
- `DROP INDEX [IF EXISTS]` to remove indexes
- `TRUNCATE TABLE` for fast table truncation
- `SELECT * FROM` to retrieve all columns from a table
- `DISTINCT` to remove duplicate rows from SELECT results
- `UNION` and `UNION ALL` to combine results from multiple SELECT queries
- `INTERSECT` and `INTERSECT ALL` to return rows common to both queries
- `EXCEPT` and `EXCEPT ALL` to return rows in first query but not in second
- Subqueries in WHERE clauses with `IN` and `EXISTS`
- `INNER JOIN` with Nested Loop algorithm
- `LEFT JOIN`, `RIGHT JOIN`, `FULL JOIN` support
- `WHERE` clause filtering with equality and comparison operators (`=`, `<>`, `<`, `>`, `<=`, `>=`)
- `LIKE` and `NOT LIKE` pattern matching with `%` and `_` wildcards
- `BETWEEN` range operator
- `IS NULL` and `IS NOT NULL` null checks
- `IN` and `NOT IN` operators for value lists
- `ORDER BY` with `ASC`/`DESC` sorting
- `LIMIT` and `OFFSET` for result set restriction
- `GROUP BY` for grouping rows
- `HAVING` for filtering groups after aggregation
- Schema persistence across server restarts
- Transactions: `BEGIN`, `COMMIT`, `ROLLBACK` for atomic operations
- Savepoints: `SAVEPOINT`, `RELEASE SAVEPOINT`, `ROLLBACK TO SAVEPOINT` for partial rollback
- B-Tree indexing: `CREATE INDEX [IF NOT EXISTS]` for O(log N) lookups
- Prepared statements: `PREPARE`, `EXECUTE`, `DEALLOCATE`
- Aggregate functions: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- `INSPECT` command for database metadata inspection (USERS, TABLES, TABLE, INDEXES, SERVER, STATUS)
- Pretty table formatting for SELECT results in CLI
- Row count information in all query results

#### INSERT Enhancements
- Multi-row INSERT: `INSERT INTO table VALUES (row1), (row2), (row3)`
- INSERT with column list: `INSERT INTO table (col1, col2) VALUES (val1, val2)`
- UPSERT with `ON CONFLICT DO NOTHING` to skip conflicting rows
- UPSERT with `ON CONFLICT DO UPDATE SET` to update conflicting rows

#### DDL Enhancements
- `IF NOT EXISTS` clause for `CREATE TABLE`, `CREATE INDEX`, `CREATE PROCEDURE`, `CREATE VIEW`, `CREATE TRIGGER`
- `IF EXISTS` clause for `DROP TABLE`, `DROP INDEX`, `DROP PROCEDURE`, `DROP VIEW`, `DROP TRIGGER`
- `OR REPLACE` clause for `CREATE PROCEDURE`, `CREATE VIEW`, `CREATE TRIGGER`

#### SQL Functions
- String functions: `UPPER`, `LOWER`, `LENGTH`, `CONCAT`, `SUBSTRING`, `TRIM`, `REPLACE`, `LEFT`, `RIGHT`, `REVERSE`, `REPEAT`
- Numeric functions: `ABS`, `ROUND`, `CEIL`, `FLOOR`, `MOD`, `POWER`, `SQRT`
- Date/Time functions: `NOW`, `CURRENT_DATE`, `CURRENT_TIME`, `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, `DATE_ADD`, `DATE_SUB`, `DATEDIFF`
- NULL handling functions: `COALESCE`, `NULLIF`, `IFNULL`
- Type conversion: `CAST`, `CONVERT`

#### Constraints
- `PRIMARY KEY` constraint with uniqueness enforcement
- `FOREIGN KEY` with `REFERENCES` for referential integrity
- `NOT NULL` constraint to prevent NULL values
- `UNIQUE` constraint for column uniqueness
- `AUTO_INCREMENT` / `SERIAL` for automatic sequence generation
- `DEFAULT` values for columns
- `CHECK` constraint for custom validation expressions

#### Stored Procedures, Views, and Triggers
- `CREATE PROCEDURE`, `CALL`, `DROP PROCEDURE` for stored procedures
- `CREATE VIEW`, `DROP VIEW` for virtual tables
- `CREATE TRIGGER`, `DROP TRIGGER` for automatic actions on INSERT/UPDATE/DELETE
- Trigger support for `BEFORE` and `AFTER` timing with `FOR EACH ROW` execution
- `ALTER TABLE` with `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, `MODIFY COLUMN`

#### Extended Column Types
- Numeric: `INT`, `BIGINT`, `SERIAL`, `FLOAT`, `DECIMAL`/`NUMERIC`
- String: `TEXT`, `VARCHAR`
- Boolean: `BOOLEAN` (`TRUE`/`FALSE`)
- Date/Time: `TIMESTAMP`, `DATE`, `TIME`
- Binary: `BLOB` (base64 encoded)
- Structured: `UUID`, `JSONB`

#### Storage Engine
- Write-Ahead Logging (WAL) for durability
- In-memory key-value store with prefix scanning
- Automatic state recovery on startup from WAL
- Binary WAL format for efficient storage
- Transaction support with write buffering
- AES-256-GCM encryption for data at rest (optional)

#### User Management
- `CREATE USER username IDENTIFIED BY 'password'` for creating new users
- `ALTER USER username IDENTIFIED BY 'new_password'` for changing passwords
- `DROP USER [IF EXISTS] username` for removing users

#### Security
- User authentication with username/password credentials
- Secure password storage with bcrypt hashing
- Timing attack prevention for authentication
- Table-level access control via `GRANT`/`REVOKE` statements
- Row-Level Security (RLS) with predicate-based filtering
- Built-in administrator account for bootstrap operations
- Secure admin password setup on first run (generated or user-specified)
- Environment variable configuration for automated deployments (`FLYDB_ADMIN_PASSWORD`)

#### Distributed Features
- Leader-Follower replication topology
- Binary replication protocol over TCP
- Offset-based synchronization for replica catch-up
- Automatic retry on connection failure
- Automatic failover with leader election (Bully algorithm)

#### Performance Features
- Connection pooling for efficient connection management
- Query caching with LRU eviction and TTL expiration
- Automatic cache invalidation on table modifications
- TLS support for encrypted client-server connections

#### Wire Protocol
- Binary protocol (default for CLI) for high-performance applications
- Text-based protocol for debugging and manual testing
- `WATCH` command for real-time table change notifications
- Event streaming for INSERT operations

#### Driver Development (JDBC/ODBC Support)
- Complete binary wire protocol for external driver development
- Server-side cursors with scrollable result sets (Forward-Only, Static, Keyset, Dynamic)
- Metadata queries: `GetTables`, `GetColumns`, `GetPrimaryKeys`, `GetForeignKeys`, `GetIndexes`, `GetTypeInfo`
- Session management with connection options and auto-commit settings
- Transaction control with isolation levels

#### Observability
- Structured logging with `DEBUG`, `INFO`, `WARN`, `ERROR` levels
- JSON log output for log aggregation systems
- Comprehensive error handling with error codes and hints

#### Configuration Management
- TOML configuration file support with auto-discovery
- Configuration file locations: `/etc/flydb/flydb.conf`, `~/.flydb/flydb.conf`, `./flydb.conf`
- Environment variable support with `FLYDB_*` prefix
- Configuration precedence: CLI flags > environment variables > config file > defaults
- Runtime configuration reload with callback support
- Configuration validation with detailed error messages
- Thread-safe configuration access

#### Tools
- `flydb` - TCP database server with interactive configuration wizard
- `flydb-shell` (command: `fsql`) - Interactive command-line client with readline support
- Professional startup banner with version display
- Interactive configuration wizard for first-time setup

#### CLI Enhancements
- Readline support with command history persistence
- Tab completion for SQL keywords and commands
- Multi-line input support for complex queries
- ANSI-aware formatting with color output
- Multiple output formats: table, JSON, plain text
- Progress indicators and spinners for long operations
- Graceful handling of Ctrl+C and Ctrl+D

### Security Notes

- Default admin credentials (`admin`/generated password) should be saved securely
- TLS is recommended for production deployments
- Encryption at rest requires secure key management

---

## [01.26.2] - 2026-01-08

### Changed

#### ⚠️ BREAKING CHANGE: Encryption Enabled by Default

Data-at-rest encryption is now **enabled by default** for improved security. This is a breaking change that affects all new installations and upgrades.

**What this means:**
- FlyDB now requires a passphrase to start when encryption is enabled (the default)
- You must set the `FLYDB_ENCRYPTION_PASSPHRASE` environment variable before starting FlyDB
- Alternatively, you can disable encryption by setting `encryption_enabled = false` in your config file

**Migration for existing users:**

| Scenario | Action Required |
|----------|-----------------|
| New installation | Set `FLYDB_ENCRYPTION_PASSPHRASE` environment variable |
| Upgrade with encryption desired | Set `FLYDB_ENCRYPTION_PASSPHRASE` environment variable |
| Upgrade without encryption | Add `encryption_enabled = false` to config file |

**Example:**

```bash
# Option 1: Enable encryption (recommended)
export FLYDB_ENCRYPTION_PASSPHRASE="your-secure-passphrase"
./flydb -role standalone

# Option 2: Disable encryption (not recommended for production)
# Add to flydb.conf: encryption_enabled = false
./flydb -role standalone
```

> ⚠️ **WARNING**: Keep your passphrase safe! Data cannot be recovered without it.

### Security
- Improved default security posture with encryption enabled by default
- Enhanced error messages when passphrase is missing
- Updated wizard to default to encryption enabled

## [01.26.3] - 2026-01-08

### Fixed

#### Encryption Environment Variable Detection
- Fixed a bug where the `FLYDB_ENCRYPTION_PASSPHRASE` environment variable was not being read when using the interactive wizard mode
- Environment variables are now properly applied after wizard configuration

### Added

#### Interactive Passphrase Setup in Wizard
- The setup wizard now prompts for an encryption passphrase when encryption is enabled
- If `FLYDB_ENCRYPTION_PASSPHRASE` is already set, the wizard detects and uses it automatically
- Users can enter a passphrase interactively or skip to set it via environment variable later
- Configuration summary now shows passphrase status: "enabled (passphrase set)" or "enabled (passphrase required)"

**Wizard Passphrase Flow:**
1. Wizard asks if encryption should be enabled (default: yes)
2. If enabled, checks for `FLYDB_ENCRYPTION_PASSPHRASE` environment variable
3. If not set, prompts user to enter a passphrase interactively
4. User can skip by pressing Enter (must set env var before starting)
5. Summary displays passphrase configuration status

---

## [01.26.4] - 2026-01-08

### ⚠️ BREAKING CHANGE: Multi-Database Mode Now Default

FlyDB now operates in multi-database mode by default. This is a fundamental change that aligns FlyDB with standard database server behavior (like PostgreSQL, MySQL).

**What Changed:**
- Multi-database support is now **always enabled** - there is no single-database mode
- Default data directory changed from `./data` to `/var/lib/flydb`
- The `-db` flag for single database file is deprecated; use `-data-dir` instead
- Server always initializes with a `default` database

**Migration:**
- Existing single-database deployments should migrate data to the new multi-database structure
- Set `FLYDB_DATA_DIR` environment variable or use `-data-dir` flag to specify data location

### Added

#### CLI Database Selection Flag
- Added `-d` / `--database` flag to `fsql` CLI to specify database on startup
- Added `FLYDB_DATABASE` environment variable support
- CLI prompt now always shows current database: `flydb:dbname>`
- Added `\c <database>` shortcut command to switch databases (PostgreSQL-style)

#### Environment Variable for Data Directory
- Added `FLYDB_DATA_DIR` environment variable for configuring the data directory
- Follows the Filesystem Hierarchy Standard with default `/var/lib/flydb`

#### Improved INSPECT Commands

**INSPECT DATABASES** now shows detailed information:
| Column | Description |
|--------|-------------|
| name | Database name |
| owner | Database owner |
| encoding | Character encoding (UTF8, etc.) |
| locale | Locale setting |
| collation | Collation setting |
| tables | Number of tables |
| size | Database size (human-readable) |

**INSPECT DATABASE <name>** now shows:
- Database name, owner, encoding, locale, collation
- Number of tables
- Size (human-readable: KB, MB, GB)
- Creation timestamp
- File path
- Status and storage type

**INSPECT TABLES** now shows:
| Column | Description |
|--------|-------------|
| table_name | Table name |
| columns | Number of columns |
| rows | Number of rows |
| indexes | Number of indexes |

**INSPECT INDEXES** now shows:
| Column | Description |
|--------|-------------|
| index_name | Index name (e.g., `idx_users_email`) |
| table_name | Table the index belongs to |
| column_name | Column being indexed |
| type | Index type (btree) |

**INSPECT USERS** now shows:
| Column | Description |
|--------|-------------|
| username | User name |
| role | User role (admin/user) |
| created | Creation timestamp |

**INSPECT SERVER** now shows additional metrics:
- Server version (from banner)
- Number of tables, indexes, and users

### Changed

#### Default Data Directory
- Changed default data directory from `./data` to `/var/lib/flydb`
- Follows Filesystem Hierarchy Standard for Unix-like systems
- Wizard now displays the default location and explains the directory structure

#### Wizard Improvements
- Removed single-database mode option (multi-database is now always enabled)
- Simplified storage configuration to just ask for data directory
- Added helpful information about database directory structure

### Fixed

#### Binary Protocol Database Commands
- Fixed `CREATE DATABASE` not working through binary protocol (fsql CLI)
- Fixed `DROP DATABASE` not working through binary protocol
- Fixed `USE <database>` not working through binary protocol
- Added helpful error hints when multi-database operations fail

---

## [Unreleased]

### Planned
- Window functions: `OVER`, `PARTITION BY`, `ROW_NUMBER`

---

## Version Numbering

FlyDB uses a date-based versioning scheme: `YY.MM.patch`

- `YY` - Two-digit year (e.g., 26 for 2026)
- `MM` - Two-digit month (e.g., 01 for January)
- `patch` - Patch number within the month

Example: `01.26.1` = January 2026, patch 1

---

## See Also

- [README](README.md) - Project overview and quick start
- [Roadmap](ROADMAP.md) - Future development plans
- [Architecture](docs/architecture.md) - System design

