# Changelog

All notable changes to FlyDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [01.26.1] - 2026-01-06

### Initial Public Release

This is the first public release of FlyDB, a lightweight SQL database written in Go.

### Added

#### Core Database Features
- SQL query support: `CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`
- `SELECT * FROM` to retrieve all columns from a table
- `DISTINCT` to remove duplicate rows from SELECT results
- `UNION` and `UNION ALL` to combine results from multiple SELECT queries
- Subqueries in WHERE clauses with `IN` and `EXISTS`
- `INNER JOIN` with Nested Loop algorithm
- `LEFT JOIN`, `RIGHT JOIN`, `FULL JOIN` support
- `WHERE` clause filtering with equality and comparison operators
- `ORDER BY` with `ASC`/`DESC` sorting
- `LIMIT` and `OFFSET` for result set restriction
- `GROUP BY` for grouping rows
- `HAVING` for filtering groups after aggregation
- Schema persistence across server restarts
- Transactions: `BEGIN`, `COMMIT`, `ROLLBACK` for atomic operations
- B-Tree indexing: `CREATE INDEX` for O(log N) lookups
- Prepared statements: `PREPARE`, `EXECUTE`, `DEALLOCATE`
- Aggregate functions: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- `INTROSPECT` command for database metadata inspection
- Pretty table formatting for SELECT results in CLI
- Row count information in all query results

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

#### Security
- User authentication with username/password credentials
- Secure password storage with bcrypt hashing
- Timing attack prevention for authentication
- Table-level access control via `GRANT`/`REVOKE` statements
- Row-Level Security (RLS) with predicate-based filtering
- Built-in administrator account for bootstrap operations

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
- `flydb` - TCP database server
- `fly-cli` - Interactive command-line client
- Professional startup banner with version display

### Security Notes

- Default admin credentials (`admin`/`admin`) should be changed in production
- TLS is recommended for production deployments
- Encryption at rest requires secure key management

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

