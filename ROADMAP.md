# FlyDB Roadmap

This document outlines the development roadmap for FlyDB, including completed features and planned enhancements.

**Version:** 01.26.1
**Last Updated:** January 7, 2026

---

## Completed Features

### Core Database Features

| Feature | Description | Version |
|---------|-------------|---------|
| SQL Query Support | CREATE TABLE, INSERT, SELECT, UPDATE, DELETE | 01.26.1 |
| DROP TABLE | Remove tables with optional IF EXISTS clause | 01.26.1 |
| DROP INDEX | Remove indexes with optional IF EXISTS clause | 01.26.1 |
| TRUNCATE TABLE | Fast table truncation | 01.26.1 |
| SELECT * FROM | Retrieve all columns from a table | 01.26.1 |
| INNER JOIN | Nested Loop algorithm for table joins | 01.26.1 |
| LEFT/RIGHT/FULL JOIN | Outer join support for all join types | 01.26.1 |
| WHERE Clause | Filtering with =, <>, <, >, <=, >= operators | 01.26.1 |
| LIKE/NOT LIKE | Pattern matching with % and _ wildcards | 01.26.1 |
| BETWEEN | Range operator for value ranges | 01.26.1 |
| IS NULL/IS NOT NULL | NULL value checks | 01.26.1 |
| IN/NOT IN | Value list membership operators | 01.26.1 |
| ORDER BY | ASC/DESC sorting | 01.26.1 |
| LIMIT/OFFSET | Result set restriction and pagination | 01.26.1 |
| Schema Persistence | Schemas survive server restarts | 01.26.1 |
| Transactions | BEGIN, COMMIT, ROLLBACK for atomic operations | 01.26.1 |
| Savepoints | SAVEPOINT, RELEASE SAVEPOINT, ROLLBACK TO SAVEPOINT | 01.26.1 |
| B-Tree Indexing | CREATE INDEX for O(log N) lookups | 01.26.1 |
| Prepared Statements | PREPARE, EXECUTE, DEALLOCATE for query reuse | 01.26.1 |
| Aggregate Functions | COUNT, SUM, AVG, MIN, MAX | 01.26.1 |
| GROUP BY | Group rows for aggregate calculations | 01.26.1 |
| HAVING | Filter groups after aggregation | 01.26.1 |
| DISTINCT | Remove duplicate rows from SELECT results | 01.26.1 |
| UNION/UNION ALL | Combine results from multiple SELECT queries | 01.26.1 |
| INTERSECT/INTERSECT ALL | Return rows common to both queries | 01.26.1 |
| EXCEPT/EXCEPT ALL | Return rows in first query but not in second | 01.26.1 |
| Subqueries | Nested SELECT statements in WHERE clauses | 01.26.1 |
| Stored Procedures | CREATE PROCEDURE, CALL, DROP PROCEDURE | 01.26.1 |
| Views | Virtual tables (CREATE VIEW, DROP VIEW) | 01.26.1 |
| Triggers | Automatic actions on INSERT/UPDATE/DELETE (BEFORE/AFTER) | 01.26.1 |
| ALTER TABLE | ADD/DROP/RENAME/MODIFY COLUMN | 01.26.1 |
| INTROSPECT Command | Database metadata inspection (USERS, TABLES, TABLE, INDEXES, SERVER, STATUS) | 01.26.1 |
| Row Count Information | All queries return affected row counts | 01.26.1 |
| Pretty Table Formatting | Formatted table output in CLI | 01.26.1 |

### INSERT Enhancements

| Feature | Description | Version |
|---------|-------------|---------|
| Multi-row INSERT | INSERT INTO table VALUES (row1), (row2), (row3) | 01.26.1 |
| INSERT with Column List | INSERT INTO table (col1, col2) VALUES (val1, val2) | 01.26.1 |
| ON CONFLICT DO NOTHING | Skip conflicting rows during INSERT (UPSERT) | 01.26.1 |
| ON CONFLICT DO UPDATE | Update conflicting rows during INSERT (UPSERT) | 01.26.1 |

### DDL Enhancements

| Feature | Description | Version |
|---------|-------------|---------|
| IF NOT EXISTS | Clause for CREATE TABLE, INDEX, PROCEDURE, VIEW, TRIGGER | 01.26.1 |
| IF EXISTS | Clause for DROP TABLE, INDEX, PROCEDURE, VIEW, TRIGGER | 01.26.1 |
| OR REPLACE | Clause for CREATE PROCEDURE, VIEW, TRIGGER | 01.26.1 |

### User Management

| Feature | Description | Version |
|---------|-------------|---------|
| CREATE USER | Create new database users with password | 01.26.1 |
| ALTER USER | Change user passwords | 01.26.1 |
| DROP USER | Remove users with optional IF EXISTS clause | 01.26.1 |

### Constraints

| Feature | Description | Version |
|---------|-------------|---------|
| PRIMARY KEY | Unique identifier with NOT NULL | 01.26.1 |
| FOREIGN KEY | REFERENCES constraint with referential integrity | 01.26.1 |
| NOT NULL | Prevent NULL values in columns | 01.26.1 |
| UNIQUE | Ensure column values are unique | 01.26.1 |
| AUTO_INCREMENT/SERIAL | Automatic sequence generation | 01.26.1 |
| DEFAULT | Default values for columns | 01.26.1 |
| CHECK | Custom validation expressions | 01.26.1 |

### SQL Functions

| Category | Functions | Version |
|----------|-----------|---------|
| String Functions | UPPER, LOWER, LENGTH, CONCAT, SUBSTRING, TRIM, REPLACE, LEFT, RIGHT, REVERSE, REPEAT | 01.26.1 |
| Numeric Functions | ABS, ROUND, CEIL, FLOOR, MOD, POWER, SQRT | 01.26.1 |
| Date/Time Functions | NOW, CURRENT_DATE, CURRENT_TIME, YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DATE_ADD, DATE_SUB, DATEDIFF | 01.26.1 |
| NULL Handling | COALESCE, NULLIF, IFNULL | 01.26.1 |
| Type Conversion | CAST, CONVERT | 01.26.1 |

### Extended Column Types

| Type | Description | Version |
|------|-------------|---------|
| INT, BIGINT | Integer types | 01.26.1 |
| SERIAL | Auto-incrementing integer | 01.26.1 |
| FLOAT, DECIMAL/NUMERIC | Floating-point and decimal types | 01.26.1 |
| TEXT, VARCHAR | String types | 01.26.1 |
| BOOLEAN | True/false values | 01.26.1 |
| TIMESTAMP, DATE, TIME | Date and time types | 01.26.1 |
| UUID | Universally unique identifier | 01.26.1 |
| BLOB | Binary data (base64 encoded) | 01.26.1 |
| JSONB | Binary JSON for structured data | 01.26.1 |

### Storage Engine

| Feature | Description | Version |
|---------|-------------|---------|
| Write-Ahead Logging (WAL) | Durability through append-only log | 01.26.1 |
| In-Memory KV Store | Fast reads with prefix scanning | 01.26.1 |
| Automatic Recovery | State reconstruction from WAL on startup | 01.26.1 |
| Binary WAL Format | Efficient storage format | 01.26.1 |
| Transaction Support | Write buffering with commit/rollback | 01.26.1 |
| Data Encryption at Rest | AES-256-GCM encryption for WAL entries | 01.26.1 |

### Security

| Feature | Description | Version |
|---------|-------------|---------|
| User Authentication | Username/password credentials | 01.26.1 |
| bcrypt Password Hashing | Secure credential storage | 01.26.1 |
| Timing Attack Prevention | Constant-time password comparison | 01.26.1 |
| Table-Level Access Control | GRANT/REVOKE statements | 01.26.1 |
| Row-Level Security (RLS) | Predicate-based row filtering | 01.26.1 |
| Built-in Admin Account | Bootstrap operations | 01.26.1 |
| Secure Admin Password Setup | Generated or user-specified on first run | 01.26.1 |
| Environment Variable Config | FLYDB_ADMIN_PASSWORD for automated deployments | 01.26.1 |

### Distributed Features

| Feature | Description | Version |
|---------|-------------|---------|
| Leader-Follower Replication | WAL streaming to followers | 01.26.1 |
| Binary Replication Protocol | Efficient TCP-based replication | 01.26.1 |
| Offset-Based Sync | Replica catch-up from any position | 01.26.1 |
| Automatic Retry | Connection failure recovery | 01.26.1 |
| Automatic Failover | Leader election using Bully algorithm | 01.26.1 |

### Performance Features

| Feature | Description | Version |
|---------|-------------|---------|
| Connection Pooling | Efficient connection management | 01.26.1 |
| Query Caching | LRU cache with TTL and auto-invalidation | 01.26.1 |
| TLS Support | Encrypted client-server connections | 01.26.1 |

### Wire Protocol

| Feature | Description | Version |
|---------|-------------|---------|
| Binary Protocol | High-performance binary encoding (default) | 01.26.1 |
| Text Protocol | Human-readable for debugging | 01.26.1 |
| WATCH Command | Real-time table change notifications | 01.26.1 |

### Observability

| Feature | Description | Version |
|---------|-------------|---------|
| Structured Logging | DEBUG, INFO, WARN, ERROR levels | 01.26.1 |
| JSON Log Output | For log aggregation systems | 01.26.1 |
| Error Codes & Hints | Comprehensive error handling | 01.26.1 |

### Configuration

| Feature | Description | Version |
|---------|-------------|---------|
| Configuration File Support | TOML configuration file with auto-discovery | 01.26.1 |
| Environment Variables | FLYDB_* environment variable support | 01.26.1 |
| Configuration Precedence | CLI flags > env vars > file > defaults | 01.26.1 |
| Runtime Reload | Hot reload of configuration without restart | 01.26.1 |

### Driver Development (JDBC/ODBC Support)

| Feature | Description | Version |
|---------|-------------|---------|
| Binary Wire Protocol | Complete protocol for external driver development | 01.26.1 |
| Server-Side Cursors | Scrollable cursors (Forward-Only, Static, Keyset, Dynamic) | 01.26.1 |
| Metadata Queries | GetTables, GetColumns, GetPrimaryKeys, GetForeignKeys, GetIndexes, GetTypeInfo | 01.26.1 |
| Session Management | Connection options, auto-commit settings | 01.26.1 |
| Transaction Control | Isolation levels support | 01.26.1 |

### CLI Features

| Feature | Description | Version |
|---------|-------------|---------|
| Interactive Configuration Wizard | First-time setup wizard for server configuration | 01.26.1 |
| Readline Support | Command history persistence and line editing | 01.26.1 |
| Tab Completion | Auto-complete for SQL keywords and commands | 01.26.1 |
| Multi-line Input | Support for complex multi-line queries | 01.26.1 |
| ANSI-aware Formatting | Color output with proper terminal handling | 01.26.1 |
| Multiple Output Formats | Table, JSON, and plain text output | 01.26.1 |
| Progress Indicators | Spinners and progress bars for long operations | 01.26.1 |

---

## Planned Features

### Critical Priority

| Feature | Description | Status |
|---------|-------------|--------|
| WAL Compaction | Implement checkpointing and WAL compaction to prevent unbounded file growth | Planned |
| Backup and Restore | BACKUP DATABASE and RESTORE DATABASE commands for data protection | Planned |
| Transaction Atomicity Fix | Improve commit mechanism to prevent partial commits on failure | Planned |

### High Priority

| Feature | Description | Status |
|---------|-------------|--------|
| ALTER TABLE ADD/DROP CONSTRAINT | Complete ALTER TABLE support for adding and dropping constraints | Planned |
| EXPLAIN Command | Query plan visualization for debugging and optimization | Planned |
| Health Check Endpoint | HTTP endpoint for load balancer health checks | Planned |
| Connection Limits | Configurable max connections and rate limiting | Planned |
| Slow Query Logging | Log queries exceeding configurable time threshold | Planned |

### Medium Priority

| Feature | Description | Status |
|---------|-------------|--------|
| Window Functions | OVER, PARTITION BY, ROW_NUMBER, RANK, DENSE_RANK | Planned |
| CASE Expressions | CASE WHEN ... THEN ... ELSE ... END support | Planned |
| Multi-column Indexes | CREATE INDEX on multiple columns for composite lookups | Planned |
| Index Range Queries | Use B-Tree indexes for BETWEEN, >, <, >=, <= operators | Planned |
| Prometheus Metrics | Export database metrics in Prometheus format | Planned |

### Low Priority

| Feature | Description | Status |
|---------|-------------|--------|
| Point-in-Time Recovery | Restore database to specific timestamp using WAL | Planned |
| Query Plan Caching | Cache parsed query plans for repeated queries | Planned |
| Hash Join Algorithm | Alternative join algorithm for large table joins | Planned |
| Merge Join Algorithm | Sorted merge join for pre-sorted data | Planned |
| Table Statistics | Collect and maintain table statistics for query optimization | Planned |
| Cost-Based Optimizer | Use statistics to choose optimal query execution plans | Planned |
| Go Benchmark Tests | Performance regression testing with Go benchmarks | Planned |
| COPY Command | Bulk data import/export in CSV format | Planned |
| Materialized Views | Precomputed views with automatic refresh | Planned |

---

## Strategic Initiative: Hybrid Disk-Based Storage Engine

<!--
This initiative enables FlyDB to handle datasets larger than available RAM, similar to PostgreSQL's
architecture. Currently, FlyDB requires all data to fit in memory, which limits its use cases to
smaller datasets. By implementing a disk-based storage engine with an in-memory buffer pool, FlyDB
can scale to handle terabytes of data while maintaining high performance for frequently accessed data.
-->

### Overview

**Current Architecture Limitation:**
FlyDB currently uses an in-memory key-value store with Write-Ahead Logging (WAL) for durability. While this provides excellent read/write performance, it requires all data to fit in available RAM, limiting FlyDB to datasets smaller than system memory.

**Target Architecture (PostgreSQL-style):**
A hybrid storage engine where disk-based storage serves as the primary data store, with an intelligent in-memory buffer pool/cache for frequently accessed data. This enables FlyDB to handle databases significantly larger than available RAM while maintaining high performance.

### Architecture Comparison

| Aspect | Current (In-Memory) | Target (Disk-Based) |
|--------|---------------------|---------------------|
| Primary Storage | RAM | Disk (pages/blocks) |
| Data Capacity | Limited by RAM | Limited by disk space |
| Read Performance | O(1) memory access | O(1) for cached, O(disk) for uncached |
| Write Performance | O(1) + WAL append | Buffer pool + WAL + background flush |
| Crash Recovery | Full WAL replay | Checkpoint + partial WAL replay |
| Memory Usage | 100% of data | Configurable buffer pool size |

### Implementation Phases

#### Phase 1: Storage Abstraction Layer

**Priority:** High | **Status:** Planned

| Milestone | Description | Status |
|-----------|-------------|--------|
| Storage Interface | Define abstract `StorageEngine` interface supporting both engines | Planned |
| Engine Registry | Runtime engine selection via configuration | Planned |
| Dual-Engine Support | Allow both engines to coexist in the same binary | Planned |
| Configuration Options | `--storage-engine=memory\|disk` flag and config file support | Planned |

**Deliverable:** FlyDB can be configured to use either storage engine at startup.

#### Phase 2: Page-Based Storage

**Priority:** High | **Status:** Planned

| Milestone | Description | Status |
|-----------|-------------|--------|
| Page Format | Fixed-size pages (8KB default) with header, slots, and tuples | Planned |
| Page Manager | Read/write pages to disk with page ID addressing | Planned |
| Free Space Map | Track available space in each page for insertions | Planned |
| Heap File Structure | Organize table data as a collection of pages | Planned |
| Slotted Page Layout | Variable-length tuple storage within pages | Planned |

**Deliverable:** Tables can be stored as page files on disk.

#### Phase 3: Buffer Pool Manager

**Priority:** High | **Status:** Planned

| Milestone | Description | Status |
|-----------|-------------|--------|
| Buffer Pool | Fixed-size in-memory cache for disk pages | Planned |
| Page Replacement | LRU-K or Clock algorithm for eviction | Planned |
| Pin/Unpin Semantics | Prevent eviction of pages in active use | Planned |
| Dirty Page Tracking | Track modified pages for write-back | Planned |
| Background Flusher | Async write-back of dirty pages | Planned |
| Buffer Pool Sizing | Configurable via `--buffer-pool-size` | Planned |

**Deliverable:** Frequently accessed pages are cached in memory with configurable pool size.

#### Phase 4: Disk-Based Indexes

**Priority:** Medium | **Status:** Planned

| Milestone | Description | Status |
|-----------|-------------|--------|
| B+Tree on Disk | Persistent B+Tree index structure | Planned |
| Index Page Format | Internal nodes and leaf nodes as pages | Planned |
| Index Buffer Integration | Index pages managed by buffer pool | Planned |
| Index-Only Scans | Return results directly from index when possible | Planned |

**Deliverable:** Indexes persist to disk and support datasets larger than RAM.

#### Phase 5: Checkpointing and Recovery

**Priority:** Medium | **Status:** Planned

| Milestone | Description | Status |
|-----------|-------------|--------|
| Checkpoint Manager | Periodic flush of all dirty pages to disk | Planned |
| Checkpoint WAL Record | Mark checkpoint position in WAL | Planned |
| Fast Recovery | Replay WAL only from last checkpoint | Planned |
| Fuzzy Checkpoints | Non-blocking checkpoint with concurrent writes | Planned |

**Deliverable:** Crash recovery is fast regardless of total data size.

#### Phase 6: Migration Tools

**Priority:** Medium | **Status:** Planned

| Milestone | Description | Status |
|-----------|-------------|--------|
| Online Migration | `MIGRATE TO DISK ENGINE` command | Planned |
| Progress Tracking | Report migration progress for large databases | Planned |
| Rollback Support | Ability to revert to in-memory engine | Planned |
| Data Verification | Checksum validation during migration | Planned |

**Deliverable:** Existing FlyDB deployments can migrate to disk-based storage without downtime.

### Configuration Options

```yaml
# flydb.yaml configuration example
storage:
  # Engine selection: "memory" (default) or "disk"
  engine: disk

  # Disk engine settings (only used when engine=disk)
  disk:
    # Directory for data files
    data_directory: /var/lib/flydb/data

    # Page size in bytes (default: 8192)
    page_size: 8192

    # Buffer pool size (default: 25% of system RAM)
    buffer_pool_size: 2GB

    # Background flusher interval
    flush_interval: 1s

    # Checkpoint interval
    checkpoint_interval: 5m

  # Memory engine settings (only used when engine=memory)
  memory:
    # Maximum memory usage (default: unlimited)
    max_memory: 8GB
```

### Performance Trade-offs

| Workload | In-Memory Engine | Disk-Based Engine |
|----------|------------------|-------------------|
| Small datasets (< RAM) | Optimal | Good (slight overhead) |
| Large datasets (> RAM) | Not supported | Optimal |
| Read-heavy (hot data) | Optimal | Optimal (cached) |
| Read-heavy (cold data) | Optimal | Disk I/O bound |
| Write-heavy | Optimal | Good (buffered writes) |
| Mixed workloads | Optimal | Good to Optimal |
| Memory-constrained | Limited | Optimal |

### Migration Strategy

1. **Backward Compatibility:** The in-memory engine remains the default and is fully supported
2. **Gradual Adoption:** Users can test disk engine on non-production workloads first
3. **Online Migration:** Production databases can migrate without downtime
4. **Hybrid Operation:** Different tables could potentially use different engines (future)

### Success Criteria

- [ ] Disk-based engine passes all existing integration tests
- [ ] Performance within 2x of in-memory engine for cached workloads
- [ ] Successfully handles 10x RAM dataset size
- [ ] Migration completes without data loss
- [ ] Crash recovery completes in < 30 seconds regardless of data size

---

## Version History

| Version | Release Date | Highlights |
|---------|--------------|------------|
| 01.26.1 | January 2026 | Initial public release with full SQL support, triggers, replication, security, and encryption |

---

## Contributing

We welcome contributions! If you'd like to work on a planned feature or propose a new one:

1. Check the [Issues](https://github.com/firefly-oss/flydb/issues) for existing discussions
2. Open a new issue to discuss your proposal
3. Submit a pull request with your implementation

---

## See Also

- [README](README.md) - Project overview and quick start
- [Architecture](docs/architecture.md) - System design
- [Changelog](CHANGELOG.md) - Detailed version history

