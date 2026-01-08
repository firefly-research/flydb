# FlyDB Architecture

This document describes the overall system architecture of FlyDB, including component relationships, data flow, and high-level design.

## Overview

FlyDB is a lightweight SQL database written in Go, designed to demonstrate core database concepts while remaining fully functional. It follows a layered architecture that separates concerns and enables modularity.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Client Applications                        │
│         (fsql CLI, TCP clients, connection pools, drivers)      │
└─────────────────────────────────────────────────────────────────┘
                                │
                    ┌───────────┴───────────┐
                    ▼                       ▼
┌──────────────────────────┐   ┌──────────────────────────┐
│   Text Protocol (:8888)  │   │  Binary Protocol (:8889) │
│   • Line-based commands  │   │  • Length-prefixed JSON  │
│   • Human-readable       │   │  • Efficient for drivers │
└──────────────────────────┘   └──────────────────────────┘
                    │                       │
                    └───────────┬───────────┘
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Server Layer                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │ TCP Server  │  │ Replicator  │  │   Cluster Manager       │  │
│  │ (server.go) │  │(replication)│  │   (leader election)     │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              Database Manager                           │    │
│  │  • Multi-database support    • Per-connection context   │    │
│  │  • Database lifecycle        • Metadata management      │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                         SQL Layer                               │
│  ┌─────────┐  ┌─────────┐  ┌──────────┐  ┌─────────────────┐    │
│  │  Lexer  │→ │ Parser  │→ │ Executor │→ │ Query Cache     │    │
│  └─────────┘  └─────────┘  └──────────┘  └─────────────────┘    │
│                                │                                │
│              ┌─────────────────┼─────────────────┐              │
│              ▼                 ▼                 ▼              │
│      ┌─────────────┐   ┌─────────────┐   ┌─────────────┐        │
│      │   Catalog   │   │  Prepared   │   │  Triggers   │        │
│      │  (schemas)  │   │ Statements  │   │  Manager    │        │
│      └─────────────┘   └─────────────┘   └─────────────┘        │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              Internationalization (I18N)                │    │
│  │  ┌───────────┐  ┌───────────┐  ┌───────────────────┐    │    │
│  │  │ Collator  │  │  Encoder  │  │  Locale Support   │    │    │
│  │  │ (sorting) │  │(validation│  │  (golang.org/x/   │    │    │
│  │  │           │  │           │  │   text/collate)   │    │    │
│  │  └───────────┘  └───────────┘  └───────────────────┘    │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Auth Layer                               │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │ Authentication  │  │  Authorization  │  │ Row-Level       │  │
│  │ (bcrypt)        │  │  (GRANT/REVOKE) │  │ Security (RLS)  │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                       Storage Layer                             │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              Unified Disk Storage Engine                │    │
│  │   (Page-Based Storage with Intelligent Caching)         │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                    Buffer Pool                          │    │
│  │   - LRU-K page replacement    - Auto-sized by memory    │    │
│  │   - Pin/unpin semantics       - Dirty page tracking     │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                    Heap File                            │    │
│  │   - 8KB pages               - Slotted page layout       │    │
│  │   - Free space management   - Record-level operations   │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │ Transaction │  │  Encryptor  │  │   Database Instance     │  │
│  │  Manager    │  │ (AES-256)   │  │   (per-db isolation)    │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              Write-Ahead Log (WAL)                      │    │
│  │   - Durability guarantees   - Crash recovery            │    │
│  │   - Replication support     - Optional encryption       │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                       File System                               │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  data_dir/                                              │    │
│  │  ├── default/                                           │    │
│  │  │   ├── data.db      (heap file with pages)            │    │
│  │  │   └── wal.fdb      (write-ahead log)                 │    │
│  │  ├── myapp/                                             │    │
│  │  │   ├── data.db      (heap file with pages)            │    │
│  │  │   └── wal.fdb      (write-ahead log)                 │    │
│  │  └── _system/                                           │    │
│  │      ├── data.db      (system metadata)                 │    │
│  │      └── wal.fdb      (system WAL)                      │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

## Component Overview

### Server Layer (`internal/server/`)

The server layer handles all network communication and coordination:

| Component | File | Purpose |
|-----------|------|---------|
| TCP Server | `server.go` | Multi-threaded connection handling, command dispatch |
| Replicator | `replication.go` | Leader-Follower WAL streaming replication |
| Cluster Manager | `cluster.go` | Automatic failover via Bully algorithm |

### SQL Layer (`internal/sql/`)

The SQL layer processes and executes SQL statements:

| Component | File | Purpose |
|-----------|------|---------|
| Lexer | `lexer.go` | Tokenizes SQL input into tokens |
| Parser | `parser.go` | Builds Abstract Syntax Tree (AST) |
| AST | `ast.go` | Statement and expression node definitions |
| Executor | `executor.go` | Executes AST against storage |
| Catalog | `catalog.go` | Manages table schemas |
| Types | `types.go` | SQL type validation and normalization |
| Prepared Statements | `prepared.go` | Parameterized query support |

### Auth Layer (`internal/auth/`)

The auth layer provides security features:

| Component | File | Purpose |
|-----------|------|---------|
| AuthManager | `auth.go` | User management, authentication, authorization |

### Storage Layer (`internal/storage/`)

The storage layer provides persistence and data management using a unified disk-based engine:

| Component | File | Purpose |
|-----------|------|---------|
| Engine | `engine.go` | Storage interface definition |
| StorageEngine | `storage_engine.go` | Extended interface with Sync/Stats |
| Factory | `factory.go` | Engine creation with auto-sizing buffer pool |
| WAL | `wal.go` | Write-Ahead Log for durability |
| B-Tree | `btree.go` | Index data structure |
| Index Manager | `index.go` | Index lifecycle management |
| Transaction | `transaction.go` | ACID transaction support |
| Encryptor | `encryption.go` | AES-256-GCM encryption |
| Database | `database.go` | Database struct with metadata |
| DatabaseManager | `database.go` | Multi-database lifecycle management |
| Collation | `collation.go` | String comparison rules |
| Encoding | `encoding.go` | Character encoding validation |

### Disk Storage Engine (`internal/storage/disk/`)

The disk storage engine provides PostgreSQL-style page-based storage with intelligent caching:

| Component | File | Purpose |
|-----------|------|---------|
| Page | `page.go` | 8KB page with slotted layout |
| HeapFile | `heap_file.go` | Page allocation and free list management |
| BufferPool | `buffer_pool.go` | LRU-K page caching with auto-sizing |
| DiskStorageEngine | `disk_engine.go` | Unified disk-based Engine implementation |
| Checkpoint | `checkpoint.go` | Periodic full database snapshots |

**Key Features:**
- **Auto-sized Buffer Pool**: Automatically sizes based on available system memory (25% of RAM)
- **LRU-K Replacement**: Intelligent page eviction based on access frequency
- **Dirty Page Tracking**: Efficient write-back of modified pages
- **Checkpoint Recovery**: Fast startup by loading from checkpoint + WAL replay

### CLI/Shell (`cmd/flydb-shell/`)

The interactive shell provides a PostgreSQL-like experience:

| Feature | Description |
|---------|-------------|
| Multi-mode | Normal mode (commands) and SQL mode (direct SQL) |
| Tab completion | Commands, keywords, table names |
| History | Persistent command history with readline |
| Database context | Tracks current database, shown in prompt |
| Backslash commands | `\dt`, `\du`, `\db`, `\c`, etc. |

**Shell Prompt Format:**
```
flydb>                    -- Normal mode (default database)
flydb/mydb>               -- Normal mode (non-default database)
flydb/mydb[sql]>          -- SQL mode (non-default database)
```

### Supporting Packages

| Package | Purpose |
|---------|---------|
| `internal/cache/` | LRU query result cache with TTL expiration |
| `internal/pool/` | Client-side connection pooling |
| `internal/protocol/` | Binary wire protocol for JDBC/ODBC drivers |
| `internal/sdk/` | SDK types for driver development (cursors, sessions, transactions) |
| `internal/errors/` | Structured error handling with codes |
| `internal/logging/` | Structured logging framework |
| `internal/banner/` | Startup banner display |
| `internal/config/` | Configuration management with YAML support |
| `internal/wizard/` | Interactive setup wizard |
| `pkg/cli/` | CLI utilities (colors, formatting, prompts) |

## Data Flow

### Query Execution Flow

```
1. Client sends: "SQL SELECT * FROM users WHERE id = 1"
                            │
                            ▼
2. Server receives command, extracts SQL statement
                            │
                            ▼
3. Lexer tokenizes: [SELECT, *, FROM, users, WHERE, id, =, 1]
                            │
                            ▼
4. Parser builds AST: SelectStmt{
                        TableName: "users",
                        Columns: ["*"],
                        Where: {Column: "id", Value: "1"}
                      }
                            │
                            ▼
5. Executor checks permissions via AuthManager
                            │
                            ▼
6. Executor queries KVStore: Scan("row:users:")
                            │
                            ▼
7. KVStore returns matching rows from in-memory HashMap
                            │
                            ▼
8. Executor applies WHERE filter and RLS conditions
                            │
                            ▼
9. Results formatted and sent to client
```

### Write Path (Durability)

```
1. Client sends: "SQL INSERT INTO users VALUES ('alice', 25)"
                            │
                            ▼
2. Executor validates against schema (Catalog)
                            │
                            ▼
3. WAL.Write() appends operation to disk
   ┌─────────┬───────────┬─────────────┬─────────────┬─────────────┐
   │ Op (1B) │ KeyLen(4B)│ Key (var)   │ ValLen (4B) │ Value (var) │
   └─────────┴───────────┴─────────────┴─────────────┴─────────────┘
                            │
                            ▼
4. KVStore updates in-memory HashMap
                            │
                            ▼
5. IndexManager updates B-Tree indexes
                            │
                            ▼
6. OnInsert callback notifies WATCH subscribers
                            │
                            ▼
7. "INSERT 1" returned to client
```

### Replication Flow

```
┌─────────────────┐                    ┌─────────────────┐
│     LEADER      │                    │    FOLLOWER     │
│                 │                    │                 │
│  ┌───────────┐  │                    │  ┌───────────┐  │
│  │  KVStore  │  │                    │  │  KVStore  │  │
│  └─────┬─────┘  │                    │  └─────▲─────┘  │
│        │        │                    │        │        │
│  ┌─────▼─────┐  │   WAL Streaming    │  ┌─────┴─────┐  │
│  │    WAL    │──┼───────────────────►│  │    WAL    │  │
│  └───────────┘  │   (100ms polling)  │  └───────────┘  │
│                 │                    │                 │
└─────────────────┘                    └─────────────────┘

1. Follower connects to Leader's replication port
2. Follower sends current WAL offset
3. Leader streams WAL entries from that offset
4. Follower applies entries to local KVStore
5. Follower updates its WAL offset
```

## Multi-Database Architecture

FlyDB supports multiple isolated databases within a single server instance:

```
┌─────────────────────────────────────────────────────────────────┐
│                      DatabaseManager                            │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │   Database:     │  │   Database:     │  │   Database:     │  │
│  │   "default"     │  │   "analytics"   │  │   "staging"     │  │
│  │                 │  │                 │  │                 │  │
│  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │  │
│  │  │  KVStore  │  │  │  │  KVStore  │  │  │  │  KVStore  │  │  │
│  │  └───────────┘  │  │  └───────────┘  │  │  └───────────┘  │  │
│  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │  │
│  │  │  Executor │  │  │  │  Executor │  │  │  │  Executor │  │  │
│  │  └───────────┘  │  │  └───────────┘  │  │  └───────────┘  │  │
│  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │  │
│  │  │  Catalog  │  │  │  │  Catalog  │  │  │  │  Catalog  │  │  │
│  │  └───────────┘  │  │  └───────────┘  │  │  └───────────┘  │  │
│  │                 │  │                 │  │                 │  │
│  │  Metadata:      │  │  Metadata:      │  │  Metadata:      │  │
│  │  - Encoding     │  │  - Encoding     │  │  - Encoding     │  │
│  │  - Locale       │  │  - Locale       │  │  - Locale       │  │
│  │  - Collation    │  │  - Collation    │  │  - Collation    │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
│                                                                 │
│  File System:                                                   │
│  data_dir/                                                      │
│  ├── default.fdb                                                │
│  ├── analytics.fdb                                              │
│  └── staging.fdb                                                │
└─────────────────────────────────────────────────────────────────┘
```

### Database Isolation

Each database has:
- **Separate KVStore**: Complete data isolation
- **Own Executor**: Independent query execution
- **Own Catalog**: Separate table schemas
- **Own Indexes**: B-Tree indexes scoped to database
- **Metadata**: Encoding, locale, collation, owner

### Database Metadata

| Property | Description | Values |
|----------|-------------|--------|
| Owner | User who created the database | Username |
| Encoding | Character encoding | UTF8, LATIN1, ASCII, UTF16 |
| Locale | Locale for sorting | en_US, de_DE, fr_FR, etc. |
| Collation | String comparison rules | default, binary, nocase, unicode |
| Description | Optional description | Free text |
| ReadOnly | Read-only mode | true/false |

### Connection Database Context

Each client connection maintains its own database context:

```
┌─────────────────┐     ┌─────────────────┐
│  Connection 1   │     │  Connection 2   │
│  Database: app  │     │  Database: logs │
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
┌─────────────────┐     ┌─────────────────┐
│  Executor: app  │     │  Executor: logs │
└─────────────────┘     └─────────────────┘
```

## Collation, Encoding, and Locale System

FlyDB provides comprehensive internationalization support through its collation, encoding, and locale subsystems. These features enable proper handling of text data across different languages and character sets.

### Collation Architecture

Collation determines how strings are compared and sorted. FlyDB implements the `Collator` interface:

```go
type Collator interface {
    Compare(a, b string) int  // Returns -1, 0, or 1
    Equal(a, b string) bool   // Equality check
    SortKey(s string) []byte  // Key for efficient sorting
}
```

**Supported Collations:**

| Collation | Description | Use Case |
|-----------|-------------|----------|
| `default` | Go's native string comparison | General purpose, fast |
| `nocase` | Case-insensitive comparison | Email addresses, usernames |
| `binary` | Byte-by-byte comparison | Exact matching, hashes |
| `unicode` | Locale-aware Unicode collation | International text |

**Collation Flow in Query Execution:**

```
┌─────────────────────────────────────────────────────────────────┐
│                      SQL Query                                  │
│  SELECT * FROM users WHERE name = 'José' ORDER BY name          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Executor                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ Collator (from database metadata)                       │    │
│  │ - Unicode collator with locale "es_ES"                  │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                  │
│              ┌───────────────┴───────────────┐                  │
│              ▼                               ▼                  │
│  ┌─────────────────────┐         ┌─────────────────────┐        │
│  │ WHERE Evaluation    │         │ ORDER BY Sorting    │        │
│  │ collator.Equal()    │         │ collator.Compare()  │        │
│  └─────────────────────┘         └─────────────────────┘        │
└─────────────────────────────────────────────────────────────────┘
```

### Encoding Architecture

Encoding ensures data integrity by validating that text conforms to the database's character encoding:

```go
type Encoder interface {
    Validate(s string) error      // Check if string is valid
    Encode(s string) ([]byte, error)   // Convert to bytes
    Decode(b []byte) (string, error)   // Convert from bytes
    Name() string                 // Encoding name
}
```

**Supported Encodings:**

| Encoding | Description | Byte Range |
|----------|-------------|------------|
| `UTF8` | Unicode UTF-8 (default) | 1-4 bytes per char |
| `ASCII` | 7-bit ASCII | 1 byte per char |
| `LATIN1` | ISO-8859-1 | 1 byte per char |
| `UTF16` | Unicode UTF-16 | 2-4 bytes per char |

**Encoding Validation Flow:**

```
┌─────────────────────────────────────────────────────────────────┐
│  INSERT INTO users (name) VALUES ('Müller')                     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Executor                                   │
│                              │                                  │
│              ┌───────────────┴───────────────┐                  │
│              ▼                               ▼                  │
│  ┌─────────────────────┐         ┌─────────────────────┐        │
│  │ Type Validation     │         │ Encoding Validation │        │
│  │ ValidateValue()     │         │ encoder.Validate()  │        │
│  └─────────────────────┘         └─────────────────────┘        │
│              │                               │                  │
│              │         ┌─────────────────────┘                  │
│              │         │                                        │
│              │    ┌────▼────┐                                   │
│              │    │ ASCII?  │──No──► Error: invalid encoding    │
│              │    │ UTF8?   │──Yes─► Continue                   │
│              │    └─────────┘                                   │
│              ▼                                                  │
│  ┌─────────────────────┐                                        │
│  │ Store in KVStore    │                                        │
│  └─────────────────────┘                                        │
└─────────────────────────────────────────────────────────────────┘
```

### Locale Support

Locale affects how the Unicode collator sorts and compares strings. FlyDB uses Go's `golang.org/x/text/collate` package for locale-aware operations:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Locale Examples                              │
├─────────────────────────────────────────────────────────────────┤
│  Locale: en_US                                                  │
│  Sort: A, B, C, ... Z, a, b, c, ... z                           │
│                                                                 │
│  Locale: de_DE                                                  │
│  Sort: A, Ä, B, C, ... (ä sorts with a)                         │
│  Special: ß = ss                                                │
│                                                                 │
│  Locale: sv_SE                                                  │
│  Sort: A, B, C, ... Z, Å, Ä, Ö                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Database Creation with I18N Options

```sql
CREATE DATABASE german_app
  ENCODING UTF8
  LOCALE de_DE
  COLLATION unicode;
```

This creates a database where:
- All text is validated as UTF-8
- String comparisons use German sorting rules
- Umlauts (ä, ö, ü) sort correctly with their base letters

## Key Storage Conventions

FlyDB uses a key prefix convention to organize data in the KVStore:

| Prefix | Purpose | Example |
|--------|---------|---------|
| `row:<table>:<id>` | Table row data (JSON) | `row:users:1` |
| `seq:<table>` | Auto-increment sequence | `seq:users` |
| `schema:<table>` | Table schema definition | `schema:users` |
| `_sys_users:<username>` | User credentials | `_sys_users:alice` |
| `_sys_privs:<user>:<table>` | User permissions | `_sys_privs:alice:orders` |
| `_sys_db_privs:<user>:<db>` | Database permissions | `_sys_db_privs:alice:analytics` |
| `_sys_db_meta` | Database metadata | `_sys_db_meta` |
| `index:<table>:<column>` | Index metadata | `index:users:email` |
| `proc:<name>` | Stored procedure | `proc:get_user` |
| `view:<name>` | View definition | `view:active_users` |

## Disk Storage Engine Architecture

The disk storage engine provides PostgreSQL-style page-based storage for datasets larger than RAM:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Disk Storage Engine                          │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                   Buffer Pool (LRU)                     │    │
│  │   ┌─────────┐ ┌─────────┐ ┌─────────┐     ┌─────────┐   │    │
│  │   │ Frame 0 │ │ Frame 1 │ │ Frame 2 │ ... │ Frame N │   │    │
│  │   │ [Page]  │ │ [Page]  │ │ [Page]  │     │ [Page]  │   │    │
│  │   │ pin:2   │ │ pin:0   │ │ pin:1   │     │ pin:0   │   │    │
│  │   └─────────┘ └─────────┘ └─────────┘     └─────────┘   │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                  │
│                              ▼                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                    Heap File                            │    │
│  │   ┌─────────┐ ┌─────────┐ ┌─────────┐     ┌─────────┐   │    │
│  │   │ Header  │ │ Page 1  │ │ Page 2  │ ... │ Page N  │   │    │
│  │   │ (8KB)   │ │ (8KB)   │ │ (8KB)   │     │ (8KB)   │   │    │
│  │   └─────────┘ └─────────┘ └─────────┘     └─────────┘   │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

### Page Layout (Slotted Page)

Each 8KB page uses a slotted page layout for variable-length records:

```
┌─────────────────────────────────────────────────────────────────┐
│                        Page (8192 bytes)                        │
├─────────────────────────────────────────────────────────────────┤
│  Page Header (24 bytes)                                         │
│  ┌─────────┬──────────┬───────┬───────────┬───────────┬───────┐ │
│  │ PageID  │ PageType │ Flags │ SlotCount │ FreeSpace │  LSN  │ │
│  │  (4B)   │   (1B)   │ (1B)  │   (2B)    │   (4B)    │ (4B)  │ │
│  └─────────┴──────────┴───────┴───────────┴───────────┴───────┘ │
├─────────────────────────────────────────────────────────────────┤
│  Slot Array (grows down)                                        │
│  ┌──────────────┬──────────────┬──────────────┐                 │
│  │ Slot 0       │ Slot 1       │ Slot 2       │ ...             │
│  │ offset:len   │ offset:len   │ offset:len   │                 │
│  └──────────────┴──────────────┴──────────────┘                 │
├─────────────────────────────────────────────────────────────────┤
│                     Free Space                                  │
├─────────────────────────────────────────────────────────────────┤
│  Records (grow up from bottom)                                  │
│  ┌──────────────┬──────────────┬──────────────┐                 │
│  │ Record 2     │ Record 1     │ Record 0     │                 │
│  │ (variable)   │ (variable)   │ (variable)   │                 │
│  └──────────────┴──────────────┴──────────────┘                 │
└─────────────────────────────────────────────────────────────────┘
```

### Configuration

The storage engine is selected via configuration:

```toml
# Storage engine: "memory" (default) or "disk"
storage_engine = "disk"

# Buffer pool size in pages (8KB each)
buffer_pool_size = 1024  # 8MB

# Checkpoint interval in seconds (0 = disabled)
checkpoint_secs = 60
```

Or via environment variables:
- `FLYDB_STORAGE_ENGINE`: "memory" or "disk"
- `FLYDB_BUFFER_POOL_SIZE`: Number of pages
- `FLYDB_CHECKPOINT_SECS`: Checkpoint interval

## Thread Safety Model

FlyDB uses Go's concurrency primitives for thread safety:

- **KVStore**: `sync.RWMutex` for concurrent read access, exclusive writes
- **BufferPool**: `sync.Mutex` for frame allocation and eviction
- **WAL**: `sync.Mutex` for serialized append operations
- **Server**: Per-connection goroutines with mutex-protected shared state
- **Subscribers**: `sync.Mutex` protects the WATCH subscription map
- **Transactions**: Per-connection transaction isolation

## Network Protocols

### Text Protocol (Port 8888)

Simple line-based protocol for human interaction:

```
Request:  <COMMAND> [arguments]\n
Response: <RESULT>\n

Commands:
  PING              → PONG
  AUTH user pass    → AUTH OK | ERROR
  SQL <statement>   → Result | ERROR
  WATCH <table>     → WATCH OK (then EVENT notifications)
```

### Binary Protocol (Port 8889)

The binary protocol provides a complete wire protocol for developing external JDBC/ODBC drivers:

```
┌───────────┬─────────┬──────────┬───────────┬────────────┬─────────────────┐
│ Magic (1B)│ Ver (1B)│ Type (1B)│ Flags (1B)│ Length (4B)│ Payload (var)   │
│   0xFD    │   0x01  │          │           │ Big-endian │                 │
└───────────┴─────────┴──────────┴───────────┴────────────┴─────────────────┘
```

**Message Categories:**

| Category | Types | Purpose |
|----------|-------|---------|
| Core | Query, Prepare, Execute, Deallocate | SQL execution |
| Cursors | CursorOpen, CursorFetch, CursorClose | Large result sets |
| Metadata | GetTables, GetColumns, GetTypeInfo | Schema discovery |
| Transactions | BeginTx, CommitTx, RollbackTx | Transaction control |
| Sessions | SetOption, GetOption, GetServerInfo | Connection management |

See [Driver Development Guide](driver-development.md) for complete protocol specification.

## Cluster Architecture

FlyDB supports automatic failover using the Bully algorithm:

```
┌────────────────────────────────────────────────────────────────┐
│                        Cluster                                 │
│                                                                │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐         │
│  │   Node A    │    │   Node B    │    │   Node C    │         │
│  │  (Leader)   │◄──►│ (Follower)  │◄──►│ (Follower)  │         │
│  │             │    │             │    │             │         │
│  │ Heartbeats  │───►│             │    │             │         │
│  │ every 500ms │───►│             │    │             │         │
│  └─────────────┘    └─────────────┘    └─────────────┘         │
│                                                                │
│  If Leader fails:                                              │
│  1. Followers detect missing heartbeat (2s timeout)            │
│  2. Highest-ID node initiates election                         │
│  3. New leader announces via COORDINATOR message               │
│  4. Followers update their leader reference                    │
└────────────────────────────────────────────────────────────────┘
```

## SQL Command Summary

FlyDB supports a comprehensive SQL dialect:

### Data Definition Language (DDL)

| Command | Description |
|---------|-------------|
| `CREATE TABLE` | Create a new table with columns and constraints |
| `DROP TABLE` | Remove a table |
| `ALTER TABLE` | Modify table structure |
| `CREATE INDEX` | Create a B-Tree index on a column |
| `DROP INDEX` | Remove an index |
| `CREATE DATABASE` | Create a new database with options |
| `DROP DATABASE` | Remove a database |
| `USE <database>` | Switch to a database |

### Data Manipulation Language (DML)

| Command | Description |
|---------|-------------|
| `SELECT` | Query data with WHERE, ORDER BY, LIMIT, JOIN |
| `INSERT` | Insert rows (single or bulk) |
| `UPDATE` | Modify existing rows |
| `DELETE` | Remove rows |
| `INSERT ... ON CONFLICT` | Upsert support |

### Transaction Control

| Command | Description |
|---------|-------------|
| `BEGIN` | Start a transaction |
| `COMMIT` | Commit the transaction |
| `ROLLBACK` | Rollback the transaction |
| `SAVEPOINT` | Create a savepoint |
| `RELEASE SAVEPOINT` | Release a savepoint |

### Access Control

| Command | Description |
|---------|-------------|
| `CREATE USER` | Create a new user |
| `DROP USER` | Remove a user |
| `GRANT` | Grant permissions |
| `REVOKE` | Revoke permissions |

### Inspection

| Command | Description |
|---------|-------------|
| `INSPECT TABLES` | List all tables |
| `INSPECT TABLE <name>` | Show table details |
| `INSPECT USERS` | List all users |
| `INSPECT INDEXES` | List all indexes |
| `INSPECT DATABASES` | List all databases |
| `INSPECT DATABASE <name>` | Show database details |
| `INSPECT SERVER` | Show server information |
| `INSPECT STATUS` | Show server status |

## Shell Commands (fsql)

The `fsql` shell provides PostgreSQL-like backslash commands:

| Command | Description |
|---------|-------------|
| `\q`, `\quit` | Exit the shell |
| `\h`, `\help` | Show help |
| `\clear` | Clear the screen |
| `\s`, `\status` | Show connection status |
| `\v`, `\version` | Show version |
| `\timing` | Toggle query timing |
| `\x` | Toggle expanded output |
| `\o [file]` | Set output to file |
| `\! <cmd>` | Execute shell command |
| `\dt` | List tables |
| `\du` | List users |
| `\di` | List indexes |
| `\db`, `\l` | List databases |
| `\c`, `\connect <db>` | Switch database |
| `\sql` | Enter SQL mode |
| `\normal` | Return to normal mode |

## See Also

- [Implementation Details](implementation.md) - Technical deep-dives
- [Design Decisions](design-decisions.md) - Rationale and trade-offs
- [API Reference](api.md) - SQL syntax and commands
- [Driver Development Guide](driver-development.md) - Building database drivers

