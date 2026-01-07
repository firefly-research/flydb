# FlyDB Architecture

This document describes the overall system architecture of FlyDB, including component relationships, data flow, and high-level design.

## Overview

FlyDB is a lightweight SQL database written in Go, designed to demonstrate core database concepts while remaining fully functional. It follows a layered architecture that separates concerns and enables modularity.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Client Applications                        │
│              (TCP clients, connection pools, CLI)               │
└─────────────────────────────────────────────────────────────────┘
                                │
                    ┌───────────┴───────────┐
                    ▼                       ▼
┌──────────────────────────┐   ┌──────────────────────────┐
│   Text Protocol (:8888)  │   │  Binary Protocol (:8889) │
│   • Line-based commands  │   │  • Length-prefixed JSON  │
│   • Human-readable       │   │  • Efficient for clients │
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
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                         SQL Layer                               │
│  ┌─────────┐  ┌─────────┐  ┌──────────┐  ┌─────────────────┐    │
│  │  Lexer  │→ │ Parser  │→ │ Executor │→ │ Query Cache     │    │
│  └─────────┘  └─────────┘  └──────────┘  └─────────────────┘    │
│                                │                                │
│                    ┌───────────┴───────────┐                    │
│                    ▼                       ▼                    │
│            ┌─────────────┐         ┌─────────────┐              │
│            │   Catalog   │         │  Prepared   │              │
│            │  (schemas)  │         │ Statements  │              │
│            └─────────────┘         └─────────────┘              │
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
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │   KVStore   │  │    WAL      │  │     Index Manager       │  │
│  │ (in-memory) │  │ (durability)│  │     (B-Tree indexes)    │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐                               │
│  │ Transaction │  │  Encryptor  │                               │
│  │  Manager    │  │ (AES-256)   │                               │
│  └─────────────┘  └─────────────┘                               │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │     File System       │
                    │   (WAL file: .wal)    │
                    └───────────────────────┘
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

The storage layer provides persistence and data management:

| Component | File | Purpose |
|-----------|------|---------|
| Engine | `engine.go` | Storage interface definition |
| KVStore | `kvstore.go` | In-memory key-value store |
| WAL | `wal.go` | Write-Ahead Log for durability |
| B-Tree | `btree.go` | Index data structure |
| Index Manager | `index.go` | Index lifecycle management |
| Transaction | `transaction.go` | ACID transaction support |
| Encryptor | `encryption.go` | AES-256-GCM encryption |

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

## Key Storage Conventions

FlyDB uses a key prefix convention to organize data in the KVStore:

| Prefix | Purpose | Example |
|--------|---------|---------|
| `row:<table>:<id>` | Table row data (JSON) | `row:users:1` |
| `seq:<table>` | Auto-increment sequence | `seq:users` |
| `schema:<table>` | Table schema definition | `schema:users` |
| `_sys_users:<username>` | User credentials | `_sys_users:alice` |
| `_sys_privs:<user>:<table>` | User permissions | `_sys_privs:alice:orders` |
| `index:<table>:<column>` | Index metadata | `index:users:email` |
| `proc:<name>` | Stored procedure | `proc:get_user` |
| `view:<name>` | View definition | `view:active_users` |

## Thread Safety Model

FlyDB uses Go's concurrency primitives for thread safety:

- **KVStore**: `sync.RWMutex` for concurrent read access, exclusive writes
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

## See Also

- [Implementation Details](implementation.md) - Technical deep-dives
- [Design Decisions](design-decisions.md) - Rationale and trade-offs
- [API Reference](api.md) - SQL syntax and commands

