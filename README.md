<div align="center">

```
  _____.__            .______.
_/ ____\  | ___.__. __| _/\_ |__
\   __\|  |<   |  |/ __ |  | __ \
 |  |  |  |_\___  / /_/ |  | \_\ \
 |__|  |____/ ____\____ |  |___  /
            \/         \/      \/
```

  <p><strong>The Lightweight, Embeddable SQL Database for Go Applications</strong></p>

  <p>
    <a href="https://github.com/firefly-oss/flydb/releases"><img src="https://img.shields.io/badge/version-01.26.14-blue.svg" alt="Version"></a>
    <a href="https://github.com/firefly-oss/flydb/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-green.svg" alt="License"></a>
    <a href="https://go.dev/"><img src="https://img.shields.io/badge/Go-1.24%2B-00ADD8?logo=go" alt="Go Version"></a>
    <a href="https://github.com/firefly-oss/flydb"><img src="https://img.shields.io/badge/platform-Linux%20%7C%20macOS-lightgrey.svg" alt="Platform"></a>
  </p>
</div>

---

## Overview

FlyDB is a production-ready SQL database engine built from the ground up in pure Go. Designed for developers who need the power of a relational database without the operational complexity of traditional database servers, FlyDB compiles to a single binary with zero external dependencies.

Whether you are building microservices that need local persistence, edge applications requiring embedded storage, or distributed systems demanding replication and high availability, FlyDB delivers enterprise-grade features in a lightweight package. The PostgreSQL-inspired storage engine provides familiar semantics with modern performance characteristics, while built-in encryption and row-level security ensure your data remains protected.

### Why FlyDB

- **Zero Dependencies** — Pure Go implementation with no CGO. Deploy a single binary anywhere Go runs.
- **Production Storage Engine** — 8KB slotted pages, LRU-K buffer pool, and write-ahead logging deliver the durability and performance you expect from a real database.
- **Security by Default** — TLS 1.2+ for connections, AES-256-GCM encryption at rest, and row-level security policies protect sensitive data without additional configuration.
- **Scale When Ready** — Start embedded, then seamlessly transition to cluster mode with automatic failover as your needs grow.
- **Full SQL Support** — Joins, subqueries, transactions, stored procedures, triggers, and prepared statements. No compromises on query capabilities.
- **JSONB Support** — Store and query semi-structured JSON data with PostgreSQL-compatible operators (`->`, `->>`, `@>`, `<@`, `?`) and functions.
- **ODBC/JDBC Ready** — Binary wire protocol with complete metadata APIs enables building standard database drivers for any language or platform.

### Quick Example

```bash
# Start the server
./flydb -data-dir ./data

# Connect with the CLI
./fsql
```

```sql
-- Create a table
CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE);

-- Insert data
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');

-- Query data
SELECT * FROM users WHERE name LIKE 'A%';
```

---

## Table of Contents

- [Overview](#overview)
  - [Why FlyDB](#why-flydb)
  - [Quick Example](#quick-example)
- [Architecture](#architecture)
  - [Storage Engine](#storage-engine)
  - [Write-Ahead Logging (WAL)](#write-ahead-logging-wal)
  - [B-Tree Indexes](#b-tree-indexes)
  - [Transaction Support](#transaction-support)
  - [Encryption at Rest](#encryption-at-rest)
  - [SQL Processing Pipeline](#sql-processing-pipeline)
  - [Binary Protocol](#binary-protocol)
  - [Replication](#replication)
  - [Authentication & Authorization](#authentication--authorization)
- [Installation](#installation)
  - [Quick Install (Recommended)](#quick-install-recommended)
  - [Installation Options](#installation-options)
  - [Build from Source](#build-from-source)
  - [Installed Components](#installed-components)
  - [Uninstallation](#uninstallation)
- [Quick Start](#quick-start)
  - [1. Start the Server](#1-start-the-server)
  - [2. Connect with the CLI](#2-connect-with-the-cli)
  - [3. Authenticate](#3-authenticate)
  - [4. Basic Operations](#4-basic-operations)
  - [5. Interactive Shell Commands](#5-interactive-shell-commands)
  - [6. Multi-Line Statements](#6-multi-line-statements)
- [Configuration](#configuration)
  - [Interactive Wizard](#interactive-wizard)
  - [Operative Modes](#operative-modes)
  - [Configuration File](#configuration-file)
  - [Configuration Precedence](#configuration-precedence)
  - [Environment Variables](#environment-variables)
  - [Server Options](#server-options)
  - [Client Options](#client-options)
  - [CLI Local Commands](#cli-local-commands)
  - [Multi-line Editing](#multi-line-editing)
  - [Examples](#examples)
- [Security](#security)
  - [TLS Encryption (Transport Security)](#tls-encryption-transport-security)
  - [Authentication](#authentication)
  - [Encryption at Rest](#encryption-at-rest-1)
  - [Row-Level Security](#row-level-security)
  - [TLS Troubleshooting](#tls-troubleshooting)
- [Observability](#observability)
  - [Prometheus Metrics](#prometheus-metrics)
  - [Health Endpoints](#health-endpoints)
  - [Environment Variables](#environment-variables-1)
- [Replication & Clustering](#replication--clustering)
  - [Unified Architecture](#unified-architecture)
  - [Start a Cluster](#start-a-cluster)
  - [Consistency Levels](#consistency-levels)
  - [Cluster Features](#cluster-features)
  - [HA Client Connections](#ha-client-connections)
  - [Database Dump Utility](#database-dump-utility)
- [Documentation](#documentation)
- [Development](#development)
  - [Running Tests](#running-tests)
  - [Integration Tests](#integration-tests)
  - [Manual Protocol Testing](#manual-protocol-testing)
  - [Project Structure](#project-structure)
- [License](#license)

---

## Architecture

FlyDB implements a production-grade, layered architecture inspired by PostgreSQL's proven design. Each layer is purpose-built for performance, durability, and maintainability.

```
┌────────────────────────────────────────────────────────────────────────┐
│                           Client Layer                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │  flydb-shell│  │ JDBC Driver │  │ ODBC Driver │  │  Custom App │    │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘    │
└─────────┼────────────────┼────────────────┼────────────────┼───────────┘
          │                │                │                │
          ▼                ▼                ▼                ▼
┌────────────────────────────────────────────────────────────────────────┐
│                         Protocol Layer                                 │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │              Binary Protocol (:8889)                            │   │
│  │   • Type-Length-Value framing                                   │   │
│  │   • Prepared statements                                         │   │
│  │   • Cursor operations                                           │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌────────────────────────────────────────────────────────────────────────┐
│                           SQL Layer                                    │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────────────┐  │
│  │  Lexer   │───▶│  Parser  │───▶│ Executor │───▶│     Catalog      │  │
│  │          │    │          │    │          │    │                  │  │
│  │ Tokenize │    │ Build    │    │ Execute  │    │ Schema Registry  │  │
│  │ SQL text │    │ AST      │    │ Plan     │    │ Table Metadata   │  │
│  └──────────┘    └──────────┘    └──────────┘    └──────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌────────────────────────────────────────────────────────────────────────┐
│                         Storage Layer                                  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────┐ │
│  │   Buffer Pool   │  │   Index Manager │  │   Transaction Manager   │ │
│  │                 │  │                 │  │                         │ │
│  │ • LRU-K (K=2)   │  │ • B-Tree        │  │ • Write buffering       │ │
│  │ • Auto-sizing   │  │ • O(log N) ops  │  │ • Savepoints            │ │
│  │ • Dirty tracking│  │ • Auto-maintain │  │ • Read-committed        │ │
│  └────────┬────────┘  └─────────────────┘  └─────────────────────────┘ │
│           │                                                            │
│  ┌────────▼────────┐  ┌─────────────────┐  ┌─────────────────────────┐ │
│  │    Heap File    │  │       WAL       │  │      Encryptor          │ │
│  │                 │  │                 │  │                         │ │
│  │ • 8KB pages     │  │ • Append-only   │  │ • AES-256-GCM           │ │
│  │ • Slotted layout│  │ • Crash recovery│  │ • PBKDF2 key derivation │ │
│  │ • Free list     │  │ • Checkpointing │  │ • Per-record encryption │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────────────┘ │
└────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌────────────────────────────────────────────────────────────────────────┐
│                    Cluster & Replication Layer                         │
│  ┌────────────────────┐  ┌────────────────────┐  ┌──────────────────┐  │
│  │  Partition Manager │  │  Consensus Engine  │  │ Replication Eng. │  │
│  │                    │  │                    │  │                  │  │
│  │ • Consistent hash  │  │ • Leader election  │  │ • WAL streaming  │  │
│  │ • Virtual nodes    │  │ • Term-based fence │  │ • Multi-mode     │  │
│  │ • Auto-rebalance   │  │ • Quorum decisions │  │ • Auto-failover  │  │
│  └────────────────────┘  └────────────────────┘  └──────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```

### Storage Engine

The storage engine is the heart of FlyDB, implementing PostgreSQL-style page-based storage with intelligent caching.

**Slotted Page Architecture (8KB Pages)**

Each page uses a slotted layout that efficiently handles variable-length records:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Page Header (24 bytes)                       │
│  PageID │ Type │ Flags │ SlotCount │ FreeStart │ FreeEnd │ LSN  │
├─────────────────────────────────────────────────────────────────┤
│  Slot Array (grows →)                                           │
│  [Slot 0: offset,len] [Slot 1: offset,len] [Slot 2: offset,len] │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│                         Free Space                              │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│  ← Records (grow ←)                                             │
│  [Record N] [Record N-1] ... [Record 1] [Record 0]              │
└─────────────────────────────────────────────────────────────────┘
```

- **Slot array** grows forward from the header
- **Records** grow backward from the end of the page
- **Free space** in the middle allows both to grow independently
- **LSN (Log Sequence Number)** tracks the last WAL entry affecting this page

**LRU-K Buffer Pool**

The buffer pool caches frequently accessed pages in memory, using the LRU-K algorithm (K=2) for superior cache behavior with database workloads:

| Feature | Description |
|---------|-------------|
| **Auto-sizing** | Automatically uses 25% of available RAM (min 2MB, max 1GB) |
| **LRU-K eviction** | Tracks last K accesses to distinguish hot vs. cold pages |
| **Dirty page tracking** | Only writes modified pages back to disk |
| **Pin counting** | Prevents eviction of pages in active use |
| **Hit rate monitoring** | Exposes cache statistics for tuning |

**Heap File Management**

The heap file manages page allocation with an efficient free list:

```
FreeListHead → Page 5 → Page 12 → Page 3 → InvalidPageID
```

- Deleted pages are added to the free list for reuse
- New allocations prefer free list pages over file extension
- Eliminates the need for expensive file compaction

### Write-Ahead Logging (WAL)

Every write operation is logged before being applied, ensuring durability even during crashes:

```
┌─────────────────────────────────────────────────────────────────┐
│                        WAL Record Format                        │
├──────────┬──────────┬──────────┬───────────┬──────────┬─────────┤
│ Op (1B)  │ KeyLen   │   Key    │ ValueLen  │  Value   │  ...    │
│ PUT=1    │ (4B)     │ (var)    │ (4B)      │  (var)   │         │
│ DEL=2    │          │          │           │          │         │
└──────────┴──────────┴──────────┴───────────┴──────────┴─────────┘
```

**Recovery Process:**
1. On startup, load the last checkpoint (if available)
2. Replay WAL entries after the checkpoint
3. Rebuild in-memory state from replayed operations

**Checkpointing:**
- Periodically flushes all dirty pages to disk
- Records checkpoint marker with timestamp and page count
- Enables WAL truncation to bound recovery time

### B-Tree Indexes

FlyDB implements classic B-Tree indexes for O(log N) lookups:

| Property | Value |
|----------|-------|
| **Minimum degree (t)** | 16 (configurable) |
| **Max keys per node** | 2t-1 = 31 |
| **Min keys per node** | t-1 = 15 (except root) |
| **Search complexity** | O(log N) |
| **Insert complexity** | O(log N) |
| **Delete complexity** | O(log N) |

Indexes are automatically maintained on INSERT, UPDATE, and DELETE operations.

### Transaction Support

FlyDB provides ACID transactions with optimistic concurrency:

| Property | Implementation |
|----------|----------------|
| **Atomicity** | Write buffering — all operations applied together or not at all |
| **Consistency** | Constraint validation before commit |
| **Isolation** | Read-committed — reads see only committed data |
| **Durability** | WAL persistence before commit acknowledgment |

**Savepoint Support:**
```sql
BEGIN;
INSERT INTO users (name) VALUES ('Alice');
SAVEPOINT sp1;
INSERT INTO users (name) VALUES ('Bob');
ROLLBACK TO sp1;  -- Bob's insert is undone
COMMIT;           -- Only Alice is committed
```

### Encryption at Rest

All data is encrypted using AES-256-GCM with authenticated encryption:

| Component | Details |
|-----------|---------|
| **Algorithm** | AES-256-GCM (hardware-accelerated via AES-NI) |
| **Key derivation** | PBKDF2 with SHA-256, 100,000 iterations |
| **Nonce** | Random 12-byte nonce per record |
| **Authentication** | GCM tag provides integrity verification |
| **Overhead** | 28 bytes per record (12B nonce + 16B tag) |

### SQL Processing Pipeline

SQL statements flow through a three-stage pipeline:

**1. Lexer (Tokenization)**
```
"SELECT name FROM users WHERE id = 1"
    ↓
[SELECT] [name] [FROM] [users] [WHERE] [id] [=] [1] [EOF]
```

**2. Parser (AST Construction)**

Recursive descent parser builds an Abstract Syntax Tree:
```
SelectStmt {
    Columns: ["name"]
    TableName: "users"
    Where: BinaryExpr{Left: "id", Op: "=", Right: 1}
}
```

**3. Executor (Query Execution)**
- Validates table existence via Catalog
- Applies Row-Level Security filters
- Performs nested-loop joins for multi-table queries
- Sorts results for ORDER BY
- Applies LIMIT/OFFSET pagination

### Binary Protocol

The binary protocol enables efficient client-server communication:

```
┌────────┬─────────┬─────────┬───────┬────────────┬─────────────┐
│ Magic  │ Version │ MsgType │ Flags │ Length(4B) │ Payload...  │
│ 0xFD   │ 0x01    │         │       │            │             │
└────────┴─────────┴─────────┴───────┴────────────┴─────────────┘
```

| Message Type | Code | Description |
|--------------|------|-------------|
| Query | 0x01 | SQL query request |
| QueryResult | 0x02 | Query response with rows |
| Error | 0x03 | Error response |
| Prepare | 0x04 | Prepare statement |
| Execute | 0x06 | Execute prepared statement |
| Auth | 0x08 | Authentication request |

### Replication

FlyDB uses Raft-based consensus and log replication in cluster mode:

**Replication Flow:**
1. Follower connects to leader's replication port (default: 9999)
2. Follower sends its current WAL offset
3. Leader streams WAL entries from that offset
4. Follower applies entries to local storage

**Replication Modes:**
| Mode | Description |
|------|-------------|
| `ASYNC` | Return immediately, replicate in background (default) |
| `SEMI_SYNC` | Wait for at least one replica to acknowledge |
| `SYNC` | Wait for all replicas to acknowledge |

**Consistency Model:** Configurable from eventual consistency (~100ms lag) to synchronous replication

**Failure Handling:**
- Followers automatically reconnect after network partitions
- WAL offset ensures no data loss or duplication
- Followers catch up by replaying missed entries
- Per-follower health monitoring and lag tracking

### Reactive Events (WATCH)

FlyDB provides real-time data change notifications:

```sql
-- Subscribe to table changes
WATCH users

-- Subscribe to schema changes
WATCH SCHEMA

-- Unsubscribe
UNWATCH users
UNWATCH ALL
```

**Event Types:**
- `EVENT INSERT <table> <json>` — Row inserted
- `EVENT UPDATE <table> <old_json> <new_json>` — Row updated
- `EVENT DELETE <table> <json>` — Row deleted
- `EVENT SCHEMA <type> <object> <details>` — Schema changed

### Authentication & Authorization

**Authentication:**
- bcrypt password hashing with timing-attack resistance
- Secure password generation for initial admin setup
- Dummy comparison for non-existent users (prevents enumeration)

**Row-Level Security (RLS):**
```sql
-- User can only see their own orders
GRANT SELECT ON orders WHERE user_id = 'alice' TO alice;
```

RLS predicates are automatically applied to all queries, ensuring data isolation at the row level.

---

## Installation

### Quick Install (Recommended)

Install FlyDB with a single command. The installer automatically detects your platform and downloads the appropriate pre-built binaries:

```bash
curl -sSL https://raw.githubusercontent.com/firefly-oss/flydb/main/install.sh | bash
```

To install with specific options (non-interactive):

```bash
curl -sSL https://raw.githubusercontent.com/firefly-oss/flydb/main/install.sh | bash -s -- --prefix ~/.local --yes
```

### Installation Options

| Option | Description |
|--------|-------------|
| `--prefix <path>` | Installation directory (default: `/usr/local` for root, `~/.local` for user) |
| `--version <ver>` | Install a specific version |
| `--from-source` | Build from source instead of downloading binaries (requires Go 1.21+) |
| `--from-binary` | Force download of pre-built binaries |
| `--no-service` | Skip system service installation (systemd/launchd) |
| `--no-config` | Skip configuration file creation |
| `--init-db` | Initialize a new database during installation |
| `--yes`, `-y` | Non-interactive mode, accept all defaults |
| `--uninstall` | Remove FlyDB installation |

### Build from Source

If you prefer to build from source or need to modify the code:

```bash
git clone https://github.com/firefly-oss/flydb.git
cd flydb

# Option 1: Use the installer (recommended)
./install.sh --from-source

# Option 2: Build manually (requires Go 1.21+)
go build -o flydb ./cmd/flydb
go build -o flydb-shell ./cmd/flydb-shell
```

### Installed Components

The installation creates:

| Binary | Description |
|--------|-------------|
| `flydb` | The database server daemon |
| `flydb-shell` | The interactive SQL client |
| `fsql` | Symlink to `flydb-shell` for convenience |
| `flydb-dump` | Database export/import utility |
| `fdump` | Symlink to `flydb-dump` for convenience |
| `flydb-discover` | Network node discovery tool for cluster setup |

Default locations:
- **Binaries**: `/usr/local/bin` (system) or `~/.local/bin` (user)
- **Configuration**: `/etc/flydb/flydb.conf` (system) or `~/.config/flydb/flydb.conf` (user)
- **Data**: `/var/lib/flydb` (system) or `~/.local/share/flydb` (user)

### Uninstallation

To remove FlyDB:

```bash
# Interactive uninstallation
./uninstall.sh

# Non-interactive (removes binaries, services, and config)
./uninstall.sh --yes

# Preview what would be removed without deleting anything
./uninstall.sh --dry-run

# Also remove data directories (WARNING: deletes all databases!)
./uninstall.sh --remove-data --yes
```

Or use the installer's uninstall mode:

```bash
./install.sh --uninstall
```

---

## Quick Start

### 1. Start the Server

**Interactive Mode (Recommended for First-Time Setup):**

```bash
flydb
```

Running `flydb` without arguments launches an interactive wizard that guides you through configuration. On first run, the server will:
- Generate an admin password (save this securely!)
- Generate an encryption passphrase if encryption is enabled
- Create the default database

**Command-Line Mode:**

```bash
# Start with default settings (data stored in ~/.local/share/flydb)
flydb -role standalone

# Start with custom data directory
flydb -data-dir /var/lib/flydb -port 8889

# Start as cluster node
flydb -role cluster -port 8889 -repl-port 9999 -data-dir /var/lib/flydb
```

**Server Options:**

| Option | Description | Default |
|--------|-------------|---------|
| `-port` | Server port (binary protocol) | 8889 |
| `-repl-port` | Replication port | 9999 |
| `-role` | Server role: `standalone`, `cluster` | standalone |
| `-data-dir` | Data storage directory | `~/.local/share/flydb` |
| `-log-level` | Log level: `debug`, `info`, `warn`, `error` | info |
| `-log-json` | Enable JSON log output | false |
| `-config` | Path to configuration file | - |

**Environment Variables:**

| Variable | Description |
|----------|-------------|
| `FLYDB_DATA_DIR` | Data directory for database storage |
| `FLYDB_PORT` | Server port (binary protocol) |
| `FLYDB_ENCRYPTION_PASSPHRASE` | Encryption passphrase (required if encryption enabled) |
| `FLYDB_ADMIN_PASSWORD` | Admin password for first-time setup |

### 2. Connect with the CLI

The `flydb-shell` (aliased as `fsql`) is an interactive SQL client with readline support, tab completion, and command history.

**Connect to Local Server:**

```bash
# Connect to localhost:8889 (default)
fsql

# Connect to a specific database
fsql -d mydb
```

**Connect to Remote Server:**

```bash
fsql -H 192.168.1.100 -p 8889
```

**Execute a Command and Exit:**

```bash
fsql -e "SELECT * FROM users"
fsql -d mydb -e "INSPECT TABLES"
```

**CLI Options:**

| Option | Description | Default |
|--------|-------------|---------|
| `-H`, `--host` | Server hostname or IP | localhost |
| `-p`, `--port` | Server port (binary protocol) | 8889 |
| `-d`, `--database` | Database to connect to | default |
| `-e`, `--execute` | Execute command and exit | - |
| `-f`, `--format` | Output format: `table`, `json`, `plain` | table |
| `--verbose` | Enable verbose output with timing | false |
| `--no-color` | Disable colored output | false |

### 3. Authenticate

FlyDB requires authentication before executing SQL commands. On first server start, admin credentials are generated and displayed.

```
flydb> AUTH admin <password>
AUTH OK

flydb:default>
```

For secure password entry, use `AUTH` or `AUTH <username>` without the password:

```
flydb> AUTH admin
Password: ********
AUTH OK
```

### 4. Basic Operations

```sql
-- Create a database
CREATE DATABASE myapp;

-- Switch to the database
USE myapp;

-- Create a table with constraints
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert data
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');
INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com');

-- Query data
SELECT * FROM users WHERE name LIKE 'A%';

-- Create an index for faster lookups
CREATE INDEX idx_users_email ON users (email);

-- Update data
UPDATE users SET name = 'Alice Smith' WHERE id = 1;

-- Delete data
DELETE FROM users WHERE id = 2;

-- View table structure
INSPECT TABLE users;
```

### 5. Interactive Shell Commands

The CLI supports PostgreSQL-style backslash commands:

| Command | Description |
|---------|-------------|
| `\q`, `\quit` | Exit the CLI |
| `\h`, `\help` | Display help information |
| `\c <db>` | Switch to a different database |
| `\dt` | List all tables |
| `\di` | List all indexes |
| `\du` | List all users |
| `\db`, `\l` | List all databases |
| `\s`, `\status` | Show connection status |
| `\timing` | Toggle query timing display |
| `\x` | Toggle expanded (vertical) output |
| `\clear` | Clear the screen |
| `\! <cmd>` | Execute a shell command |
| `\sql` | Enter SQL mode (all input treated as SQL) |
| `\normal` | Return to normal mode |

### 6. Multi-Line Statements

SQL statements can span multiple lines. The CLI waits for a semicolon before executing:

```
flydb:default> SELECT id, name, email
        -> FROM users
        -> WHERE created_at > '2024-01-01'
        -> ORDER BY name;
```

Use a backslash at the end of a line for explicit continuation:

```
flydb:default> SELECT * FROM users \
        -> WHERE id = 1;
```

For complete SQL syntax, data types, functions, and query features, see the **[API Reference](docs/api.md)**.

---

## Configuration

### Interactive Wizard

When you run `./flydb` without any arguments, an interactive wizard guides you through configuration:

```
╔════════════════════════════════════════════════════════════╗
║                FlyDB Initialization Wizard                 ║
╚════════════════════════════════════════════════════════════╝

Step 1: Select Operative Mode
─────────────────────────────────────────────────────────────

  1) Standalone  - Single server (development/small deployments)
  2) Cluster     - Distributed cluster with automatic failover
```

The wizard prompts for all relevant settings based on your selected mode and displays a summary before starting.

### Operative Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `standalone` | Single server, no replication | Development, small deployments |
| `cluster` | Cluster with automatic failover | Production distributed deployments |

### Configuration File

FlyDB supports both **JSON** and **TOML** configuration files for easier deployment and management. The format is auto-detected based on file extension (`.json` or `.conf`/`.toml`).

The configuration file is automatically loaded from these locations (in order of precedence):

1. Path specified by `--config` flag
2. Path specified by `FLYDB_CONFIG_FILE` environment variable
3. `/etc/flydb/flydb.json` or `/etc/flydb/flydb.conf` (system-wide)
4. `~/.config/flydb/flydb.json` or `~/.config/flydb/flydb.conf` (user-specific)
5. `./flydb.json` or `./flydb.conf` (current directory)

**Example JSON configuration** (recommended):

```json
{
  "role": "standalone",
  "port": 8889,
  "replication_port": 9999,
  "data_dir": "/var/lib/flydb",
  "default_database": "default",
  "default_encoding": "UTF8",
  "tls_enabled": true,
  "tls_auto_gen": true,
  "encryption_enabled": true,
  "log_level": "info",
  "log_json": false,
  "observability": {
    "metrics": {
      "enabled": false,
      "addr": ":9194"
    },
    "health": {
      "enabled": true,
      "addr": ":9195"
    }
  }
}
```

**Example TOML configuration** (legacy, still supported):

```toml
# Server role: standalone or cluster
role = "standalone"

# Network ports
port = 8889
replication_port = 9999

# Storage - Multi-database mode (always enabled)
# Each database is stored in a separate directory under data_dir
data_dir = "/var/lib/flydb"

# Default settings for new databases
default_database = "default"
default_encoding = "UTF8"
default_locale = "en_US"
default_collation = "default"

# TLS Configuration (ENABLED BY DEFAULT for security)
# TLS encrypts client-server connections using TLS 1.2+
tls_enabled = true
# tls_cert_file = "/etc/flydb/certs/server.crt"  # Auto-determined if not set
# tls_key_file = "/etc/flydb/certs/server.key"   # Auto-determined if not set
tls_auto_gen = true  # Auto-generate self-signed certificates if not found

# Data-at-rest encryption (ENABLED BY DEFAULT)
# When enabled, you MUST set FLYDB_ENCRYPTION_PASSPHRASE environment variable
# To disable encryption, set encryption_enabled = false
encryption_enabled = true

# Logging
log_level = "info"
log_json = false
```

### Configuration Precedence

Configuration values are applied in the following order (highest priority first):

1. **Command-line flags** - Override all other sources
2. **Environment variables** - Override file and defaults
3. **Configuration file** - Override defaults
4. **Default values** - Built-in defaults

### Environment Variables

| Variable | Description |
|----------|-------------|
| `FLYDB_PORT` | Server port (binary protocol) |
| `FLYDB_REPL_PORT` | Replication port |
| `FLYDB_ROLE` | Server role (standalone, cluster) |
| `FLYDB_DB_PATH` | Path to database file |
| `FLYDB_DATA_DIR` | Directory for multi-database storage |
| `FLYDB_DEFAULT_ENCODING` | Default encoding for new databases (UTF8, LATIN1, ASCII, UTF16) |
| `FLYDB_DEFAULT_LOCALE` | Default locale for new databases (e.g., en_US, de_DE) |
| `FLYDB_DEFAULT_COLLATION` | Default collation for new databases (default, binary, nocase, unicode) |
| `FLYDB_TLS_ENABLED` | Enable TLS for client connections (true/false, default: **true**) |
| `FLYDB_TLS_CERT_FILE` | Path to TLS certificate file (auto-determined if not set) |
| `FLYDB_TLS_KEY_FILE` | Path to TLS private key file (auto-determined if not set) |
| `FLYDB_TLS_AUTO_GEN` | Auto-generate self-signed certificates (true/false, default: **true**) |
| `FLYDB_ENCRYPTION_ENABLED` | Enable data-at-rest encryption (true/false, default: **true**) |
| `FLYDB_ENCRYPTION_PASSPHRASE` | **Required** when encryption enabled - passphrase for key derivation |
| `FLYDB_LOG_LEVEL` | Log level (debug, info, warn, error) |
| `FLYDB_LOG_JSON` | Enable JSON logging (true/false) |
| `FLYDB_ADMIN_PASSWORD` | Initial admin password (first-time setup) |
| `FLYDB_CONFIG_FILE` | Path to configuration file |

### Server Options

| Flag | Default | Description |
|------|---------|-------------|
| `-port` | `8889` | Server port (binary protocol) |
| `-repl-port` | `9999` | Replication port (cluster only) |
| `-data-dir` | `/var/lib/flydb` | Directory for database storage |
| `-role` | `standalone` | Server role: `standalone`, `cluster` |
| `-tls-enabled` | `true` | Enable TLS for client connections |
| `-tls-cert-file` | auto | Path to TLS certificate file |
| `-tls-key-file` | auto | Path to TLS private key file |
| `-tls-auto-gen` | `true` | Auto-generate self-signed certificates |
| `-log-level` | `info` | Log level: debug, info, warn, error |
| `-log-json` | `false` | JSON log output |
| `-config` | - | Path to configuration file |

### Client Options

| Flag | Default | Description |
|------|---------|-------------|
| `-H`, `--host` | `localhost` | Server hostname(s), comma-separated for HA cluster |
| `-p`, `--port` | `8889` | Server port (used when host doesn't include port) |
| `-d`, `--database` | `default` | Database to connect to |
| `-v`, `--verbose` | `false` | Verbose mode (show query timing) |
| `-f`, `--format` | `table` | Output format: table, json, plain |
| `--target-primary` | `false` | Prefer connecting to primary/leader in cluster |
| `--no-tls` | `false` | Disable TLS (connect to legacy servers) |
| `--tls-insecure` | `false` | Skip TLS certificate verification (for self-signed certs) |

### CLI Local Commands

| Command | Description |
|---------|-------------|
| `\h`, `\help` | Display help |
| `\q`, `\quit` | Exit the CLI |
| `\c <database>` | Switch to a different database |
| `\db`, `\l` | List all databases |
| `\dt` | List tables |
| `\du` | List users |
| `\di` | List indexes |
| `\s`, `\status` | Show connection status |
| `\timing` | Toggle query timing display |
| `\o [file]` | Redirect output to file |
| `\! <cmd>` | Execute shell command |
| `\sql` | Enter SQL mode (all input = SQL) |
| `\normal` | Return to normal mode |
| `SQL <query>` | Execute any SQL explicitly |

### Multi-line Editing

FlyDB's CLI supports professional multi-line editing like PostgreSQL's psql and MySQL CLI:

- **Semicolon termination**: SQL statements require a semicolon (`;`) to execute
- **Continuation prompt**: When a statement is incomplete, the prompt changes to `->`
- **Cancel input**: Press `Ctrl+C` to cancel multi-line input
- **Explicit continuation**: End a line with `\` to continue on the next line

**Example:**

```
flydb:default> SELECT id, name
        -> FROM users
        -> WHERE id > 0
        -> ORDER BY name;
+----+-------+
| id | name  |
+----+-------+
| 1  | Alice |
| 2  | Bob   |
+----+-------+
(2 rows)
```

> **Note:** Commands like `PING`, `AUTH`, and `USE` don't require semicolons.

### Examples

Standalone server (development):

```bash
./flydb -role standalone -data-dir /var/lib/flydb
```

Cluster node:

```bash
./flydb -port 8889 -repl-port 9999 -role cluster -data-dir /var/lib/flydb
```

---

## Security

FlyDB provides comprehensive security features enabled by default to protect your data both at rest and in transit.

### TLS Encryption (Transport Security)

**TLS is enabled by default** for all client-server connections, encrypting data in transit using TLS 1.2+.

**Auto-Generated Certificates (Development/Testing):**

```bash
# TLS is enabled automatically with self-signed certificates
./flydb -data-dir ./data

# Server will auto-generate certificates at:
# - Root user: /etc/flydb/certs/
# - Non-root: ~/.config/flydb/certs/
```

**Custom Certificates (Production):**

```bash
# Use your own certificates from a trusted CA
./flydb -data-dir ./data \
  -tls-cert-file /path/to/server.crt \
  -tls-key-file /path/to/server.key
```

**Disable TLS (Legacy/Testing):**

```bash
# Disable TLS for backward compatibility
export FLYDB_TLS_ENABLED=false
./flydb -data-dir ./data

# Or via config file: tls_enabled = false
```

**Client Connections:**

```bash
# Connect with TLS (default, skips certificate verification for self-signed certs)
flydb-shell --host localhost --port 8889 --tls-insecure

# Connect without TLS (to legacy servers)
flydb-shell --host localhost --port 8889 --no-tls
```

### Authentication

FlyDB uses bcrypt for password hashing. On first startup, an admin user is created:

```bash
# Option 1: Set password via environment variable
export FLYDB_ADMIN_PASSWORD="your-secure-password"
./flydb -data-dir ./data

# Option 2: Let FlyDB generate a password (displayed once at startup)
./flydb -data-dir ./data
```

### Encryption at Rest

All data is encrypted using AES-256-GCM by default. Set the encryption passphrase:

```bash
export FLYDB_ENCRYPTION_PASSPHRASE="your-passphrase"
./flydb -data-dir ./data
```

To disable encryption:

```bash
./flydb -data-dir ./data -encryption-enabled=false
```

### Row-Level Security

Grant access to specific rows using predicate filters:

```sql
-- User can only see their own orders
GRANT SELECT ON orders WHERE user_id = 'alice' TO alice;
```

### TLS Troubleshooting

**Certificate Verification Errors:**

If you see certificate verification errors when connecting:

```bash
# For self-signed certificates (development/testing)
flydb-shell --host localhost --port 8889 --tls-insecure

# For production, use certificates from a trusted CA
flydb-shell --host localhost --port 8889
```

**Certificate Location:**

Auto-generated certificates are stored at:
- **Root user:** `/etc/flydb/certs/server.crt` and `/etc/flydb/certs/server.key`
- **Non-root user:** `~/.config/flydb/certs/server.crt` and `~/.config/flydb/certs/server.key`

**Certificate Permissions:**

Ensure correct permissions:
```bash
chmod 644 /path/to/server.crt
chmod 600 /path/to/server.key
```

**Regenerate Certificates:**

To regenerate auto-generated certificates:
```bash
# Remove existing certificates
rm -rf ~/.config/flydb/certs/

# Restart server - new certificates will be generated
./flydb -data-dir ./data
```

**Connection Refused:**

If you can't connect, verify TLS is enabled on both client and server:
```bash
# Check server logs for "TLS protocol listening"
# If not present, TLS may be disabled

# Enable TLS on server
export FLYDB_TLS_ENABLED=true
./flydb -data-dir ./data

# Connect with TLS
flydb-shell --host localhost --port 8889 --tls-insecure
```

---

## Observability

FlyDB provides comprehensive observability features for monitoring, health checks, and metrics collection.

### Prometheus Metrics

FlyDB exposes Prometheus-compatible metrics for monitoring query performance, connections, storage, and cluster health.

**Enable metrics:**

```bash
# Via environment variable
FLYDB_METRICS_ENABLED=true flydb

# Via JSON config
{
  "observability": {
    "metrics": {
      "enabled": true,
      "addr": ":9194"
    }
  }
}
```

**Available metrics:**

- `flydb_queries_total` - Total queries executed
- `flydb_queries_by_type_total{type="SELECT|INSERT|UPDATE|DELETE"}` - Queries by type
- `flydb_query_latency_avg_microseconds` - Average query latency
- `flydb_connections_active` - Current active connections
- `flydb_databases_count` - Number of databases
- `flydb_transactions_active` - Active transactions
- `flydb_storage_size_bytes` - Total storage size
- `flydb_replication_lag_ms` - Replication lag (cluster mode)
- `flydb_cluster_nodes` - Number of cluster nodes
- `flydb_is_leader` - Is this node the leader (1=yes, 0=no)

**Prometheus scrape configuration:**

```yaml
scrape_configs:
  - job_name: 'flydb'
    static_configs:
      - targets: ['localhost:9194']
```

### Health Endpoints

FlyDB provides health check endpoints for Kubernetes liveness and readiness probes.

**Endpoints:**

- `GET /health` - Overall health check
- `GET /health/live` - Liveness probe (is the process running?)
- `GET /health/ready` - Readiness probe (is the service ready for traffic?)

**Enable health checks:**

```bash
# Via environment variable (enabled by default)
FLYDB_HEALTH_ENABLED=true flydb

# Via JSON config
{
  "observability": {
    "health": {
      "enabled": true,
      "addr": ":9195"
    }
  }
}
```

**Kubernetes deployment example:**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flydb
spec:
  template:
    spec:
      containers:
      - name: flydb
        image: flydb:latest
        ports:
        - containerPort: 8889
          name: flydb
        - containerPort: 9195
          name: health
        livenessProbe:
          httpGet:
            path: /health/live
            port: 9195
          initialDelaySeconds: 10
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /health/ready
            port: 9195
          initialDelaySeconds: 5
          periodSeconds: 10
```

**Health response format:**

```json
{
  "status": "healthy",
  "timestamp": "2026-01-17T10:52:16Z",
  "version": "01.26.14",
  "checks": [
    {
      "name": "storage",
      "status": "healthy",
      "latency_ms": 0
    }
  ]
}
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FLYDB_METRICS_ENABLED` | Enable Prometheus metrics endpoint | `false` |
| `FLYDB_METRICS_ADDR` | Metrics server address | `:9194` |
| `FLYDB_HEALTH_ENABLED` | Enable health check endpoints | `true` |
| `FLYDB_HEALTH_ADDR` | Health server address | `:9195` |

---

## Replication & Clustering

FlyDB provides a unified cluster-replication architecture that integrates leader election, data sharding, and replication into a single cohesive system.

### Unified Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                    Unified Cluster Manager                       │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                  Partition Manager                          │ │
│  │  - Consistent hash ring for data distribution               │ │
│  │  - Partition ownership tracking                             │ │
│  │  - Automatic rebalancing on node changes                    │ │
│  └─────────────────────────────────────────────────────────────┘ │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                  Consensus Engine                           │ │
│  │  - Leader election with term-based fencing                  │ │
│  │  - Quorum-based decisions                                   │ │
│  │  - Split-brain prevention                                   │ │
│  └─────────────────────────────────────────────────────────────┘ │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                  Replication Engine                         │ │
│  │  - WAL streaming with configurable consistency              │ │
│  │  - Automatic failover handling                              │ │
│  │  - Per-partition replication state                          │ │
│  └─────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

### Deployment Modes

FlyDB supports multiple deployment modes:

| Mode | Description | Use Case |
|------|-------------|----------|
| `standalone` | Single server, no replication | Development, small deployments |
| `cluster` | Unified cluster with integrated replication | Production distributed deployments |

### Cluster Mode (Recommended for Production)

For production deployments, use cluster mode with integrated replication:

```bash
# Start first node (becomes leader)
./flydb -role cluster -port 8889 -cluster-port 7000 -repl-port 9999 -data-dir ./node1

# Start additional nodes (join cluster)
./flydb -role cluster -port 8890 -cluster-port 7001 -repl-port 9998 \
  -cluster-peers localhost:7000 -data-dir ./node2
```

Cluster mode provides:
- **Horizontal Scaling**: True data sharding across nodes with consistent hashing
- **Data Partitioning**: 256 partitions by default, distributed across cluster nodes
- **Automatic Rebalancing**: Partitions redistribute when nodes join or leave
- **Partition-Aware Routing**: Requests automatically routed to the correct node
- **Cross-Partition Queries**: Scatter-gather support for queries spanning partitions
- **Automatic leader election**: Raft consensus for reliable leader election
- **Integrated WAL replication**: Leader streams WAL to followers in real-time
- **Automatic failover**: When a leader fails, followers detect it and elect a new leader
- **Split-brain resolution**: Multiple leaders automatically resolve via term-based fencing
- **Rejoin as follower**: When a failed leader returns, it rejoins as a follower
- **Data Migration**: Automatic partition migration during rebalancing

**Horizontal Scaling Architecture:**
```
┌─────────────────────────────────────────────────────────────────┐
│                    FlyDB Cluster (3 nodes)                      │
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │   Node 1     │    │   Node 2     │    │   Node 3     │       │
│  │              │    │              │    │              │       │
│  │ Partitions:  │    │ Partitions:  │    │ Partitions:  │       │
│  │ 0-84 (lead)  │    │ 85-169 (lead)│    │ 170-255(lead)│       │
│  │ 85-169 (rep) │    │ 170-255(rep) │    │ 0-84 (rep)   │       │
│  │ 170-255(rep) │    │ 0-84 (rep)   │    │ 85-169 (rep) │       │
│  └──────────────┘    └──────────────┘    └──────────────┘       │
│                                                                 │
│  Client Request: SET user:123 = {...}                           │
│  1. Hash("user:123") → Partition 42                             │
│  2. Partition 42 owned by Node 1                                │
│  3. Request routed to Node 1                                    │
│  4. Node 1 writes locally + replicates to Node 2 & 3            │
└─────────────────────────────────────────────────────────────────┘
```

**Failover Process:**
1. Followers send heartbeats to all cluster nodes every 500ms (configurable)
2. If heartbeat to leader fails twice consecutively, leader is marked as dead
3. Re-election is triggered among remaining alive nodes
4. Raft consensus elects new leader with majority vote
5. Other nodes connect to the new leader for replication
6. Partitions automatically rebalance if needed

### Consistency Levels

| Level | Description |
|-------|-------------|
| `EVENTUAL` | Returns immediately, replicates asynchronously |
| `ONE` | Waits for at least one replica to acknowledge |
| `QUORUM` | Waits for majority acknowledgment |
| `ALL` | Waits for all replicas to acknowledge |

### Cluster Features

#### Data Sharding & Distribution
- **Consistent Hashing**: Data sharding with virtual nodes (150 per node) for even distribution
- **256 Partitions**: Default partition count, configurable via `partition_count`
- **Replication Factor**: 3 replicas per partition by default (configurable)
- **Partition-Aware Routing**: Automatic request routing to partition owners
- **Cross-Partition Queries**: Scatter-gather support for full table scans
- **Data Migration**: Automatic partition migration during rebalancing

#### Advanced Routing Strategies
FlyDB supports **5 routing strategies** optimized for different workloads:

- **Key-Based** (Default): Consistent hashing for data locality and minimal rebalancing
- **Round-Robin**: Perfect load distribution for write-heavy workloads
- **Least-Loaded**: Automatic load balancing for heterogeneous hardware
- **Locality-Aware**: Datacenter/rack/zone awareness for geo-distributed clusters
- **Hybrid** (Recommended): Production-ready combination of all strategies

```yaml
# Configuration
routing_strategy: "hybrid"  # Choose: key_based, round_robin, least_loaded, locality_aware, hybrid
datacenter: "us-east-1"
rack: "rack-1"
zone: "zone-a"
```

#### Performance Optimizations

**Zero-Copy I/O** (5-10x faster data migration):
- sendfile() syscall on Linux/macOS
- splice() syscall on Linux
- Memory-mapped I/O for large files
- 50-70% reduction in CPU usage

**Connection Pooling** (3-5x reduction in overhead):
- Per-node connection pools
- Automatic health checking
- Configurable pool limits
- Connection reuse across requests

```yaml
connection_pool:
  max_idle_per_node: 10
  max_open_per_node: 100
  idle_timeout: 5m
```

**Adaptive Buffering** (20-30% less memory):
- Automatically adjusts buffer size
- 40-60% reduction in allocations
- Reduced GC pressure

**Performance Metrics**:
| Optimization | Improvement |
|--------------|-------------|
| Zero-Copy I/O | 5-10x faster migration (17 MB/s → 100 MB/s) |
| Connection Pooling | 3-5x reduction in overhead |
| Buffer Pooling | 40-60% fewer allocations |
| Adaptive Buffering | 20-30% less memory usage |

#### Comprehensive Metadata Management
- **Cluster Metadata**: Version tracking, node registry, partition assignments
- **Node Metadata**: Capacity, load, topology, health status
- **Partition Metadata**: Leader, replicas, state, statistics, replication lag
- **Routing Tables**: O(1) partition lookups
- **Persistence**: CRC32 integrity verification

#### Consensus & Leader Election
- **Raft Consensus**: Production-ready Raft implementation with pre-vote
- **Term-based Elections**: Monotonically increasing term numbers prevent stale leaders
- **Quorum Requirements**: Majority required for leader election and decisions
- **Split-brain Prevention**: Leaders step down if they lose quorum
- **Fast Failover**: Sub-second leader election on failure

#### High Availability
- **Dynamic Membership**: Nodes can join/leave without cluster restart
- **Health Monitoring**: Per-node health tracking with automatic failure detection
- **Automatic Rebalancing**: Partitions redistribute when nodes join or leave
- **Replication Lag Tracking**: Monitor lag per follower for capacity planning
- **Graceful Degradation**: Cluster remains available with reduced capacity

#### Consistency Levels
FlyDB supports tunable consistency for different use cases:

| Level | Acknowledgments | Use Case |
|-------|----------------|----------|
| `EVENTUAL` | None (async) | Maximum throughput, eventual consistency |
| `ONE` | 1 replica | Balanced performance and durability |
| `QUORUM` | Majority | Strong consistency, fault tolerance |
| `ALL` | All replicas | Maximum durability, slower writes |

Configure via `replication_mode` in config:
```yaml
replication_mode: "semi_sync"  # Maps to QUORUM consistency
```

### HA Client Connections

The `fsql` client supports PostgreSQL-style high-availability connections with automatic failover. Connect to multiple cluster nodes using comma-separated hosts:

```bash
# Connect to a 3-node cluster
fsql -H node1,node2,node3 -p 8889

# Hosts with individual ports
fsql -H node1:8889,node2:8890,node3:8891

# Prefer connecting to primary/leader
fsql -H node1,node2,node3 -p 8889 --target-primary
```

**Automatic Failover:**
- If the current connection fails, the client automatically reconnects to another node
- Authentication credentials are cached and re-applied on reconnection
- Use `\status` or `\conninfo` to see current connection and cluster info

**Configuration File:**

You can also configure HA hosts in a config file (`~/.flydbrc`):

```ini
# Multiple hosts for HA cluster
hosts = node1,node2,node3
port = 8889
target_primary = true
```

### Database Dump Utility

The `flydb-dump` utility (command: `fdump`) provides database export and import functionality with support for both local and remote modes.

**Local Mode** (direct file access):
```bash
# Export database to SQL
fdump -d /var/lib/flydb -db mydb -o backup.sql

# Export with encryption passphrase
fdump -d /var/lib/flydb --passphrase secret -o backup.sql

# Import from SQL dump
fdump -d /var/lib/flydb --import backup.sql
```

**Remote Mode** (network connection):
```bash
# Export from remote server
fdump --host localhost --port 8889 -U admin -P -o backup.sql

# Export from cluster (connects to any available node)
fdump --host node1,node2,node3 -U admin -P -o backup.sql

# Import to cluster (discovers leader for writes)
fdump --host node1,node2,node3 -U admin -P --import backup.sql
```

**Export Formats:**
| Format | Flag | Description |
|--------|------|-------------|
| SQL | `-f sql` | Standard INSERT statements (default) |
| CSV | `-f csv` | RFC 4180 compliant with headers |
| JSON | `-f json` | Structured with metadata |

**Common Options:**
| Option | Description |
|--------|-------------|
| `-d <path>` | Data directory (local mode) |
| `--host <hosts>` | Server hostname(s), comma-separated for cluster |
| `--port <port>` | Server port (default: 8889) |
| `-db <name>` | Database name (default: default) |
| `-o <file>` | Output file (default: stdout) |
| `-f <format>` | Output format: sql, csv, json |
| `-t <tables>` | Comma-separated list of tables |
| `--schema-only` | Export schema only, no data |
| `--data-only` | Export data only, no schema |
| `-z` | Compress output with gzip |
| `--import <file>` | Import from SQL dump file |
| `-U <user>` | Username for authentication |
| `-P` | Prompt for password |

---

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/architecture.md) | System design, component diagrams, data flow |
| [Cluster Setup](docs/CLUSTER_SETUP.md) | **NEW!** Complete guide to cluster setup, service discovery, and deployment |
| [Implementation](docs/implementation.md) | WAL, B-Tree, SQL processing, transactions |
| [Design Decisions](docs/design-decisions.md) | Rationale and trade-offs |
| [API Reference](docs/api.md) | SQL syntax, protocol commands, configuration |
| [Driver Development](docs/driver-development.md) | Binary protocol specification for JDBC/ODBC drivers |
| [Changelog](CHANGELOG.md) | Version history and release notes |
| [Roadmap](ROADMAP.md) | Completed and planned features |

---

## Development

### Running Tests

```bash
go test ./...
```

### Integration Tests

```bash
./integration_test.sh    # Replication and JOIN tests
./auth_test.sh           # Authentication and RLS tests
./prod_test.sh           # UPDATE, DELETE, ORDER BY tests
```

### Manual Protocol Testing

Use the `fsql` CLI client to test the binary protocol:

```bash
fsql -h localhost -p 8889
# Connect and run SQL commands
```

### Project Structure

```
flydb/
    cmd/
        flydb/              Server entry point
        flydb-shell/        CLI client entry point (fsql command)
        flydb-dump/         Database dump/restore utility (fdump command)
    internal/
        auth/               Authentication and authorization
        banner/             Startup banner display
        cache/              Query caching
        cluster/            Unified cluster-replication management
        config/             Configuration management
        errors/             Error handling
        logging/            Structured logging
        pool/               Connection pooling
        protocol/           Binary wire protocol (JDBC/ODBC compatible)
        sdk/                SDK types for driver development
        server/             TCP server and replication
        sql/                Lexer, parser, executor, catalog
        storage/            Storage engine, WAL, B-Tree, encryption
            disk/           Buffer pool, heap file, page management
        wizard/             Interactive configuration wizard
    docs/                   Technical documentation
    install.sh              Installation script
    uninstall.sh            Uninstallation script
```

---

## License

```
Copyright 2026 Firefly Software Solutions Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```