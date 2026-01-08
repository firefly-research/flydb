# FlyDB Implementation Details

This document provides technical details on the key algorithms, data structures, and implementation approaches used in FlyDB.

## Table of Contents

1. [Storage Engine Architecture](#storage-engine-architecture)
2. [Page-Based Disk Storage](#page-based-disk-storage)
3. [Buffer Pool](#buffer-pool)
4. [Write-Ahead Log (WAL)](#write-ahead-log-wal)
5. [Multi-Database Architecture](#multi-database-architecture)
6. [Collation System](#collation-system)
7. [Encoding System](#encoding-system)
8. [B-Tree Indexes](#b-tree-indexes)
9. [SQL Processing Pipeline](#sql-processing-pipeline)
10. [Prepared Statements](#prepared-statements)
11. [Triggers](#triggers)
12. [Transaction Support](#transaction-support)
13. [Authentication & Authorization](#authentication--authorization)
14. [Replication](#replication)
15. [Clustering](#clustering)
16. [Query Cache](#query-cache)

---

## Storage Engine Architecture

### Unified Disk-Based Storage

FlyDB uses a **unified disk-based storage engine** that combines page-based storage with intelligent buffer pool caching. This architecture provides optimal performance for both small datasets (that fit entirely in the buffer pool) and large datasets (that exceed available RAM).

```
┌─────────────────────────────────────────────────────────────────┐
│                      SQL Executor                               │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Engine Interface                              │
│         (Put, Get, Delete, Scan, Close)                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  UnifiedStorageEngine                           │
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │              Buffer Pool (LRU-K Caching)                │   │
│   │   - Auto-sized based on available memory                │   │
│   │   - Intelligent page prefetching                        │   │
│   │   - Optimized for sequential and random access          │   │
│   └─────────────────────────────────────────────────────────┘   │
│                              │                                  │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │              Page-Based Disk Storage                    │   │
│   │   - Heap file organization                              │   │
│   │   - Slotted page format (8KB pages)                     │   │
│   │   - Efficient space management                          │   │
│   └─────────────────────────────────────────────────────────┘   │
│                              │                                  │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │              Write-Ahead Log (WAL)                      │   │
│   │   - Durability guarantees                               │   │
│   │   - Crash recovery                                      │   │
│   │   - Optional AES-256-GCM encryption                     │   │
│   └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Engine Interface

The storage engine implements a simple key-value interface:

```go
type Engine interface {
    Put(key string, value []byte) error
    Get(key string) ([]byte, error)
    Delete(key string) error
    Scan(prefix string) (map[string][]byte, error)
    Close() error
}
```

### Creating a Storage Engine

```go
config := storage.StorageConfig{
    DataDir:            "./data",
    BufferPoolSize:     0,  // 0 = auto-size based on available memory
    CheckpointInterval: 60 * time.Second,
    Encryption: storage.EncryptionConfig{
        Enabled:    true,
        Passphrase: "your-secure-passphrase",
    },
}
engine, err := storage.NewStorageEngine(config)
```

### Key Encoding Convention

FlyDB uses a prefix-based key encoding scheme:

| Data Type | Key Format | Example |
|-----------|------------|---------|
| Row Data | `row:<table>:<id>` | `row:users:42` |
| Schema | `schema:<table>` | `schema:users` |
| Sequence | `seq:<table>` | `seq:users` |
| User | `_sys_users:<name>` | `_sys_users:alice` |
| Permission | `_sys_privs:<user>:<table>` | `_sys_privs:alice:orders` |

This convention enables efficient prefix scans for operations like "get all rows from table X".

---

## Page-Based Disk Storage

FlyDB uses a PostgreSQL-style page-based storage architecture with 8KB slotted pages.

### Slotted Page Layout

Each 8KB page uses a "slotted page" design that efficiently handles variable-length records:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Page Header (24 bytes)                       │
│  [PageID | Type | Flags | SlotCount | FreeStart | FreeEnd]      │
├─────────────────────────────────────────────────────────────────┤
│  Slot Array (grows →)                                           │
│  [Slot 0: offset,len] [Slot 1: offset,len] [Slot 2: offset,len] │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│                    Free Space                                   │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│  Record Data (← grows)                                          │
│  [Record 2 data] [Record 1 data] [Record 0 data]                │
└─────────────────────────────────────────────────────────────────┘
```

**Key Design Decisions:**
- **Slot Array** grows forward from the header
- **Record Data** grows backward from the end of the page
- **Free Space** is in the middle, allowing both to grow independently
- Deleted records leave "holes" that can be reclaimed via compaction

### Record Format

Records are stored with a simple key-length prefix:

```
┌────────────────────────────────────────────────────────┐
│ Key Length (4 bytes) │ Key (variable) │ Value (variable) │
└────────────────────────────────────────────────────────┘
```

### Heap File Organization

The heap file stores pages sequentially on disk:

```
┌─────────────────────────────────────────────────────────────────┐
│ Header Page (8KB)                                               │
│ - Magic number, version, page count                             │
├─────────────────────────────────────────────────────────────────┤
│ Data Page 1 (8KB)                                               │
├─────────────────────────────────────────────────────────────────┤
│ Data Page 2 (8KB)                                               │
├─────────────────────────────────────────────────────────────────┤
│ ...                                                             │
└─────────────────────────────────────────────────────────────────┘
```

---

## Buffer Pool

The buffer pool is a critical component that caches database pages in memory, reducing expensive disk I/O operations.

### Why Buffer Pools Matter

Disk I/O is orders of magnitude slower than memory access:
- Memory access: ~100 nanoseconds
- SSD random read: ~100 microseconds (1000x slower)
- HDD random read: ~10 milliseconds (100,000x slower)

A well-designed buffer pool can achieve 90%+ cache hit rates.

### LRU-K Eviction Algorithm

FlyDB uses **LRU-K (specifically LRU-2)** instead of simple LRU for page eviction. This algorithm was introduced by O'Neil, O'Neil, and Weikum in 1993.

**Simple LRU Problem:**
- A sequential scan can flush the entire buffer pool
- Frequently accessed pages get evicted by one-time accesses

**LRU-K Solution:**
- Track the last K access times for each page
- Evict the page with the oldest K-th access
- Pages accessed only once are evicted before frequently accessed pages

### Auto-Sizing

The buffer pool automatically sizes itself based on available system memory:
- Uses **25% of available RAM**
- Minimum: 2MB (256 pages)
- Maximum: 1GB (131,072 pages)

This provides optimal out-of-box performance without manual tuning.

### Buffer Pool Operations

| Operation | Description |
|-----------|-------------|
| `FetchPage(pageID)` | Returns page from cache or loads from disk |
| `NewPage()` | Allocates a new page |
| `UnpinPage(pageID, dirty)` | Releases page, marks dirty if modified |
| `FlushPage(pageID)` | Writes dirty page to disk |
| `FlushAllPages()` | Writes all dirty pages (for checkpoint) |
| `Prefetch(pageIDs...)` | Asynchronously loads pages for sequential scans |

### Checkpoint Manager

Checkpoints create consistent snapshots for recovery:

1. Flush all dirty pages to disk
2. Sync heap file
3. Record checkpoint in metadata
4. Continue normal operation

Checkpoints run automatically at configurable intervals (default: 60 seconds).

### Performance Characteristics

| Operation | Cached (in buffer pool) | Uncached (disk I/O) |
|-----------|-------------------------|---------------------|
| Put       | O(1) + WAL              | O(1) + WAL          |
| Get       | O(1)                    | O(disk)             |
| Delete    | O(1) + WAL              | O(1) + WAL          |
| Scan      | O(N) with prefetch      | O(N) + disk I/O     |

---

## Write-Ahead Log (WAL)

### Purpose

The WAL provides durability by persisting all operations to disk before they are applied to storage. This ensures committed data survives crashes and restarts.

### Record Format

Each WAL record uses a binary format:

```
┌─────────┬───────────┬─────────────┬─────────────┬─────────────┐
│ Op (1B) │ KeyLen(4B)│ Key (var)   │ ValLen (4B) │ Value (var) │
└─────────┴───────────┴─────────────┴─────────────┴─────────────┘

Op Types:
  1 = OpPut    (insert or update)
  2 = OpDelete (remove key)
```

### Encrypted WAL Format

When encryption is enabled, records are wrapped:

```
┌──────────────┬─────────────────────────────────────────────────────┐
│ EncLen (4B)  │ Encrypted Payload (nonce + ciphertext + tag)        │
└──────────────┴─────────────────────────────────────────────────────┘
```

### Write Path

```go
func (w *WAL) Write(op byte, key string, value []byte) error {
    w.mu.Lock()
    defer w.mu.Unlock()
    
    // 1. Serialize the record
    buf := make([]byte, 1+4+len(key)+4+len(value))
    buf[0] = op
    binary.BigEndian.PutUint32(buf[1:], uint32(len(key)))
    copy(buf[5:], []byte(key))
    // ... value encoding
    
    // 2. Encrypt if enabled
    if w.encryptor != nil {
        buf, _ = w.encryptor.Encrypt(buf)
    }
    
    // 3. Append to file
    _, err := w.file.Write(buf)
    return err
}
```

### Recovery (Replay)

On startup, the WAL is replayed from offset 0:

```go
err := wal.Replay(0, func(op byte, key string, value []byte) {
    if op == OpPut {
        data[key] = value
    } else {
        delete(data, key)
    }
})
```

---

## Multi-Database Architecture

FlyDB supports MySQL-like multi-database functionality, allowing users to create, drop, and switch between multiple isolated databases.

### Architecture

```
┌─────────────────────────────────────────────────────┐
│                  DatabaseManager                    │
├─────────────────────────────────────────────────────┤
│  dataDir: /var/lib/flydb/                           │
│  databases: map[string]*Database                    │
│  encConfig: EncryptionConfig                        │
└─────────────────────────────────────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  default/   │  │   mydb/     │  │  testdb/    │
│  data.db    │  │   data.db   │  │  data.db    │
│  wal.fdb    │  │   wal.fdb   │  │  wal.fdb    │
└─────────────┘  └─────────────┘  └─────────────┘
```

Each database has its own:
- Data directory with heap file (`data.db`)
- Write-ahead log (`wal.fdb`)
- Buffer pool instance
- Metadata configuration

### Database Metadata

Each database stores its configuration in the key `_sys_db_meta`:

```go
type DatabaseMetadata struct {
    Name        string            // Database name
    Owner       string            // Creator username
    Encoding    CharacterEncoding // UTF8, LATIN1, ASCII, UTF16
    Collation   Collation         // default, binary, nocase, unicode
    Locale      string            // e.g., "en_US", "de_DE"
    CreatedAt   time.Time
    UpdatedAt   time.Time
    Description string
    ReadOnly    bool
    MaxSize     int64             // Maximum size in bytes (0 = unlimited)
    Properties  map[string]string // Extensible properties
}
```

### Per-Connection Database Context

The server tracks which database each connection is using:

```go
// Each connection has an associated database
connDatabases map[net.Conn]string  // Connection -> database name

// Switch database for a connection
func (s *Server) SetConnectionDatabase(conn net.Conn, dbName string)

// Get current database for a connection
func (s *Server) GetConnectionDatabase(conn net.Conn) string
```

### Database Operations

| Operation | SQL Command | Description |
|-----------|-------------|-------------|
| Create | `CREATE DATABASE mydb` | Creates new database with default settings |
| Create with options | `CREATE DATABASE mydb ENCODING UTF8 COLLATION nocase` | Creates with specific settings |
| Drop | `DROP DATABASE mydb` | Removes database and all data |
| Switch | `USE mydb` | Changes current database for connection |
| List | `SHOW DATABASES` | Lists all available databases |

### Lazy Loading

Databases are loaded on-demand when first accessed:

1. Check if database is already loaded in memory
2. If not, verify database directory exists on disk
3. Create storage engine for the database
4. Load metadata from `_sys_db_meta` key
5. Cache database instance for future access
---

## Collation System

### Collator Interface

Collation determines how strings are compared and sorted:

```go
type Collator interface {
    Compare(a, b string) int  // -1 if a < b, 0 if a == b, 1 if a > b
    Equal(a, b string) bool
}
```

### Implementations

**DefaultCollator** - Uses Go's native string comparison:
```go
type DefaultCollator struct{}

func (c *DefaultCollator) Compare(a, b string) int {
    if a < b { return -1 }
    if a > b { return 1 }
    return 0
}
```

**BinaryCollator** - Byte-by-byte comparison:
```go
type BinaryCollator struct{}

func (c *BinaryCollator) Compare(a, b string) int {
    return bytes.Compare([]byte(a), []byte(b))
}
```

**NocaseCollator** - Case-insensitive comparison:
```go
type NocaseCollator struct{}

func (c *NocaseCollator) Compare(a, b string) int {
    return strings.Compare(strings.ToLower(a), strings.ToLower(b))
}
```

**UnicodeCollator** - Locale-aware Unicode collation:
```go
type UnicodeCollator struct {
    collator *collate.Collator
}

func NewUnicodeCollator(locale string) *UnicodeCollator {
    tag := language.Make(locale)
    return &UnicodeCollator{
        collator: collate.New(tag),
    }
}

func (c *UnicodeCollator) Compare(a, b string) int {
    return c.collator.CompareString(a, b)
}
```

### Usage in Executor

The executor uses the database's collator for string comparisons:

```go
func (e *Executor) stringsEqual(a, b string) bool {
    if e.collator != nil {
        return e.collator.Equal(a, b)
    }
    return a == b
}

func (e *Executor) compareStrings(a, b string) int {
    if e.collator != nil {
        return e.collator.Compare(a, b)
    }
    if a < b { return -1 }
    if a > b { return 1 }
    return 0
}
```

---

## Encoding System

### Encoder Interface

Encoders validate and convert string data:

```go
type Encoder interface {
    Encode(s string) ([]byte, error)
    Decode(b []byte) (string, error)
    Validate(s string) error
    Name() string
}
```

### Implementations

**UTF8Encoder** - Go's native encoding:
```go
type UTF8Encoder struct{}

func (e *UTF8Encoder) Validate(s string) error {
    if !utf8.ValidString(s) {
        return errors.New("invalid UTF-8 string")
    }
    return nil
}
```

**ASCIIEncoder** - 7-bit ASCII only:
```go
type ASCIIEncoder struct{}

func (e *ASCIIEncoder) Validate(s string) error {
    for _, r := range s {
        if r > 127 {
            return fmt.Errorf("character %q is not valid ASCII", r)
        }
    }
    return nil
}
```

**Latin1Encoder** - ISO-8859-1:
```go
type Latin1Encoder struct{}

func (e *Latin1Encoder) Validate(s string) error {
    for _, r := range s {
        if r > 255 {
            return fmt.Errorf("character %q is not valid Latin-1", r)
        }
    }
    return nil
}
```

### Validation During INSERT/UPDATE

The executor validates string data against the database encoding:

```go
func (e *Executor) validateEncoding(value interface{}) error {
    if e.encoder == nil {
        return nil
    }
    if str, ok := value.(string); ok {
        return e.encoder.Validate(str)
    }
    return nil
}
```

---

## B-Tree Indexes

### Structure

FlyDB implements a classic B-Tree for secondary indexes:

```go
type BTree struct {
    root *BTreeNode
    t    int           // Minimum degree
    mu   sync.RWMutex
}

type BTreeNode struct {
    keys     []string      // Sorted keys
    values   []string      // Row keys (pointers to actual data)
    children []*BTreeNode  // Child nodes (nil for leaves)
    leaf     bool
}
```

### Properties

- **Minimum Degree (t)**: Each node has at most `2t-1` keys and at least `t-1` keys (except root)
- **Balanced**: All leaves are at the same depth
- **Time Complexity**: O(log N) for search, insert, delete

### Operations

**Search:**
```go
func (bt *BTree) Search(key string) (string, bool) {
    return bt.searchNode(bt.root, key)
}

func (bt *BTree) searchNode(node *BTreeNode, key string) (string, bool) {
    i := 0
    for i < len(node.keys) && key > node.keys[i] {
        i++
    }
    if i < len(node.keys) && node.keys[i] == key {
        return node.values[i], true  // Found
    }
    if node.leaf {
        return "", false  // Not found
    }
    return bt.searchNode(node.children[i], key)  // Recurse
}
```

**Range Queries:**
```go
func (bt *BTree) Range(start, end string) []struct{ Key, Value string }
```

### Index Manager

The `IndexManager` coordinates index operations:

```go
type IndexManager struct {
    indexes map[string]map[string]*BTree  // table -> column -> index
    store   Engine
    mu      sync.RWMutex
}
```

Indexes are automatically updated on INSERT, UPDATE, and DELETE operations.

---

## SQL Processing Pipeline

### Overview

SQL processing follows a classic three-stage pipeline:

```
SQL Text → Lexer → Tokens → Parser → AST → Executor → Results
```

### Lexer (`lexer.go`)

The lexer tokenizes SQL input into a stream of tokens:

```go
type TokenType int

const (
    TOKEN_KEYWORD TokenType = iota  // SELECT, INSERT, etc.
    TOKEN_IDENT                      // table names, column names
    TOKEN_NUMBER                     // numeric literals
    TOKEN_STRING                     // string literals
    TOKEN_OPERATOR                   // =, <, >, <=, >=, <>
    TOKEN_COMMA
    TOKEN_LPAREN
    TOKEN_RPAREN
    TOKEN_STAR
    TOKEN_EOF
)

type Token struct {
    Type  TokenType
    Value string
}
```

**Tokenization Process:**
1. Skip whitespace
2. Identify token type by first character
3. Consume characters until token boundary
4. Return token with type and value

### Parser (`parser.go`)

The parser builds an Abstract Syntax Tree (AST) from tokens:

**Supported Statements:**
- `SELECT` with WHERE, ORDER BY, LIMIT, OFFSET, GROUP BY, HAVING
- `INSERT INTO ... VALUES`
- `UPDATE ... SET ... WHERE`
- `DELETE FROM ... WHERE`
- `CREATE TABLE [IF NOT EXISTS]` with column types and constraints
- `CREATE INDEX [IF NOT EXISTS]`, `DROP INDEX [IF EXISTS]`
- `DROP TABLE [IF EXISTS]`
- `TRUNCATE TABLE`
- `JOIN` (INNER, LEFT, RIGHT, FULL)
- `UNION`, `UNION ALL`
- Subqueries in WHERE and FROM clauses
- `CREATE [OR REPLACE] PROCEDURE [IF NOT EXISTS]`, `DROP PROCEDURE [IF EXISTS]`, `CALL`
- `CREATE [OR REPLACE] VIEW [IF NOT EXISTS]`, `DROP VIEW [IF EXISTS]`
- `CREATE [OR REPLACE] TRIGGER [IF NOT EXISTS]`, `DROP TRIGGER [IF EXISTS]`

**AST Node Types:**

```go
type Statement interface{}

type SelectStmt struct {
    TableName  string
    Columns    []string
    Where      *WhereClause
    OrderBy    string
    OrderDir   string
    Limit      int
    Offset     int
    GroupBy    []string
    Having     *WhereClause
    Joins      []JoinClause
    Subquery   *SelectStmt
}

type InsertStmt struct {
    TableName string
    Columns   []string
    Values    [][]string
}

type WhereClause struct {
    Column   string
    Operator string
    Value    string
    And      *WhereClause
    Or       *WhereClause
}
```

### Executor (`executor.go`)

The executor traverses the AST and performs operations:

```go
type Executor struct {
    store       Engine
    catalog     *Catalog
    auth        *AuthManager
    indexMgr    *IndexManager
    prepMgr     *PreparedStatementManager
    cache       *Cache
    subscribers map[string][]chan string
}

func (e *Executor) Execute(stmt Statement, user string) (string, error) {
    switch s := stmt.(type) {
    case *SelectStmt:
        return e.executeSelect(s, user)
    case *InsertStmt:
        return e.executeInsert(s, user)
    case *UpdateStmt:
        return e.executeUpdate(s, user)
    case *DeleteStmt:
        return e.executeDelete(s, user)
    // ... other statement types
    }
}
```

**Query Execution Steps (SELECT):**
1. Check user permissions via AuthManager
2. Check query cache for cached results
3. Retrieve table schema from Catalog
4. Scan rows from KVStore (or use index if available)
5. Apply WHERE filters (using collator for string comparisons)
6. Apply Row-Level Security (RLS) conditions
7. Apply JOINs if present
8. Apply GROUP BY and aggregate functions
9. Apply HAVING filter
10. Apply ORDER BY (using collator for string sorting)
11. Apply LIMIT and OFFSET
12. Format and return results

---

## Prepared Statements

### PreparedStatementManager

Prepared statements allow parsing once and executing multiple times:

```go
type PreparedStatementManager struct {
    statements map[string]*PreparedStatement
    mu         sync.RWMutex
}

type PreparedStatement struct {
    Name       string
    Query      string
    ParamCount int
}
```

### Operations

**Prepare:**
```go
func (m *PreparedStatementManager) Prepare(name, query string) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    // Count parameter placeholders (?)
    paramCount := strings.Count(query, "?")

    m.statements[name] = &PreparedStatement{
        Name:       name,
        Query:      query,
        ParamCount: paramCount,
    }
    return nil
}
```

**Execute:**
```go
func (m *PreparedStatementManager) Execute(name string, params []string) (string, error) {
    m.mu.RLock()
    stmt, ok := m.statements[name]
    m.mu.RUnlock()

    if !ok {
        return "", fmt.Errorf("prepared statement %s not found", name)
    }

    if len(params) != stmt.ParamCount {
        return "", fmt.Errorf("expected %d parameters, got %d", stmt.ParamCount, len(params))
    }

    // Substitute parameters
    query := stmt.Query
    for _, param := range params {
        query = strings.Replace(query, "?", "'"+param+"'", 1)
    }

    // Parse and execute
    lexer := NewLexer(query)
    parser := NewParser(lexer)
    parsedStmt, err := parser.Parse()
    if err != nil {
        return "", err
    }
    return e.Execute(parsedStmt, user)
}
```

**Deallocate:**
```go
func (m *PreparedStatementManager) Deallocate(name string) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    delete(m.statements, name)
    return nil
}
```

---

## Triggers

### Overview

Triggers are automatic actions that execute in response to INSERT, UPDATE, or DELETE operations on tables. FlyDB supports both BEFORE and AFTER triggers.

### Trigger Storage

Triggers are stored in the KVStore with the key format:

```
trigger:<table>:<name> → Trigger JSON
```

### TriggerManager

The `TriggerManager` handles trigger registration, storage, and execution:

```go
type TriggerManager struct {
    triggers map[string]map[string]*Trigger  // table -> name -> trigger
    store    storage.Engine
    mu       sync.RWMutex
}
```

### Trigger Execution Flow

1. A DML operation (INSERT, UPDATE, DELETE) is initiated
2. BEFORE triggers are executed (if any)
3. The DML operation is performed
4. AFTER triggers are executed (if any)

### Trigger Definition

```go
type Trigger struct {
    Name      string        // Trigger name
    Timing    TriggerTiming // BEFORE or AFTER
    Event     TriggerEvent  // INSERT, UPDATE, or DELETE
    TableName string        // The table the trigger is attached to
    ActionSQL string        // The SQL statement to execute
}
```

### Execution

When a trigger fires, its ActionSQL is parsed and executed:

```go
func (e *Executor) executeTriggers(tableName string, timing TriggerTiming, event TriggerEvent) error {
    triggers := e.triggerMgr.GetTriggers(tableName, timing, event)
    for _, trigger := range triggers {
        lexer := NewLexer(trigger.ActionSQL)
        parser := NewParser(lexer)
        stmt, err := parser.Parse()
        if err != nil {
            return err
        }
        _, err = e.Execute(stmt)
        if err != nil {
            return err
        }
    }
    return nil
}
```

---

## Transaction Support

### Transaction Model

FlyDB uses an optimistic concurrency model with the following ACID properties:

- **Atomicity**: All operations in a transaction are applied together or not at all
- **Consistency**: Transactions maintain database invariants
- **Isolation**: Read-committed isolation level (reads see committed data only)
- **Durability**: Committed transactions are persisted to WAL

### Transaction Lifecycle

```
┌─────────┐     ┌─────────────┐     ┌──────────┐
│  BEGIN  │ ──▶ │  Operations │ ──▶ │  COMMIT  │
└─────────┘     │  (buffered) │     └──────────┘
                └─────────────┘           │
                      │                   ▼
                      │           ┌──────────────┐
                      └─────────▶ │   ROLLBACK   │
                                  └──────────────┘
```

1. **BEGIN**: Creates a new transaction with a write buffer
2. **Operations**: Writes go to the buffer, reads check buffer then storage
3. **COMMIT**: Applies all buffered writes to storage atomically
4. **ROLLBACK**: Discards the write buffer

### Transaction Structure

```go
type Transaction struct {
    store      Engine              // Underlying storage engine
    buffer     []TxOperation       // Ordered list of pending operations
    readCache  map[string][]byte   // Read-your-writes cache
    deleteSet  map[string]bool     // Pending deletes
    state      TxState             // Active, Committed, or RolledBack
    savepoints []Savepoint         // Named savepoints
    mu         sync.Mutex
}
```

### Read Path (Read-Your-Writes)

```go
func (tx *Transaction) Get(key string) ([]byte, error) {
    // 1. Check if key was deleted in this transaction
    if tx.deleteSet[key] {
        return nil, ErrNotFound
    }
    // 2. Check local write buffer (read-your-writes)
    if val, ok := tx.readCache[key]; ok {
        return val, nil
    }
    // 3. Fall back to underlying storage
    return tx.store.Get(key)
}
```

### Commit Process

```go
func (tx *Transaction) Commit() error {
    tx.mu.Lock()
    defer tx.mu.Unlock()

    // Apply all buffered operations in order
    for _, op := range tx.buffer {
        switch op.Op {
        case OpPut:
            if err := tx.store.Put(op.Key, op.Value); err != nil {
                return err
            }
        case OpDelete:
            if err := tx.store.Delete(op.Key); err != nil {
                return err
            }
        }
    }
    tx.state = TxStateCommitted
    return nil
}
```

**Rollback:**
```go
func (tx *Transaction) Rollback() error {
    tx.mu.Lock()
    defer tx.mu.Unlock()
    tx.buffer = nil
    tx.readCache = nil
    tx.deleteSet = nil
    tx.state = TxRolledBack
    return nil
}
```

### Savepoints

Savepoints allow partial rollback within a transaction:

**Create Savepoint:**
```go
func (tx *Transaction) Savepoint(name string) error {
    tx.mu.Lock()
    defer tx.mu.Unlock()

    // Snapshot current state
    sp := Savepoint{
        Name:           name,
        BufferPosition: len(tx.buffer),
        ReadCacheSnap:  copyMap(tx.readCache),
        DeleteSetSnap:  copyMap(tx.deleteSet),
    }
    tx.savepoints = append(tx.savepoints, sp)
    return nil
}
```

**Rollback to Savepoint:**
```go
func (tx *Transaction) RollbackToSavepoint(name string) error {
    tx.mu.Lock()
    defer tx.mu.Unlock()

    // Find the savepoint
    idx := -1
    for i, sp := range tx.savepoints {
        if sp.Name == name {
            idx = i
            break
        }
    }
    if idx == -1 {
        return fmt.Errorf("savepoint %s not found", name)
    }

    sp := tx.savepoints[idx]

    // Restore state to savepoint
    tx.buffer = tx.buffer[:sp.BufferPosition]
    tx.readCache = sp.ReadCacheSnap
    tx.deleteSet = sp.DeleteSetSnap

    // Remove this and all later savepoints
    tx.savepoints = tx.savepoints[:idx]
    return nil
}
```

**Release Savepoint:**
```go
func (tx *Transaction) ReleaseSavepoint(name string) error {
    tx.mu.Lock()
    defer tx.mu.Unlock()

    for i, sp := range tx.savepoints {
        if sp.Name == name {
            tx.savepoints = append(tx.savepoints[:i], tx.savepoints[i+1:]...)
            return nil
        }
    }
    return fmt.Errorf("savepoint %s not found", name)
}
```

---

## Authentication & Authorization

### User Management

Users are stored in the storage engine with bcrypt-hashed passwords:

```go
type User struct {
    Username     string `json:"username"`
    PasswordHash string `json:"password_hash"`
    Roles        []string `json:"roles"`
}

// Storage key: _sys_users:<username>
```

**Password Hashing:**
```go
func (a *AuthManager) CreateUser(username, password string) error {
    hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
    // Store user with hashed password
}

func (a *AuthManager) Authenticate(username, password string) bool {
    user := a.getUser(username)
    err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password))
    return err == nil
}
```

### Table-Level Permissions

Permissions are stored per user per table:

```go
type Permission struct {
    CanSelect bool `json:"can_select"`
    CanInsert bool `json:"can_insert"`
    CanUpdate bool `json:"can_update"`
    CanDelete bool `json:"can_delete"`
}

// Storage key: _sys_privs:<username>:<tablename>
```

**GRANT/REVOKE:**
```go
func (a *AuthManager) Grant(user, table string, perms ...string) error
func (a *AuthManager) Revoke(user, table string, perms ...string) error
```

### Row-Level Security (RLS)

RLS policies filter rows based on the current user:

```go
type RLSPolicy struct {
    Table     string `json:"table"`
    Column    string `json:"column"`
    Operator  string `json:"operator"`
    Value     string `json:"value"`  // Can be "CURRENT_USER"
}
```

**Example Policy:**
```sql
-- Users can only see their own orders
CREATE POLICY ON orders USING (user_id = CURRENT_USER)
```

**Enforcement:**
During SELECT execution, RLS conditions are automatically appended to the WHERE clause.

---

## Replication

### Leader-Follower Model

FlyDB uses asynchronous WAL-based replication:

```go
type Replicator struct {
    role       string  // "leader" or "follower"
    leaderAddr string
    wal        *WAL
    store      Engine
    offset     int64   // Current WAL position
    mu         sync.Mutex
}
```

### Replication Protocol

**Follower → Leader:**
```
REPLICATE <offset>\n
```

**Leader → Follower:**
```
WAL entries from <offset> to current position
```

### Polling Mechanism

Followers poll the leader every 100ms:

```go
func (r *Replicator) startFollowerLoop() {
    ticker := time.NewTicker(100 * time.Millisecond)
    for range ticker.C {
        r.pullFromLeader()
    }
}

func (r *Replicator) pullFromLeader() {
    conn, _ := net.Dial("tcp", r.leaderAddr)
    fmt.Fprintf(conn, "REPLICATE %d\n", r.offset)
    // Read and apply WAL entries
}
```

---

## Clustering

### ClusterManager

The `ClusterManager` handles node discovery and leader election:

```go
type ClusterManager struct {
    nodeID     string
    nodes      map[string]*ClusterNode
    leader     string
    isLeader   bool
    listenAddr string
    mu         sync.RWMutex
}

type ClusterNode struct {
    ID       string
    Address  string
    LastSeen time.Time
    IsLeader bool
}
```

### Bully Algorithm for Leader Election

FlyDB uses the Bully algorithm for automatic leader election:

```go
func (cm *ClusterManager) StartElection() {
    cm.mu.Lock()
    defer cm.mu.Unlock()

    // Send ELECTION to all nodes with higher IDs
    higherNodes := cm.getHigherNodes()

    if len(higherNodes) == 0 {
        // No higher nodes, become leader
        cm.becomeLeader()
        cm.broadcastCoordinator()
        return
    }

    // Wait for response from higher nodes
    responses := cm.sendElectionMessages(higherNodes)

    if len(responses) == 0 {
        // No responses, become leader
        cm.becomeLeader()
        cm.broadcastCoordinator()
    }
    // Otherwise, wait for COORDINATOR message
}

func (cm *ClusterManager) handleCoordinator(nodeID string) {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    cm.leader = nodeID
    cm.isLeader = false
}
```

### Heartbeat Mechanism

Nodes send periodic heartbeats to detect failures:

```go
func (cm *ClusterManager) startHeartbeat() {
    ticker := time.NewTicker(1 * time.Second)
    for range ticker.C {
        cm.sendHeartbeats()
        cm.checkForDeadNodes()
    }
}

func (cm *ClusterManager) checkForDeadNodes() {
    cm.mu.Lock()
    defer cm.mu.Unlock()

    timeout := 5 * time.Second
    for id, node := range cm.nodes {
        if time.Since(node.LastSeen) > timeout {
            delete(cm.nodes, id)
            if id == cm.leader {
                // Leader is dead, start election
                go cm.StartElection()
            }
        }
    }
}
```

### Cluster Protocol Messages

| Message | Format | Description |
|---------|--------|-------------|
| ELECTION | `ELECTION <node_id>` | Request election |
| OK | `OK <node_id>` | Acknowledge election |
| COORDINATOR | `COORDINATOR <node_id>` | Announce new leader |
| HEARTBEAT | `HEARTBEAT <node_id>` | Keep-alive signal |
| JOIN | `JOIN <node_id> <address>` | Join cluster |

---

## Query Cache

### LRU Cache with TTL

The query cache stores recent SELECT results:

```go
type Cache struct {
    capacity int
    ttl      time.Duration
    items    map[string]*CacheItem
    order    *list.List  // LRU order
    mu       sync.RWMutex
}

type CacheItem struct {
    key       string
    value     string
    expiresAt time.Time
    element   *list.Element
}
```

### Cache Operations

**Get (with TTL check):**
```go
func (c *Cache) Get(key string) (string, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()

    item, ok := c.items[key]
    if !ok || time.Now().After(item.expiresAt) {
        return "", false
    }
    c.order.MoveToFront(item.element)  // Update LRU
    return item.value, true
}
```

**Set (with eviction):**
```go
func (c *Cache) Set(key, value string) {
    c.mu.Lock()
    defer c.mu.Unlock()

    // Evict if at capacity
    if len(c.items) >= c.capacity {
        oldest := c.order.Back()
        delete(c.items, oldest.Value.(*CacheItem).key)
        c.order.Remove(oldest)
    }

    item := &CacheItem{
        key:       key,
        value:     value,
        expiresAt: time.Now().Add(c.ttl),
    }
    item.element = c.order.PushFront(item)
    c.items[key] = item
}
```

### Cache Invalidation

The cache is invalidated on write operations (INSERT, UPDATE, DELETE) to affected tables.

---

## See Also

- [Architecture Overview](architecture.md) - High-level system design
- [Design Decisions](design-decisions.md) - Rationale and trade-offs
- [API Reference](api.md) - SQL syntax and commands

