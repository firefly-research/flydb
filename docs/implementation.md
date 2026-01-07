# FlyDB Implementation Details

This document provides technical details on the key algorithms, data structures, and implementation approaches used in FlyDB.

## Table of Contents

1. [Storage Engine](#storage-engine)
2. [Write-Ahead Log (WAL)](#write-ahead-log-wal)
3. [B-Tree Indexes](#b-tree-indexes)
4. [SQL Processing Pipeline](#sql-processing-pipeline)
5. [Transaction Support](#transaction-support)
6. [Authentication & Authorization](#authentication--authorization)
7. [Replication](#replication)
8. [Query Cache](#query-cache)

---

## Storage Engine

### KVStore Architecture

The `KVStore` is FlyDB's primary storage engine, implementing the `Engine` interface:

```go
type Engine interface {
    Put(key string, value []byte) error
    Get(key string) ([]byte, error)
    Delete(key string) error
    Scan(prefix string) (map[string][]byte, error)
    Close() error
}
```

**Implementation Details:**

- **In-Memory Storage**: Uses Go's native `map[string][]byte` for O(1) average-case lookups
- **Thread Safety**: Protected by `sync.RWMutex` allowing concurrent reads
- **Durability**: All writes are first persisted to WAL before updating memory
- **Recovery**: On startup, WAL is replayed to rebuild in-memory state

```go
type KVStore struct {
    data map[string][]byte  // In-memory key-value storage
    wal  *WAL               // Write-ahead log for durability
    mu   sync.RWMutex       // Protects concurrent access
}
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

## Write-Ahead Log (WAL)

### Purpose

The WAL provides durability by persisting all operations to disk before they are applied to memory. This ensures committed data survives crashes.

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
5. Apply WHERE filters
6. Apply Row-Level Security (RLS) conditions
7. Apply JOINs if present
8. Apply GROUP BY and aggregate functions
9. Apply HAVING filter
10. Apply ORDER BY
11. Apply LIMIT and OFFSET
12. Format and return results

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

FlyDB supports ACID transactions with snapshot isolation:

```go
type Transaction struct {
    id          uint64
    store       Engine
    writeBuffer map[string][]byte  // Uncommitted writes
    deleteSet   map[string]bool    // Uncommitted deletes
    committed   bool
    mu          sync.Mutex
}
```

### Operations

**Begin Transaction:**
```go
func (s *KVStore) BeginTransaction() *Transaction {
    return &Transaction{
        id:          atomic.AddUint64(&s.txCounter, 1),
        store:       s,
        writeBuffer: make(map[string][]byte),
        deleteSet:   make(map[string]bool),
    }
}
```

**Read (with snapshot isolation):**
```go
func (tx *Transaction) Get(key string) ([]byte, error) {
    // Check local write buffer first
    if val, ok := tx.writeBuffer[key]; ok {
        return val, nil
    }
    if tx.deleteSet[key] {
        return nil, errors.New("key not found")
    }
    // Fall back to underlying store
    return tx.store.Get(key)
}
```

**Write (buffered):**
```go
func (tx *Transaction) Put(key string, value []byte) error {
    tx.mu.Lock()
    defer tx.mu.Unlock()
    tx.writeBuffer[key] = value
    delete(tx.deleteSet, key)
    return nil
}
```

**Commit:**
```go
func (tx *Transaction) Commit() error {
    tx.mu.Lock()
    defer tx.mu.Unlock()

    // Apply all buffered writes
    for key, value := range tx.writeBuffer {
        if err := tx.store.Put(key, value); err != nil {
            return err
        }
    }
    // Apply all deletes
    for key := range tx.deleteSet {
        if err := tx.store.Delete(key); err != nil {
            return err
        }
    }
    tx.committed = true
    return nil
}
```

**Rollback:**
```go
func (tx *Transaction) Rollback() error {
    tx.mu.Lock()
    defer tx.mu.Unlock()
    tx.writeBuffer = nil
    tx.deleteSet = nil
    return nil
}
```

---

## Authentication & Authorization

### User Management

Users are stored in the KVStore with bcrypt-hashed passwords:

```go
type User struct {
    Username     string `json:"username"`
    PasswordHash string `json:"password_hash"`
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

