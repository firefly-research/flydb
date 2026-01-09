# FlyDB Implementation Details

This document provides an in-depth technical guide to FlyDB's internal architecture, algorithms, and design decisions. Rather than just showing code, we explain *why* each component exists and *how* it solves real database engineering challenges.

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
17. [Binary Wire Protocol](#binary-wire-protocol)

---

## Storage Engine Architecture

### The Problem: Balancing Speed and Durability

Every database faces a fundamental tension: memory is fast but volatile, while disk is slow but persistent. A naive approach—storing everything in memory—loses data on crashes. Storing everything on disk is too slow for practical use.

FlyDB solves this with a **unified disk-based storage engine** that combines three key components:

1. **Buffer Pool**: Caches frequently-accessed pages in memory for speed
2. **Heap File**: Stores all data durably on disk in 8KB pages
3. **Write-Ahead Log (WAL)**: Ensures durability by logging changes before applying them

This architecture provides optimal performance for both small datasets (that fit entirely in the buffer pool) and large datasets (that exceed available RAM).

### How Components Work Together

```
┌─────────────────────────────────────────────────────────────────┐
│                      SQL Executor                               │
│   Translates SQL operations into key-value operations           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Engine Interface                              │
│         (Put, Get, Delete, Scan, Close)                         │
│   Simple abstraction that hides storage complexity              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  UnifiedStorageEngine                           │
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │              Buffer Pool (LRU-K Caching)                │   │
│   │   - Caches hot pages in memory                          │   │
│   │   - Auto-sized to 25% of available RAM                  │   │
│   │   - Prefetches pages for sequential scans               │   │
│   └─────────────────────────────────────────────────────────┘   │
│                              │                                  │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │              Page-Based Disk Storage                    │   │
│   │   - Heap file with slotted pages (8KB each)             │   │
│   │   - Free list for space reclamation                     │   │
│   │   - Efficient variable-length record storage            │   │
│   └─────────────────────────────────────────────────────────┘   │
│                              │                                  │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │              Write-Ahead Log (WAL)                      │   │
│   │   - All changes logged before being applied             │   │
│   │   - Enables crash recovery                              │   │
│   │   - Optional AES-256-GCM encryption                     │   │
│   └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

**Data Flow for a Write Operation (PUT):**

1. The SQL executor calls `engine.Put(key, value)`
2. The WAL appends the operation to disk (ensuring durability)
3. The buffer pool locates or loads the target page
4. The record is inserted into the page in memory
5. The page is marked "dirty" for later flushing

**Data Flow for a Read Operation (GET):**

1. The SQL executor calls `engine.Get(key)`
2. The buffer pool checks if the page is cached (cache hit = fast)
3. If not cached, the page is loaded from the heap file (cache miss)
4. The record is extracted from the page and returned

### Engine Interface

The storage engine exposes a simple key-value interface that hides all the complexity of pages, caching, and durability. This abstraction allows the SQL layer to focus on query processing without worrying about storage details.

```go
// Engine defines the interface for the storage engine.
// All implementations must be thread-safe for concurrent access.
// Operations are durable - once Put returns, the data survives crashes.
type Engine interface {
    // Put stores a value associated with a key.
    // If the key already exists, the value is overwritten.
    Put(key string, value []byte) error

    // Get retrieves the value associated with a key.
    // Returns ErrNotFound if the key does not exist.
    Get(key string) ([]byte, error)

    // Delete removes a key and its associated value.
    // Deleting a non-existent key is not an error (idempotent).
    Delete(key string) error

    // Scan returns all key-value pairs matching the prefix.
    // Used for operations like "get all rows from table X".
    Scan(prefix string) (map[string][]byte, error)

    // Close shuts down the engine and flushes all data.
    Close() error
}
```

**Design Decision: Why Key-Value?**

FlyDB uses a key-value interface internally because:
- It's simple to implement and reason about
- It maps naturally to B-tree and LSM-tree storage structures
- It enables efficient prefix scans for table operations
- It's easy to add features like encryption and compression

### Key Encoding Convention

FlyDB uses a prefix-based key encoding scheme that enables efficient operations:

| Data Type | Key Format | Example |
|-----------|------------|---------|
| Row Data | `row:<table>:<id>` | `row:users:42` |
| Schema | `schema:<table>` | `schema:users` |
| Sequence | `seq:<table>` | `seq:users` |
| User | `_sys_users:<name>` | `_sys_users:alice` |
| Permission | `_sys_privs:<user>:<table>` | `_sys_privs:alice:orders` |

**Why This Matters:**

This convention enables efficient prefix scans. For example:
- `Scan("row:users:")` retrieves all rows from the users table
- `Scan("schema:")` retrieves all table schemas
- `Scan("_sys_users:")` retrieves all user accounts

The prefix-based approach means we don't need separate indexes for these common operations—the key structure itself provides the organization we need.

---

## Page-Based Disk Storage

### The Problem: Storing Variable-Length Records Efficiently

Database records vary wildly in size. A user's name might be 10 bytes, while their biography could be 10,000 bytes. We need a storage format that:

1. **Handles variable-length records** without wasting space
2. **Allows efficient updates** without rewriting entire files
3. **Supports deletions** with space reclamation
4. **Enables random access** to any record

FlyDB solves this with a **slotted page** design, a technique used by PostgreSQL, MySQL InnoDB, and most modern databases.

### Why 8KB Pages?

FlyDB uses 8KB pages, which is a carefully chosen trade-off:

| Page Size | Pros | Cons |
|-----------|------|------|
| Smaller (4KB) | Less wasted space, faster for small records | More I/O operations, higher overhead |
| Larger (16KB+) | Fewer I/O operations, better for large records | More wasted space, slower for small updates |
| **8KB** | **Good balance for mixed workloads** | **Industry standard (PostgreSQL, SQLite)** |

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

**Why This Design Works:**

1. **Slot Array grows forward** from the header, containing fixed-size pointers (offset + length)
2. **Record Data grows backward** from the end of the page
3. **Free Space is in the middle**, allowing both to grow independently until they meet
4. **Slot indirection** means we can move records within the page without updating external references

**Handling Deletions:**

When a record is deleted:
1. Its slot is marked as "deleted" (offset = 0)
2. The record data becomes a "hole" in the page
3. When free space runs low, the page is **compacted**: live records are moved to eliminate holes
4. This approach avoids expensive immediate compaction on every delete

### Record Format

Records are stored with a simple key-length prefix:

```
┌────────────────────────────────────────────────────────────────┐
│ Key Length (4 bytes) │ Key (variable) │ Value (variable) │
└────────────────────────────────────────────────────────────────┘
```

The key length prefix allows us to parse records without knowing their structure in advance. The value length is computed as: `slot.length - 4 - keyLength`.

### Heap File Organization

The heap file stores pages sequentially on disk:

```
┌─────────────────────────────────────────────────────────────────┐
│ Header Page (8KB) - Page 0                                      │
│ - Magic number (0x464C5944 = "FLYD")                            │
│ - Version, page count, free list head                           │
├─────────────────────────────────────────────────────────────────┤
│ Data Page 1 (8KB)                                               │
├─────────────────────────────────────────────────────────────────┤
│ Data Page 2 (8KB)                                               │
├─────────────────────────────────────────────────────────────────┤
│ ...                                                             │
└─────────────────────────────────────────────────────────────────┘
```

**Free List for Space Reclamation:**

When pages become empty (all records deleted), they're added to a **free list**:
- The header page stores the head of the free list
- Each free page stores a pointer to the next free page
- New allocations check the free list before extending the file

This prevents the heap file from growing indefinitely when data is deleted and re-inserted.

---

## Buffer Pool

### The Problem: Bridging the Memory-Disk Speed Gap

The buffer pool is the most performance-critical component in any database. It bridges the enormous speed gap between memory and disk:

| Storage Type | Random Read Latency | Relative Speed |
|--------------|---------------------|----------------|
| L1 Cache | ~1 nanosecond | 1x (baseline) |
| RAM | ~100 nanoseconds | 100x slower |
| NVMe SSD | ~20 microseconds | 20,000x slower |
| SATA SSD | ~100 microseconds | 100,000x slower |
| HDD | ~10 milliseconds | 10,000,000x slower |

A well-designed buffer pool can achieve 90%+ cache hit rates, meaning most operations never touch disk.

### LRU-K Eviction Algorithm

FlyDB uses **LRU-K (specifically LRU-2)** instead of simple LRU for page eviction. This algorithm was introduced by O'Neil, O'Neil, and Weikum in their 1993 paper "The LRU-K Page Replacement Algorithm For Database Disk Buffering."

**The Problem with Simple LRU:**

Simple LRU evicts the least recently used page. This sounds reasonable, but it fails badly for database workloads:

```
Scenario: Buffer pool has 100 pages, all frequently accessed.
          A sequential scan reads 200 pages once each.

With Simple LRU:
  - The scan evicts ALL 100 frequently-accessed pages
  - After the scan, the buffer pool contains 100 pages that will never be accessed again
  - This is called "buffer pool pollution"
```

**How LRU-K Solves This:**

LRU-K tracks the last K access times for each page (FlyDB uses K=2):

1. **First access**: Page goes to a "probationary" list
2. **Second access**: Page is "promoted" to the main cache
3. **Eviction**: Evict from probationary list first, then main cache

This means:
- Sequential scan pages (accessed once) stay in probationary list and are evicted first
- Frequently accessed pages (accessed 2+ times) are protected in the main cache

### Auto-Sizing

The buffer pool automatically sizes itself based on available system memory:

```
Buffer Pool Size = min(max(available_memory * 0.25, 2MB), 1GB)
```

| Available RAM | Buffer Pool Size | Pages Cached |
|---------------|------------------|--------------|
| 4 GB | 1 GB (capped) | 131,072 pages |
| 2 GB | 512 MB | 65,536 pages |
| 512 MB | 128 MB | 16,384 pages |
| 8 MB | 2 MB (minimum) | 256 pages |

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

**Pin Counting:**

Pages in the buffer pool use **pin counting** to track active users:
- `FetchPage` increments the pin count
- `UnpinPage` decrements the pin count
- A page can only be evicted when its pin count is 0

This prevents evicting pages that are currently being used by queries.

### Checkpoint Manager

Checkpoints create consistent snapshots for recovery. Without checkpoints, crash recovery would need to replay the entire WAL from the beginning of time.

**Checkpoint Process:**

1. **Pause new writes** (briefly)
2. **Flush all dirty pages** to the heap file
3. **Sync the heap file** to ensure data is on disk
4. **Record checkpoint LSN** in metadata
5. **Resume normal operation**
6. **Truncate old WAL entries** (optional, for space reclamation)

Checkpoints run automatically at configurable intervals (default: 60 seconds).

**Trade-off:**
- More frequent checkpoints = faster crash recovery, but more I/O overhead
- Less frequent checkpoints = less I/O overhead, but slower crash recovery

### Performance Characteristics

| Operation | Cached (in buffer pool) | Uncached (disk I/O) |
|-----------|-------------------------|---------------------|
| Put       | O(1) + WAL              | O(1) + WAL          |
| Get       | O(1)                    | O(disk)             |
| Delete    | O(1) + WAL              | O(1) + WAL          |
| Scan      | O(N) with prefetch      | O(N) + disk I/O     |

---

## Write-Ahead Log (WAL)

### The Problem: Ensuring Durability Without Sacrificing Performance

Databases must guarantee **durability**: once a transaction commits, its changes survive crashes. The naive approach—writing every change directly to the data file—is too slow because:

1. Random writes to the data file require seeking to different locations
2. Each write must be synced to disk to guarantee durability
3. Updating data in place risks corruption if a crash occurs mid-write

The **Write-Ahead Log (WAL)** solves this with a simple insight: **sequential writes are fast**.

### How WAL Provides Durability

The WAL protocol has one simple rule: **log before you modify**.

```
Write Operation Flow:
1. Append operation to WAL (sequential write, fast)
2. Sync WAL to disk (ensures durability)
3. Apply change to buffer pool (in-memory, fast)
4. Return success to client
5. Later: checkpoint flushes dirty pages to heap file
```

**Why This Works:**

- WAL writes are sequential (append-only), which is 10-100x faster than random writes
- If we crash after step 2, the WAL contains the operation and we can replay it
- If we crash before step 2, the operation was never durable, so losing it is correct
- The heap file is updated lazily during checkpoints, not on every write

### Record Format

Each WAL record uses a compact binary format:

```
┌─────────┬───────────┬─────────────┬─────────────┬─────────────┐
│ Op (1B) │ KeyLen(4B)│ Key (var)   │ ValLen (4B) │ Value (var) │
└─────────┴───────────┴─────────────┴─────────────┴─────────────┘

Op Types:
  1 = OpPut    (insert or update)
  2 = OpDelete (remove key)
```

**Design Decisions:**

- **Binary format**: Compact and fast to parse (no JSON/XML overhead)
- **Length-prefixed strings**: Allows variable-length keys and values
- **No checksums in basic format**: Simplicity; encryption provides integrity when enabled

### Encrypted WAL Format

When encryption is enabled, each record is wrapped with AES-256-GCM:

```
┌──────────────┬─────────────────────────────────────────────────────┐
│ EncLen (4B)  │ Encrypted Payload (nonce + ciphertext + tag)        │
└──────────────┴─────────────────────────────────────────────────────┘
```

**Security Properties:**

- **Confidentiality**: AES-256 encryption protects data at rest
- **Integrity**: GCM authentication tag detects tampering
- **Unique nonces**: Each record uses a unique 12-byte nonce

### Crash Recovery

On startup, the storage engine replays the WAL to recover any changes that weren't checkpointed:

```
Recovery Process:
1. Open the heap file (contains last checkpoint state)
2. Open the WAL file
3. Replay all WAL records from the last checkpoint LSN
4. The database is now in a consistent state
```

**Idempotent Replay:**

WAL replay is **idempotent**—replaying the same record twice produces the same result. This is crucial because we might crash during recovery and need to replay again.

### WAL Truncation

The WAL grows indefinitely unless truncated. After a successful checkpoint:

1. All changes up to the checkpoint LSN are in the heap file
2. WAL records before the checkpoint LSN are no longer needed
3. The WAL can be truncated to reclaim disk space

FlyDB truncates the WAL automatically after each checkpoint.

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

### The Problem: Turning Text into Actions

SQL is a declarative language—users describe *what* they want, not *how* to get it. The database must:

1. **Parse** the SQL text into a structured representation
2. **Validate** that the query is syntactically and semantically correct
3. **Optimize** the query plan (future enhancement)
4. **Execute** the plan and return results

FlyDB implements a classic three-stage pipeline:

```
┌──────────┐    ┌───────┐    ┌────────┐    ┌──────────┐    ┌─────────┐
│ SQL Text │───▶│ Lexer │───▶│ Tokens │───▶│  Parser  │───▶│   AST   │
└──────────┘    └───────┘    └────────┘    └──────────┘    └─────────┘
                                                                │
                                                                ▼
                                                          ┌──────────┐
                                                          │ Executor │
                                                          └──────────┘
                                                                │
                                                                ▼
                                                          ┌─────────┐
                                                          │ Results │
                                                          └─────────┘
```

### Stage 1: Lexer (Tokenization)

The lexer breaks SQL text into a stream of **tokens**—the smallest meaningful units:

```
Input:  "SELECT name, age FROM users WHERE age > 21"

Tokens: [KEYWORD:SELECT] [IDENT:name] [COMMA] [IDENT:age] [KEYWORD:FROM]
        [IDENT:users] [KEYWORD:WHERE] [IDENT:age] [OPERATOR:>] [NUMBER:21]
```

**Token Types:**

| Token Type | Examples | Description |
|------------|----------|-------------|
| KEYWORD | SELECT, INSERT, WHERE | Reserved SQL keywords |
| IDENT | users, name, my_table | Identifiers (table/column names) |
| NUMBER | 42, 3.14, -17 | Numeric literals |
| STRING | 'hello', "world" | String literals |
| OPERATOR | =, <>, >, <=, LIKE | Comparison operators |
| PUNCT | (, ), ,, ; | Punctuation |

**Handling Edge Cases:**

- **Comments**: `-- single line` and `/* multi-line */` are skipped
- **Quoted identifiers**: `"column with spaces"` preserves the identifier
- **Escape sequences**: `'it''s'` becomes `it's`

### Stage 2: Parser (AST Construction)

The parser transforms tokens into an **Abstract Syntax Tree (AST)**—a tree structure representing the query's logical structure.

**Why AST?**

An AST separates *syntax* from *semantics*. The parser handles syntax (is this valid SQL?), while the executor handles semantics (what does this query mean?).

**Example AST:**

```
SELECT name FROM users WHERE age > 21

         SelectStmt
        /    |     \
   Columns  Table   Where
      |       |       |
   ["name"] "users"  WhereClause
                    /    |    \
               Column  Op   Value
                 |      |     |
               "age"   ">"   "21"
```

**Supported Statements:**

| Statement Type | Features |
|----------------|----------|
| SELECT | WHERE, ORDER BY, LIMIT, OFFSET, GROUP BY, HAVING, DISTINCT |
| INSERT | Single and multi-row, column lists |
| UPDATE | SET with multiple columns, WHERE |
| DELETE | WHERE clause |
| CREATE TABLE | Column types, constraints, IF NOT EXISTS |
| CREATE INDEX | Single and composite, IF NOT EXISTS |
| JOIN | INNER, LEFT, RIGHT, FULL OUTER |
| UNION | UNION, UNION ALL |
| Subqueries | In WHERE and FROM clauses |

**AST Node Types (simplified):**

```go
// SelectStmt represents a SELECT query
type SelectStmt struct {
    Columns    []SelectColumn  // Columns to select (may include expressions)
    TableName  string          // Primary table
    TableAlias string          // Optional alias
    Joins      []JoinClause    // JOIN clauses
    Where      *WhereClause    // WHERE condition
    GroupBy    []string        // GROUP BY columns
    Having     *WhereClause    // HAVING condition
    OrderBy    []OrderByClause // ORDER BY clauses
    Limit      int             // LIMIT value (-1 if not specified)
    Offset     int             // OFFSET value
}

// WhereClause represents a condition (recursive for AND/OR)
type WhereClause struct {
    Column   string       // Left side of comparison
    Operator string       // =, <>, <, >, <=, >=, LIKE, IN, BETWEEN, IS NULL
    Value    interface{}  // Right side (string, number, list, subquery)
    And      *WhereClause // Chained AND condition
    Or       *WhereClause // Chained OR condition
}
```

### Stage 3: Executor

The executor traverses the AST and performs the actual database operations. See the [Executor section](#executor-executorgo) for details.

### Executor (`executor.go`)

The executor traverses the AST and performs operations:

```go
type Executor struct {
    store       Engine
    catalog     *Catalog
    auth        *AuthManager
    indexMgr    *IndexManager
    prepMgr     *PreparedStatementManager
    queryCache  *cache.QueryCache  // LRU cache with TTL and table-based invalidation
    triggerMgr  *TriggerManager
    collator    storage.Collator
    encoder     storage.Encoder
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
1. Generate cache key from query components and user context
2. Check query cache for cached results (return immediately if found)
3. Check user permissions via AuthManager
4. Retrieve table schema from Catalog
5. Scan rows from KVStore (or use index if available)
6. Apply WHERE filters (using collator for string comparisons)
7. Apply Row-Level Security (RLS) conditions
8. Apply JOINs if present
9. Apply GROUP BY and aggregate functions
10. Apply HAVING filter
11. Apply ORDER BY (using collator for string sorting)
12. Apply LIMIT and OFFSET
13. Format results
14. Store results in query cache (for cacheable queries)
15. Return results

---

## Prepared Statements

### The Problem: Repeated Query Overhead

When an application executes the same query repeatedly with different values:

```sql
SELECT * FROM users WHERE id = 1;
SELECT * FROM users WHERE id = 2;
SELECT * FROM users WHERE id = 3;
-- ... thousands more
```

Each execution requires:
1. Lexing the SQL text
2. Parsing into an AST
3. Validating the query
4. Executing

Steps 1-3 are identical for each query—wasted work!

### Solution: Prepared Statements

Prepared statements separate query structure from data:

```sql
-- Prepare once (parse and validate)
PREPARE get_user AS SELECT * FROM users WHERE id = $1;

-- Execute many times (just substitute and run)
EXECUTE get_user(1);
EXECUTE get_user(2);
EXECUTE get_user(3);
```

**Benefits:**
- **Performance**: Parse once, execute many times
- **Security**: Parameters are properly escaped, preventing SQL injection
- **Clarity**: Query structure is separate from data

### Parameter Placeholders

FlyDB uses PostgreSQL-style numbered placeholders (`$1`, `$2`, etc.):

```sql
-- Single parameter
PREPARE find_user AS SELECT * FROM users WHERE name = $1;

-- Multiple parameters
PREPARE find_orders AS SELECT * FROM orders WHERE user_id = $1 AND status = $2;
```

**Why Numbered Placeholders?**

- **Reusability**: Same parameter can be used multiple times (`WHERE a = $1 OR b = $1`)
- **Clarity**: Parameter order is explicit, not positional
- **Compatibility**: Matches PostgreSQL syntax

### Usage Examples

```sql
-- Prepare a statement
PREPARE insert_user AS INSERT INTO users (name, email) VALUES ($1, $2);

-- Execute with parameters
EXECUTE insert_user('Alice', 'alice@example.com');
EXECUTE insert_user('Bob', 'bob@example.com');

-- Deallocate when done
DEALLOCATE insert_user;
```

### Implementation

The `PreparedStatementManager` stores prepared statements by name:

```go
type PreparedStatement struct {
    Name       string   // Statement name
    Query      string   // Original query with $1, $2, etc.
    ParamCount int      // Number of parameters
}
```

**Execution Flow:**

1. Look up the prepared statement by name
2. Validate parameter count matches
3. Substitute parameters into the query (with proper escaping)
4. Parse and execute the substituted query

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

### The Problem: All-or-Nothing Operations

Consider transferring money between bank accounts:

```sql
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
```

If the system crashes between these two statements, money disappears! We need **transactions** to ensure both statements succeed or both fail.

### ACID Properties

FlyDB transactions provide the classic ACID guarantees:

| Property | Meaning | How FlyDB Implements It |
|----------|---------|-------------------------|
| **Atomicity** | All operations succeed or all fail | Write buffer applied atomically on commit |
| **Consistency** | Database invariants are maintained | Constraints checked before commit |
| **Isolation** | Concurrent transactions don't interfere | Read-committed isolation level |
| **Durability** | Committed data survives crashes | WAL ensures persistence |

### Isolation Levels Explained

FlyDB uses **Read Committed** isolation, which means:

- You see only committed data from other transactions
- Your own writes are visible to you immediately (read-your-writes)
- Non-repeatable reads are possible (same query may return different results)
- Phantom reads are possible (new rows may appear)

**Why Read Committed?**

It's a good balance between consistency and performance. Stricter levels (Serializable) require more locking and reduce concurrency.

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

**How It Works:**

1. **BEGIN**: Creates a new transaction with an empty write buffer
2. **Operations**: Writes are buffered in memory, not applied to storage
3. **COMMIT**: All buffered writes are applied atomically to storage
4. **ROLLBACK**: The write buffer is discarded, nothing changes

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

### The Problem: High Availability and Read Scaling

A single database server has two critical limitations:

1. **Single point of failure**: If the server crashes, the database is unavailable
2. **Read bottleneck**: All queries go to one server, limiting throughput

**Replication** solves both problems by maintaining copies of data on multiple servers.

### Leader-Follower Architecture

FlyDB uses **leader-follower** (also called master-slave) replication:

```
┌──────────────────────────────────────────────────────────────┐
│                         Clients                              │
└──────────────────────────────────────────────────────────────┘
         │ writes                    │ reads
         ▼                           ▼
┌─────────────────┐         ┌─────────────────┐
│     Leader      │────────▶│    Follower 1   │
│  (read/write)   │   WAL   │   (read-only)   │
└─────────────────┘  stream └─────────────────┘
         │
         │                  ┌─────────────────┐
         └─────────────────▶│    Follower 2   │
                     WAL    │   (read-only)   │
                    stream  └─────────────────┘
```

**How It Works:**

1. **All writes go to the leader**: The leader is the single source of truth
2. **Leader streams WAL to followers**: Changes are sent as WAL entries
3. **Followers apply WAL entries**: They replay the same operations
4. **Reads can go to any node**: Followers serve read queries, reducing leader load

### Asynchronous vs. Synchronous Replication

FlyDB uses **asynchronous replication**:

| Type | Behavior | Trade-off |
|------|----------|-----------|
| **Synchronous** | Leader waits for follower acknowledgment | Slower writes, stronger consistency |
| **Asynchronous** | Leader doesn't wait for followers | Faster writes, possible data loss on leader failure |

Asynchronous replication is faster but means followers may lag behind the leader.

### Replication Lag

The delay between a write on the leader and its appearance on followers is called **replication lag**. FlyDB followers poll the leader every 100ms, so lag is typically under 200ms.

**Implications:**
- Reading from a follower immediately after writing to the leader may return stale data
- For read-your-writes consistency, read from the leader after writes

---

## Clustering

### The Problem: Automatic Failover

Replication provides data redundancy, but what happens when the leader fails? Without automatic failover:

1. An operator must manually detect the failure
2. Promote a follower to leader
3. Reconfigure other followers to point to the new leader
4. Resume operations

This manual process can take minutes or hours, causing significant downtime.

### Solution: Automatic Leader Election

FlyDB's clustering layer provides **automatic leader election**. When the leader fails:

1. Followers detect the failure (no heartbeat)
2. An election is triggered automatically
3. A new leader is elected within seconds
4. Followers reconfigure to follow the new leader

### The Bully Algorithm

FlyDB uses the **Bully Algorithm** for leader election, chosen for its simplicity and determinism:

**How It Works:**

1. Each node has a unique ID (higher ID = higher priority)
2. When a node detects leader failure, it starts an election
3. It sends ELECTION messages to all nodes with higher IDs
4. If no higher node responds, it becomes leader
5. If a higher node responds, it waits for that node to become leader

**Why "Bully"?**

The highest-ID node always "bullies" its way to leadership. This is deterministic—given the same set of nodes, the same leader is always elected.

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

### Production-Ready Cluster Features

The `ClusterManager` includes production-ready features for reliable distributed operation:

```go
type ClusterManager struct {
    config       ClusterConfig
    term         uint64              // Election term (monotonically increasing)
    state        NodeState           // Follower, Candidate, or Leader
    peers        map[string]*NodeHealth
    metrics      *ClusterMetrics
    eventCh      chan ClusterEvent
    selfJoinedAt time.Time
}

type ClusterConfig struct {
    NodeID            string
    NodeAddr          string
    Peers             []string
    HeartbeatInterval time.Duration
    HeartbeatTimeout  time.Duration
    ElectionTimeout   time.Duration
    MinQuorum         int           // Minimum nodes for quorum
    EnablePreVote     bool          // Pre-vote protocol
    MaxReplicationLag time.Duration
}
```

**Key Features:**

1. **Term-based Elections**: Each election increments a term number, preventing stale leaders from accepting writes.

2. **Quorum Requirements**: Decisions require a majority of nodes, preventing split-brain scenarios.

3. **Dynamic Membership**: Nodes can join and leave the cluster at runtime:
   ```go
   cm.AddNode("192.168.1.100:9000")
   cm.RemoveNode("192.168.1.100:9000")
   ```

4. **Health Monitoring**: Per-node health tracking with automatic unhealthy detection:
   ```go
   type NodeHealth struct {
       NodeID        string
       Address       string
       State         NodeState
       IsHealthy     bool
       LastHeartbeat time.Time
       ReplicationLag time.Duration
       FailedChecks  int
       JoinedAt      time.Time
   }
   ```

5. **Cluster Events**: Event channel for monitoring and integration:
   ```go
   for event := range cm.Events() {
       log.Printf("Cluster event: %s node=%s term=%d",
           event.Type, event.NodeID, event.Term)
   }
   ```

6. **Cluster Metrics**: Comprehensive metrics for monitoring:
   ```go
   type ClusterMetrics struct {
       Term                  uint64
       ElectionCount         uint64
       LeaderChanges         uint64
       LastElectionTime      time.Time
       AverageReplicationLag time.Duration
       HealthyNodes          int
       TotalNodes            int
       QuorumSize            int
       HasQuorum             bool
   }
   ```

---

## Replication Modes and Configuration

The `Replicator` supports multiple consistency modes for different durability requirements:

### Replication Modes

```go
type ReplicationMode int

const (
    ReplicationAsync    ReplicationMode = iota  // Return immediately
    ReplicationSemiSync                         // Wait for one replica
    ReplicationSync                             // Wait for all replicas
)
```

### Replication Configuration

```go
type ReplicationConfig struct {
    Mode              ReplicationMode
    SyncTimeout       time.Duration  // Timeout for sync acknowledgments
    MaxLagThreshold   time.Duration  // Maximum acceptable lag
    PollInterval      time.Duration  // How often to check for new entries
    ReconnectInterval time.Duration  // Reconnection delay
    EnableCompression bool           // Compress replication traffic
}
```

### Follower State Tracking

The leader tracks the state of each follower:

```go
type FollowerState struct {
    Address       string
    WALOffset     int64
    LastAckTime   time.Time
    Lag           time.Duration
    IsHealthy     bool
    FailedSends   int
}
```

### Replication Metrics

```go
type ReplicationMetrics struct {
    TotalBytesReplicated   int64
    TotalEntriesReplicated int64
    AverageLag             time.Duration
    MaxLag                 time.Duration
    HealthyFollowers       int
    TotalFollowers         int
    LastReplicationTime    time.Time
}
```

### Synchronous Replication

For maximum durability, synchronous replication waits for acknowledgments:

```go
// Wait for replication with timeout
err := replicator.WaitForReplication(walOffset, 5*time.Second)
if err != nil {
    // Handle timeout or failure
}
```

---

## Query Cache

### The Problem: Repeated Identical Queries

Many applications execute the same queries repeatedly:

```sql
SELECT * FROM products WHERE category = 'electronics';  -- Homepage load
SELECT * FROM products WHERE category = 'electronics';  -- Another user
SELECT * FROM products WHERE category = 'electronics';  -- Yet another user
```

Each execution requires parsing, planning, and fetching data from storage—even though the result is identical.

### Solution: Query Result Caching

FlyDB caches the results of SELECT queries, keyed by the query text:

```
Cache Key: "SELECT * FROM products WHERE category = 'electronics'"
Cache Value: [{"id": 1, "name": "Laptop", ...}, {"id": 2, "name": "Phone", ...}]
```

Subsequent identical queries return the cached result instantly.

### Cache Design: LRU with TTL

The query cache uses two eviction strategies:

1. **LRU (Least Recently Used)**: When the cache is full, evict the least recently accessed entry
2. **TTL (Time To Live)**: Entries expire after a configurable duration (default: 5 minutes)

**Why Both?**

- LRU prevents the cache from growing unbounded
- TTL ensures stale data is eventually refreshed

### Cache Invalidation

The hardest problem in caching is knowing when cached data is stale. FlyDB uses **table-level invalidation**:

- When a table is modified (INSERT, UPDATE, DELETE), all cached queries involving that table are invalidated
- This is conservative (may invalidate queries that weren't affected) but safe

**Example:**

```sql
-- This query is cached
SELECT * FROM products WHERE category = 'electronics';

-- This INSERT invalidates ALL cached queries on 'products'
INSERT INTO products (name, category) VALUES ('Tablet', 'electronics');

-- Next SELECT will re-execute and cache the new result
SELECT * FROM products WHERE category = 'electronics';
```

### When NOT to Use Query Cache

The query cache is disabled for:

- Queries with non-deterministic functions (NOW(), RANDOM())
- Queries inside transactions (may see uncommitted data)
- Queries with user-specific data (unless RLS is considered)

---

## Binary Wire Protocol

### The Problem: Efficient Client-Server Communication

When a client application communicates with a database server over a network, every byte matters:

1. **Latency**: Network round-trips are expensive (milliseconds vs. nanoseconds for memory)
2. **Bandwidth**: Large payloads consume network resources and slow down transfers
3. **Parsing overhead**: Text protocols require parsing on every message
4. **Reliability**: Messages must be unambiguous and error-detectable

Many databases use text-based protocols (like MySQL's early protocol or Redis's RESP) for simplicity. However, text protocols have drawbacks:

- **Parsing overhead**: Converting "12345" to an integer on every message
- **Ambiguity**: How do you send a string containing a newline?
- **Size**: Numbers like "1000000000" take 10 bytes as text, 4 bytes as binary

### Solution: Binary Protocol with Message Framing

FlyDB uses a **binary wire protocol** that provides:

- **Compact encoding**: Numbers are stored in fixed-size binary format
- **Unambiguous framing**: Every message has a clear start and end
- **Extensibility**: New message types can be added without breaking compatibility
- **Debuggability**: JSON payloads inside binary frames for human readability

### Message Frame Format

Every message follows this structure:

```
┌─────────┬─────────┬─────────┬─────────┬─────────────────┬─────────────────┐
│ Magic   │ Version │ MsgType │ Flags   │ Length (4B)     │ Payload (var)   │
│ (1 byte)│ (1 byte)│ (1 byte)│ (1 byte)│ (big-endian)    │                 │
└─────────┴─────────┴─────────┴─────────┴─────────────────┴─────────────────┘
     │         │         │         │           │                  │
     │         │         │         │           │                  └── Message-specific data
     │         │         │         │           └── Payload size (max 16 MB)
     │         │         │         └── Bit flags (compression, encryption)
     │         │         └── What kind of message (query, result, error, etc.)
     │         └── Protocol version (currently 0x01)
     └── Magic byte 0xFD (identifies FlyDB protocol)
```

**Why This Design?**

| Field | Purpose | Design Rationale |
|-------|---------|------------------|
| Magic | Protocol identification | Quickly reject non-FlyDB connections |
| Version | Forward compatibility | Servers can support multiple protocol versions |
| MsgType | Message routing | Handler knows how to parse payload without inspecting it |
| Flags | Optional features | Compression/encryption without changing frame format |
| Length | Message framing | Receiver knows exactly how many bytes to read |

### Message Types

FlyDB defines message types in logical groups:

| Code Range | Category | Examples |
|------------|----------|----------|
| 0x01-0x0F | Core Operations | Query, QueryResult, Error |
| 0x10-0x1F | Prepared Statements | Prepare, Execute, Deallocate |
| 0x20-0x2F | Authentication | Auth, AuthResult |
| 0x30-0x3F | Control | Ping, Pong |
| 0x40-0x4F | Cursors | CursorOpen, CursorFetch, CursorClose |
| 0x50-0x5F | Database Operations | UseDatabase, GetDatabases |

**Core Message Types:**

```go
const (
    MsgQuery        = 0x01  // SQL query request
    MsgQueryResult  = 0x02  // Query response with rows
    MsgError        = 0x03  // Error response
    MsgPrepare      = 0x04  // Prepare statement
    MsgExecute      = 0x06  // Execute prepared statement
    MsgAuth         = 0x08  // Authentication request
    MsgPing         = 0x0A  // Keep-alive ping
    MsgPong         = 0x0B  // Keep-alive response
)
```

### Payload Encoding: JSON Inside Binary

FlyDB uses a hybrid approach: **binary framing with JSON payloads**.

```
Binary Frame                          JSON Payload
┌────────────────────────┐           ┌──────────────────────────────────┐
│ FD 01 01 00 00 00 2A   │ ────────▶ │ {"query":"SELECT * FROM users"}  │
└────────────────────────┘           └──────────────────────────────────┘
  │  │  │  │  └───────┘                        42 bytes (0x2A)
  │  │  │  │   Length
  │  │  │  └── No flags
  │  │  └── MsgQuery (0x01)
  │  └── Version 1
  └── Magic 0xFD
```

**Why JSON Payloads?**

| Approach | Pros | Cons |
|----------|------|------|
| Pure binary | Fastest, smallest | Hard to debug, rigid schema |
| Pure text | Easy to debug | Slow parsing, ambiguous framing |
| **Binary + JSON** | Easy to debug, clear framing | Slightly larger than pure binary |

For a database focused on correctness and debuggability, the hybrid approach is ideal. Production databases often use pure binary for maximum performance.

### Example: Query Request/Response

**Client sends a query:**

```
Bytes: FD 01 01 00 00 00 1F {"query":"SELECT id FROM users"}
       ├──────────────────┤ ├──────────────────────────────┤
       Header (8 bytes)     Payload (31 bytes)
```

**Server responds with results:**

```
Bytes: FD 01 02 00 00 00 3C {"success":true,"columns":["id"],"rows":[[1],[2]],"row_count":2}
       ├──────────────────┤ ├──────────────────────────────────────────────────────────────┤
       Header (8 bytes)     Payload (60 bytes)
```

### Connection Lifecycle

```
┌────────┐                                    ┌────────┐
│ Client │                                    │ Server │
└────┬───┘                                    └────┬───┘
     │                                             │
     │ ──── TCP Connect ────────────────────────▶  │
     │                                             │
     │ ──── Auth (username, password) ──────────▶  │
     │                                             │
     │ ◀──── AuthResult (success/failure) ───────  │
     │                                             │
     │ ──── Query (SELECT * FROM users) ────────▶  │
     │                                             │
     │ ◀──── QueryResult (rows) ─────────────────  │
     │                                             │
     │ ──── Ping ───────────────────────────────▶  │
     │                                             │
     │ ◀──── Pong ───────────────────────────────  │
     │                                             │
     │ ──── TCP Close ──────────────────────────▶  │
     │                                             │
```

### Error Handling

Errors are returned as structured messages with codes:

```json
{
  "code": 1001,
  "message": "Table 'users' not found"
}
```

**Error Code Ranges:**

| Range | Category | Examples |
|-------|----------|----------|
| 400-499 | Client errors | Invalid message, bad syntax |
| 500-599 | Server errors | Internal error, not implemented |
| 1000-1999 | SQL errors | Table not found, constraint violation |

### Metadata Query Processing

The binary protocol supports ODBC/JDBC-style metadata queries through dedicated message types. These queries are processed by the `MetadataProvider` interface:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Metadata Query Flow                          │
│                                                                 │
│  1. Client sends MsgGetTables (0x20)                            │
│     {"catalog": "", "schema": "public", "table_name": "%"}      │
│                              │                                  │
│                              ▼                                  │
│  2. BinaryHandler routes to handleGetTables()                   │
│                              │                                  │
│                              ▼                                  │
│  3. MetadataProvider.GetTables() queries SQL Catalog            │
│     - Iterates over catalog.Tables                              │
│     - Applies pattern matching for table_name                   │
│     - Filters by table_types (TABLE, VIEW)                      │
│                              │                                  │
│                              ▼                                  │
│  4. Server sends MsgMetadataResult (0x26)                       │
│     {"success": true, "rows": [...]}                            │
└─────────────────────────────────────────────────────────────────┘
```

**Metadata Message Types:**

| Message | Code | Description |
|---------|------|-------------|
| MsgGetTables | 0x20 | List tables matching pattern |
| MsgGetColumns | 0x21 | Get column definitions |
| MsgGetPrimaryKeys | 0x22 | Get primary key constraints |
| MsgGetForeignKeys | 0x23 | Get foreign key relationships |
| MsgGetIndexes | 0x24 | Get index information |
| MsgGetTypeInfo | 0x25 | Get supported SQL types |
| MsgMetadataResult | 0x26 | Metadata query response |

**Implementation Details:**

The `serverMetadataProvider` in `internal/server/metadata.go` implements the `MetadataProvider` interface by accessing the SQL Executor's catalog and index manager:

```go
// GetColumns returns column metadata for matching tables
func (m *serverMetadataProvider) GetColumns(catalog, schema, tablePattern, columnPattern string) ([][]interface{}, error) {
    cat := m.srv.executor.GetCatalog()
    if cat == nil {
        return nil, nil  // Graceful degradation
    }

    var rows [][]interface{}
    for tableName, tableSchema := range cat.Tables {
        // Apply pattern matching
        if !matchPattern(tableName, tablePattern) {
            continue
        }
        for i, col := range tableSchema.Columns {
            // Build ODBC-compatible column metadata
            row := []interface{}{
                catalog, schema, tableName, col.Name,
                getSQLType(col.Type), col.Type, getColumnSize(col.Type),
                // ... additional ODBC fields
            }
            rows = append(rows, row)
        }
    }
    return rows, nil
}
```

**Design Rationale:**

1. **Separation of Concerns**: MetadataProvider abstracts catalog access from protocol handling
2. **ODBC/JDBC Compatibility**: Response formats match standard driver expectations
3. **Pattern Matching**: SQL wildcards (%) supported for flexible queries
4. **Graceful Degradation**: Returns empty results when catalog is unavailable
5. **Default Values**: Some metadata fields return sensible defaults when actual values aren't tracked

### Flags: Compression and Encryption

The Flags byte enables optional features:

```go
const (
    FlagNone       = 0x00  // No special handling
    FlagCompressed = 0x01  // Payload is zlib-compressed
    FlagEncrypted  = 0x02  // Payload is encrypted
)
```

**Compression Example:**

For large result sets, compression can significantly reduce bandwidth:

```
Uncompressed: 100,000 bytes
Compressed:    15,000 bytes (85% reduction)
```

The receiver checks the Flags byte and decompresses before parsing JSON.

### Implementation: Reading and Writing Messages

**Writing a message:**

```go
func WriteMessage(w io.Writer, msgType MessageType, payload []byte) error {
    header := Header{
        Magic:   0xFD,
        Version: 0x01,
        Type:    msgType,
        Flags:   FlagNone,
        Length:  uint32(len(payload)),
    }

    // Write 8-byte header
    buf := make([]byte, 8)
    buf[0] = header.Magic
    buf[1] = header.Version
    buf[2] = byte(header.Type)
    buf[3] = byte(header.Flags)
    binary.BigEndian.PutUint32(buf[4:], header.Length)

    if _, err := w.Write(buf); err != nil {
        return err
    }

    // Write payload
    _, err := w.Write(payload)
    return err
}
```

**Reading a message:**

```go
func ReadMessage(r io.Reader) (*Message, error) {
    // Read 8-byte header
    buf := make([]byte, 8)
    if _, err := io.ReadFull(r, buf); err != nil {
        return nil, err
    }

    // Validate magic byte
    if buf[0] != 0xFD {
        return nil, ErrInvalidMagic
    }

    header := Header{
        Magic:   buf[0],
        Version: buf[1],
        Type:    MessageType(buf[2]),
        Flags:   MessageFlag(buf[3]),
        Length:  binary.BigEndian.Uint32(buf[4:]),
    }

    // Validate message size
    if header.Length > MaxMessageSize {
        return nil, ErrMessageTooLarge
    }

    // Read payload
    payload := make([]byte, header.Length)
    if _, err := io.ReadFull(r, payload); err != nil {
        return nil, err
    }

    return &Message{Header: header, Payload: payload}, nil
}
```

### Comparison with Other Database Protocols

| Database | Protocol Type | Framing | Payload |
|----------|--------------|---------|---------|
| PostgreSQL | Binary | Length-prefixed | Custom binary |
| MySQL | Binary | Length-prefixed | Custom binary |
| Redis | Text (RESP) | CRLF-delimited | Text |
| MongoDB | Binary | Length-prefixed | BSON |
| **FlyDB** | **Binary** | **Length-prefixed** | **JSON** |

FlyDB's approach is similar to PostgreSQL and MySQL in framing, but uses JSON payloads for easier debugging and driver development.

---

## See Also

- [Architecture Overview](architecture.md) - High-level system design
- [Design Decisions](design-decisions.md) - Rationale and trade-offs
- [API Reference](api.md) - SQL syntax and commands
- [Driver Development Guide](driver-development.md) - Building client drivers

