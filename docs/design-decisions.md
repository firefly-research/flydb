# FlyDB Design Decisions

This document explains the key design decisions made in FlyDB, including the rationale, trade-offs, and alternatives considered.

## Table of Contents

1. [Unified Disk-Based Storage](#unified-disk-based-storage)
2. [Page-Based Storage with Slotted Pages](#page-based-storage-with-slotted-pages)
3. [Buffer Pool with LRU-K](#buffer-pool-with-lru-k)
4. [Key-Value Foundation](#key-value-foundation)
5. [Multi-Database Architecture](#multi-database-architecture)
6. [Internationalization (I18N)](#internationalization-i18n)
7. [Binary Wire Protocol](#binary-wire-protocol)
8. [Bully Algorithm for Leader Election](#bully-algorithm-for-leader-election)
9. [Replication Strategy](#replication-strategy)
10. [B-Tree for Indexes](#b-tree-for-indexes)
11. [Transaction Model](#transaction-model)
12. [bcrypt for Password Hashing](#bcrypt-for-password-hashing)
13. [Row-Level Security](#row-level-security)
14. [AES-256-GCM for Encryption](#aes-256-gcm-for-encryption)
15. [Go as Implementation Language](#go-as-implementation-language)
16. [Role-Based Access Control](#role-based-access-control)

---

## Unified Disk-Based Storage

### Decision

Use a unified disk-based storage engine that combines page-based storage, intelligent buffer pool caching, and Write-Ahead Logging (WAL) for durability.

### The Problem

Every database faces a fundamental tension:

- **Memory is fast but volatile**: RAM provides nanosecond access but loses data on power loss
- **Disk is slow but persistent**: SSDs/HDDs survive crashes but are 1000x slower than RAM
- **Datasets vary in size**: Some fit in RAM, others are terabytes

A pure in-memory database is fast but limited by RAM and loses data on crashes. A pure disk-based database is slow for every operation. We need the best of both worlds.

### Rationale

1. **Scalability**: Datasets can exceed available RAM—only hot data needs to be in memory
2. **Performance**: Buffer pool provides memory-speed access for frequently-used data
3. **Durability**: WAL ensures committed data survives crashes
4. **Simplicity**: Single storage engine handles all dataset sizes

### Architecture

```
┌────────────────────────────────────────────────────────────┐
│                         SQL Executor                       │
└────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌────────────────────────────────────────────────────────────┐
│                 Unified Storage Engine                     │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Buffer Pool (LRU-K)                    │   │
│  │         Caches hot pages in memory                  │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           Page-Based Disk Storage                   │   │
│  │         8KB slotted pages on disk                   │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │            Write-Ahead Log (WAL)                    │   │
│  │         Ensures durability before writes            │   │
│  └─────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────┘
```

### Trade-offs

| Advantage | Disadvantage |
|-----------|--------------|
| Handles any dataset size | More complex than pure in-memory |
| Memory-speed for hot data | Cold data requires disk I/O |
| Crash-safe by design | Buffer pool management overhead |
| Single unified engine | Page management complexity |

### Alternatives Considered

1. **Pure in-memory with WAL**: Simpler, but limited by RAM and slow startup (must replay entire WAL)
2. **Memory-mapped files**: Platform-specific behavior, OS controls caching (less predictable)
3. **LSM-Tree (like RocksDB)**: Better for write-heavy workloads, but complex compaction
4. **Separate engines for small/large**: Complexity of choosing and switching engines

### Why Not Pure In-Memory?

While simpler, pure in-memory storage has critical limitations:

- **RAM ceiling**: Dataset must fit entirely in memory
- **Slow startup**: Must replay entire WAL on restart (can take minutes for large datasets)
- **No partial loading**: Can't load just the data you need
- **Cost**: RAM is 10-100x more expensive than SSD per GB

The unified disk-based approach provides the same performance for hot data while supporting datasets of any size.

---

## Page-Based Storage with Slotted Pages

### Decision

Store data in fixed-size 8KB pages using a slotted page layout for variable-length records.

### The Problem

Database records (rows) have variable lengths—a user's name might be 5 characters or 50. How do we store variable-length data efficiently on disk?

**Naive approaches fail:**
- **Fixed-size records**: Wastes space (reserve 255 bytes for every VARCHAR(255))
- **Heap allocation**: Fragmentation, no locality, complex free space management
- **Append-only log**: Fast writes, but reads require scanning entire log

### Rationale

1. **Efficient space usage**: Variable-length records packed tightly within pages
2. **Fast lookups**: Slot array provides O(1) access to any record in a page
3. **Stable record IDs**: Slot numbers don't change when records move within page
4. **Simple free space management**: Track free space per page, not per byte

### Slotted Page Layout

```
┌────────────────────────────────────────────────────────────────┐
│                        Page Header                             │
│  ┌──────────┬──────────┬──────────┬──────────┐                 │
│  │ Page ID  │ Num Slots│ Free Ptr │ Checksum │                 │
│  └──────────┴──────────┴──────────┴──────────┘                 │
├────────────────────────────────────────────────────────────────┤
│                        Slot Array                              │
│  ┌──────────┬──────────┬──────────┬──────────┐                 │
│  │ Slot 0   │ Slot 1   │ Slot 2   │   ...    │  ← Grows down   │
│  │ (off,len)│ (off,len)│ (off,len)│          │                 │
│  └──────────┴──────────┴──────────┴──────────┘                 │
│                           ...                                  │
│                      Free Space                                │
│                           ...                                  │
├────────────────────────────────────────────────────────────────┤
│                        Record Data                             │
│  ┌──────────────────────────────────────────────┐              │
│  │ Record 2 │ Record 1 │ Record 0 │             │  ← Grows up  │
│  └──────────────────────────────────────────────┘              │
└────────────────────────────────────────────────────────────────┘
```

**Key insight**: Slot array grows down from the header, record data grows up from the bottom. They meet in the middle when the page is full.

### Why 8KB Pages?

| Page Size | Pros | Cons |
|-----------|------|------|
| 4KB | Matches OS page size, less wasted space | More I/O for large scans |
| **8KB** | Good balance, matches PostgreSQL | Slightly more internal fragmentation |
| 16KB | Fewer I/Os for scans | More wasted space, larger buffer pool |

FlyDB chose 8KB as a balance between I/O efficiency and space utilization, matching PostgreSQL's default.

### Trade-offs

| Advantage | Disadvantage |
|-----------|--------------|
| Efficient variable-length storage | Page splits on overflow |
| O(1) record access within page | Compaction needed after deletes |
| Stable slot-based record IDs | Internal fragmentation |
| Simple free space tracking | Fixed page size limits max record |

### Alternatives Considered

1. **Log-structured storage**: Append-only, but requires compaction and slow reads
2. **Heap files without slots**: Simpler, but fragile record addressing
3. **B+ tree leaf pages**: Good for indexes, overkill for heap storage

---

## Buffer Pool with LRU-K

### Decision

Use an LRU-K (K=2) page replacement algorithm with automatic sizing based on available system memory.

### The Problem

The buffer pool caches disk pages in memory. When it's full and we need a new page, which page should we evict?

**Simple LRU fails for databases:**

```
Scenario: Buffer pool has 3 pages, all frequently accessed
Query: Full table scan reads 1000 pages sequentially

With simple LRU:
- Each scan page evicts a hot page
- After scan, all hot pages are gone
- This is called "buffer pool pollution"
```

### Rationale

1. **Scan resistance**: LRU-K distinguishes between frequently-accessed and one-time-access pages
2. **Automatic sizing**: Buffer pool adapts to available system memory
3. **Predictable performance**: Hot data stays in memory regardless of scan patterns
4. **Simple implementation**: K=2 provides good results with minimal complexity

### How LRU-K Works

LRU-K tracks the **K-th most recent access** for each page, not just the most recent:

```
Page A: Accessed at times [100, 200, 300, 400]  → K=2 reference: 300
Page B: Accessed at times [350, 400]            → K=2 reference: 350
Page C: Accessed at times [400]                 → K=2 reference: -∞ (only 1 access)

Eviction order: C first (only accessed once), then A (older K-th reference)
```

**Key insight**: A page accessed only once (like during a table scan) has K-th reference of -∞ and is evicted first, protecting frequently-accessed pages.

### Auto-Sizing Formula

```go
// Buffer pool uses 25% of available RAM
bufferPoolSize = availableMemory * 0.25

// With bounds:
// Minimum: 2MB (256 pages × 8KB)
// Maximum: 1GB (131,072 pages × 8KB)
```

| Available RAM | Buffer Pool Size | Pages |
|---------------|------------------|-------|
| 4 GB | 1 GB (capped) | 131,072 |
| 8 GB | 1 GB (capped) | 131,072 |
| 1 GB | 256 MB | 32,768 |
| 512 MB | 128 MB | 16,384 |
| 64 MB | 16 MB | 2,048 |

### Trade-offs

| Advantage | Disadvantage |
|-----------|--------------|
| Scan-resistant | More memory per page (track K accesses) |
| Automatic sizing | May compete with OS page cache |
| Predictable eviction | K=2 is heuristic, not optimal for all workloads |
| Simple implementation | No workload-adaptive tuning |

### Alternatives Considered

1. **Simple LRU**: Easier to implement, but vulnerable to scans
2. **LRU-K with K>2**: Diminishing returns, more memory overhead
3. **ARC (Adaptive Replacement Cache)**: Self-tuning, but patented and complex
4. **2Q**: Similar benefits to LRU-K, different implementation
5. **Clock/Second-Chance**: Simpler, but less effective than LRU-K

---

## Key-Value Foundation

### Decision

Build the SQL layer on top of a simple key-value store abstraction.

### Rationale

1. **Separation of Concerns**: Storage logic is isolated from SQL logic
2. **Flexibility**: Easy to swap storage backends
3. **Simplicity**: Key-value operations are easy to reason about
4. **Educational**: Demonstrates how SQL databases can be built on simpler primitives

### Key Encoding Scheme

```
row:<table>:<id>     → Row data (JSON)
schema:<table>       → Table schema
seq:<table>          → Auto-increment counter
_sys_users:<name>    → User credentials
_sys_privs:<u>:<t>   → Permissions
```

### Trade-offs

| Advantage | Disadvantage |
|-----------|--------------|
| Clean abstraction | Prefix scans for table queries |
| Easy to test | No native range queries |
| Swappable backends | JSON serialization overhead |
| Simple transactions | No columnar storage benefits |

### Alternatives Considered

1. **Page-based storage**: More efficient for large tables, but complex
2. **Columnar storage**: Better for analytics, but complex for OLTP
3. **Document store**: Similar trade-offs, less educational value

---

## Multi-Database Architecture

### Decision

Support multiple isolated databases, each stored in its own `.fdb` file.

### Rationale

1. **Isolation**: Databases share no state, preventing cross-contamination
2. **Management**: Easy to backup, restore, or delete individual databases
3. **Lazy Loading**: Databases loaded on first access, reducing startup time
4. **Per-Connection Context**: Each client connection tracks its current database

### Implementation

```go
type DatabaseManager struct {
    dataDir   string               // Base directory (e.g., /var/lib/flydb/)
    databases map[string]*Database // Lazy-loaded databases
}

type Database struct {
    Name     string
    Path     string            // e.g., /var/lib/flydb/myapp.fdb
    Store    *KVStore
    Metadata *DatabaseMetadata
}
```

### Database Metadata

Each database stores its own metadata (encoding, collation, locale) in the key `_sys_db_meta`:

```go
type DatabaseMetadata struct {
    Name      string            // Database name
    Owner     string            // Creator username
    Encoding  CharacterEncoding // UTF8, LATIN1, ASCII, UTF16
    Collation Collation         // default, binary, nocase, unicode
    Locale    string            // e.g., "en_US", "de_DE"
}
```

### Trade-offs

| Advantage | Disadvantage |
|-----------|--------------|
| Complete isolation | One file per database |
| Independent management | No cross-database queries |
| Portable databases | Separate executor per database |
| Per-database settings | Memory for each loaded database |

---

## Internationalization (I18N)

### Decision

Support pluggable character encodings and collations per database.

### Rationale

1. **Flexibility**: Different applications have different text requirements
2. **Locale-Aware Sorting**: Unicode collation for proper international sorting
3. **Storage Efficiency**: ASCII databases use less space than UTF-16
4. **Validation**: Catch encoding errors at write time, not read time

### Supported Encodings

| Encoding | Description | Use Case |
|----------|-------------|----------|
| UTF-8 | Go's native encoding | General purpose (default) |
| ASCII | 7-bit characters only | Legacy systems |
| Latin-1 | Western European | ISO-8859-1 compatibility |
| UTF-16 | 16-bit Unicode | Windows compatibility |

### Supported Collations

| Collation | Implementation | Use Case |
|-----------|----------------|----------|
| `default` | Go's native `<` operator | General purpose, fast |
| `binary` | Byte-by-byte comparison | Exact matching, hashes |
| `nocase` | `strings.ToLower()` comparison | Case-insensitive search |
| `unicode` | `golang.org/x/text/collate` | Locale-aware sorting |

### Trade-offs

| Advantage | Disadvantage |
|-----------|--------------|
| Proper international sorting | Unicode collation is slower |
| Encoding validation | Validation overhead on writes |
| Per-database settings | Complexity in executor |
| Locale support | External dependency for Unicode |

---

## Binary Wire Protocol

### Decision

Use a binary protocol with 8-byte headers, length-prefixed framing, and JSON payloads.

### The Problem

Binary protocols require careful design decisions:

1. **Framing**: How does the receiver know where one message ends and the next begins?
2. **Versioning**: How do we evolve the protocol without breaking clients?
3. **Extensibility**: How do we add new message types?
4. **Debugging**: Binary is hard to read—how do we debug issues?

### Rationale

1. **Length-prefixed framing**: Unambiguous message boundaries, no delimiter escaping
2. **Magic byte**: Quick rejection of non-FlyDB connections
3. **Version field**: Forward compatibility for protocol evolution
4. **JSON payloads**: Human-readable when debugging, easy to extend

### Message Frame Format

```
┌─────────┬─────────┬─────────┬─────────┬─────────────────┬─────────────────┐
│ Magic   │ Version │ MsgType │ Flags   │ Length (4B)     │ Payload (var)   │
│ 0xFD    │ 0x01    │         │         │ big-endian      │ JSON            │
└─────────┴─────────┴─────────┴─────────┴─────────────────┴─────────────────┘
```

| Field | Size | Purpose |
|-------|------|---------|
| Magic | 1 byte | Protocol identification (0xFD = "FlyDB") |
| Version | 1 byte | Protocol version for compatibility |
| MsgType | 1 byte | Message type for routing |
| Flags | 1 byte | Optional features (compression, encryption) |
| Length | 4 bytes | Payload size (max 16 MB) |
| Payload | variable | JSON-encoded message data |

### Why JSON Payloads?

| Approach | Pros | Cons |
|----------|------|------|
| Pure binary (like PostgreSQL) | Smallest, fastest | Hard to debug, rigid schema |
| Pure text (like Redis RESP) | Easy to debug | Slow parsing, framing issues |
| **Binary frame + JSON** | Easy to debug, clear framing | Slightly larger than pure binary |

For FlyDB, debuggability and ease of driver development outweigh the small size overhead.

### Message Type Organization

```go
// Core operations (0x01-0x0F)
MsgQuery        = 0x01  // SQL query
MsgQueryResult  = 0x02  // Query response
MsgError        = 0x03  // Error response

// Prepared statements (0x04-0x07)
MsgPrepare      = 0x04  // Prepare statement
MsgExecute      = 0x06  // Execute prepared

// Authentication (0x08-0x09)
MsgAuth         = 0x08  // Login request
MsgAuthResult   = 0x09  // Login response

// Control (0x0A-0x0F)
MsgPing         = 0x0A  // Keep-alive
MsgPong         = 0x0B  // Keep-alive response
```

### Trade-offs

| Advantage | Disadvantage |
|-----------|--------------|
| Clear message boundaries | 8-byte header overhead |
| Easy to extend | JSON parsing overhead |
| Version negotiation | More complex than text |
| Compression support | Requires binary-aware tools |

### Alternatives Considered

1. **Protocol Buffers**: Efficient, but adds dependency and complexity
2. **MessagePack**: Binary JSON, but less human-readable
3. **Custom binary format**: Maximum efficiency, but hard to debug
4. **WebSocket**: Good for browsers, but overkill for database protocol

---

## Bully Algorithm for Leader Election

### Decision

Use the Bully algorithm for automatic leader election in cluster mode.

### Rationale

1. **Simplicity**: Easy to understand and implement
2. **Deterministic**: Highest-ID node always wins
3. **Fast Convergence**: Election completes quickly
4. **Educational**: Classic distributed systems algorithm

### How It Works

```
1. Node detects leader failure (missed heartbeats)
2. Node sends ELECTION to all higher-ID nodes
3. If no response, node becomes leader (COORDINATOR)
4. If response received, wait for COORDINATOR from higher node
```

### Trade-offs

| Advantage | Disadvantage |
|-----------|--------------|
| Simple to implement | Highest-ID bias |
| Fast election | Network partition issues |
| Deterministic outcome | Not Byzantine fault tolerant |
| Well-understood | May elect unavailable node |

### Alternatives Considered

1. **Raft**: More robust, but significantly more complex
2. **Paxos**: Proven correct, but notoriously difficult
3. **ZAB (ZooKeeper)**: Production-ready, but external dependency

### When to Reconsider

- Need for stronger consistency guarantees
- Complex network topologies
- Byzantine fault tolerance requirements

---

## Replication Strategy

### Decision

Use WAL-based leader-follower replication with configurable consistency modes (async, semi-sync, sync).

### The Problem

A single database server has critical limitations:

1. **Single point of failure**: Server crash = database unavailable
2. **Read bottleneck**: All queries go to one server
3. **Data loss risk**: Disk failure can lose data

Replication solves these by maintaining copies on multiple servers.

### Rationale

1. **WAL-based**: Reuse existing WAL infrastructure—no separate replication log
2. **Leader-follower**: Simple model, clear write path, easy to reason about
3. **Configurable modes**: Applications choose their consistency/performance trade-off
4. **Pull-based**: Followers poll leader, simplifying leader implementation

### Replication Modes

FlyDB supports three replication modes:

| Mode | Behavior | Durability | Latency |
|------|----------|------------|---------|
| **Async** | Return immediately, replicate in background | May lose recent writes | Lowest |
| **Semi-sync** | Wait for at least one follower | Survives single failure | Medium |
| **Sync** | Wait for all followers | Strongest | Highest |

```go
type ReplicationMode int

const (
    ReplicationAsync    ReplicationMode = iota  // Default
    ReplicationSemiSync
    ReplicationSync
)
```

### Replication Flow

```
Leader                              Follower
   │                                   │
   │ ◄──── REPLICATE <offset> ─────────│ (poll every 100ms)
   │                                   │
   │ ───── WAL entries ───────────────►│
   │       (binary format)             │
   │                                   │
   │                                   │ (apply to local store)
   │                                   │
   │ ◄──── ACK <new_offset> ───────────│ (for semi-sync/sync)
```

### Why Pull-Based?

| Approach | Pros | Cons |
|----------|------|------|
| **Pull (FlyDB)** | Simple leader, follower controls pace | Polling overhead |
| Push | Lower latency, no polling | Leader tracks all followers |
| Hybrid | Best of both | Complex implementation |

Pull-based is simpler: the leader just responds to requests, followers manage their own state.

### Consistency Window

- **Async mode**: 100ms poll interval + network latency + apply time ≈ 100-300ms typical lag
- **Semi-sync mode**: Network round-trip + apply time ≈ 10-50ms
- **Sync mode**: All followers must acknowledge ≈ slowest follower determines latency

### Trade-offs

| Advantage | Disadvantage |
|-----------|--------------|
| Simple implementation | Async may lose data on leader failure |
| Configurable consistency | Semi-sync/sync add latency |
| WAL reuse | No multi-master support |
| Pull-based simplicity | Polling overhead |

### Alternatives Considered

1. **Raft consensus**: Strong consistency, but complex leader election and log management
2. **Paxos**: Proven correct, but notoriously difficult to implement
3. **Multi-master**: Higher availability, but conflict resolution is complex
4. **Logical replication**: More flexible, but requires parsing and transforming operations

---

## B-Tree for Indexes

### Decision

Use B-Tree data structure for secondary indexes with configurable minimum degree.

### The Problem

Without indexes, finding a row requires scanning the entire table:

```sql
SELECT * FROM users WHERE email = 'alice@example.com';
-- Without index: O(N) - scan all rows
-- With index: O(log N) - tree traversal
```

For a table with 1 million rows, that's the difference between 1 million comparisons and ~20 comparisons.

### Rationale

1. **Balanced**: Guaranteed O(log N) operations regardless of insertion order
2. **Range Queries**: Efficient for ORDER BY, BETWEEN, and inequality comparisons
3. **Well-Understood**: Classic database index structure with decades of research
4. **In-Memory Optimized**: FlyDB's B-Tree is tuned for memory, not disk

### How B-Trees Work

A B-Tree is a self-balancing tree where:
- Each node can have multiple keys (not just one like binary trees)
- All leaves are at the same depth (perfectly balanced)
- Nodes split when full, maintaining balance automatically

```
                    [50]
                   /    \
           [20, 30]      [70, 80, 90]
          /   |   \      /   |   |   \
        [10] [25] [35] [60] [75] [85] [95]
```

**Key insight**: More keys per node = shorter tree = fewer comparisons.

### Implementation Details

```go
type BTree struct {
    root *BTreeNode
    t    int  // Minimum degree
}

type BTreeNode struct {
    keys     []string      // Indexed values
    values   []string      // Row key references
    children []*BTreeNode  // Child nodes (nil for leaves)
    leaf     bool
}
```

**Parameters:**
- **Minimum degree (t)**: Default 16 for in-memory use
- **Keys per node**: Between t-1 and 2t-1 (15 to 31 with t=16)
- **Children per node**: Between t and 2t (16 to 32 with t=16)

### Why t=16?

| Minimum Degree | Keys/Node | Tree Height (1M rows) | Memory/Node |
|----------------|-----------|----------------------|-------------|
| t=2 | 1-3 | ~20 levels | Small |
| t=4 | 3-7 | ~10 levels | Medium |
| **t=16** | **15-31** | **~5 levels** | **Larger** |
| t=64 | 63-127 | ~3 levels | Very large |

t=16 provides a good balance: short trees (fast lookups) without excessive memory per node.

### Trade-offs

| Advantage | Disadvantage |
|-----------|--------------|
| Guaranteed O(log N) | More complex than hash index |
| Range query support | Split/merge on insert/delete |
| Ordered traversal | Higher memory per node |
| Self-balancing | Not cache-line optimized |

### Alternatives Considered

1. **Hash Index**: O(1) average lookups, but no range queries or ordering
2. **B+ Tree**: Leaves linked for efficient scans, but more complex
3. **Skip List**: Probabilistic balance, simpler but less predictable
4. **LSM Tree**: Write-optimized, but requires background compaction
5. **Red-Black Tree**: Binary tree, but taller than B-Tree (more comparisons)

---

## Transaction Model

### Decision

Use optimistic concurrency with write buffering, read-your-writes semantics, and savepoint support.

### The Problem

Transactions must provide ACID guarantees, but different approaches have different trade-offs:

| Approach | Consistency | Performance | Complexity |
|----------|-------------|-------------|------------|
| No transactions | None | Fastest | Simplest |
| Pessimistic locking | Strong | Slower (blocking) | Deadlock handling |
| Optimistic concurrency | Read-committed | Fast (no blocking) | Conflict detection |
| MVCC | Snapshot isolation | Fast | Version management |

### Rationale

1. **Simplicity**: No complex locking protocols or version chains
2. **Performance**: Reads never block, writes are buffered
3. **Read-Your-Writes**: Transactions see their own uncommitted changes
4. **Partial Rollback**: Savepoints allow undoing part of a transaction

### How It Works

```
BEGIN TRANSACTION
    │
    ▼
┌──────────────────────────────────────────────────────┐
│                   Transaction State                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │
│  │ Write Buffer│  │ Read Cache  │  │ Delete Set  │   │
│  │ (pending    │  │ (read-your- │  │ (pending    │   │
│  │  writes)    │  │  writes)    │  │  deletes)   │   │
│  └─────────────┘  └─────────────┘  └─────────────┘   │
└──────────────────────────────────────────────────────┘
    │                                    │
    ▼                                    ▼
  COMMIT                              ROLLBACK
    │                                    │
    ▼                                    ▼
  Apply all                           Discard
  buffered ops                        everything
```

### Implementation

```go
type Transaction struct {
    store      Engine              // Underlying storage
    buffer     []TxOperation       // Ordered pending operations
    readCache  map[string][]byte   // Read-your-writes cache
    deleteSet  map[string]bool     // Pending deletes
    savepoints []Savepoint         // Named rollback points
    state      TxState             // Active, Committed, RolledBack
}
```

### Read Path (Read-Your-Writes)

```go
func (tx *Transaction) Get(key string) ([]byte, error) {
    // 1. Check if deleted in this transaction
    if tx.deleteSet[key] {
        return nil, ErrNotFound
    }
    // 2. Check local write buffer
    if val, ok := tx.readCache[key]; ok {
        return val, nil
    }
    // 3. Fall back to committed data
    return tx.store.Get(key)
}
```

### Isolation Level: Read Committed

FlyDB provides **Read Committed** isolation:

| Guarantee | Provided? | Explanation |
|-----------|-----------|-------------|
| No dirty reads | Yes | Only see committed data from others |
| Read-your-writes | Yes | See your own uncommitted changes |
| Repeatable reads | No | Same query may return different results |
| No phantom reads | No | New rows may appear between queries |

**Why Read Committed?**

It's the sweet spot for most applications—strong enough to prevent data corruption, weak enough to allow high concurrency.

### Savepoints

Savepoints allow partial rollback within a transaction:

```sql
BEGIN;
INSERT INTO orders VALUES (1, 'pending');
SAVEPOINT sp1;
INSERT INTO orders VALUES (2, 'pending');
-- Oops, rollback just the second insert
ROLLBACK TO sp1;
COMMIT;  -- Only first insert is committed
```

### Trade-offs

| Advantage | Disadvantage |
|-----------|--------------|
| Simple implementation | No serializable isolation |
| Reads never block | Last writer wins |
| Easy rollback | No automatic conflict detection |
| Savepoint support | Memory grows with transaction size |

### Alternatives Considered

1. **MVCC (Multi-Version Concurrency Control)**: Snapshot isolation, but requires version chains and garbage collection
2. **Pessimistic Locking (2PL)**: Serializable, but deadlock-prone and blocking
3. **Serializable Snapshot Isolation**: Strongest, but complex conflict detection

---

## bcrypt for Password Hashing

### Decision

Use bcrypt with cost factor 10 for password hashing.

### Rationale

1. **Security**: Designed specifically for password hashing
2. **Adaptive**: Cost factor can be increased over time
3. **Salt Built-in**: Automatic salt generation and storage
4. **Industry Standard**: Widely recommended and audited

### Implementation

```go
const DefaultBcryptCost = 10

// Hashing
hash, _ := bcrypt.GenerateFromPassword([]byte(password), DefaultBcryptCost)

// Verification
err := bcrypt.CompareHashAndPassword(hash, []byte(password))
```

### Trade-offs

| Advantage | Disadvantage |
|-----------|--------------|
| Purpose-built for passwords | Slower than SHA-256 |
| Automatic salting | CPU-intensive (by design) |
| Adjustable work factor | Fixed output format |
| Resistant to GPU attacks | Memory usage |

### Alternatives Considered

1. **Argon2**: Newer, memory-hard, but less library support
2. **scrypt**: Memory-hard, good alternative
3. **PBKDF2**: Widely supported, but less resistant to GPU attacks

---

## Row-Level Security

### Decision

Implement Row-Level Security (RLS) as part of the GRANT statement.

### Rationale

1. **Simplicity**: RLS tied to permissions, not separate policies
2. **Transparency**: Users see filtered data without knowing RLS exists
3. **Enforcement**: Applied at executor level, cannot be bypassed
4. **Familiar Syntax**: Extends standard GRANT with WHERE clause

### Syntax

```sql
-- Grant SELECT with row filter
GRANT SELECT ON orders WHERE user_id = 'alice' TO alice;

-- User 'alice' only sees rows where user_id = 'alice'
SELECT * FROM orders;  -- Automatically filtered
```

### Implementation

```go
type Permission struct {
    TableName string `json:"table_name"`
    RLS       *RLS   `json:"rls,omitempty"`
}

type RLS struct {
    Column string `json:"column"`
    Value  string `json:"value"`
}
```

### Trade-offs

| Advantage | Disadvantage |
|-----------|--------------|
| Simple to understand | Only single-column filters |
| Integrated with GRANT | No complex expressions |
| Automatic enforcement | Per-table, not per-query |
| No separate policy table | Limited to equality checks |

### Alternatives Considered

1. **Separate RLS policies**: More flexible, but more complex
2. **View-based security**: Requires creating views per user
3. **Application-level filtering**: Not enforced by database

---

## AES-256-GCM for Encryption

### Decision

Use AES-256-GCM for encrypting data at rest (WAL and stored values).

### Rationale

1. **Security**: 256-bit key provides strong encryption
2. **Authenticated**: GCM mode provides integrity checking
3. **Performance**: Hardware acceleration on modern CPUs
4. **Standard**: NIST-approved, widely audited

### Implementation

```go
type Encryptor struct {
    gcm cipher.AEAD
}

func (e *Encryptor) Encrypt(plaintext []byte) ([]byte, error) {
    nonce := make([]byte, e.gcm.NonceSize())
    io.ReadFull(rand.Reader, nonce)
    return e.gcm.Seal(nonce, nonce, plaintext, nil), nil
}
```

### Trade-offs

| Advantage | Disadvantage |
|-----------|--------------|
| Strong encryption | Key management complexity |
| Integrity verification | Performance overhead |
| Hardware acceleration | Nonce management required |
| Industry standard | Increased storage size |

---

## Go as Implementation Language

### Decision

Implement FlyDB in Go.

### Rationale

1. **Concurrency**: Goroutines and channels for server workloads
2. **Simplicity**: Clean syntax, easy to read and understand
3. **Performance**: Compiled language with good performance
4. **Standard Library**: Excellent networking and crypto support
5. **Educational**: Popular language, accessible to learners

### Trade-offs

| Advantage | Disadvantage |
|-----------|--------------|
| Easy concurrency | Garbage collection pauses |
| Fast compilation | No generics (until Go 1.18) |
| Single binary deployment | Less control than C/Rust |
| Strong standard library | Verbose error handling |

### Alternatives Considered

1. **Rust**: Better performance, but steeper learning curve
2. **C++**: Maximum control, but complex memory management
3. **Java**: Good ecosystem, but JVM overhead
4. **Python**: Easy to write, but too slow for database

---

## Role-Based Access Control

### Decision

Implement a role-based access control (RBAC) system with roles, privileges, and user-role assignments, supporting both global and database-scoped permissions.

### Rationale

1. **Scalability**: Managing permissions through roles is more scalable than per-user grants
2. **Maintainability**: Changing a role's privileges automatically affects all users with that role
3. **Flexibility**: Database-scoped roles allow fine-grained control per database
4. **Industry Standard**: RBAC is the de facto standard for enterprise access control

### Design

The RBAC system consists of three main components:

1. **Roles**: Named collections of privileges (e.g., `admin`, `reader`, `writer`)
2. **Privileges**: Specific permissions on objects (SELECT, INSERT, UPDATE, DELETE, ALL)
3. **User-Role Assignments**: Mappings between users and roles, optionally scoped to databases

### Built-in Roles

| Role | Description | Privileges |
|------|-------------|------------|
| `admin` | Full administrative access | ALL on all objects |
| `reader` | Read-only access | SELECT on all tables |
| `writer` | Write access | INSERT, UPDATE, DELETE on all tables |
| `owner` | Full access to owned objects | ALL on database-scoped objects |

### Trade-offs

| Advantage | Disadvantage |
|-----------|--------------|
| Centralized permission management | Additional complexity |
| Easy to audit who has what access | Role explosion if not managed |
| Supports principle of least privilege | Requires careful role design |
| Database-scoped roles for multi-tenancy | More storage for role metadata |

### Alternatives Considered

1. **ACL (Access Control Lists)**: More flexible but harder to manage at scale
2. **ABAC (Attribute-Based Access Control)**: More powerful but significantly more complex
3. **Simple user-table grants**: Simpler but doesn't scale well

### Implementation Details

- Roles stored with `_sys_roles:` prefix in system database
- Role privileges stored with `_sys_role_privs:` prefix
- User-role assignments stored with `_sys_user_roles:` prefix
- Built-in roles are marked with `is_built_in: true` and cannot be dropped
- Authorization checks cascade: admin status → role privileges → direct grants

---

## Metadata Provider Architecture

### Decision

Implement a `MetadataProvider` interface that bridges the binary protocol handler with the SQL catalog, providing ODBC/JDBC-compatible metadata responses.

### The Problem

ODBC and JDBC drivers require standardized metadata APIs to discover database schema:

- `getTables()` - List available tables and views
- `getColumns()` - Get column definitions with types and constraints
- `getPrimaryKeys()` - Retrieve primary key information
- `getForeignKeys()` - Get foreign key relationships
- `getIndexInfo()` - List indexes on tables
- `getTypeInfo()` - Enumerate supported SQL types

These APIs expect specific response formats with ODBC type codes, nullability flags, and other standardized fields.

### Rationale

1. **Separation of Concerns**: The MetadataProvider abstracts catalog access from protocol handling
2. **ODBC/JDBC Compatibility**: Response formats match standard driver expectations
3. **Extensibility**: New metadata operations can be added without modifying the protocol handler
4. **Testability**: The interface can be mocked for unit testing

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    BinaryHandler                                 │
│  Receives: MsgGetTables, MsgGetColumns, etc.                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  MetadataProvider Interface                      │
│  GetTables(), GetColumns(), GetPrimaryKeys(), GetForeignKeys(), │
│  GetIndexes(), GetTypeInfo()                                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  serverMetadataProvider                          │
│  Accesses Executor.GetCatalog() and Executor.GetIndexManager()  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      SQL Catalog                                 │
│  Tables, Views, Columns, Constraints, Indexes                   │
└─────────────────────────────────────────────────────────────────┘
```

### Trade-offs

| Advantage | Disadvantage |
|-----------|--------------|
| Clean abstraction | Additional interface layer |
| ODBC/JDBC compatible | Some fields return defaults |
| Graceful nil handling | Pattern matching overhead |
| Easy to extend | Requires catalog access |

### Default Values vs. Actual Schema Information

Some metadata fields return default values rather than actual schema information:

| Field | Behavior | Rationale |
|-------|----------|-----------|
| TABLE_CAT | Returns provided catalog | FlyDB uses database names, not catalogs |
| TABLE_SCHEM | Returns provided schema | Single schema per database |
| REMARKS | Returns empty string | TableSchema doesn't store comments |
| COLUMN_SIZE | Returns type-based default | Actual size not always tracked |
| DECIMAL_DIGITS | Returns 0 | Scale not tracked for all types |

**Why Accept Defaults?**

1. **Simplicity**: Tracking every ODBC field would add significant complexity
2. **Compatibility**: Drivers work correctly with sensible defaults
3. **Performance**: Fewer fields to store and retrieve
4. **Pragmatism**: Most applications don't use all metadata fields

### Performance Considerations

Metadata queries iterate over the entire catalog, which could be slow for databases with many tables. Current mitigations:

1. **Pattern Matching**: Clients can filter with SQL wildcards (%)
2. **Lazy Loading**: Catalog is only accessed when metadata is requested
3. **No Caching**: Results are computed fresh (catalog may change)

For very large schemas, future enhancements could include:
- Metadata result caching with invalidation
- Indexed catalog lookups
- Pagination for large result sets

---

## See Also

- [Architecture Overview](architecture.md) - High-level system design
- [Implementation Details](implementation.md) - Technical deep-dives
- [API Reference](api.md) - SQL syntax and commands

