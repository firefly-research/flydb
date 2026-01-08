# FlyDB Design Decisions

This document explains the key design decisions made in FlyDB, including the rationale, trade-offs, and alternatives considered.

## Table of Contents

1. [In-Memory Storage with WAL](#in-memory-storage-with-wal)
2. [Key-Value Foundation](#key-value-foundation)
3. [Multi-Database Architecture](#multi-database-architecture)
4. [Internationalization (I18N)](#internationalization-i18n)
5. [Simple Text Protocol](#simple-text-protocol)
6. [Bully Algorithm for Leader Election](#bully-algorithm-for-leader-election)
7. [Eventual Consistency for Replication](#eventual-consistency-for-replication)
8. [B-Tree for Indexes](#b-tree-for-indexes)
9. [Transaction Model](#transaction-model)
10. [bcrypt for Password Hashing](#bcrypt-for-password-hashing)
11. [Row-Level Security](#row-level-security)
12. [AES-256-GCM for Encryption](#aes-256-gcm-for-encryption)
13. [Go as Implementation Language](#go-as-implementation-language)
14. [Role-Based Access Control](#role-based-access-control)

---

## In-Memory Storage with WAL

### Decision

Store all data in memory using a Go `map[string][]byte`, with a Write-Ahead Log (WAL) for durability.

### Rationale

1. **Simplicity**: In-memory storage is straightforward to implement and understand
2. **Performance**: Memory access is orders of magnitude faster than disk I/O
3. **Educational Value**: Clearly demonstrates the WAL concept without complex buffer management
4. **Durability**: WAL ensures committed data survives crashes

### Trade-offs

| Advantage | Disadvantage |
|-----------|--------------|
| Fast reads (O(1) average) | Limited by available RAM |
| Simple implementation | Full dataset must fit in memory |
| Easy debugging | Startup time grows with data size |
| No buffer pool complexity | No partial loading of data |

### Alternatives Considered

1. **Disk-based storage (like SQLite)**: More complex, requires buffer pool management
2. **Memory-mapped files**: Platform-specific behavior, complex crash recovery
3. **LSM-Tree (like RocksDB)**: Better for write-heavy workloads, but more complex

### When to Reconsider

- Dataset exceeds available RAM
- Need for partial data loading
- Cold start time becomes unacceptable

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

## Simple Text Protocol

### Decision

Use a line-based text protocol for the primary interface (port 8888).

### Rationale

1. **Debuggability**: Can interact with database using telnet/netcat
2. **Simplicity**: Easy to implement clients in any language
3. **Educational**: Protocol is human-readable
4. **Testing**: Easy to write integration tests

### Protocol Format

```
Request:  COMMAND [args]\n
Response: RESULT\n

Examples:
  PING           → PONG
  AUTH user pass → AUTH OK
  SQL SELECT ... → id|name|age\n1|Alice|30
```

### Trade-offs

| Advantage | Disadvantage |
|-----------|--------------|
| Human-readable | Parsing overhead |
| Easy debugging | No binary data support |
| Simple clients | Escaping issues with special chars |
| Telnet-friendly | Less efficient than binary |

### Mitigation

A binary protocol is also available on port 8889 for clients that need efficiency.

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

## Eventual Consistency for Replication

### Decision

Use asynchronous WAL streaming with 100ms polling interval.

### Rationale

1. **Simplicity**: No complex consensus protocol
2. **Performance**: Leader doesn't wait for follower acknowledgment
3. **Availability**: Leader can continue if followers are slow
4. **Educational**: Demonstrates replication concepts clearly

### Replication Flow

```
Leader                          Follower
   │                               │
   │ ◄─── REPLICATE <offset> ──────│ (every 100ms)
   │                               │
   │ ──── WAL entries ────────────►│
   │                               │
   │                               │ (apply to local store)
```

### Trade-offs

| Advantage | Disadvantage |
|-----------|--------------|
| Simple implementation | Data loss on leader failure |
| High write throughput | Stale reads on followers |
| Leader availability | No read-your-writes guarantee |
| Low latency writes | Replication lag visible |

### Consistency Window

- **Maximum lag**: 100ms + network latency + apply time
- **Typical lag**: < 200ms under normal conditions

### Alternatives Considered

1. **Synchronous replication**: Stronger consistency, but higher latency
2. **Semi-synchronous**: Wait for one follower, balance of both
3. **Raft-based replication**: Strong consistency, complex implementation

---

## B-Tree for Indexes

### Decision

Use B-Tree data structure for secondary indexes.

### Rationale

1. **Balanced**: Guaranteed O(log N) operations
2. **Range Queries**: Efficient for ORDER BY and BETWEEN
3. **Well-Understood**: Classic database index structure
4. **Educational**: Demonstrates fundamental CS concepts

### Implementation Details

- **Minimum degree (t)**: Configurable, default 16 (optimized for in-memory use)
- **Node capacity**: 2t-1 keys per node (up to 31 keys with default t=16)
- **Leaf nodes**: Store actual row key references
- **Automatic maintenance**: Indexes updated on INSERT, UPDATE, DELETE

### Trade-offs

| Advantage | Disadvantage |
|-----------|--------------|
| Balanced height | More complex than hash index |
| Range query support | Higher memory overhead |
| Ordered traversal | Split/merge complexity |
| Proven algorithm | Not cache-optimized |

### Alternatives Considered

1. **Hash Index**: O(1) lookups, but no range queries
2. **B+ Tree**: Better for disk, leaves linked for scans
3. **Skip List**: Simpler, probabilistic balance
4. **LSM Tree**: Better for writes, complex compaction

---

## Transaction Model

### Decision

Use optimistic concurrency with write buffering and savepoint support.

### Rationale

1. **Simplicity**: No complex locking protocols
2. **Performance**: Reads don't block writes
3. **Easy Rollback**: Discard buffer on ROLLBACK
4. **Partial Rollback**: Savepoints allow undoing part of a transaction

### Implementation

```go
type Transaction struct {
    store      *KVStore
    buffer     []TxOperation      // Pending operations
    readCache  map[string][]byte  // Read-your-writes
    deleteSet  map[string]bool    // Pending deletes
    savepoints []Savepoint
}

type Savepoint struct {
    Name           string
    BufferPosition int  // Position in buffer to rollback to
}
```

### Isolation Level

**Read Committed**: Reads see committed data from the underlying store, plus any uncommitted writes from the current transaction (read-your-writes).

### Trade-offs

| Advantage | Disadvantage |
|-----------|--------------|
| Simple implementation | No serializable isolation |
| Reads don't block | Last writer wins (no conflict detection) |
| Easy rollback | No MVCC |
| Savepoint support | Single writer at a time |

### Alternatives Considered

1. **MVCC**: True snapshot isolation, but complex implementation
2. **Pessimistic Locking**: Stronger isolation, but potential deadlocks
3. **Serializable**: Strongest isolation, but significant overhead

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

## See Also

- [Architecture Overview](architecture.md) - High-level system design
- [Implementation Details](implementation.md) - Technical deep-dives
- [API Reference](api.md) - SQL syntax and commands

