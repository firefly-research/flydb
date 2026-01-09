# FlyDB Driver Development Guide

This document provides a complete specification for developing JDBC, ODBC, or other database drivers that connect to FlyDB using the binary wire protocol.

## Table of Contents

1. [Overview](#overview)
2. [Protocol Specification](#protocol-specification)
3. [Connection Lifecycle](#connection-lifecycle)
4. [Message Types](#message-types)
5. [Data Encoding](#data-encoding)
6. [Cursor Operations](#cursor-operations)
7. [Transaction Management](#transaction-management)
8. [Metadata Queries](#metadata-queries)
9. [Session Management](#session-management)
10. [Code Examples](#code-examples)

---

## Overview

FlyDB provides a binary wire protocol on port 8889 (configurable) that supports all features required for JDBC and ODBC driver development:

| Feature | Support |
|---------|---------|
| SQL Queries | Full SQL execution with structured results |
| Prepared Statements | Parameterized queries with type binding |
| Server-Side Cursors | Scrollable cursors for large result sets |
| Transactions | BEGIN, COMMIT, ROLLBACK with isolation levels |
| Metadata | GetTables, GetColumns, GetTypeInfo for schema discovery |
| Session Options | Auto-commit, isolation level, connection settings |

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    External Driver (JDBC/ODBC)                  │
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │ Connection  │  │  Statement  │  │      ResultSet          │  │
│  │   Pool      │  │   Cache     │  │   (Cursor wrapper)      │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                    TCP Connection (Port 8889)
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                        FlyDB Server                             │
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │  Protocol   │  │    SDK      │  │      SQL Engine         │  │
│  │   Handler   │  │  (Cursors,  │  │   (Lexer, Parser,       │  │
│  │             │  │  Sessions)  │  │    Executor)            │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Protocol Specification

### Message Frame Format

Every message uses an 8-byte header followed by a variable-length payload:

```
┌───────────┬─────────┬──────────┬───────────┬────────────┬─────────────────┐
│ Magic (1B)│ Ver (1B)│ Type (1B)│ Flags (1B)│ Length (4B)│ Payload (var)   │
└───────────┴─────────┴──────────┴───────────┴────────────┴─────────────────┘
```

| Field | Size | Description |
|-------|------|-------------|
| Magic | 1 byte | Always `0xFD` - identifies FlyDB protocol |
| Version | 1 byte | Protocol version (currently `0x01`) |
| Type | 1 byte | Message type code (see Message Types) |
| Flags | 1 byte | Bit flags: `0x01` = compressed |
| Length | 4 bytes | Payload length (big-endian, max 16 MB) |
| Payload | variable | Message-specific data |

### Constants

```
Magic Byte:      0xFD
Protocol Version: 0x01
Max Message Size: 16,777,216 bytes (16 MB)
Header Size:      8 bytes
```

---

## Connection Lifecycle

### 1. Establish Connection

Open a TCP connection to the binary protocol port (default: 8889).

### 2. Authenticate

Send an Auth message immediately after connecting:

```
Auth Message Payload:
┌────────────────┬──────────────┬────────────────┬──────────────┐
│ UsernameLen(4B)│ Username(var)│ PasswordLen(4B)│ Password(var)│
└────────────────┴──────────────┴────────────────┴──────────────┘
```

Wait for AuthResult response:

```
AuthResult Payload:
┌───────────┬────────────┬─────────────┐
│ Success(1)│ MsgLen(4B) │ Message(var)│
└───────────┴────────────┴─────────────┘
```

### 3. Execute Commands

After successful authentication, send Query, Prepare, Execute, or other messages.

### 4. Disconnect

Close the TCP connection. The server will clean up any open cursors and transactions.

---

## Message Types

### Core Messages (0x01-0x0B)

| Type | Code | Description |
|------|------|-------------|
| MsgQuery | 0x01 | Execute SQL query |
| MsgQueryResult | 0x02 | Query result |
| MsgError | 0x03 | Error response |
| MsgPrepare | 0x04 | Prepare statement |
| MsgPrepareResult | 0x05 | Prepare result |
| MsgExecute | 0x06 | Execute prepared statement |
| MsgDeallocate | 0x07 | Deallocate prepared statement |
| MsgAuth | 0x08 | Authentication request |
| MsgAuthResult | 0x09 | Authentication result |
| MsgPing | 0x0A | Keepalive ping |
| MsgPong | 0x0B | Keepalive pong |

### Cursor Messages (0x10-0x14)

| Type | Code | Description |
|------|------|-------------|
| MsgCursorOpen | 0x10 | Open server-side cursor |
| MsgCursorFetch | 0x11 | Fetch rows from cursor |
| MsgCursorClose | 0x12 | Close cursor |
| MsgCursorScroll | 0x13 | Scroll cursor position |
| MsgCursorResult | 0x14 | Cursor operation result |

### Metadata Messages (0x20-0x26)

| Type | Code | Description |
|------|------|-------------|
| MsgGetTables | 0x20 | Get table metadata |
| MsgGetColumns | 0x21 | Get column metadata |
| MsgGetPrimaryKeys | 0x22 | Get primary key info |
| MsgGetForeignKeys | 0x23 | Get foreign key info |
| MsgGetIndexes | 0x24 | Get index info |
| MsgGetTypeInfo | 0x25 | Get type information |
| MsgMetadataResult | 0x26 | Metadata result |

### Transaction Messages (0x30-0x34)

| Type | Code | Description |
|------|------|-------------|
| MsgBeginTx | 0x30 | Begin transaction |
| MsgCommitTx | 0x31 | Commit transaction |
| MsgRollbackTx | 0x32 | Rollback transaction |
| MsgSavepoint | 0x33 | Create/release savepoint |
| MsgTxResult | 0x34 | Transaction result |

### Session Messages (0x40-0x43)

| Type | Code | Description |
|------|------|-------------|
| MsgSetOption | 0x40 | Set session option |
| MsgGetOption | 0x41 | Get session option |
| MsgGetServerInfo | 0x42 | Get server information |
| MsgSessionResult | 0x43 | Session operation result |

### Database Messages (0x50-0x52)

| Type | Code | Description |
|------|------|-------------|
| MsgUseDatabase | 0x50 | Switch to a different database |
| MsgGetDatabases | 0x51 | List available databases |
| MsgDatabaseResult | 0x52 | Database operation result |

---

## Data Encoding

All multi-byte integers are encoded in big-endian format.

### Primitive Types

| Type | Encoding |
|------|----------|
| Bool | 1 byte: 0x00 = false, 0x01 = true |
| Int32 | 4 bytes, big-endian |
| Int64 | 8 bytes, big-endian |
| Float64 | 8 bytes, IEEE 754 |
| String | 4-byte length prefix + UTF-8 bytes |
| Bytes | 4-byte length prefix + raw bytes |

### JSON Encoding

Complex payloads use JSON encoding for flexibility. The JSON is UTF-8 encoded and length-prefixed.

---

## Cursor Operations

Server-side cursors enable efficient handling of large result sets.

### Cursor Types

| Type | Code | Description |
|------|------|-------------|
| Forward-Only | 0 | Can only move forward (most efficient) |
| Static | 1 | Snapshot of data at open time |
| Keyset | 2 | Keys fixed, values updated |
| Dynamic | 3 | Fully dynamic (least efficient) |

### Fetch Directions

| Direction | Code | Description |
|-----------|------|-------------|
| Next | 0 | Fetch next N rows |
| Prior | 1 | Fetch previous N rows |
| First | 2 | Fetch from beginning |
| Last | 3 | Fetch from end |
| Absolute | 4 | Fetch from absolute position |
| Relative | 5 | Fetch relative to current |

### Cursor Open Request

```json
{
  "query": "SELECT * FROM users WHERE active = $1",
  "cursor_type": 1,
  "concurrency": 0,
  "fetch_size": 100,
  "parameters": [true]
}
```

### Cursor Fetch Request

```json
{
  "cursor_id": "cur_abc123",
  "direction": 0,
  "offset": 0,
  "count": 50
}
```

---

## Transaction Management

### Isolation Levels

| Level | Code | Description |
|-------|------|-------------|
| Read Uncommitted | 0 | Dirty reads allowed |
| Read Committed | 1 | Default level |
| Repeatable Read | 2 | Phantom reads possible |
| Serializable | 3 | Full isolation |

### Begin Transaction Request

```json
{
  "isolation_level": 1,
  "read_only": false,
  "deferrable": false
}
```

### Transaction Result

```json
{
  "success": true,
  "transaction_id": "tx_abc123",
  "message": "transaction started"
}
```

---

## Metadata Queries

### GetTables Request

```json
{
  "catalog": "",
  "schema": "public",
  "table_name": "%",
  "table_types": ["TABLE", "VIEW"]
}
```

### GetColumns Request

```json
{
  "catalog": "",
  "schema": "public",
  "table_name": "users",
  "column_name": "%"
}
```

### Column Metadata Response

Each column includes:

| Field | Description |
|-------|-------------|
| index | Column ordinal (0-based) |
| name | Column name |
| type | SQL type name |
| sql_type | JDBC/ODBC type code |
| nullable | Whether NULL is allowed |
| precision | Numeric precision |
| scale | Numeric scale |
| auto_increment | Auto-generated values |

### GetPrimaryKeys Request

```json
{
  "catalog": "",
  "schema": "public",
  "table_name": "users"
}
```

### Primary Key Response

```json
{
  "success": true,
  "rows": [
    {"column_name": "id", "key_seq": 1, "pk_name": "users_pkey"}
  ]
}
```

### GetForeignKeys Request

```json
{
  "catalog": "",
  "schema": "public",
  "table_name": "orders"
}
```

### Foreign Key Response

```json
{
  "success": true,
  "rows": [
    {
      "pk_table": "users",
      "pk_column": "id",
      "fk_table": "orders",
      "fk_column": "user_id",
      "key_seq": 1,
      "fk_name": "orders_user_id_fkey",
      "update_rule": 1,
      "delete_rule": 1
    }
  ]
}
```

### GetIndexes Request

```json
{
  "catalog": "",
  "schema": "public",
  "table_name": "users",
  "unique": false
}
```

### Index Response

```json
{
  "success": true,
  "rows": [
    {
      "index_name": "users_email_idx",
      "column_name": "email",
      "ordinal_position": 1,
      "non_unique": false,
      "type": 3
    }
  ]
}
```

### GetTypeInfo Request

Request type information for all supported SQL types:

```json
{}
```

### GetTypeInfo Response

Returns ODBC/JDBC-compatible type information:

```json
{
  "success": true,
  "rows": [
    {
      "type_name": "INTEGER",
      "data_type": 4,
      "precision": 10,
      "literal_prefix": null,
      "literal_suffix": null,
      "create_params": null,
      "nullable": 1,
      "case_sensitive": false,
      "searchable": 3,
      "unsigned_attribute": false,
      "fixed_prec_scale": false,
      "auto_unique_value": false,
      "local_type_name": "INTEGER",
      "minimum_scale": 0,
      "maximum_scale": 0,
      "sql_data_type": 4,
      "sql_datetime_sub": null,
      "num_prec_radix": 10
    }
  ]
}
```

### Supported SQL Types

| Type Name | ODBC Code | Description |
|-----------|-----------|-------------|
| INTEGER | 4 | 32-bit signed integer |
| BIGINT | -5 | 64-bit signed integer |
| SMALLINT | 5 | 16-bit signed integer |
| REAL | 7 | Single-precision float |
| DOUBLE | 8 | Double-precision float |
| VARCHAR | 12 | Variable-length string |
| TEXT | -1 | Long text |
| BOOLEAN | -7 | Boolean value |
| DATE | 91 | Date value |
| TIME | 92 | Time value |
| TIMESTAMP | 93 | Date and time |
| BLOB | -4 | Binary large object |

### Metadata Provider Architecture

The FlyDB server implements metadata queries through a `MetadataProvider` interface that bridges the binary protocol handler with the SQL catalog:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Binary Protocol Handler                       │
│                                                                 │
│  Receives: MsgGetTables, MsgGetColumns, MsgGetPrimaryKeys, etc. │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     MetadataProvider                             │
│                                                                 │
│  GetTables()      - Returns table/view metadata                 │
│  GetColumns()     - Returns column definitions with types       │
│  GetPrimaryKeys() - Returns primary key constraints             │
│  GetForeignKeys() - Returns foreign key relationships           │
│  GetIndexes()     - Returns index information                   │
│  GetTypeInfo()    - Returns supported SQL types                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      SQL Catalog                                 │
│                                                                 │
│  Tables map[string]*TableSchema                                 │
│  Views  map[string]*ViewDefinition                              │
└─────────────────────────────────────────────────────────────────┘
```

**Design Rationale:**

1. **Separation of Concerns**: The MetadataProvider abstracts catalog access from protocol handling
2. **ODBC/JDBC Compatibility**: Response formats match standard driver expectations
3. **Pattern Matching**: Table and column patterns support SQL wildcards (%)
4. **Graceful Degradation**: Returns empty results when catalog is unavailable

---

## Session Management

### Session Options

| Option | Type | Description |
|--------|------|-------------|
| auto_commit | bool | Auto-commit mode |
| isolation_level | int | Transaction isolation |
| read_only | bool | Read-only mode |
| query_timeout | int | Query timeout (seconds) |
| fetch_size | int | Default fetch size |

### Set Option Request

```json
{
  "option": "auto_commit",
  "value": false
}
```

### Get Server Info Response

```json
{
  "success": true,
  "server_version": "1.0.0",
  "protocol_version": 1,
  "database": "flydb",
  "capabilities": ["sql", "prepared_statements", "transactions", "cursors"],
  "max_statement_length": 16777216
}
```

---

## Database Operations

FlyDB supports multiple databases. Drivers can switch databases and enumerate available databases.

### Use Database Request

Switch the connection to a different database:

```json
{
  "database": "mydb"
}
```

### Use Database Response

```json
{
  "success": true,
  "database": "mydb",
  "message": "database changed"
}
```

### Get Databases Request

List available databases (optional pattern with % wildcards):

```json
{
  "pattern": "prod%"
}
```

### Get Databases Response

```json
{
  "success": true,
  "databases": ["default", "production", "prod_backup"]
}
```

### Authentication with Database

Specify the initial database during authentication:

```json
{
  "username": "admin",
  "password": "secret",
  "database": "mydb"
}
```

If the database doesn't exist, authentication fails with an error.

---

## Code Examples

### Go: Connect and Authenticate

```go
package main

import (
    "bufio"
    "encoding/binary"
    "net"
)

const (
    MagicByte = 0xFD
    Version   = 0x01
    MsgAuth   = 0x08
)

func connect(host string, user, pass string) (net.Conn, error) {
    conn, err := net.Dial("tcp", host+":8889")
    if err != nil {
        return nil, err
    }

    // Build auth payload
    payload := make([]byte, 0, 8+len(user)+len(pass))
    payload = binary.BigEndian.AppendUint32(payload, uint32(len(user)))
    payload = append(payload, []byte(user)...)
    payload = binary.BigEndian.AppendUint32(payload, uint32(len(pass)))
    payload = append(payload, []byte(pass)...)

    // Write header + payload
    header := []byte{MagicByte, Version, MsgAuth, 0x00}
    header = binary.BigEndian.AppendUint32(header, uint32(len(payload)))
    conn.Write(header)
    conn.Write(payload)

    // Read response...
    return conn, nil
}
```

### Python: Execute Query

```python
import socket
import struct
import json

MAGIC = 0xFD
VERSION = 0x01
MSG_QUERY = 0x01

def send_query(sock, sql):
    # Encode query as JSON
    payload = json.dumps({"query": sql}).encode('utf-8')

    # Build header
    header = struct.pack('>BBBBI', MAGIC, VERSION, MSG_QUERY, 0, len(payload))

    # Send
    sock.sendall(header + payload)

    # Read response header
    resp_header = sock.recv(8)
    magic, ver, msg_type, flags, length = struct.unpack('>BBBBI', resp_header)

    # Read payload
    resp_payload = sock.recv(length)
    return json.loads(resp_payload)
```

### Java: JDBC-Style Connection

```java
public class FlyDBConnection {
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;

    public void connect(String host, int port, String user, String pass)
            throws IOException {
        socket = new Socket(host, port);
        in = new DataInputStream(socket.getInputStream());
        out = new DataOutputStream(socket.getOutputStream());

        // Send auth message
        byte[] userBytes = user.getBytes(StandardCharsets.UTF_8);
        byte[] passBytes = pass.getBytes(StandardCharsets.UTF_8);

        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(payload);
        dos.writeInt(userBytes.length);
        dos.write(userBytes);
        dos.writeInt(passBytes.length);
        dos.write(passBytes);

        byte[] payloadBytes = payload.toByteArray();

        // Write header
        out.writeByte(0xFD);  // Magic
        out.writeByte(0x01);  // Version
        out.writeByte(0x08);  // MsgAuth
        out.writeByte(0x00);  // Flags
        out.writeInt(payloadBytes.length);
        out.write(payloadBytes);
        out.flush();

        // Read auth result...
    }
}
```

---

## Error Handling

### Error Response Format

```json
{
  "code": 500,
  "message": "table not found: users",
  "sqlstate": "42P01"
}
```

### Common Error Codes

| Code | Description |
|------|-------------|
| 400 | Bad request (malformed message) |
| 401 | Authentication required |
| 403 | Permission denied |
| 404 | Not found (table, cursor, etc.) |
| 500 | Internal server error |
| 501 | Feature not supported |

### SQLSTATE Codes

FlyDB uses standard SQLSTATE codes for error classification:

| SQLSTATE | Class | Description |
|----------|-------|-------------|
| 00000 | Success | Successful completion |
| 08000 | Connection | Connection exception |
| 08P01 | Connection | Protocol violation |
| 22000 | Data | Data exception |
| 23000 | Integrity | Integrity constraint violation |
| 23505 | Integrity | Unique violation |
| 25000 | Transaction | Invalid transaction state |
| 28000 | Authorization | Invalid authorization specification |
| 3D000 | Catalog | Invalid catalog name (database not found) |
| 40001 | Transaction | Serialization failure |
| 40P01 | Transaction | Deadlock detected |
| 42000 | Syntax | Syntax error or access rule violation |
| 42501 | Syntax | Insufficient privilege |
| 42601 | Syntax | Syntax error |
| 42703 | Syntax | Undefined column |
| 42804 | Syntax | Datatype mismatch |
| 42P01 | Syntax | Undefined table |
| HY000 | General | General error |

---

## Connection Pooling

Drivers should implement connection pooling for production use:

### Recommended Pool Settings

| Setting | Default | Description |
|---------|---------|-------------|
| MinConnections | 1 | Minimum idle connections |
| MaxConnections | 10 | Maximum total connections |
| MaxIdleTime | 5 min | Close idle connections after this time |
| MaxLifetime | 1 hour | Maximum connection lifetime |
| AcquireTimeout | 30 sec | Timeout waiting for connection |

### Connection Validation

Before returning a connection from the pool:

1. Check if connection is still open
2. Send a PING message and wait for PONG
3. Verify connection lifetime hasn't exceeded MaxLifetime
4. Reset session state if needed (auto-commit, isolation level)

### Connection String Formats

**ODBC Format:**
```
Driver={FlyDB};Server=localhost;Port=8889;Database=mydb;Uid=user;Pwd=pass
```

**JDBC Format:**
```
jdbc:flydb://localhost:8889/mydb?user=user&password=pass
```

---

## See Also

- [Architecture Overview](architecture.md) - System design
- [API Reference](api.md) - SQL syntax and commands
- [Implementation Details](implementation.md) - Technical deep-dives
