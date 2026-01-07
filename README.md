<div align="center">
  <img src="docs/assets/logo.png" alt="FlyDB Logo" width="600">
  
  <h1>FlyDB</h1>
  
  <p><strong>A lightweight, distributed SQL database engine written in Go</strong></p>
  
  <p>
    <a href="https://github.com/firefly-oss/flydb/releases"><img src="https://img.shields.io/badge/version-01.26.1-blue.svg" alt="Version"></a>
    <a href="https://github.com/firefly-oss/flydb/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-green.svg" alt="License"></a>
    <a href="https://go.dev/"><img src="https://img.shields.io/badge/Go-1.21%2B-00ADD8?logo=go" alt="Go Version"></a>
    <a href="https://github.com/firefly-oss/flydb"><img src="https://img.shields.io/badge/platform-Linux%20%7C%20macOS-lightgrey.svg" alt="Platform"></a>
    <a href="https://github.com/firefly-oss/flydb/actions"><img src="https://img.shields.io/badge/build-passing-brightgreen.svg" alt="Build Status"></a>
    <a href="https://github.com/firefly-oss/flydb/issues"><img src="https://img.shields.io/github/issues/firefly-oss/flydb.svg" alt="Issues"></a>
    <a href="https://github.com/firefly-oss/flydb/stargazers"><img src="https://img.shields.io/github/stars/firefly-oss/flydb.svg?style=social" alt="Stars"></a>
  </p>
</div>

---

FlyDB is a lightweight, distributed SQL database engine written in Go. It provides ACID-compliant storage through a Write-Ahead Log (WAL), supports SQL queries with JOIN operations, and includes built-in security features such as user authentication and Row-Level Security (RLS).

| | |
|------------|----------------------------------------------|
| Version    | 01.26.1                                      |
| License    | Apache License 2.0                           |
| Copyright  | 2026 Firefly Software Solutions Inc.         |

---

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Documentation](#documentation)
- [Development](#development)
- [License](#license)

---

## Features

### SQL Support

| Category | Features |
|----------|----------|
| DDL | CREATE TABLE, DROP TABLE, ALTER TABLE, CREATE INDEX, CREATE VIEW |
| DML | SELECT, INSERT, UPDATE, DELETE |
| Queries | JOIN (INNER, LEFT, RIGHT, FULL), UNION, Subqueries, DISTINCT |
| Clauses | WHERE, ORDER BY, GROUP BY, HAVING, LIMIT, OFFSET |
| Functions | COUNT, SUM, AVG, MIN, MAX |
| Advanced | Transactions, Prepared Statements, Stored Procedures, Views |

### Data Types

| Category | Types |
|----------|-------|
| Numeric | INT, BIGINT, SERIAL, FLOAT, DECIMAL |
| String | TEXT, VARCHAR |
| Boolean | BOOLEAN |
| Date/Time | TIMESTAMP, DATE, TIME |
| Binary | BLOB |
| Structured | UUID, JSONB |

### Constraints

PRIMARY KEY, FOREIGN KEY, NOT NULL, UNIQUE, AUTO_INCREMENT, DEFAULT, CHECK

### Storage and Durability

- Write-Ahead Logging (WAL) for crash recovery
- In-memory storage with automatic persistence
- B-Tree indexes for O(log N) lookups
- Optional AES-256-GCM encryption at rest

### Security

- Secure admin password setup on first run (generated or user-specified)
- User authentication with bcrypt password hashing
- Table-level access control (GRANT/REVOKE)
- Row-Level Security (RLS) with predicate filtering
- TLS support for encrypted connections
- Environment variable configuration for automated deployments

### Distributed Features

- Leader-Follower replication
- Automatic failover with leader election (Bully algorithm)
- Binary replication protocol

### Performance

- Connection pooling
- Query caching with LRU eviction and TTL
- Binary wire protocol for efficient communication

### Real-Time

- WATCH command for table change notifications
- Event streaming for data changes

---

## Installation

### Prerequisites

- Go 1.21 or later (for building from source)
- Linux or macOS

### Using the Installation Script (Recommended)

The installation script automatically downloads pre-built binaries, sets up the system service, and configures FlyDB for your environment.

```bash
# Clone the repository
git clone https://github.com/firefly-oss/flydb.git
cd flydb

# Interactive installation (recommended for first-time users)
./install.sh

# Non-interactive installation with defaults
./install.sh --yes
```

#### Installation Options

| Option | Description |
|--------|-------------|
| `--prefix <path>` | Installation prefix (default: `~/.local` or `/usr/local` with sudo) |
| `--yes` | Non-interactive mode, accept all defaults |
| `--no-service` | Skip system service installation |
| `--no-config` | Skip configuration file creation |
| `--dry-run` | Preview installation without making changes |
| `--help` | Show all available options |

#### Examples

```bash
# Install to custom location
./install.sh --prefix /opt/flydb

# System-wide installation (requires sudo)
sudo ./install.sh --prefix /usr/local

# Preview what would be installed
./install.sh --dry-run

# Automated/CI installation
./install.sh --yes --no-service --prefix ~/.local
```

### Uninstallation

To remove FlyDB from your system:

```bash
# Interactive uninstallation
./uninstall.sh

# Preview what would be removed
./uninstall.sh --dry-run

# Non-interactive uninstallation
./uninstall.sh --yes

# Also remove data directories (WARNING: deletes databases!)
./uninstall.sh --remove-data --yes
```

The uninstall script automatically detects and removes:
- FlyDB binaries (`flydb`, `fly-cli`)
- System service files (systemd on Linux, launchd on macOS)
- Configuration directories (use `--no-config` to preserve)
- Data directories (only with `--remove-data` flag)

### Manual Build

If you prefer to build from source:

```bash
git clone https://github.com/firefly-oss/flydb.git
cd flydb
go build -o flydb ./cmd/flydb
go build -o fly-cli ./cmd/fly-cli
```

---

## Quick Start

### 1. Start the Server

#### Interactive Wizard (Recommended for First-Time Setup)

Run FlyDB without any arguments to launch the interactive configuration wizard:

```bash
./flydb
```

The wizard guides you through:
- Selecting the operative mode (Standalone, Master, or Slave)
- Configuring network ports
- Setting the database file path
- Configuring logging options

#### Command-Line Mode

For scripted or automated deployments, use command-line flags:

```bash
./flydb -port 8888 -role standalone
```

### 2. First-Time Setup (Admin Password)

On first startup with a new database, FlyDB will automatically create an admin user:

**Option A: Generated Password (Default)**

If no password is specified, FlyDB generates a secure random password and displays it:

```
═══════════════════════════════════════════════════════════════
  FIRST-TIME SETUP: Admin credentials generated
═══════════════════════════════════════════════════════════════

  Username: admin
  Password: xK7#mP2$nQ9@wL4

  IMPORTANT: Save this password securely!
  This password will NOT be shown again.
═══════════════════════════════════════════════════════════════
```

**Option B: Environment Variable**

Set the `FLYDB_ADMIN_PASSWORD` environment variable before starting:

```bash
export FLYDB_ADMIN_PASSWORD="your-secure-password"
./flydb -port 8888 -role standalone
```

### 3. Connect with the CLI

```bash
./fly-cli
```

### 4. Authenticate

```
flydb> AUTH admin <your-admin-password>
AUTH OK (admin)
```

### 5. Create a Table

```sql
flydb> CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)
CREATE TABLE OK
```

### 6. Insert Data

```sql
flydb> INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')
INSERT 1
flydb> INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')
INSERT 1
```

### 7. Query Data

```sql
flydb> SELECT * FROM users
+----+-------+-------------------+
| 1  | Alice | alice@example.com |
| 2  | Bob   | bob@example.com   |
+----+-------+-------------------+
(2 rows)

flydb> SELECT name FROM users WHERE id = 1
+-------+
| Alice |
+-------+
(1 row)
```

### 8. Update and Delete

```sql
flydb> UPDATE users SET email = 'alice@newdomain.com' WHERE id = 1
UPDATE 1

flydb> DELETE FROM users WHERE id = 2
DELETE 1
```

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
  2) Master      - Leader node (accepts writes, replicates to slaves)
  3) Slave       - Follower node (receives replication from master)
```

The wizard prompts for all relevant settings based on your selected mode and displays a summary before starting.

### Operative Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `standalone` | Single server, no replication | Development, small deployments |
| `master` | Leader node with replication | Production leader |
| `slave` | Follower node | Read replicas, failover |

### Server Options

| Flag | Default | Description |
|------|---------|-------------|
| `-port` | `8888` | Text protocol port |
| `-binary-port` | `8889` | Binary protocol port |
| `-repl-port` | `9999` | Replication port (master only) |
| `-db` | `flydb.wal` | WAL file path |
| `-role` | `master` | Server role: `standalone`, `master`, or `slave` |
| `-master` | - | Leader address for followers |
| `-log-level` | `info` | Log level: debug, info, warn, error |
| `-log-json` | `false` | JSON log output |

### Client Options

| Flag | Default | Description |
|------|---------|-------------|
| `-h` | `localhost` | Server hostname |
| `-p` | `8889` | Server port |

### Examples

Standalone server (development):

```bash
./flydb -role standalone -db /var/lib/flydb/data.wal
```

Leader with replication:

```bash
./flydb -port 8888 -repl-port 9999 -role master -db leader.wal
```

Follower:

```bash
./flydb -port 8889 -role slave -master localhost:9999 -db follower.wal
```

### Security Configuration

#### Admin Password Setup

On first startup with a new database, FlyDB requires an admin password to be set. There are three ways to configure this:

| Method | Use Case | Description |
|--------|----------|-------------|
| Environment Variable | Automated/CI deployments | Set `FLYDB_ADMIN_PASSWORD` before starting |
| Generated Password | Interactive use | FlyDB generates and displays a secure password |
| Interactive Wizard | First-time setup | Wizard prompts for password when run without args |

**Environment Variable (Recommended for Production)**

```bash
export FLYDB_ADMIN_PASSWORD="your-secure-password-here"
./flydb -port 8888 -role standalone -db production.wal
```

**Generated Password**

If no password is provided, FlyDB generates a 16-character cryptographically secure password containing letters, numbers, and special characters. This password is displayed once at startup and must be saved securely.

#### Changing the Admin Password

After initial setup, you can change the admin password using SQL:

```sql
ALTER USER admin IDENTIFIED BY 'new-secure-password'
```

#### Security Best Practices

1. **Never use default or weak passwords** - Always use strong, unique passwords
2. **Save generated passwords securely** - Use a password manager
3. **Use environment variables in production** - Avoid hardcoding passwords in scripts
4. **Enable TLS** - Use encrypted connections in production
5. **Rotate passwords regularly** - Change admin password periodically

---

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/architecture.md) | System design, component diagrams, data flow |
| [Implementation](docs/implementation.md) | WAL, B-Tree, SQL processing, transactions |
| [Design Decisions](docs/design-decisions.md) | Rationale and trade-offs |
| [API Reference](docs/api.md) | SQL syntax, protocol commands, configuration |
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

```bash
echo "PING" | nc localhost 8888
# Response: PONG
```

### Project Structure

```
flydb/
    cmd/
        flydb/              Server entry point
        fly-cli/            CLI client entry point
    internal/
        auth/               Authentication and authorization
        banner/             Startup banner display
        cache/              Query caching
        errors/             Error handling
        logging/            Structured logging
        pool/               Connection pooling
        protocol/           Binary wire protocol
        server/             TCP server and replication
        sql/                Lexer, parser, executor, catalog
        storage/            KVStore, WAL, B-Tree, encryption
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