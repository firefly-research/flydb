<div align="center">
  <img src="docs/assets/logo.png" alt="FlyDB Logo" width="600">

  <h1>FlyDB</h1>

  <p><strong>A lightweight, distributed SQL database engine written in Go</strong></p>

  <p>
    <a href="https://github.com/firefly-oss/flydb/releases"><img src="https://img.shields.io/badge/version-01.26.7-blue.svg" alt="Version"></a>
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
| Version    | 01.26.7                                      |
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
| Queries | JOIN (INNER, LEFT, RIGHT, FULL), UNION, INTERSECT, EXCEPT, Subqueries, DISTINCT |
| Clauses | WHERE, ORDER BY, GROUP BY, HAVING, LIMIT, OFFSET |
| Functions | COUNT, SUM, AVG, MIN, MAX, String, Numeric, Date/Time functions |
| Advanced | Transactions (with Savepoints), Prepared Statements, Stored Procedures, Views, Triggers |
| Databases | CREATE DATABASE, DROP DATABASE, USE, Multi-database support with isolation |

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
- **AES-256-GCM encryption at rest (enabled by default)**
- **Multi-database support** with complete isolation between databases
- Per-database encoding, locale, and collation settings

### Security

- **Data-at-rest encryption enabled by default** (AES-256-GCM)
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

### Driver Development

FlyDB provides a complete binary wire protocol for developing external database drivers:

| Feature | Description |
|---------|-------------|
| JDBC/ODBC Support | Full protocol support for standard database drivers |
| Server-Side Cursors | Scrollable cursors for large result sets |
| Metadata Queries | GetTables, GetColumns, GetTypeInfo for schema discovery |
| Transaction Control | BEGIN, COMMIT, ROLLBACK with isolation levels |
| Session Management | Connection options, auto-commit, server info |

See [Driver Development Guide](docs/driver-development.md) for complete protocol specification.

### Real-Time

- WATCH command for table change notifications
- Event streaming for data changes

---

## Installation

### Prerequisites

- Go 1.21 or later (only required for building from source)
- Linux or macOS

### Quick Install (Recommended)

Install FlyDB with a single command:

```bash
curl -sSL https://get.flydb.dev | bash
```

This downloads pre-built binaries and installs them to `~/.local/bin` (or `/usr/local/bin` with sudo).

#### Quick Install Options

```bash
# Install with custom options
curl -sSL https://get.flydb.dev | bash -s -- --prefix ~/.local --yes

# Install specific version
curl -sSL https://get.flydb.dev | bash -s -- --version 01.26.0 --yes

# Install without system service
curl -sSL https://get.flydb.dev | bash -s -- --no-service --yes
```

### Install from Source

If you prefer to build from source or want to contribute:

```bash
# Clone the repository
git clone https://github.com/firefly-oss/flydb.git
cd flydb

# Interactive installation (recommended for first-time users)
./install.sh

# Non-interactive installation with defaults
./install.sh --yes

# Force build from source (even if binaries are available)
./install.sh --from-source --yes
```

#### Installation Options

| Option | Description |
|--------|-------------|
| `--prefix <path>` | Installation prefix (default: `~/.local` or `/usr/local` with sudo) |
| `--version <ver>` | Install specific version |
| `--from-source` | Force building from source (requires Go 1.21+) |
| `--from-binary` | Force downloading pre-built binaries |
| `--yes` | Non-interactive mode, accept all defaults |
| `--no-service` | Skip system service installation |
| `--no-config` | Skip configuration file creation |
| `--help` | Show all available options |

#### Examples

```bash
# Install to custom location
./install.sh --prefix /opt/flydb

# System-wide installation (requires sudo)
sudo ./install.sh --prefix /usr/local

# Download binaries instead of building (in source directory)
./install.sh --from-binary --yes

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
- FlyDB binaries (`flydb`, `flydb-shell`, `fsql`)
- System service files (systemd on Linux, launchd on macOS)
- Configuration directories (use `--no-config` to preserve)
- Data directories (only with `--remove-data` flag)

### Manual Build

If you prefer to build from source:

```bash
git clone https://github.com/firefly-oss/flydb.git
cd flydb
go build -o flydb ./cmd/flydb
go build -o flydb-shell ./cmd/flydb-shell
ln -s flydb-shell fsql  # Optional: create fsql symlink
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
fsql
# or: ./flydb-shell
```

The CLI provides an interactive REPL with tab completion, command history, and helpful local commands. Type `\h` for help.

### 4. Authenticate

The CLI supports secure password entry with masking:

```
flydb[not authenticated]> AUTH
Username: admin
Password: ********
AUTH OK (admin)
```

Or authenticate with username on the command line:

```
flydb[not authenticated]> AUTH admin
Password: ********
AUTH OK (admin)
```

### 5. Create a Table

```sql
flydb:default> CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE);
CREATE TABLE OK
```

### 6. Insert Data

```sql
flydb:default> INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
INSERT 1
flydb:default> INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');
INSERT 1
```

### 7. Query Data

```sql
flydb:default> SELECT * FROM users;
+----+-------+-------------------+
| id | name  | email             |
+----+-------+-------------------+
| 1  | Alice | alice@example.com |
| 2  | Bob   | bob@example.com   |
+----+-------+-------------------+
(2 rows)

flydb:default> SELECT name FROM users WHERE id = 1;
+-------+
| name  |
+-------+
| Alice |
+-------+
(1 row)
```

### 8. Update and Delete

```sql
flydb:default> UPDATE users SET email = 'alice@newdomain.com' WHERE id = 1;
UPDATE 1

flydb:default> DELETE FROM users WHERE id = 2;
DELETE 1
```

### 9. Working with Multiple Databases

FlyDB supports multiple isolated databases within a single server instance:

```sql
-- Create a new database with custom settings
flydb:default> CREATE DATABASE analytics WITH ENCODING 'UTF8' LOCALE 'en_US' COLLATION 'unicode';
CREATE DATABASE OK

-- Switch to the new database
flydb:default> USE analytics
USE analytics OK

-- Create tables in the new database (isolated from default)
flydb:analytics> CREATE TABLE events (id INT PRIMARY KEY, name TEXT, timestamp TIMESTAMP);
CREATE TABLE OK

-- List all databases
flydb:analytics> INSPECT DATABASES;
+------------+-------+----------+--------+-----------+--------+------+
| name       | owner | encoding | locale | collation | tables | size |
+------------+-------+----------+--------+-----------+--------+------+
| default    | admin | UTF8     | en_US  | default   | 1      | 4 KB |
| analytics  | admin | UTF8     | en_US  | unicode   | 1      | 2 KB |
+------------+-------+----------+--------+-----------+--------+------+
(2 rows)

-- Switch back to default database
flydb:analytics> USE default
USE default OK
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

### Configuration File

FlyDB supports TOML configuration files for easier deployment and management. The configuration file is automatically loaded from these locations (in order of precedence):

1. Path specified by `--config` flag
2. Path specified by `FLYDB_CONFIG_FILE` environment variable
3. `/etc/flydb/flydb.conf` (system-wide)
4. `~/.config/flydb/flydb.conf` (user-specific)
5. `./flydb.conf` (current directory)

Example configuration file:

```toml
# Server role: standalone, master, or slave
role = "standalone"

# Network ports
port = 8888
binary_port = 8889
replication_port = 9999

# Storage - Multi-database mode (always enabled)
# Each database is stored in a separate directory under data_dir
data_dir = "/var/lib/flydb"

# Default settings for new databases
default_database = "default"
default_encoding = "UTF8"
default_locale = "en_US"
default_collation = "default"

# Data-at-rest encryption (ENABLED BY DEFAULT)
# When enabled, you MUST set FLYDB_ENCRYPTION_PASSPHRASE environment variable
# To disable encryption, set encryption_enabled = false
encryption_enabled = true

# Logging
log_level = "info"
log_json = false

# Master address for slave mode
# master_addr = "localhost:9999"
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
| `FLYDB_PORT` | Server port for text protocol |
| `FLYDB_BINARY_PORT` | Server port for binary protocol |
| `FLYDB_REPL_PORT` | Replication port |
| `FLYDB_ROLE` | Server role (standalone, master, slave) |
| `FLYDB_MASTER_ADDR` | Master address for slave mode |
| `FLYDB_DB_PATH` | Path to database file |
| `FLYDB_DATA_DIR` | Directory for multi-database storage |
| `FLYDB_DEFAULT_ENCODING` | Default encoding for new databases (UTF8, LATIN1, ASCII, UTF16) |
| `FLYDB_DEFAULT_LOCALE` | Default locale for new databases (e.g., en_US, de_DE) |
| `FLYDB_DEFAULT_COLLATION` | Default collation for new databases (default, binary, nocase, unicode) |
| `FLYDB_ENCRYPTION_ENABLED` | Enable data-at-rest encryption (true/false, default: **true**) |
| `FLYDB_ENCRYPTION_PASSPHRASE` | **Required** when encryption enabled - passphrase for key derivation |
| `FLYDB_LOG_LEVEL` | Log level (debug, info, warn, error) |
| `FLYDB_LOG_JSON` | Enable JSON logging (true/false) |
| `FLYDB_ADMIN_PASSWORD` | Initial admin password (first-time setup) |
| `FLYDB_CONFIG_FILE` | Path to configuration file |

### Server Options

| Flag | Default | Description |
|------|---------|-------------|
| `-port` | `8888` | Text protocol port |
| `-binary-port` | `8889` | Binary protocol port |
| `-repl-port` | `9999` | Replication port (master only) |
| `-data-dir` | `/var/lib/flydb` | Directory for database storage |
| `-role` | `standalone` | Server role: `standalone`, `master`, or `slave` |
| `-master` | - | Leader address for followers |
| `-log-level` | `info` | Log level: debug, info, warn, error |
| `-log-json` | `false` | JSON log output |
| `-config` | - | Path to configuration file |

### Client Options

| Flag | Default | Description |
|------|---------|-------------|
| `-h` | `localhost` | Server hostname |
| `-p` | `8889` | Server port |
| `-d` | `default` | Database to connect to |
| `-v` | `false` | Verbose mode (show query timing) |
| `-f` | `table` | Output format: table, json, plain |

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

Leader with replication:

```bash
./flydb -port 8888 -repl-port 9999 -role master -data-dir /var/lib/flydb
```

Follower:

```bash
./flydb -port 8889 -role slave -master localhost:9999 -data-dir /var/lib/flydb-replica
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
./flydb -port 8888 -role standalone -db production.fdb
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

#### Data-at-Rest Encryption
FlyDB encrypts all data stored on disk using AES-256-GCM. When encryption is enabled, you **must** provide a passphrase. There are two ways to set the passphrase:

**Option 1: Environment Variable (Recommended for Production)**

```bash
export FLYDB_ENCRYPTION_PASSPHRASE="your-secure-passphrase"
./flydb -role standalone
```

**Option 2: Interactive Wizard**

When running FlyDB without arguments, the setup wizard will prompt you to enter a passphrase:

```bash
./flydb
# Wizard will prompt:
#   Step 4: Data-at-Rest Encryption
#   Enable encryption? (y/n) [y]: y
#   Encryption passphrase: ********
```

The wizard automatically detects if `FLYDB_ENCRYPTION_PASSPHRASE` is already set and uses it.

---

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/architecture.md) | System design, component diagrams, data flow |
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

```bash
echo "PING" | nc localhost 8888
# Response: PONG
```

### Project Structure

```
flydb/
    cmd/
        flydb/              Server entry point
        flydb-shell/        CLI client entry point (fsql command)
    internal/
        auth/               Authentication and authorization
        banner/             Startup banner display
        cache/              Query caching
        config/             Configuration management
        errors/             Error handling
        logging/            Structured logging
        pool/               Connection pooling
        protocol/           Binary wire protocol (JDBC/ODBC compatible)
        sdk/                SDK types for driver development
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