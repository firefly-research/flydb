# FlyDB Systemd Service Files

This directory contains systemd service unit files for running FlyDB as a system service on Linux.

## Service Files

### `flydb.service`
Standalone FlyDB instance service file.

**Installation:**
```bash
sudo cp flydb.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable flydb
sudo systemctl start flydb
```

**Status:**
```bash
sudo systemctl status flydb
journalctl -u flydb -f
```

### `flydb-cluster@.service`
Template service file for running multiple FlyDB cluster nodes on the same machine.

**Installation:**
```bash
sudo cp flydb-cluster@.service /etc/systemd/system/
sudo systemctl daemon-reload
```

**Usage:**
```bash
# Start node1
sudo systemctl enable flydb-cluster@node1
sudo systemctl start flydb-cluster@node1

# Start node2
sudo systemctl enable flydb-cluster@node2
sudo systemctl start flydb-cluster@node2

# Start node3
sudo systemctl enable flydb-cluster@node3
sudo systemctl start flydb-cluster@node3
```

**Status:**
```bash
sudo systemctl status flydb-cluster@node1
journalctl -u flydb-cluster@node1 -f
```

## Configuration

### Standalone Mode
- Config: `/etc/flydb/flydb.toml`
- Data: `/var/lib/flydb/`
- User: `flydb`
- Group: `flydb`

### Cluster Mode
- Config: `/etc/flydb/<node-id>/flydb.toml`
- Data: `/var/lib/flydb/<node-id>/`
- User: `flydb`
- Group: `flydb`

## Prerequisites

1. **Create flydb user and group:**
```bash
sudo useradd -r -s /bin/false flydb
```

2. **Create directories:**
```bash
# Standalone
sudo mkdir -p /var/lib/flydb /etc/flydb
sudo chown flydb:flydb /var/lib/flydb
sudo chmod 755 /etc/flydb

# Cluster (for each node)
sudo mkdir -p /var/lib/flydb/node1 /etc/flydb/node1
sudo chown flydb:flydb /var/lib/flydb/node1
sudo chmod 755 /etc/flydb/node1
```

3. **Install FlyDB binary:**
```bash
sudo cp flydb /usr/local/bin/
sudo chmod +x /usr/local/bin/flydb
```

## Service Discovery

For cluster mode with automatic node discovery, ensure the Avahi daemon is running:

```bash
sudo systemctl enable avahi-daemon
sudo systemctl start avahi-daemon
```

The cluster service files include dependencies on `avahi-daemon.service` to ensure mDNS discovery is available.

## Security

The service files include security hardening:
- `NoNewPrivileges=true` - Prevents privilege escalation
- `PrivateTmp=true` - Isolated /tmp directory
- `ProtectSystem=strict` - Read-only system directories
- `ProtectHome=true` - Inaccessible home directories
- `ReadWritePaths` - Explicit write permissions for data directory
- `ReadOnlyPaths` - Explicit read permissions for config directory

## Resource Limits

Default limits:
- File descriptors: 65536
- Processes: 4096

Adjust these in the service file if needed for your workload.

## Troubleshooting

**Service won't start:**
```bash
# Check service status
sudo systemctl status flydb

# View logs
journalctl -u flydb -n 50

# Check configuration
flydb --config /etc/flydb/flydb.toml --validate
```

**Permission errors:**
```bash
# Verify ownership
ls -la /var/lib/flydb
ls -la /etc/flydb

# Fix permissions
sudo chown -R flydb:flydb /var/lib/flydb
```

**Port conflicts:**
```bash
# Check if ports are in use
sudo netstat -tlnp | grep -E ':(6380|7946|7947|8080)'

# Or with ss
sudo ss -tlnp | grep -E ':(6380|7946|7947|8080)'
```

## Automatic Installation

The `install.sh` script can automatically install and configure systemd services:

```bash
./install.sh --install-service
```

For cluster mode:
```bash
./install.sh --cluster --node-id node1
```

