# FlyDB Cluster Setup Guide

This guide covers setting up a FlyDB cluster with automatic node discovery and production-ready configuration.

## Table of Contents

- [Overview](#overview)
- [Service Discovery](#service-discovery)
- [Installation](#installation)
- [Cluster Setup](#cluster-setup)
- [Systemd Services](#systemd-services)
- [Troubleshooting](#troubleshooting)

## Overview

FlyDB supports horizontal scaling through clustering with:
- **Automatic Node Discovery** via mDNS (Bonjour/Avahi)
- **Raft Consensus** for leader election and strong consistency
- **Gossip Protocol** for cluster membership and failure detection
- **Partition-based Sharding** for data distribution
- **Multiple Routing Strategies** (key-based, round-robin, least-loaded, locality-aware, hybrid)

## Service Discovery

FlyDB includes built-in mDNS-based service discovery for zero-configuration cluster setup.

### Discovery Tool

The `flydb-discover` tool finds FlyDB nodes on your local network:

```bash
# Discover nodes with default 5-second timeout
flydb-discover

# Increase timeout for slower networks
flydb-discover --timeout 10

# Get JSON output for automation
flydb-discover --json

# Get just addresses for scripting
flydb-discover --quiet

# Use in scripts
PEERS=$(flydb-discover --quiet)
```

### How It Works

1. **mDNS Advertisement**: Each FlyDB node advertises itself as `_flydb._tcp.local`
2. **TXT Records**: Nodes publish metadata (node_id, cluster_id, ports, version)
3. **Discovery**: New nodes scan the network to find existing cluster members
4. **Auto-Join**: Nodes automatically join discovered clusters

### Network Requirements

- **Protocol**: mDNS (multicast DNS)
- **Port**: UDP 5353
- **Scope**: Local network segment only
- **Firewall**: Must allow mDNS traffic

## Installation

### Quick Install (Standalone)

```bash
# Interactive installation
./install.sh

# Non-interactive with defaults
./install.sh --yes

# Custom prefix
./install.sh --prefix /opt/flydb --yes
```

### Cluster Installation

#### Option 1: Bootstrap New Cluster

On the first node:

```bash
./install.sh
# Choose "cluster" mode
# Choose "bootstrap" role
# Configure node ID and addresses
```

#### Option 2: Join Existing Cluster

On additional nodes:

```bash
./install.sh
# Choose "cluster" mode
# Choose "join" role
# Discovery will automatically find existing nodes
# Or manually enter peer addresses
```

#### Option 3: Automated Discovery

```bash
# Enable discovery during installation
./install.sh --cluster --discovery
```

## Cluster Setup

### 3-Node Cluster Example

**Node 1 (Bootstrap):**
```bash
./install.sh \
  --cluster \
  --node-id node1 \
  --cluster-addr 192.168.1.10:7946 \
  --raft-addr 192.168.1.10:7947 \
  --discovery
```

**Node 2 (Join):**
```bash
./install.sh \
  --cluster \
  --node-id node2 \
  --cluster-addr 192.168.1.11:7946 \
  --raft-addr 192.168.1.11:7947 \
  --peers 192.168.1.10:7946 \
  --discovery
```

**Node 3 (Join):**
```bash
./install.sh \
  --cluster \
  --node-id node3 \
  --cluster-addr 192.168.1.12:7946 \
  --raft-addr 192.168.1.12:7947 \
  --peers 192.168.1.10:7946,192.168.1.11:7946 \
  --discovery
```

### Configuration

Edit `/etc/flydb/flydb.toml`:

```toml
# Node identity
node_id = "node1"

# Network addresses
bind_addr = ":6380"          # Redis protocol
http_addr = ":8080"          # HTTP API
cluster_addr = ":7946"       # Gossip protocol
raft_addr = ":7947"          # Raft consensus

# Cluster peers (comma-separated)
cluster_peers = "192.168.1.11:7946,192.168.1.12:7946"

# Service discovery
discovery_enabled = true
discovery_cluster_id = "production"

# Routing strategy
routing_strategy = "hybrid"  # key-based, round-robin, least-loaded, locality-aware, hybrid

# Replication
replication_factor = 3
partition_count = 256
```

## Systemd Services

### Standalone Mode

```bash
# Install service
sudo cp deploy/systemd/flydb.service /etc/systemd/system/
sudo systemctl daemon-reload

# Enable and start
sudo systemctl enable flydb
sudo systemctl start flydb

# Check status
sudo systemctl status flydb
journalctl -u flydb -f
```

### Cluster Mode

```bash
# Install template service
sudo cp deploy/systemd/flydb-cluster@.service /etc/systemd/system/
sudo systemctl daemon-reload

# Start multiple nodes
sudo systemctl enable flydb-cluster@node1
sudo systemctl start flydb-cluster@node1

sudo systemctl enable flydb-cluster@node2
sudo systemctl start flydb-cluster@node2

sudo systemctl enable flydb-cluster@node3
sudo systemctl start flydb-cluster@node3

# Check status
sudo systemctl status flydb-cluster@node1
journalctl -u flydb-cluster@node1 -f
```

## Troubleshooting

### Discovery Not Working

**Check mDNS service:**
```bash
# Linux
sudo systemctl status avahi-daemon

# macOS
sudo launchctl list | grep mDNSResponder
```

**Test discovery manually:**
```bash
flydb-discover --timeout 10
```

**Check firewall:**
```bash
# Allow mDNS
sudo ufw allow 5353/udp
```

### Nodes Can't Join Cluster

**Verify connectivity:**
```bash
# Test gossip port
nc -zv 192.168.1.10 7946

# Test Raft port
nc -zv 192.168.1.10 7947
```

**Check logs:**
```bash
journalctl -u flydb -f
```

**Verify configuration:**
```bash
cat /etc/flydb/flydb.toml | grep -A 5 cluster
```

### Performance Issues

**Check cluster health:**
```bash
curl http://localhost:8080/cluster/status
```

**Monitor metrics:**
```bash
curl http://localhost:8080/metrics
```

**Adjust routing strategy:**
```toml
# Try different strategies
routing_strategy = "least-loaded"  # For balanced load
routing_strategy = "locality-aware"  # For geo-distributed
routing_strategy = "hybrid"  # Best of all
```

## Best Practices

1. **Odd Number of Nodes**: Use 3, 5, or 7 nodes for proper quorum
2. **Enable Discovery**: Simplifies cluster management
3. **Monitor Health**: Use HTTP API and metrics
4. **Backup Configuration**: Keep config files in version control
5. **Test Failover**: Regularly test node failures
6. **Use Systemd**: For automatic restarts and logging
7. **Secure Network**: Use firewalls and VPNs for production

## Next Steps

- [Configuration Reference](CONFIGURATION.md)
- [API Documentation](API.md)
- [Performance Tuning](PERFORMANCE.md)
- [Security Guide](SECURITY.md)

