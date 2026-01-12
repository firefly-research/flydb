#!/bin/bash
#
# FlyDB Installation Script
# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0
#
# A best-in-class installation experience with both interactive wizard
# and non-interactive CLI modes. Supports both local source builds and
# remote installation via pre-built binaries.
#
# Usage:
#   Remote install:  curl -sSL https://get.flydb.dev | bash
#   With options:    curl -sSL https://get.flydb.dev | bash -s -- --prefix ~/.local --yes
#   Interactive:     ./install.sh
#   Non-interactive: ./install.sh --prefix /usr/local --yes
#   From source:     ./install.sh --from-source
#   Uninstall:       ./install.sh --uninstall
#

set -euo pipefail

# =============================================================================
# Configuration and Defaults
# =============================================================================

readonly SCRIPT_VERSION="01.26.12"
readonly FLYDB_VERSION="${FLYDB_VERSION:-01.26.12}"
readonly GITHUB_REPO="firefly-oss/flydb"
readonly DOWNLOAD_BASE_URL="https://github.com/${GITHUB_REPO}/releases/download"

# Default values (can be overridden by CLI args or interactive prompts)
PREFIX=""
INSTALL_SERVICE=true
CREATE_CONFIG=true
INIT_DATABASE=false
AUTO_CONFIRM=false
UNINSTALL=false
SPECIFIC_VERSION=""
INTERACTIVE=true
# Installation mode: "auto" (detect), "source" (build from source), "binary" (download pre-built)
INSTALL_MODE="auto"
# Temporary directory for downloads
TEMP_DIR=""

# Detected system info
OS=""
ARCH=""
DISTRO=""
INIT_SYSTEM=""

# Installation tracking for rollback
declare -a INSTALLED_FILES=()
declare -a CREATED_DIRS=()
INSTALL_STARTED=false

# Resolved installation mode after detection
RESOLVED_INSTALL_MODE=""

# =============================================================================
# Server Configuration (for interactive wizard)
# =============================================================================

# Server role: standalone, master, slave, cluster
SERVER_ROLE="standalone"

# Network ports
PORT="8889"
REPL_PORT="9999"
CLUSTER_PORT="9998"

# Master address for slave mode
MASTER_ADDR=""

# Cluster configuration
CLUSTER_PEERS=""
CLUSTER_BOOTSTRAP="false"  # true = bootstrap new cluster, false = join existing
REPLICATION_MODE="async"
HEARTBEAT_INTERVAL="500"
HEARTBEAT_TIMEOUT="2000"
ELECTION_TIMEOUT="1000"
MIN_QUORUM="0"
ENABLE_PRE_VOTE="true"
PARTITION_COUNT="256"
REPLICATION_FACTOR="3"
SYNC_TIMEOUT="5000"
MAX_REPLICATION_LAG="10000"

# Storage configuration
DATA_DIR=""
BUFFER_POOL_SIZE="0"
CHECKPOINT_SECS="60"

# Security configuration
ENCRYPTION_ENABLED="true"
ENCRYPTION_PASSPHRASE=""

# Logging configuration
LOG_LEVEL="info"
LOG_JSON="false"

# Track if advanced configuration was requested
ADVANCED_CONFIG=false

# =============================================================================
# Colors and Formatting (matching pkg/cli/colors.go)
# =============================================================================

# Check if colors should be enabled
if [[ -t 1 ]] && [[ -z "${NO_COLOR:-}" ]]; then
    readonly COLOR_ENABLED=true
else
    readonly COLOR_ENABLED=false
fi

# ANSI color codes
if [[ "$COLOR_ENABLED" == true ]]; then
    readonly RESET='\033[0m'
    readonly BOLD='\033[1m'
    readonly DIM='\033[2m'
    readonly RED='\033[31m'
    readonly GREEN='\033[32m'
    readonly YELLOW='\033[33m'
    readonly BLUE='\033[34m'
    readonly MAGENTA='\033[35m'
    readonly CYAN='\033[36m'
    readonly WHITE='\033[37m'
    readonly BRIGHT_BLACK='\033[90m'
else
    readonly RESET=''
    readonly BOLD=''
    readonly DIM=''
    readonly RED=''
    readonly GREEN=''
    readonly YELLOW=''
    readonly BLUE=''
    readonly MAGENTA=''
    readonly CYAN=''
    readonly WHITE=''
    readonly BRIGHT_BLACK=''
fi

# Icons (matching pkg/cli/colors.go)
readonly ICON_SUCCESS="✓"
readonly ICON_ERROR="✗"
readonly ICON_WARNING="⚠"
readonly ICON_INFO="ℹ"
readonly ICON_ARROW="→"

# Spinner frames (matching pkg/cli/spinner.go)
readonly SPINNER_FRAMES=("⠋" "⠙" "⠹" "⠸" "⠼" "⠴" "⠦" "⠧" "⠇" "⠏")

# =============================================================================
# Output Functions
# =============================================================================

print_success() {
    echo -e "${GREEN}${ICON_SUCCESS}${RESET} ${GREEN}$1${RESET}"
}

print_error() {
    echo -e "${RED}${ICON_ERROR}${RESET} ${RED}$1${RESET}" >&2
}

print_warning() {
    echo -e "${YELLOW}${ICON_WARNING}${RESET} ${YELLOW}$1${RESET}"
}

print_info() {
    echo -e "${CYAN}${ICON_INFO}${RESET} ${CYAN}$1${RESET}"
}

print_step() {
    echo -e "${BLUE}${BOLD}==>${RESET} ${BOLD}$1${RESET}"
}

print_substep() {
    echo -e "    ${ICON_ARROW} $1"
}

print_dim() {
    echo -e "${DIM}$1${RESET}"
}

separator() {
    local width="${1:-60}"
    printf '%*s\n' "$width" '' | tr ' ' '─'
}

double_separator() {
    local width="${1:-60}"
    printf '%*s\n' "$width" '' | tr ' ' '═'
}

# Key-value display (matching pkg/cli/output.go KeyValue function)
print_kv() {
    local key="$1"
    local value="$2"
    local width="${3:-22}"
    printf "  %-${width}s %b\n" "${key}:" "$value"
}

# =============================================================================
# Spinner Functions
# =============================================================================

SPINNER_PID=""
SPINNER_ACTIVE=false

spinner_start() {
    local message="$1"

    # Don't start spinner if not interactive terminal
    if [[ ! -t 1 ]]; then
        echo "$message..."
        return
    fi

    # Stop any existing spinner first
    spinner_stop

    SPINNER_ACTIVE=true

    (
        local i=0
        while true; do
            local frame="${SPINNER_FRAMES[$((i % ${#SPINNER_FRAMES[@]}))]}"
            printf "\r${CYAN}%s${RESET} %s" "$frame" "$message"
            sleep 0.08
            ((i++))
        done
    ) &
    SPINNER_PID=$!
    disown "$SPINNER_PID" 2>/dev/null || true
}

spinner_stop() {
    if [[ -n "$SPINNER_PID" ]] && [[ "$SPINNER_ACTIVE" == true ]]; then
        kill "$SPINNER_PID" 2>/dev/null || true
        wait "$SPINNER_PID" 2>/dev/null || true
        SPINNER_PID=""
        SPINNER_ACTIVE=false
        printf "\r\033[K"  # Clear the line
    fi
}

spinner_success() {
    spinner_stop
    print_success "$1"
}

spinner_error() {
    spinner_stop
    print_error "$1"
}

# Ensure spinner is stopped before any interactive prompt
ensure_clean_prompt() {
    spinner_stop
    # Small delay to ensure terminal is ready
    sleep 0.05
}

# =============================================================================
# Banner and Help
# =============================================================================

print_banner() {
    echo ""
    echo -e "${CYAN}${BOLD}       _____.__            .______.${RESET}"
    echo -e "${CYAN}${BOLD}     _/ ____\\  | ___.__. __| _/\\_ |__${RESET}"
    echo -e "${CYAN}${BOLD}     \\   __\\|  |<   |  |/ __ |  | __ \\${RESET}"
    echo -e "${CYAN}${BOLD}      |  |  |  |_\\___  / /_/ |  | \\_\\ \\${RESET}"
    echo -e "${CYAN}${BOLD}      |__|  |____/ ____\\____ |  |___  /${RESET}"
    echo -e "${CYAN}${BOLD}                 \\/         \\/      \\/${RESET}"
    echo ""
    echo -e "  ${WHITE}High-Performance Embedded SQL Database${RESET}"
    echo -e "  ${DIM}Installation Script v${SCRIPT_VERSION}${RESET}"
    echo ""
}

print_welcome_message() {
    echo -e "${BOLD}Welcome to FlyDB!${RESET}"
    echo ""
    echo "  FlyDB is a high-performance embedded SQL database designed for"
    echo "  modern applications. It supports multiple deployment modes:"
    echo ""
    echo -e "  ${GREEN}●${RESET} ${BOLD}Standalone${RESET}  - Single server for development or small deployments"
    echo -e "  ${BLUE}●${RESET} ${BOLD}Master${RESET}      - Leader node that accepts writes and replicates to slaves"
    echo -e "  ${YELLOW}●${RESET} ${BOLD}Slave${RESET}       - Follower node that receives replication from master"
    echo -e "  ${MAGENTA}●${RESET} ${BOLD}Cluster${RESET}     - Distributed cluster with automatic failover"
    echo ""
    echo "  Key Features:"
    echo -e "    ${ICON_SUCCESS} Full SQL support with ACID transactions"
    echo -e "    ${ICON_SUCCESS} Data-at-rest encryption (AES-256-GCM)"
    echo -e "    ${ICON_SUCCESS} Multi-database support (CREATE DATABASE, USE)"
    echo -e "    ${ICON_SUCCESS} Automatic leader election and failover"
    echo -e "    ${ICON_SUCCESS} Configurable replication modes (async/semi-sync/sync)"
    echo ""
}

print_help() {
    echo -e "${BOLD}FlyDB Installation Script${RESET}"
    echo ""
    echo "A best-in-class installation experience for FlyDB - the high-performance"
    echo "embedded SQL database with support for distributed deployments."
    echo ""
    echo -e "${BOLD}USAGE:${RESET}"
    echo "    $0 [OPTIONS]"
    echo ""
    echo "    # Remote installation (download pre-built binaries)"
    echo "    curl -sSL https://get.flydb.dev | bash"
    echo ""
    echo -e "${BOLD}MODES:${RESET}"
    echo "    Interactive (default):  Run without arguments for guided installation"
    echo "    Non-interactive:        Use --yes with other options for scripted installs"
    echo ""
    echo -e "${BOLD}INSTALLATION SOURCE:${RESET}"
    echo "    By default, the script auto-detects whether to build from source or"
    echo "    download pre-built binaries:"
    echo "    - If run from a FlyDB source directory with Go installed: builds from source"
    echo "    - Otherwise: downloads pre-built binaries from GitHub releases"
    echo ""
    echo -e "${BOLD}INSTALLATION OPTIONS:${RESET}"
    echo -e "    ${BOLD}--prefix <path>${RESET}           Installation directory (default: /usr/local or ~/.local)"
    echo -e "    ${BOLD}--version <version>${RESET}       Specific FlyDB version (default: ${FLYDB_VERSION})"
    echo -e "    ${BOLD}--from-source${RESET}             Force building from source (requires Go 1.21+)"
    echo -e "    ${BOLD}--from-binary${RESET}             Force downloading pre-built binaries"
    echo -e "    ${BOLD}--no-service${RESET}              Skip system service installation"
    echo -e "    ${BOLD}--no-config${RESET}               Skip configuration file creation"
    echo -e "    ${BOLD}--init-db${RESET}                 Initialize a new database during installation"
    echo -e "    ${BOLD}--yes, -y${RESET}                 Skip all confirmation prompts"
    echo -e "    ${BOLD}--help, -h${RESET}                Show this help message"
    echo ""
    echo -e "${BOLD}UNINSTALL:${RESET}"
    echo -e "    ${BOLD}--uninstall${RESET}               Run the uninstall script"
    echo -e "                              (passes --yes, --prefix to uninstall.sh)"
    echo ""
    echo -e "${BOLD}SERVER CONFIGURATION OPTIONS:${RESET}"
    echo -e "    ${BOLD}--role <role>${RESET}             Server role: standalone, master, slave, cluster"
    echo -e "    ${BOLD}--port <port>${RESET}             Server port (default: 8889)"
    echo -e "    ${BOLD}--repl-port <port>${RESET}        Replication port (default: 9999)"
    echo -e "    ${BOLD}--cluster-port <port>${RESET}     Cluster communication port (default: 9998)"
    echo -e "    ${BOLD}--master-addr <host:port>${RESET} Master address for slave mode"
    echo -e "    ${BOLD}--data-dir <path>${RESET}         Data directory for database storage"
    echo ""
    echo -e "${BOLD}CLUSTER OPTIONS:${RESET}"
    echo -e "    ${BOLD}--cluster-bootstrap${RESET}       Bootstrap as first node (becomes leader)"
    echo -e "    ${BOLD}--cluster-peers <addrs>${RESET}   Comma-separated seed node addresses (host:port)"
    echo -e "    ${BOLD}--replication-mode <mode>${RESET} Replication mode: async, semi_sync, sync"
    echo -e "    ${BOLD}--heartbeat-interval <ms>${RESET} Heartbeat interval in milliseconds (default: 500)"
    echo -e "    ${BOLD}--heartbeat-timeout <ms>${RESET}  Heartbeat timeout in milliseconds (default: 2000)"
    echo -e "    ${BOLD}--election-timeout <ms>${RESET}   Election timeout in milliseconds (default: 1000)"
    echo -e "    ${BOLD}--min-quorum <n>${RESET}          Minimum quorum size (0=auto, default: 0)"
    echo -e "    ${BOLD}--partition-count <n>${RESET}     Number of data partitions (default: 256)"
    echo -e "    ${BOLD}--replication-factor <n>${RESET}  Number of replicas per partition (default: 3)"
    echo ""
    echo -e "${BOLD}SECURITY OPTIONS:${RESET}"
    echo -e "    ${BOLD}--encryption${RESET}              Enable data-at-rest encryption (default: enabled)"
    echo -e "    ${BOLD}--no-encryption${RESET}           Disable data-at-rest encryption"
    echo -e "    ${BOLD}--encryption-passphrase <p>${RESET} Set encryption passphrase"
    echo ""
    echo -e "${BOLD}LOGGING OPTIONS:${RESET}"
    echo -e "    ${BOLD}--log-level <level>${RESET}       Log level: debug, info, warn, error (default: info)"
    echo -e "    ${BOLD}--log-json${RESET}                Enable JSON log output"
    echo ""
    echo -e "${BOLD}STORAGE OPTIONS:${RESET}"
    echo -e "    ${BOLD}--buffer-pool-size <pages>${RESET} Buffer pool size in pages (0=auto)"
    echo -e "    ${BOLD}--checkpoint-secs <secs>${RESET}  Checkpoint interval in seconds (default: 60)"
    echo ""
    echo -e "${BOLD}EXAMPLES:${RESET}"
    echo "    # Interactive installation (recommended for first-time users)"
    echo "    ./install.sh"
    echo ""
    echo "    # Quick install with defaults"
    echo "    ./install.sh --yes"
    echo ""
    echo "    # Install as standalone server"
    echo "    ./install.sh --role standalone --port 8889 --yes"
    echo ""
    echo "    # Install as master node for replication"
    echo "    ./install.sh --role master --repl-port 9999 --yes"
    echo ""
    echo "    # Install as slave node"
    echo "    ./install.sh --role slave --master-addr master.example.com:9999 --yes"
    echo ""
    echo "    # Bootstrap first cluster node (becomes leader)"
    echo "    ./install.sh --role cluster --cluster-bootstrap --yes"
    echo ""
    echo "    # Join existing cluster via seed nodes"
    echo "    ./install.sh --role cluster --cluster-peers node1:9998,node2:9998 --yes"
    echo ""
    echo "    # Install with custom data directory and encryption"
    echo "    ./install.sh --data-dir /var/lib/flydb --encryption --yes"
    echo ""
    echo "    # Remote installation - bootstrap first cluster node"
    echo "    curl -sSL https://get.flydb.dev | bash -s -- --role cluster \\"
    echo "         --cluster-bootstrap --yes"
    echo ""
    echo "    # Remote installation - join existing cluster"
    echo "    curl -sSL https://get.flydb.dev | bash -s -- --role cluster \\"
    echo "         --cluster-peers node1:9998,node2:9998 --yes"
    echo ""
    echo -e "${BOLD}SERVER ROLES:${RESET}"
    echo -e "    ${GREEN}standalone${RESET}  Single server mode (default, no replication)"
    echo "              Best for: Development, testing, small single-server deployments"
    echo ""
    echo -e "    ${BLUE}master${RESET}      Leader node that accepts writes and replicates to slaves"
    echo "              Best for: Simple master/slave setups with manual failover"
    echo ""
    echo -e "    ${YELLOW}slave${RESET}       Follower node that receives replication from master"
    echo "              Best for: Read replicas, backup nodes in master/slave setup"
    echo ""
    echo -e "    ${MAGENTA}cluster${RESET}     Distributed cluster with automatic failover"
    echo "              Best for: Production deployments requiring high availability"
    echo ""
    echo -e "${BOLD}REPLICATION MODES:${RESET}"
    echo -e "    ${GREEN}async${RESET}       Best performance, eventual consistency"
    echo "              Writes return immediately, replicated in background"
    echo ""
    echo -e "    ${YELLOW}semi_sync${RESET}   Balanced performance and consistency"
    echo "              At least one replica must acknowledge before commit"
    echo ""
    echo -e "    ${RED}sync${RESET}        Strongest consistency, lower performance"
    echo "              All replicas must acknowledge before commit"
    echo ""
    echo -e "${BOLD}ENVIRONMENT VARIABLES:${RESET}"
    echo "    FLYDB_VERSION               Override the default version to install"
    echo "    FLYDB_ENCRYPTION_PASSPHRASE Set encryption passphrase"
    echo "    NO_COLOR                    Disable colored output"
    echo ""
    echo -e "${BOLD}MORE INFORMATION:${RESET}"
    echo "    Documentation:    https://flydb.dev/docs"
    echo "    GitHub:           https://github.com/${GITHUB_REPO}"
    echo "    Issues:           https://github.com/${GITHUB_REPO}/issues"
    echo ""
}

# =============================================================================
# System Detection
# =============================================================================

detect_os() {
    OS="$(uname -s)"
    case "$OS" in
        Linux)
            OS="linux"
            # Detect Linux distribution
            if [[ -f /etc/os-release ]]; then
                # shellcheck source=/dev/null
                source /etc/os-release
                DISTRO="${ID:-unknown}"
            elif [[ -f /etc/redhat-release ]]; then
                DISTRO="rhel"
            elif [[ -f /etc/debian_version ]]; then
                DISTRO="debian"
            else
                DISTRO="unknown"
            fi
            ;;
        Darwin)
            OS="darwin"
            DISTRO="macos"
            ;;
        MINGW*|MSYS*|CYGWIN*)
            OS="windows"
            DISTRO="windows"
            ;;
        *)
            print_error "Unsupported operating system: $OS"
            exit 1
            ;;
    esac
}

detect_arch() {
    ARCH="$(uname -m)"
    case "$ARCH" in
        x86_64|amd64)
            ARCH="amd64"
            ;;
        aarch64|arm64)
            ARCH="arm64"
            ;;
        armv7l|armv7)
            ARCH="arm"
            ;;
        i386|i686)
            ARCH="386"
            ;;
        *)
            print_error "Unsupported architecture: $ARCH"
            exit 1
            ;;
    esac
}

detect_init_system() {
    if [[ "$OS" == "darwin" ]]; then
        INIT_SYSTEM="launchd"
    elif command -v systemctl &>/dev/null && systemctl --version &>/dev/null; then
        INIT_SYSTEM="systemd"
    elif command -v rc-service &>/dev/null; then
        INIT_SYSTEM="openrc"
    elif [[ -d /etc/init.d ]]; then
        INIT_SYSTEM="sysvinit"
    else
        INIT_SYSTEM="none"
    fi
}

get_default_prefix() {
    if [[ $EUID -eq 0 ]]; then
        echo "/usr/local"
    else
        echo "${HOME}/.local"
    fi
}

# Get available disk space in MB for a given path
# Falls back to parent directories if path doesn't exist
get_available_disk_space() {
    local target_path="$1"
    local check_path="$target_path"

    # Find an existing directory to check (walk up the tree)
    while [[ ! -d "$check_path" ]] && [[ "$check_path" != "/" ]]; do
        check_path=$(dirname "$check_path")
    done

    # If we couldn't find any existing directory, use root
    if [[ ! -d "$check_path" ]]; then
        check_path="/"
    fi

    local available_space
    if [[ "$OS" == "darwin" ]]; then
        # macOS: df -m output has "Available" in column 4
        # Format: Filesystem 1M-blocks Used Available Capacity iused ifree %iused Mounted
        available_space=$(df -m "$check_path" 2>/dev/null | awk 'NR==2 {print $4}')
    else
        # Linux: df -m output typically has "Available" in column 4
        # Format: Filesystem 1M-blocks Used Available Use% Mounted
        available_space=$(df -m "$check_path" 2>/dev/null | awk 'NR==2 {print $4}')
    fi

    # Validate that we got a number
    if [[ "$available_space" =~ ^[0-9]+$ ]]; then
        echo "$available_space"
    else
        echo "unknown"
    fi
}

# =============================================================================
# Installation Mode Detection
# =============================================================================

# Detect if we're running from a local source directory or remotely
detect_install_mode() {
    if [[ "$INSTALL_MODE" == "source" ]]; then
        RESOLVED_INSTALL_MODE="source"
        return
    fi

    if [[ "$INSTALL_MODE" == "binary" ]]; then
        RESOLVED_INSTALL_MODE="binary"
        return
    fi

    # Auto-detect: check if we're in a FlyDB source directory
    if [[ -f "go.mod" ]] && grep -q "flydb" go.mod 2>/dev/null; then
        # We're in a source directory
        if command -v go &>/dev/null; then
            RESOLVED_INSTALL_MODE="source"
            print_info "Detected local source directory - will build from source"
        else
            print_warning "Source directory detected but Go not found - will download binaries"
            RESOLVED_INSTALL_MODE="binary"
        fi
    else
        # Not in source directory - download pre-built binaries
        RESOLVED_INSTALL_MODE="binary"
        print_info "Will download pre-built binaries from GitHub"
    fi
}

# Get the download URL for the release archive
get_download_url() {
    local version="${SPECIFIC_VERSION:-$FLYDB_VERSION}"

    # Remove 'v' prefix if present for consistency
    version="${version#v}"

    # Construct the archive name: flydb_<version>_<os>_<arch>.tar.gz
    local archive_name="flydb_${version}_${OS}_${ARCH}.tar.gz"

    echo "${DOWNLOAD_BASE_URL}/v${version}/${archive_name}"
}

# Create a temporary directory for downloads
create_temp_dir() {
    TEMP_DIR=$(mktemp -d 2>/dev/null || mktemp -d -t 'flydb-install')
    if [[ ! -d "$TEMP_DIR" ]]; then
        print_error "Failed to create temporary directory"
        exit 1
    fi
}

# Clean up temporary directory
cleanup_temp_dir() {
    if [[ -n "$TEMP_DIR" ]] && [[ -d "$TEMP_DIR" ]]; then
        rm -rf "$TEMP_DIR"
        TEMP_DIR=""
    fi
}

# Download and extract pre-built binaries
download_binaries() {
    print_step "Downloading FlyDB binaries..."

    local version="${SPECIFIC_VERSION:-$FLYDB_VERSION}"
    version="${version#v}"

    local download_url
    download_url=$(get_download_url)

    create_temp_dir

    local archive_path="$TEMP_DIR/flydb.tar.gz"

    spinner_start "Downloading FlyDB v${version} for ${OS}/${ARCH}"

    local http_code
    http_code=$(curl -fsSL -w "%{http_code}" -o "$archive_path" "$download_url" 2>/dev/null) || true

    if [[ "$http_code" != "200" ]] || [[ ! -f "$archive_path" ]]; then
        spinner_error "Failed to download binaries (HTTP $http_code)"
        echo ""
        print_error "Could not download from: $download_url"
        echo ""
        print_info "Possible solutions:"
        echo "  1. Check if version v${version} exists for ${OS}/${ARCH}"
        echo "  2. Check your internet connection"
        echo "  3. Build from source: git clone https://github.com/${GITHUB_REPO}.git && cd flydb && ./install.sh --from-source"
        cleanup_temp_dir
        exit 1
    fi

    spinner_success "Downloaded FlyDB v${version}"

    spinner_start "Extracting binaries"

    if ! tar -xzf "$archive_path" -C "$TEMP_DIR" 2>/dev/null; then
        spinner_error "Failed to extract archive"
        cleanup_temp_dir
        exit 1
    fi

    # Verify extracted binaries exist
    if [[ ! -f "$TEMP_DIR/flydb" ]] || [[ ! -f "$TEMP_DIR/flydb-shell" ]]; then
        spinner_error "Archive does not contain expected binaries"
        cleanup_temp_dir
        exit 1
    fi

    spinner_success "Extracted binaries"
    echo ""
}

# Install binaries from downloaded files
install_downloaded_binaries() {
    print_step "Installing binaries..."

    INSTALL_STARTED=true

    local bin_dir="${PREFIX}/bin"
    local sudo_cmd
    sudo_cmd=$(get_sudo_cmd "$bin_dir")

    # Create bin directory
    if [[ ! -d "$bin_dir" ]]; then
        spinner_start "Creating directory $bin_dir"
        if $sudo_cmd mkdir -p "$bin_dir" 2>/dev/null; then
            spinner_success "Created $bin_dir"
            CREATED_DIRS+=("$bin_dir")
        else
            spinner_error "Failed to create $bin_dir"
            cleanup_temp_dir
            exit 1
        fi
    else
        print_substep "Directory exists: $bin_dir"
    fi

    # Install flydb
    spinner_start "Installing flydb"
    if $sudo_cmd cp "$TEMP_DIR/flydb" "$bin_dir/" && $sudo_cmd chmod +x "$bin_dir/flydb"; then
        spinner_success "Installed ${bin_dir}/flydb"
        INSTALLED_FILES+=("$bin_dir/flydb")
    else
        spinner_error "Failed to install flydb"
        cleanup_temp_dir
        rollback
        exit 1
    fi

    # Install flydb-shell
    spinner_start "Installing flydb-shell"
    if $sudo_cmd cp "$TEMP_DIR/flydb-shell" "$bin_dir/" && $sudo_cmd chmod +x "$bin_dir/flydb-shell"; then
        spinner_success "Installed ${bin_dir}/flydb-shell"
        INSTALLED_FILES+=("$bin_dir/flydb-shell")
    else
        spinner_error "Failed to install flydb-shell"
        cleanup_temp_dir
        rollback
        exit 1
    fi

    # Install flydb-dump
    spinner_start "Installing flydb-dump"
    if $sudo_cmd cp "$TEMP_DIR/flydb-dump" "$bin_dir/" && $sudo_cmd chmod +x "$bin_dir/flydb-dump"; then
        spinner_success "Installed ${bin_dir}/flydb-dump"
        INSTALLED_FILES+=("$bin_dir/flydb-dump")
    else
        spinner_error "Failed to install flydb-dump"
        cleanup_temp_dir
        rollback
        exit 1
    fi

    # Create fsql symlink for convenience
    spinner_start "Creating fsql symlink"
    if $sudo_cmd ln -sf "$bin_dir/flydb-shell" "$bin_dir/fsql"; then
        spinner_success "Created ${bin_dir}/fsql symlink"
        INSTALLED_FILES+=("$bin_dir/fsql")
    else
        spinner_error "Failed to create fsql symlink"
    fi

    # Create fdump symlink for convenience
    spinner_start "Creating fdump symlink"
    if $sudo_cmd ln -sf "$bin_dir/flydb-dump" "$bin_dir/fdump"; then
        spinner_success "Created ${bin_dir}/fdump symlink"
        INSTALLED_FILES+=("$bin_dir/fdump")
    else
        spinner_error "Failed to create fdump symlink"
    fi

    # Clean up temp directory
    cleanup_temp_dir

    echo ""
}

# =============================================================================
# Prerequisite Checks
# =============================================================================

check_prerequisites() {
    print_step "Checking prerequisites..."

    local errors=0

    # Check for required commands based on installation mode
    local required_commands=("curl" "tar")
    if [[ "$RESOLVED_INSTALL_MODE" == "source" ]]; then
        required_commands+=("go")
    fi

    for cmd in "${required_commands[@]}"; do
        if command -v "$cmd" &>/dev/null; then
            print_substep "${GREEN}${ICON_SUCCESS}${RESET} $cmd found"
        else
            print_substep "${RED}${ICON_ERROR}${RESET} $cmd not found"
            ((errors++))
        fi
    done

    # Check Go version if building from source
    if [[ "$RESOLVED_INSTALL_MODE" == "source" ]] && command -v go &>/dev/null; then
        local go_version
        go_version=$(go version | grep -oE 'go[0-9]+\.[0-9]+' | sed 's/go//')
        local go_major go_minor
        go_major=$(echo "$go_version" | cut -d. -f1)
        go_minor=$(echo "$go_version" | cut -d. -f2)

        if [[ "$go_major" -lt 1 ]] || ([[ "$go_major" -eq 1 ]] && [[ "$go_minor" -lt 21 ]]); then
            print_substep "${RED}${ICON_ERROR}${RESET} Go 1.21+ required (found: $go_version)"
            ((errors++))
        else
            print_substep "${GREEN}${ICON_SUCCESS}${RESET} Go $go_version"
        fi
    fi

    # Check disk space (require at least 100MB)
    local install_dir="${PREFIX:-$(get_default_prefix)}"
    local available_space
    available_space=$(get_available_disk_space "$install_dir")

    if [[ "$available_space" == "unknown" ]]; then
        print_substep "${YELLOW}${ICON_WARNING}${RESET} Could not determine available disk space"
    elif [[ "$available_space" -lt 100 ]]; then
        print_substep "${YELLOW}${ICON_WARNING}${RESET} Low disk space: ${available_space}MB available"
    else
        print_substep "${GREEN}${ICON_SUCCESS}${RESET} Disk space: ${available_space}MB available"
    fi

    # Check write permissions
    local test_dir="${install_dir}/bin"
    if [[ -d "$test_dir" ]]; then
        if [[ -w "$test_dir" ]]; then
            print_substep "${GREEN}${ICON_SUCCESS}${RESET} Write permission to $test_dir"
        else
            if [[ $EUID -eq 0 ]]; then
                print_substep "${RED}${ICON_ERROR}${RESET} No write permission to $test_dir"
                ((errors++))
            else
                print_substep "${YELLOW}${ICON_WARNING}${RESET} Will need sudo for $test_dir"
            fi
        fi
    else
        # Directory doesn't exist yet, check parent
        local parent_dir
        parent_dir=$(dirname "$test_dir")
        if [[ -d "$parent_dir" ]] && [[ -w "$parent_dir" ]]; then
            print_substep "${GREEN}${ICON_SUCCESS}${RESET} Can create $test_dir"
        elif [[ $EUID -eq 0 ]]; then
            print_substep "${GREEN}${ICON_SUCCESS}${RESET} Can create $test_dir (as root)"
        else
            print_substep "${YELLOW}${ICON_WARNING}${RESET} Will need sudo to create $test_dir"
        fi
    fi

    if [[ $errors -gt 0 ]]; then
        echo ""
        print_error "Prerequisites check failed with $errors error(s)"
        exit 1
    fi

    echo ""
}

check_existing_installation() {
    local install_dir="${PREFIX:-$(get_default_prefix)}/bin"
    local existing_version=""

    if [[ -x "$install_dir/flydb" ]]; then
        existing_version=$("$install_dir/flydb" --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || echo "unknown")
        return 0
    fi

    # Also check common locations
    for dir in /usr/local/bin /usr/bin ~/.local/bin; do
        if [[ -x "$dir/flydb" ]]; then
            existing_version=$("$dir/flydb" --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || echo "unknown")
            echo "$dir:$existing_version"
            return 0
        fi
    done

    return 1
}

# =============================================================================
# Interactive Wizard
# =============================================================================

prompt() {
    local prompt_text="$1"
    local default="${2:-}"
    local result=""

    # Ensure any spinner is stopped before prompting
    ensure_clean_prompt

    if [[ -n "$default" ]]; then
        echo -en "${BOLD}${prompt_text}${RESET} [${CYAN}${default}${RESET}]: " >&2
    else
        echo -en "${BOLD}${prompt_text}${RESET}: " >&2
    fi

    read -r result

    if [[ -z "$result" ]]; then
        result="$default"
    fi

    echo "$result"
}

prompt_yes_no() {
    local prompt_text="$1"
    local default="${2:-y}"
    local result=""

    # In non-interactive mode (--yes), use the default value
    if [[ "${AUTO_CONFIRM:-false}" == true ]]; then
        if [[ "$default" == "y" ]]; then
            return 0
        else
            return 1
        fi
    fi

    # Ensure any spinner is stopped before prompting
    ensure_clean_prompt

    local hint
    if [[ "$default" == "y" ]]; then
        hint="Y/n"
    else
        hint="y/N"
    fi

    echo -en "${BOLD}${prompt_text}${RESET} [${CYAN}${hint}${RESET}]: " >&2
    read -r result

    if [[ -z "$result" ]]; then
        result="$default"
    fi

    # Convert to lowercase using tr for POSIX compatibility (macOS default bash is 3.2)
    local lower_result
    lower_result=$(echo "$result" | tr '[:upper:]' '[:lower:]')

    case "$lower_result" in
        y|yes) return 0 ;;
        *) return 1 ;;
    esac
}

validate_path() {
    local path="$1"

    # Expand ~ to home directory
    path="${path/#\~/$HOME}"

    # Check if path is absolute or can be made absolute
    if [[ ! "$path" = /* ]]; then
        path="$(pwd)/$path"
    fi

    echo "$path"
}

validate_port() {
    local port="$1"
    if [[ "$port" =~ ^[0-9]+$ ]] && [[ "$port" -ge 1 ]] && [[ "$port" -le 65535 ]]; then
        return 0
    fi
    return 1
}

validate_address() {
    local addr="$1"
    # Check for host:port format
    if [[ "$addr" =~ ^[a-zA-Z0-9._-]+:[0-9]+$ ]]; then
        local port="${addr##*:}"
        validate_port "$port"
        return $?
    fi
    return 1
}

validate_peers() {
    local peers="$1"
    if [[ -z "$peers" ]]; then
        return 0  # Empty is valid (will be configured later)
    fi
    # Split by comma and validate each
    IFS=',' read -ra PEER_ARRAY <<< "$peers"
    for peer in "${PEER_ARRAY[@]}"; do
        peer="${peer//[[:space:]]/}"  # Trim whitespace
        if [[ -n "$peer" ]] && ! validate_address "$peer"; then
            return 1
        fi
    done
    return 0
}

prompt_port() {
    local prompt_text="$1"
    local default="$2"
    local result

    while true; do
        result=$(prompt "$prompt_text" "$default")
        if validate_port "$result"; then
            echo "$result"
            return 0
        fi
        print_error "Invalid port number. Please enter a value between 1 and 65535."
    done
}

prompt_address() {
    local prompt_text="$1"
    local default="$2"
    local result

    while true; do
        result=$(prompt "$prompt_text" "$default")
        if validate_address "$result"; then
            echo "$result"
            return 0
        fi
        print_error "Invalid address format. Please use host:port format (e.g., localhost:9999)."
    done
}

# =============================================================================
# Interactive Wizard - Step Functions
# =============================================================================

wizard_step_header() {
    local step_num="$1"
    local title="$2"
    echo ""
    echo -e "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    echo -e "${CYAN}${BOLD}  Step ${step_num}: ${title}${RESET}"
    echo -e "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    echo ""
}

wizard_step_installation_dir() {
    wizard_step_header "1" "Installation Directory"

    echo "  Where should FlyDB binaries be installed?"
    echo ""
    echo -e "  ${GREEN}[1]${RESET} /usr/local/bin  ${DIM}(system-wide, requires sudo)${RESET}"
    echo -e "  ${BLUE}[2]${RESET} ~/.local/bin    ${DIM}(user-only, no sudo required)${RESET}"
    echo -e "  ${YELLOW}[3]${RESET} Custom path     ${DIM}(specify your own location)${RESET}"
    echo ""

    local choice
    choice=$(prompt "Select option" "1")
    choice="${choice//[[:space:]]/}"

    case "$choice" in
        1) PREFIX="/usr/local" ;;
        2) PREFIX="$HOME/.local" ;;
        3)
            local custom_path
            custom_path=$(prompt "Enter installation path" "/opt/flydb")
            PREFIX=$(validate_path "$custom_path")
            ;;
        *)
            print_warning "Invalid selection, using default (/usr/local)"
            PREFIX="/usr/local"
            ;;
    esac

    print_success "Installation directory: ${PREFIX}/bin"
}

wizard_step_server_role() {
    wizard_step_header "2" "Server Role"

    echo "  Select the deployment mode for FlyDB:"
    echo ""
    echo -e "  ${GREEN}[1]${RESET} ${BOLD}Standalone${RESET}  ${DIM}Single server, no replication${RESET}"
    echo -e "      ${DIM}Best for: Development, testing, small deployments${RESET}"
    echo ""
    echo -e "  ${BLUE}[2]${RESET} ${BOLD}Master${RESET}      ${DIM}Leader node that accepts writes${RESET}"
    echo -e "      ${DIM}Best for: Simple master/slave setups with manual failover${RESET}"
    echo ""
    echo -e "  ${YELLOW}[3]${RESET} ${BOLD}Slave${RESET}       ${DIM}Follower node receiving replication${RESET}"
    echo -e "      ${DIM}Best for: Read replicas, backup nodes${RESET}"
    echo ""
    echo -e "  ${MAGENTA}[4]${RESET} ${BOLD}Cluster${RESET}     ${DIM}Distributed with automatic failover${RESET}"
    echo -e "      ${DIM}Best for: Production high-availability deployments${RESET}"
    echo ""

    local choice
    choice=$(prompt "Select role" "1")
    choice="${choice//[[:space:]]/}"

    case "$choice" in
        1) SERVER_ROLE="standalone" ;;
        2) SERVER_ROLE="master" ;;
        3) SERVER_ROLE="slave" ;;
        4) SERVER_ROLE="cluster" ;;
        *)
            print_warning "Invalid selection, using standalone mode"
            SERVER_ROLE="standalone"
            ;;
    esac

    print_success "Server role: ${SERVER_ROLE}"
}

wizard_step_network_ports() {
    wizard_step_header "3" "Network Configuration"

    echo "  Configure network ports for FlyDB services:"
    echo ""
    echo -e "  ${DIM}• Server port: Binary protocol for fsql CLI, JDBC/ODBC drivers${RESET}"
    if [[ "$SERVER_ROLE" == "master" ]] || [[ "$SERVER_ROLE" == "cluster" ]]; then
        echo -e "  ${DIM}• Replication port: WAL streaming for data replication to followers${RESET}"
    fi
    if [[ "$SERVER_ROLE" == "cluster" ]]; then
        echo -e "  ${DIM}• Cluster port: Raft consensus and cluster coordination${RESET}"
    fi
    echo ""

    PORT=$(prompt_port "Server port" "$PORT")

    if [[ "$SERVER_ROLE" == "master" ]] || [[ "$SERVER_ROLE" == "cluster" ]]; then
        REPL_PORT=$(prompt_port "Replication port" "$REPL_PORT")
    fi

    if [[ "$SERVER_ROLE" == "cluster" ]]; then
        CLUSTER_PORT=$(prompt_port "Cluster port" "$CLUSTER_PORT")
    fi

    echo ""
    print_success "Network ports configured"
}

wizard_step_slave_config() {
    if [[ "$SERVER_ROLE" != "slave" ]]; then
        return
    fi

    wizard_step_header "4" "Master Connection"

    echo "  Configure the master server to replicate from:"
    echo ""
    echo -e "  ${DIM}• The master must be running and accessible${RESET}"
    echo -e "  ${DIM}• Use the master's replication port (default: 9999)${RESET}"
    echo ""

    MASTER_ADDR=$(prompt_address "Master address (host:port)" "localhost:9999")

    echo ""
    print_success "Master address: ${MASTER_ADDR}"
}

wizard_step_cluster_config() {
    if [[ "$SERVER_ROLE" != "cluster" ]]; then
        return
    fi

    wizard_step_header "4" "Cluster Configuration"

    echo "  FlyDB cluster mode provides automatic failover and data distribution."
    echo ""
    echo -e "  ${BOLD}Cluster Setup Scenarios:${RESET}"
    echo ""
    echo -e "  ${GREEN}[1]${RESET} ${BOLD}Bootstrap First Node${RESET}"
    echo -e "      ${DIM}Start a new cluster - this node becomes the initial leader${RESET}"
    echo -e "      ${DIM}Other nodes will join this cluster later${RESET}"
    echo ""
    echo -e "  ${BLUE}[2]${RESET} ${BOLD}Join Existing Cluster${RESET}"
    echo -e "      ${DIM}Connect to an existing cluster via seed nodes${RESET}"
    echo -e "      ${DIM}This node will sync data from the cluster${RESET}"
    echo ""
    echo -e "  ${YELLOW}[3]${RESET} ${BOLD}Rejoin After Restart${RESET}"
    echo -e "      ${DIM}Reconnect to cluster after maintenance/restart${RESET}"
    echo -e "      ${DIM}Uses existing data directory and configuration${RESET}"
    echo ""

    local cluster_scenario
    cluster_scenario=$(prompt "Select scenario" "1")
    cluster_scenario="${cluster_scenario//[[:space:]]/}"

    case "$cluster_scenario" in
        1)
            # Bootstrap first node
            CLUSTER_BOOTSTRAP="true"
            echo ""
            print_info "Bootstrapping as first cluster node (leader)"
            echo ""
            echo -e "  ${DIM}This node will start as a single-node cluster.${RESET}"
            echo -e "  ${DIM}Other nodes can join using this node's address as a seed.${RESET}"
            echo ""

            # Get this node's hostname/IP for display
            local this_host
            this_host=$(hostname 2>/dev/null || echo "localhost")
            echo -e "  ${BOLD}Other nodes should use this seed address:${RESET}"
            echo -e "  ${CYAN}${this_host}:${CLUSTER_PORT}${RESET}"
            echo ""

            # No peers for bootstrap
            CLUSTER_PEERS=""
            ;;
        2)
            # Join existing cluster
            CLUSTER_BOOTSTRAP="false"
            echo ""
            print_info "Joining existing cluster"
            echo ""
            echo -e "  ${DIM}Enter the addresses of existing cluster nodes (seeds).${RESET}"
            echo -e "  ${DIM}Format: host:port (comma-separated for multiple)${RESET}"
            echo -e "  ${DIM}Example: node1:9998,node2:9998${RESET}"
            echo ""

            local peers_input
            peers_input=$(prompt "Cluster seed nodes" "$CLUSTER_PEERS")

            # Validate peers - must have at least one for joining
            while [[ -z "$peers_input" ]] || ! validate_peers "$peers_input"; do
                if [[ -z "$peers_input" ]]; then
                    print_error "At least one seed node is required to join a cluster."
                else
                    print_error "Invalid peer format. Use host:port,host:port format."
                fi
                peers_input=$(prompt "Cluster seed nodes" "$CLUSTER_PEERS")
            done
            CLUSTER_PEERS="$peers_input"

            echo ""
            print_success "Will join cluster via: ${CLUSTER_PEERS}"
            ;;
        3)
            # Rejoin after restart
            CLUSTER_BOOTSTRAP="false"
            echo ""
            print_info "Rejoining cluster after restart"
            echo ""
            echo -e "  ${DIM}Enter the addresses of cluster nodes to reconnect.${RESET}"
            echo -e "  ${DIM}These should be the same peers from your previous configuration.${RESET}"
            echo ""

            local peers_input
            peers_input=$(prompt "Cluster peer nodes" "$CLUSTER_PEERS")

            if [[ -n "$peers_input" ]] && validate_peers "$peers_input"; then
                CLUSTER_PEERS="$peers_input"
                print_success "Will rejoin cluster via: ${CLUSTER_PEERS}"
            else
                print_warning "No valid peers specified - will bootstrap as single node"
                CLUSTER_BOOTSTRAP="true"
                CLUSTER_PEERS=""
            fi
            ;;
        *)
            print_warning "Invalid selection, defaulting to bootstrap mode"
            CLUSTER_BOOTSTRAP="true"
            CLUSTER_PEERS=""
            ;;
    esac

    echo ""
    echo "  Select replication mode:"
    echo ""
    echo -e "  ${GREEN}[1]${RESET} ${BOLD}Async${RESET}      ${DIM}Best performance, eventual consistency${RESET}"
    echo -e "      ${DIM}Writes return immediately, replicated in background${RESET}"
    echo ""
    echo -e "  ${YELLOW}[2]${RESET} ${BOLD}Semi-sync${RESET}  ${DIM}Balanced performance and consistency${RESET}"
    echo -e "      ${DIM}At least one replica must acknowledge before commit${RESET}"
    echo ""
    echo -e "  ${RED}[3]${RESET} ${BOLD}Sync${RESET}       ${DIM}Strongest consistency, lower performance${RESET}"
    echo -e "      ${DIM}All replicas must acknowledge before commit${RESET}"
    echo ""

    local repl_choice
    repl_choice=$(prompt "Select replication mode" "1")
    case "$repl_choice" in
        1) REPLICATION_MODE="async" ;;
        2) REPLICATION_MODE="semi_sync" ;;
        3) REPLICATION_MODE="sync" ;;
        *) REPLICATION_MODE="async" ;;
    esac

    echo ""
    print_success "Cluster configuration complete"

    # Ask about advanced cluster settings
    echo ""
    if prompt_yes_no "Configure advanced cluster settings?" "n"; then
        wizard_step_cluster_advanced
    fi
}

wizard_step_cluster_advanced() {
    echo ""
    echo -e "  ${BOLD}Advanced Cluster Settings${RESET}"
    echo -e "  ${DIM}Press Enter to accept defaults (recommended for most deployments)${RESET}"
    echo ""

    # Heartbeat interval
    echo -e "  ${DIM}Heartbeat interval: How often nodes send heartbeats${RESET}"
    local hb_input
    hb_input=$(prompt "Heartbeat interval (ms)" "$HEARTBEAT_INTERVAL")
    if [[ "$hb_input" =~ ^[0-9]+$ ]] && [[ "$hb_input" -ge 100 ]]; then
        HEARTBEAT_INTERVAL="$hb_input"
    fi

    # Heartbeat timeout
    echo -e "  ${DIM}Heartbeat timeout: When to consider a node dead${RESET}"
    local ht_input
    ht_input=$(prompt "Heartbeat timeout (ms)" "$HEARTBEAT_TIMEOUT")
    if [[ "$ht_input" =~ ^[0-9]+$ ]] && [[ "$ht_input" -ge "$HEARTBEAT_INTERVAL" ]]; then
        HEARTBEAT_TIMEOUT="$ht_input"
    fi

    # Election timeout
    echo -e "  ${DIM}Election timeout: When to start a new leader election${RESET}"
    local et_input
    et_input=$(prompt "Election timeout (ms)" "$ELECTION_TIMEOUT")
    if [[ "$et_input" =~ ^[0-9]+$ ]] && [[ "$et_input" -ge 500 ]]; then
        ELECTION_TIMEOUT="$et_input"
    fi

    # Min quorum
    echo -e "  ${DIM}Min quorum: Minimum nodes for cluster decisions (0=auto)${RESET}"
    local mq_input
    mq_input=$(prompt "Min quorum (0=auto)" "$MIN_QUORUM")
    if [[ "$mq_input" =~ ^[0-9]+$ ]]; then
        MIN_QUORUM="$mq_input"
    fi

    # Partition count
    echo -e "  ${DIM}Partition count: Number of data partitions (power of 2)${RESET}"
    local pc_input
    pc_input=$(prompt "Partition count" "$PARTITION_COUNT")
    if [[ "$pc_input" =~ ^[0-9]+$ ]] && [[ "$pc_input" -ge 16 ]] && [[ "$pc_input" -le 4096 ]]; then
        # Check if power of 2
        if (( (pc_input & (pc_input - 1)) == 0 )); then
            PARTITION_COUNT="$pc_input"
        fi
    fi

    # Replication factor
    echo -e "  ${DIM}Replication factor: Number of replicas per partition${RESET}"
    local rf_input
    rf_input=$(prompt "Replication factor (1-5)" "$REPLICATION_FACTOR")
    if [[ "$rf_input" =~ ^[0-9]+$ ]] && [[ "$rf_input" -ge 1 ]] && [[ "$rf_input" -le 5 ]]; then
        REPLICATION_FACTOR="$rf_input"
    fi

    # Pre-vote
    echo -e "  ${DIM}Pre-vote: Prevents disruptions from partitioned nodes${RESET}"
    if prompt_yes_no "Enable pre-vote protocol?" "y"; then
        ENABLE_PRE_VOTE="true"
    else
        ENABLE_PRE_VOTE="false"
    fi

    echo ""
    print_success "Advanced cluster settings configured"
}

wizard_step_storage() {
    # Step number depends on role: standalone=4, master=4, slave=5, cluster=5
    local step_num="4"
    if [[ "$SERVER_ROLE" == "slave" ]] || [[ "$SERVER_ROLE" == "cluster" ]]; then
        step_num="5"
    fi

    wizard_step_header "$step_num" "Storage Configuration"

    echo "  Configure data storage settings:"
    echo ""
    echo -e "  ${DIM}• FlyDB stores each database in a separate directory${RESET}"
    echo -e "  ${DIM}• Supports CREATE DATABASE, DROP DATABASE, USE commands${RESET}"
    echo ""

    # Set default data directory based on installation type
    if [[ -z "$DATA_DIR" ]]; then
        if [[ $EUID -eq 0 ]] || [[ "$PREFIX" == "/usr/local" ]]; then
            DATA_DIR="/var/lib/flydb"
        else
            DATA_DIR="$HOME/.local/share/flydb"
        fi
    fi

    DATA_DIR=$(prompt "Data directory" "$DATA_DIR")
    DATA_DIR=$(validate_path "$DATA_DIR")

    echo ""
    echo -e "  ${DIM}Buffer pool size: Memory for caching data pages (0=auto)${RESET}"
    local bp_input
    bp_input=$(prompt "Buffer pool size (pages, 0=auto)" "$BUFFER_POOL_SIZE")
    if [[ "$bp_input" =~ ^[0-9]+$ ]]; then
        BUFFER_POOL_SIZE="$bp_input"
    fi

    echo ""
    print_success "Storage configuration complete"
}

wizard_step_security() {
    # Step number depends on role: standalone=5, master=5, slave=6, cluster=6
    local step_num="5"
    if [[ "$SERVER_ROLE" == "slave" ]] || [[ "$SERVER_ROLE" == "cluster" ]]; then
        step_num="6"
    fi

    wizard_step_header "$step_num" "Security Configuration"

    echo "  Configure data-at-rest encryption:"
    echo ""
    echo -e "  ${DIM}• Encrypts WAL data on disk using AES-256-GCM${RESET}"
    echo -e "  ${YELLOW}${ICON_WARNING}${RESET} ${YELLOW}Encryption is enabled by default for security${RESET}"
    echo -e "  ${YELLOW}${ICON_WARNING}${RESET} ${YELLOW}Keep your passphrase safe - data cannot be recovered without it!${RESET}"
    echo ""

    if prompt_yes_no "Enable data-at-rest encryption?" "y"; then
        ENCRYPTION_ENABLED="true"

        echo ""
        echo -e "  ${DIM}Enter a passphrase or leave empty for auto-generated${RESET}"

        # Check for environment variable
        if [[ -n "${FLYDB_ENCRYPTION_PASSPHRASE:-}" ]]; then
            ENCRYPTION_PASSPHRASE="$FLYDB_ENCRYPTION_PASSPHRASE"
            print_success "Using passphrase from FLYDB_ENCRYPTION_PASSPHRASE environment variable"
        else
            local passphrase_input
            passphrase_input=$(prompt "Encryption passphrase (Enter for auto)" "")
            if [[ -n "$passphrase_input" ]]; then
                ENCRYPTION_PASSPHRASE="$passphrase_input"
            else
                # Generate a random passphrase
                ENCRYPTION_PASSPHRASE=$(openssl rand -base64 24 2>/dev/null || head -c 24 /dev/urandom | base64)
                echo ""
                echo -e "  ${GREEN}${ICON_SUCCESS}${RESET} Auto-generated passphrase:"
                echo -e "  ${BOLD}${ENCRYPTION_PASSPHRASE}${RESET}"
                echo ""
                echo -e "  ${YELLOW}${ICON_WARNING}${RESET} ${YELLOW}Save this passphrase securely! You will need it to access your data.${RESET}"
            fi
        fi
    else
        ENCRYPTION_ENABLED="false"
        print_warning "Encryption disabled - data will be stored unencrypted"
    fi

    echo ""
    print_success "Security configuration complete"
}

wizard_step_logging() {
    # Step number depends on role: standalone=6, master=6, slave=7, cluster=7
    local step_num="6"
    if [[ "$SERVER_ROLE" == "slave" ]] || [[ "$SERVER_ROLE" == "cluster" ]]; then
        step_num="7"
    fi

    wizard_step_header "$step_num" "Logging Configuration"

    echo "  Configure logging settings:"
    echo ""
    echo -e "  ${DIM}Available log levels:${RESET}"
    echo -e "    ${GREEN}debug${RESET} - Verbose debugging information"
    echo -e "    ${CYAN}info${RESET}  - General operational information"
    echo -e "    ${YELLOW}warn${RESET}  - Warning messages"
    echo -e "    ${RED}error${RESET} - Error messages only"
    echo ""

    local log_input
    log_input=$(prompt "Log level" "$LOG_LEVEL")
    case "$log_input" in
        debug|info|warn|error) LOG_LEVEL="$log_input" ;;
        *) LOG_LEVEL="info" ;;
    esac

    echo ""
    echo -e "  ${DIM}JSON output is useful for log aggregation systems${RESET}"
    if prompt_yes_no "Enable JSON log output?" "n"; then
        LOG_JSON="true"
    else
        LOG_JSON="false"
    fi

    echo ""
    print_success "Logging configuration complete"
}

wizard_step_service() {
    # Step number depends on role: standalone=7, master=7, slave=8, cluster=8
    local step_num="7"
    if [[ "$SERVER_ROLE" == "slave" ]] || [[ "$SERVER_ROLE" == "cluster" ]]; then
        step_num="8"
    fi

    wizard_step_header "$step_num" "System Service"

    if [[ "$INIT_SYSTEM" != "none" ]]; then
        echo "  FlyDB can be installed as a system service ($INIT_SYSTEM)"
        echo ""
        echo -e "  ${DIM}• Starts FlyDB automatically on system boot${RESET}"
        echo -e "  ${DIM}• Manages FlyDB as a background service${RESET}"
        echo -e "  ${DIM}• Provides systemctl/launchctl commands for control${RESET}"
        echo ""

        if prompt_yes_no "Install as system service?"; then
            INSTALL_SERVICE=true
        else
            INSTALL_SERVICE=false
        fi
    else
        print_warning "No supported init system detected, skipping service installation"
        INSTALL_SERVICE=false
    fi

    echo ""
}

wizard_step_init_database() {
    # Step number depends on role: standalone=8, master=8, slave=9, cluster=9
    local step_num="8"
    if [[ "$SERVER_ROLE" == "slave" ]] || [[ "$SERVER_ROLE" == "cluster" ]]; then
        step_num="9"
    fi

    wizard_step_header "$step_num" "Database Initialization"

    echo "  FlyDB can initialize a default database during installation:"
    echo ""
    echo -e "  ${DIM}• Creates a 'default' database ready for use${RESET}"
    echo -e "  ${DIM}• You can create additional databases later with CREATE DATABASE${RESET}"
    echo -e "  ${DIM}• Skip this if you want to start with an empty data directory${RESET}"
    echo ""

    if prompt_yes_no "Initialize a default database?" "n"; then
        INIT_DATABASE=true
        print_success "Will initialize default database"
    else
        INIT_DATABASE=false
        print_info "Skipping database initialization"
    fi

    echo ""
}

# =============================================================================
# Main Interactive Wizard
# =============================================================================

run_interactive_wizard() {
    # Print welcome message
    print_welcome_message

    echo -e "Press ${CYAN}Enter${RESET} to accept default values shown in [brackets]."
    echo -e "Press ${CYAN}Ctrl+C${RESET} to cancel at any time."
    echo ""

    # Check for existing installation
    local existing
    if existing=$(check_existing_installation 2>/dev/null); then
        local existing_path existing_ver
        existing_path=$(echo "$existing" | cut -d: -f1)
        existing_ver=$(echo "$existing" | cut -d: -f2)

        print_warning "Existing FlyDB installation detected"
        print_kv "Location" "$existing_path"
        print_kv "Version" "$existing_ver"
        echo ""

        if ! prompt_yes_no "Would you like to upgrade/reinstall?"; then
            echo ""
            print_info "Installation cancelled"
            exit 0
        fi
        echo ""
    fi

    # Run wizard steps
    wizard_step_installation_dir
    wizard_step_server_role
    wizard_step_network_ports

    # Role-specific configuration
    wizard_step_slave_config
    wizard_step_cluster_config

    # Common configuration
    wizard_step_storage
    wizard_step_security
    wizard_step_logging
    wizard_step_service
    wizard_step_init_database

    # Configuration file
    echo ""
    if prompt_yes_no "Create configuration file with these settings?" "y"; then
        CREATE_CONFIG=true
    else
        CREATE_CONFIG=false
    fi

    # Summary
    print_installation_summary

    # Confirmation
    echo ""
    if ! prompt_yes_no "Proceed with installation?"; then
        echo ""
        print_info "Installation cancelled"
        exit 0
    fi
    echo ""
}

print_installation_summary() {
    echo ""
    echo -e "${CYAN}${BOLD}Installation Summary${RESET}"
    separator 60
    echo ""

    local version="${SPECIFIC_VERSION:-$FLYDB_VERSION}"
    version="${version#v}"

    # System Information
    echo -e "  ${BOLD}System Information${RESET}"
    separator 60
    print_kv "FlyDB Version" "$version"
    print_kv "Operating System" "$OS ($DISTRO)"
    print_kv "Architecture" "$ARCH"
    print_kv "Install Directory" "${PREFIX}/bin"
    if [[ "$RESOLVED_INSTALL_MODE" == "source" ]]; then
        print_kv "Install Method" "${CYAN}Build from source${RESET}"
    else
        print_kv "Install Method" "${CYAN}Download binaries${RESET}"
    fi
    echo ""

    # Server Configuration
    echo -e "  ${BOLD}Server Configuration${RESET}"
    separator 60

    # Role with color coding
    local role_display
    case "$SERVER_ROLE" in
        standalone) role_display="${GREEN}Standalone${RESET}" ;;
        master) role_display="${BLUE}Master${RESET}" ;;
        slave) role_display="${YELLOW}Slave${RESET}" ;;
        cluster) role_display="${MAGENTA}Cluster${RESET}" ;;
        *) role_display="$SERVER_ROLE" ;;
    esac
    print_kv "Server Role" "$role_display"

    # Network ports
    print_kv "Server Port" "$PORT"
    if [[ "$SERVER_ROLE" == "master" ]] || [[ "$SERVER_ROLE" == "cluster" ]]; then
        print_kv "Replication Port" "$REPL_PORT"
    fi
    if [[ "$SERVER_ROLE" == "cluster" ]]; then
        print_kv "Cluster Port" "$CLUSTER_PORT"
    fi

    # Role-specific settings
    if [[ "$SERVER_ROLE" == "slave" ]]; then
        print_kv "Master Address" "$MASTER_ADDR"
    fi
    echo ""

    # Cluster Configuration (if applicable)
    if [[ "$SERVER_ROLE" == "cluster" ]]; then
        echo -e "  ${BOLD}Cluster Configuration${RESET}"
        separator 60

        # Show cluster mode (bootstrap vs join)
        if [[ "$CLUSTER_BOOTSTRAP" == "true" ]] || [[ -z "$CLUSTER_PEERS" ]]; then
            print_kv "Cluster Mode" "${GREEN}Bootstrap${RESET} (first node, becomes leader)"
        else
            print_kv "Cluster Mode" "${BLUE}Join${RESET} (connecting to existing cluster)"
        fi

        if [[ -n "$CLUSTER_PEERS" ]]; then
            print_kv "Seed Nodes" "$CLUSTER_PEERS"
        else
            print_kv "Seed Nodes" "${DIM}None (single-node bootstrap)${RESET}"
        fi

        local repl_mode_display
        case "$REPLICATION_MODE" in
            async) repl_mode_display="${GREEN}Async${RESET} (best performance)" ;;
            semi_sync) repl_mode_display="${YELLOW}Semi-sync${RESET} (balanced)" ;;
            sync) repl_mode_display="${RED}Sync${RESET} (strongest consistency)" ;;
            *) repl_mode_display="$REPLICATION_MODE" ;;
        esac
        print_kv "Replication Mode" "$repl_mode_display"
        print_kv "Heartbeat Interval" "${HEARTBEAT_INTERVAL}ms"
        print_kv "Heartbeat Timeout" "${HEARTBEAT_TIMEOUT}ms"
        print_kv "Election Timeout" "${ELECTION_TIMEOUT}ms"
        if [[ "$MIN_QUORUM" == "0" ]]; then
            print_kv "Min Quorum" "Auto"
        else
            print_kv "Min Quorum" "$MIN_QUORUM"
        fi
        print_kv "Partition Count" "$PARTITION_COUNT"
        print_kv "Replication Factor" "$REPLICATION_FACTOR"
        echo ""
    fi

    # Storage Configuration
    echo -e "  ${BOLD}Storage Configuration${RESET}"
    separator 60
    print_kv "Data Directory" "$DATA_DIR"
    if [[ "$BUFFER_POOL_SIZE" == "0" ]]; then
        print_kv "Buffer Pool Size" "Auto"
    else
        print_kv "Buffer Pool Size" "${BUFFER_POOL_SIZE} pages"
    fi
    echo ""

    # Security Configuration
    echo -e "  ${BOLD}Security Configuration${RESET}"
    separator 60
    if [[ "$ENCRYPTION_ENABLED" == "true" ]]; then
        print_kv "Encryption" "${GREEN}Enabled${RESET} (AES-256-GCM)"
        if [[ -n "$ENCRYPTION_PASSPHRASE" ]]; then
            print_kv "Passphrase" "${GREEN}Set${RESET}"
        else
            print_kv "Passphrase" "${YELLOW}Not set${RESET}"
        fi
    else
        print_kv "Encryption" "${YELLOW}Disabled${RESET}"
    fi
    echo ""

    # Logging Configuration
    echo -e "  ${BOLD}Logging Configuration${RESET}"
    separator 60
    local log_level_display
    case "$LOG_LEVEL" in
        debug) log_level_display="${GREEN}Debug${RESET}" ;;
        info) log_level_display="${CYAN}Info${RESET}" ;;
        warn) log_level_display="${YELLOW}Warn${RESET}" ;;
        error) log_level_display="${RED}Error${RESET}" ;;
        *) log_level_display="$LOG_LEVEL" ;;
    esac
    print_kv "Log Level" "$log_level_display"
    if [[ "$LOG_JSON" == "true" ]]; then
        print_kv "JSON Output" "${GREEN}Enabled${RESET}"
    else
        print_kv "JSON Output" "${DIM}Disabled${RESET}"
    fi
    echo ""

    # Installation Options
    echo -e "  ${BOLD}Installation Options${RESET}"
    separator 60
    if [[ "$INSTALL_SERVICE" == true ]]; then
        print_kv "System Service" "${GREEN}Yes${RESET} ($INIT_SYSTEM)"
    else
        print_kv "System Service" "${DIM}No${RESET}"
    fi
    if [[ "$CREATE_CONFIG" == true ]]; then
        print_kv "Create Config" "${GREEN}Yes${RESET}"
    else
        print_kv "Create Config" "${DIM}No${RESET}"
    fi
    if [[ "$INIT_DATABASE" == true ]]; then
        print_kv "Init Database" "${GREEN}Yes${RESET}"
    else
        print_kv "Init Database" "${DIM}No${RESET}"
    fi
    echo ""
}

# =============================================================================
# Installation Functions
# =============================================================================

# Track if we've already obtained sudo credentials
SUDO_OBTAINED=false
SUDO_KEEPALIVE_PID=""

get_sudo_cmd() {
    local target_dir="$1"

    if [[ -w "$target_dir" ]] || [[ -w "$(dirname "$target_dir")" ]]; then
        echo ""
    elif [[ $EUID -eq 0 ]]; then
        echo ""
    else
        echo "sudo"
    fi
}

# Check if sudo will be needed for the installation
needs_sudo() {
    local bin_dir="${PREFIX}/bin"
    local config_dir

    if [[ $EUID -eq 0 ]]; then
        return 1  # Running as root, no sudo needed
    fi

    # Check if we can write to bin directory or its parent
    if [[ -d "$bin_dir" ]]; then
        [[ ! -w "$bin_dir" ]] && return 0
    else
        [[ ! -w "$(dirname "$bin_dir")" ]] && return 0
    fi

    # Check config directory if we're creating config
    if [[ "$CREATE_CONFIG" == true ]]; then
        if [[ "$PREFIX" == "/usr/local" ]]; then
            config_dir="/etc/flydb"
            [[ ! -w "/etc" ]] && return 0
        fi
    fi

    # Check service installation
    if [[ "$INSTALL_SERVICE" == true ]]; then
        if [[ "$INIT_SYSTEM" == "systemd" ]]; then
            [[ ! -w "/etc/systemd/system" ]] && return 0
        fi
    fi

    return 1  # No sudo needed
}

# Stop the sudo keepalive background process
stop_sudo_keepalive() {
    if [[ -n "$SUDO_KEEPALIVE_PID" ]] && kill -0 "$SUDO_KEEPALIVE_PID" 2>/dev/null; then
        kill "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
        wait "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
        SUDO_KEEPALIVE_PID=""
    fi
}

# Obtain sudo credentials upfront if needed
obtain_sudo_if_needed() {
    if [[ "$SUDO_OBTAINED" == true ]]; then
        return 0
    fi

    if needs_sudo; then
        echo ""
        print_info "This installation requires elevated privileges (sudo)"
        echo ""

        # Prompt for sudo password before any spinners start
        if sudo -v; then
            SUDO_OBTAINED=true

            # Start a background process to keep sudo credentials alive
            # Use a simple approach that doesn't interfere with the main script
            local parent_pid=$$
            (
                # Disable errexit in subshell to prevent premature exit
                set +e
                while true; do
                    sleep 50
                    # Check if parent is still running
                    if ! kill -0 "$parent_pid" 2>/dev/null; then
                        exit 0
                    fi
                    # Refresh sudo credentials silently
                    sudo -n true 2>/dev/null || true
                done
            ) &
            SUDO_KEEPALIVE_PID=$!
            # Disown the background process so it doesn't affect script exit
            disown "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
        else
            print_error "Failed to obtain sudo privileges"
            return 1
        fi
    fi

    return 0
}

build_binaries() {
    print_step "Building FlyDB from source..."

    # Verify we're in the right directory
    if [[ ! -f "go.mod" ]] || ! grep -q "flydb" go.mod 2>/dev/null; then
        print_error "Not in FlyDB source directory. Please run from the repository root."
        exit 1
    fi

    spinner_start "Building flydb daemon"
    if go build -o flydb ./cmd/flydb 2>/dev/null; then
        spinner_success "Built flydb daemon"
        INSTALLED_FILES+=("./flydb")
    else
        spinner_error "Failed to build flydb daemon"
        exit 1
    fi

    spinner_start "Building flydb-shell client"
    if go build -o flydb-shell ./cmd/flydb-shell 2>/dev/null; then
        spinner_success "Built flydb-shell client"
        INSTALLED_FILES+=("./flydb-shell")
    else
        spinner_error "Failed to build flydb-shell client"
        exit 1
    fi

    spinner_start "Building flydb-dump utility"
    if go build -o flydb-dump ./cmd/flydb-dump 2>/dev/null; then
        spinner_success "Built flydb-dump utility"
        INSTALLED_FILES+=("./flydb-dump")
    else
        spinner_error "Failed to build flydb-dump utility"
        exit 1
    fi

    echo ""
}

install_binaries() {
    print_step "Installing binaries..."

    INSTALL_STARTED=true

    local bin_dir="${PREFIX}/bin"
    local sudo_cmd
    sudo_cmd=$(get_sudo_cmd "$bin_dir")

    # Create bin directory
    if [[ ! -d "$bin_dir" ]]; then
        spinner_start "Creating directory $bin_dir"
        if $sudo_cmd mkdir -p "$bin_dir" 2>/dev/null; then
            spinner_success "Created $bin_dir"
            CREATED_DIRS+=("$bin_dir")
        else
            spinner_error "Failed to create $bin_dir"
            exit 1
        fi
    else
        print_substep "Directory exists: $bin_dir"
    fi

    # Install flydb
    spinner_start "Installing flydb"
    if $sudo_cmd cp flydb "$bin_dir/" && $sudo_cmd chmod +x "$bin_dir/flydb"; then
        spinner_success "Installed ${bin_dir}/flydb"
        INSTALLED_FILES+=("$bin_dir/flydb")
    else
        spinner_error "Failed to install flydb"
        rollback
        exit 1
    fi

    # Install flydb-shell
    spinner_start "Installing flydb-shell"
    if $sudo_cmd cp flydb-shell "$bin_dir/" && $sudo_cmd chmod +x "$bin_dir/flydb-shell"; then
        spinner_success "Installed ${bin_dir}/flydb-shell"
        INSTALLED_FILES+=("$bin_dir/flydb-shell")
    else
        spinner_error "Failed to install flydb-shell"
        rollback
        exit 1
    fi

    # Install flydb-dump
    spinner_start "Installing flydb-dump"
    if $sudo_cmd cp flydb-dump "$bin_dir/" && $sudo_cmd chmod +x "$bin_dir/flydb-dump"; then
        spinner_success "Installed ${bin_dir}/flydb-dump"
        INSTALLED_FILES+=("$bin_dir/flydb-dump")
    else
        spinner_error "Failed to install flydb-dump"
        rollback
        exit 1
    fi

    # Create fsql symlink for convenience
    spinner_start "Creating fsql symlink"
    if $sudo_cmd ln -sf "$bin_dir/flydb-shell" "$bin_dir/fsql"; then
        spinner_success "Created ${bin_dir}/fsql symlink"
        INSTALLED_FILES+=("$bin_dir/fsql")
    else
        spinner_error "Failed to create fsql symlink"
    fi

    # Create fdump symlink for convenience
    spinner_start "Creating fdump symlink"
    if $sudo_cmd ln -sf "$bin_dir/flydb-dump" "$bin_dir/fdump"; then
        spinner_success "Created ${bin_dir}/fdump symlink"
        INSTALLED_FILES+=("$bin_dir/fdump")
    else
        spinner_error "Failed to create fdump symlink"
    fi

    echo ""
}

create_config_file() {
    if [[ "$CREATE_CONFIG" != true ]]; then
        return
    fi

    print_step "Creating configuration file..."

    local config_dir
    local sudo_cmd

    if [[ $EUID -eq 0 ]] || [[ "$PREFIX" == "/usr/local" ]]; then
        config_dir="/etc/flydb"
        sudo_cmd=$(get_sudo_cmd "$config_dir")
    else
        config_dir="$HOME/.config/flydb"
        sudo_cmd=""
    fi

    # Use configured data directory or set default
    if [[ -z "$DATA_DIR" ]]; then
        if [[ $EUID -eq 0 ]] || [[ "$PREFIX" == "/usr/local" ]]; then
            DATA_DIR="/var/lib/flydb"
        else
            DATA_DIR="$HOME/.local/share/flydb"
        fi
    fi

    # Create data directory if it doesn't exist
    if [[ ! -d "$DATA_DIR" ]]; then
        spinner_start "Creating data directory"
        if $sudo_cmd mkdir -p "$DATA_DIR" 2>/dev/null; then
            spinner_success "Created $DATA_DIR"
            CREATED_DIRS+=("$DATA_DIR")
        else
            spinner_error "Failed to create data directory"
            return 1
        fi
    fi

    if [[ ! -d "$config_dir" ]]; then
        spinner_start "Creating config directory"
        if $sudo_cmd mkdir -p "$config_dir" 2>/dev/null; then
            spinner_success "Created $config_dir"
            CREATED_DIRS+=("$config_dir")
        else
            spinner_error "Failed to create config directory"
            return 1
        fi
    fi

    local config_file="$config_dir/flydb.conf"

    if [[ -f "$config_file" ]]; then
        print_warning "Configuration file already exists: $config_file"
        if [[ "$AUTO_CONFIRM" != true ]]; then
            if prompt_yes_no "Overwrite existing configuration?" "n"; then
                print_info "Backing up existing config to ${config_file}.bak"
                $sudo_cmd cp "$config_file" "${config_file}.bak" 2>/dev/null || true
            else
                print_substep "Skipping config creation to preserve existing settings"
                return 0
            fi
        else
            print_substep "Skipping config creation to preserve existing settings"
            return 0
        fi
    fi

    spinner_start "Writing configuration file"

    # Build cluster peers array for TOML
    local cluster_peers_toml="[]"
    if [[ -n "$CLUSTER_PEERS" ]]; then
        local peers_array=""
        IFS=',' read -ra PEER_ARRAY <<< "$CLUSTER_PEERS"
        for peer in "${PEER_ARRAY[@]}"; do
            peer="${peer//[[:space:]]/}"
            if [[ -n "$peer" ]]; then
                if [[ -n "$peers_array" ]]; then
                    peers_array="${peers_array}, \"${peer}\""
                else
                    peers_array="\"${peer}\""
                fi
            fi
        done
        cluster_peers_toml="[${peers_array}]"
    fi

    # Build master_addr line
    local master_addr_line=""
    if [[ "$SERVER_ROLE" == "slave" ]] && [[ -n "$MASTER_ADDR" ]]; then
        master_addr_line="master_addr = \"${MASTER_ADDR}\""
    else
        master_addr_line="# master_addr = \"localhost:9999\""
    fi

    local config_content="# FlyDB Configuration File
# Generated by install.sh on $(date)
# Installation type: ${SERVER_ROLE} mode
#
# Configuration Precedence (highest to lowest):
#   1. Command-line flags
#   2. Environment variables (FLYDB_*)
#   3. This configuration file
#   4. Default values
#
# For full documentation, see: https://flydb.dev/docs/configuration

# =============================================================================
# Server Configuration
# =============================================================================

# Server role: standalone, master, slave, or cluster
# - standalone: Single server mode (no replication)
# - master: Leader node that accepts writes and replicates to slaves
# - slave: Follower node that receives replication from master
# - cluster: Automatic failover cluster with leader election
role = \"${SERVER_ROLE}\"

# =============================================================================
# Network Configuration
# =============================================================================

# Server port (binary protocol for fsql client connections)
port = ${PORT}

# Replication port (for master/cluster modes)
replication_port = ${REPL_PORT}

# Cluster communication port (for cluster mode)
cluster_port = ${CLUSTER_PORT}

# Master address for slave mode (format: host:port)
${master_addr_line}

# =============================================================================
# Storage Configuration
# =============================================================================

# Data directory for multi-database storage
# Each database is stored in a separate subdirectory
data_dir = \"${DATA_DIR}\"

# Buffer pool size in pages (0 = auto-size based on available memory)
# Each page is 4KB, so 1000 pages = 4MB
buffer_pool_size = ${BUFFER_POOL_SIZE}

# Checkpoint interval in seconds (0 = disabled)
checkpoint_secs = ${CHECKPOINT_SECS}

# =============================================================================
# Security Configuration
# =============================================================================

# Enable data-at-rest encryption (AES-256-GCM)
# When enabled, set FLYDB_ENCRYPTION_PASSPHRASE environment variable
encryption_enabled = ${ENCRYPTION_ENABLED}

# Note: Encryption passphrase should be set via environment variable
# for security reasons, not in this file:
#   export FLYDB_ENCRYPTION_PASSPHRASE=\"your-secure-passphrase\"

# =============================================================================
# Logging Configuration
# =============================================================================

# Log level: debug, info, warn, error
log_level = \"${LOG_LEVEL}\"

# Enable JSON-formatted log output (useful for log aggregation)
log_json = ${LOG_JSON}

# =============================================================================
# Cluster Configuration (for role = \"cluster\")
# =============================================================================

# Comma-separated list of peer node addresses (host:port)
cluster_peers = ${cluster_peers_toml}

# Replication mode: async, semi_sync, or sync
# - async: Best performance, eventual consistency
# - semi_sync: At least one replica acknowledges before commit
# - sync: All replicas must acknowledge (strongest consistency)
replication_mode = \"${REPLICATION_MODE}\"

# Heartbeat interval in milliseconds (how often to send heartbeats)
heartbeat_interval_ms = ${HEARTBEAT_INTERVAL}

# Heartbeat timeout in milliseconds (when to consider a node dead)
heartbeat_timeout_ms = ${HEARTBEAT_TIMEOUT}

# Election timeout in milliseconds (when to start a new leader election)
election_timeout_ms = ${ELECTION_TIMEOUT}

# Minimum quorum size for cluster decisions (0 = auto-calculate majority)
min_quorum = ${MIN_QUORUM}

# Enable pre-vote protocol to prevent disruptions from partitioned nodes
enable_pre_vote = ${ENABLE_PRE_VOTE}

# Number of data partitions (must be power of 2, range: 16-4096)
partition_count = ${PARTITION_COUNT}

# Number of replicas per partition (range: 1-5)
replication_factor = ${REPLICATION_FACTOR}

# Sync timeout in milliseconds (for sync replication mode)
sync_timeout_ms = ${SYNC_TIMEOUT}

# Maximum replication lag in milliseconds before replica is unhealthy
max_replication_lag_ms = ${MAX_REPLICATION_LAG}
"

    if echo "$config_content" | $sudo_cmd tee "$config_file" >/dev/null 2>&1; then
        spinner_success "Created $config_file"
        INSTALLED_FILES+=("$config_file")
    else
        spinner_error "Failed to create configuration file"
        return 1
    fi

    echo ""
}

install_systemd_service() {
    if [[ "$INSTALL_SERVICE" != true ]] || [[ "$INIT_SYSTEM" != "systemd" ]]; then
        return
    fi

    print_step "Installing systemd service..."

    local service_file="/etc/systemd/system/flydb.service"
    local sudo_cmd
    sudo_cmd=$(get_sudo_cmd "/etc/systemd/system")

    if [[ -f "$service_file" ]]; then
        print_warning "Service file already exists: $service_file"
        if ! prompt_yes_no "Overwrite existing service file?" "n"; then
            print_substep "Skipping service installation"
            return 0
        fi
    fi

    local service_content="[Unit]
Description=FlyDB Database Server
Documentation=https://flydb.dev/docs
After=network.target

[Service]
Type=simple
User=flydb
Group=flydb
ExecStart=${PREFIX}/bin/flydb
Restart=on-failure
RestartSec=5
LimitNOFILE=65535

# Security settings
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/flydb

[Install]
WantedBy=multi-user.target
"

    spinner_start "Creating systemd service"
    if echo "$service_content" | $sudo_cmd tee "$service_file" >/dev/null 2>&1; then
        spinner_success "Created $service_file"
        INSTALLED_FILES+=("$service_file")
    else
        spinner_error "Failed to create service file"
        return 1
    fi

    # Create flydb user if it doesn't exist
    if ! id flydb &>/dev/null; then
        spinner_start "Creating flydb system user"
        if $sudo_cmd useradd --system --no-create-home --shell /usr/sbin/nologin flydb 2>/dev/null; then
            spinner_success "Created flydb user"
        else
            spinner_error "Failed to create flydb user"
        fi
    fi

    # Create data directory
    local data_dir="/var/lib/flydb"
    if [[ ! -d "$data_dir" ]]; then
        spinner_start "Creating data directory"
        if $sudo_cmd mkdir -p "$data_dir" && $sudo_cmd chown flydb:flydb "$data_dir" 2>/dev/null; then
            spinner_success "Created $data_dir"
            CREATED_DIRS+=("$data_dir")
        else
            spinner_error "Failed to create data directory"
        fi
    fi

    # Reload systemd
    spinner_start "Reloading systemd"
    if $sudo_cmd systemctl daemon-reload 2>/dev/null; then
        spinner_success "Reloaded systemd"
    else
        spinner_error "Failed to reload systemd"
    fi

    echo ""
}

install_launchd_service() {
    if [[ "$INSTALL_SERVICE" != true ]] || [[ "$INIT_SYSTEM" != "launchd" ]]; then
        return
    fi

    print_step "Installing launchd service..."

    local plist_dir
    local plist_file
    local sudo_cmd

    if [[ $EUID -eq 0 ]]; then
        plist_dir="/Library/LaunchDaemons"
        plist_file="$plist_dir/io.flydb.flydb.plist"
        sudo_cmd=""
    else
        plist_dir="$HOME/Library/LaunchAgents"
        plist_file="$plist_dir/io.flydb.flydb.plist"
        sudo_cmd=""
        mkdir -p "$plist_dir"
    fi

    if [[ -f "$plist_file" ]]; then
        print_warning "Plist file already exists: $plist_file"
        if ! prompt_yes_no "Overwrite existing plist file?" "n"; then
            print_substep "Skipping service installation"
            return 0
        fi
    fi

    local plist_content="<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">
<plist version=\"1.0\">
<dict>
    <key>Label</key>
    <string>io.flydb.flydb</string>
    <key>ProgramArguments</key>
    <array>
        <string>${PREFIX}/bin/flydb</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardErrorPath</key>
    <string>/var/log/flydb/error.log</string>
    <key>StandardOutPath</key>
    <string>/var/log/flydb/output.log</string>
</dict>
</plist>
"

    spinner_start "Creating launchd plist"
    if echo "$plist_content" | $sudo_cmd tee "$plist_file" >/dev/null 2>&1; then
        spinner_success "Created $plist_file"
        INSTALLED_FILES+=("$plist_file")
    else
        spinner_error "Failed to create plist file"
        return 1
    fi

    # Create log directory
    local log_dir="/var/log/flydb"
    if [[ ! -d "$log_dir" ]]; then
        spinner_start "Creating log directory"
        local log_sudo
        log_sudo=$(get_sudo_cmd "$log_dir")
        if $log_sudo mkdir -p "$log_dir" 2>/dev/null; then
            spinner_success "Created $log_dir"
            CREATED_DIRS+=("$log_dir")
        else
            print_warning "Could not create log directory: $log_dir"
        fi
    fi

    echo ""
}

verify_installation() {
    print_step "Verifying installation..."

    local bin_dir="${PREFIX}/bin"
    local errors=0

    # Check flydb binary
    if [[ -x "$bin_dir/flydb" ]]; then
        local version
        version=$("$bin_dir/flydb" --version 2>/dev/null | head -1 || echo "unknown")
        print_substep "${GREEN}${ICON_SUCCESS}${RESET} flydb: $version"
    else
        print_substep "${RED}${ICON_ERROR}${RESET} flydb: not found or not executable"
        ((errors++))
    fi

    # Check flydb-shell binary
    if [[ -x "$bin_dir/flydb-shell" ]]; then
        local version
        version=$("$bin_dir/flydb-shell" --version 2>/dev/null | head -1 || echo "unknown")
        print_substep "${GREEN}${ICON_SUCCESS}${RESET} flydb-shell: $version"
    else
        print_substep "${RED}${ICON_ERROR}${RESET} flydb-shell: not found or not executable"
        ((errors++))
    fi

    # Check flydb-dump binary
    if [[ -x "$bin_dir/flydb-dump" ]]; then
        local version
        version=$("$bin_dir/flydb-dump" --version 2>/dev/null | head -1 || echo "unknown")
        print_substep "${GREEN}${ICON_SUCCESS}${RESET} flydb-dump: $version"
    else
        print_substep "${RED}${ICON_ERROR}${RESET} flydb-dump: not found or not executable"
        ((errors++))
    fi

    # Check fsql symlink
    if [[ -x "$bin_dir/fsql" ]]; then
        print_substep "${GREEN}${ICON_SUCCESS}${RESET} fsql: symlink OK"
    else
        print_substep "${YELLOW}${ICON_WARNING}${RESET} fsql: symlink not found"
    fi

    # Check fdump symlink
    if [[ -x "$bin_dir/fdump" ]]; then
        print_substep "${GREEN}${ICON_SUCCESS}${RESET} fdump: symlink OK"
    else
        print_substep "${YELLOW}${ICON_WARNING}${RESET} fdump: symlink not found"
    fi

    echo ""

    if [[ $errors -gt 0 ]]; then
        print_error "Installation verification failed"
        return 1
    fi

    return 0
}

# =============================================================================
# Uninstallation (delegates to standalone uninstall.sh)
# =============================================================================

run_uninstall() {
    # Build arguments to pass to uninstall.sh
    local uninstall_args=()
    [[ "$AUTO_CONFIRM" == true ]] && uninstall_args+=("--yes")
    [[ -n "$PREFIX" ]] && uninstall_args+=("--prefix" "$PREFIX")

    # Try to find uninstall.sh in common locations
    local uninstall_script=""
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

    for path in "$script_dir/uninstall.sh" "/usr/local/share/flydb/uninstall.sh" "$HOME/.local/share/flydb/uninstall.sh"; do
        if [[ -f "$path" ]]; then
            uninstall_script="$path"
            break
        fi
    done

    if [[ -z "$uninstall_script" ]]; then
        # Download uninstall.sh
        print_info "Downloading uninstall script..."
        local temp_uninstall
        temp_uninstall=$(mktemp)

        if curl -fsSL "https://raw.githubusercontent.com/${GITHUB_REPO}/main/uninstall.sh" -o "$temp_uninstall" 2>/dev/null ||
           wget -qO "$temp_uninstall" "https://raw.githubusercontent.com/${GITHUB_REPO}/main/uninstall.sh" 2>/dev/null; then
            chmod +x "$temp_uninstall"
            uninstall_script="$temp_uninstall"
        else
            print_error "Could not find or download uninstall.sh"
            echo -e "  ${DIM}Run manually: curl -fsSL https://raw.githubusercontent.com/${GITHUB_REPO}/main/uninstall.sh | bash${RESET}"
            return 1
        fi
    fi

    exec bash "$uninstall_script" "${uninstall_args[@]}"
}

# =============================================================================
# Rollback
# =============================================================================

rollback() {
    if [[ "$INSTALL_STARTED" != true ]]; then
        return
    fi

    echo ""
    print_warning "Rolling back installation..."

    # Remove installed files
    for file in "${INSTALLED_FILES[@]}"; do
        if [[ -f "$file" ]]; then
            local sudo_cmd
            sudo_cmd=$(get_sudo_cmd "$(dirname "$file")")
            $sudo_cmd rm -f "$file" 2>/dev/null && print_substep "Removed $file"
        fi
    done

    # Remove created directories (only if empty)
    for dir in "${CREATED_DIRS[@]}"; do
        if [[ -d "$dir" ]] && [[ -z "$(ls -A "$dir")" ]]; then
            local sudo_cmd
            sudo_cmd=$(get_sudo_cmd "$dir")
            $sudo_cmd rmdir "$dir" 2>/dev/null && print_substep "Removed $dir"
        fi
    done

    print_info "Rollback complete"
}

# =============================================================================
# Post-Installation
# =============================================================================

print_post_install() {
    echo ""
    echo -e "${GREEN}${BOLD}${ICON_SUCCESS} Installation Complete!${RESET}"
    separator 60
    echo ""

    local bin_dir="${PREFIX}/bin"
    local in_path=false

    # Check if bin_dir is in PATH
    if echo "$PATH" | tr ':' '\n' | grep -q "^${bin_dir}$"; then
        in_path=true
    fi

    echo -e "${BOLD}Next Steps:${RESET}"
    echo ""

    local step_num=1

    # Step: Add to PATH (only if not already in PATH)
    if [[ "$in_path" != true ]]; then
        echo -e "  ${YELLOW}${step_num}. Add FlyDB to your PATH:${RESET}"
        echo ""
        echo -e "     ${DIM}# Add to ~/.bashrc or ~/.zshrc:${RESET}"
        echo -e "     ${CYAN}export PATH=\"${bin_dir}:\$PATH\"${RESET}"
        echo ""
        ((step_num++))
    fi

    # Step: Start FlyDB
    echo -e "  ${YELLOW}${step_num}. Start FlyDB:${RESET}"
    echo ""
    echo -e "     ${DIM}# Interactive wizard (first-time setup):${RESET}"
    if [[ "$in_path" == true ]]; then
        echo -e "     ${CYAN}flydb${RESET}"
    else
        echo -e "     ${CYAN}${bin_dir}/flydb${RESET}"
    fi
    echo ""
    echo -e "     ${DIM}# Or with command-line options:${RESET}"
    if [[ "$in_path" == true ]]; then
        echo -e "     ${CYAN}flydb -port 8889 -role standalone${RESET}"
    else
        echo -e "     ${CYAN}${bin_dir}/flydb -port 8889 -role standalone${RESET}"
    fi
    ((step_num++))

    # Step: Manage service (only if service was installed)
    if [[ "$INSTALL_SERVICE" == true ]]; then
        echo ""
        echo -e "  ${YELLOW}${step_num}. Manage the service:${RESET}"
        echo ""

        if [[ "$INIT_SYSTEM" == "systemd" ]]; then
            echo -e "     ${CYAN}sudo systemctl start flydb${RESET}    # Start the service"
            echo -e "     ${CYAN}sudo systemctl enable flydb${RESET}   # Enable at boot"
            echo -e "     ${CYAN}sudo systemctl status flydb${RESET}   # Check status"
        elif [[ "$INIT_SYSTEM" == "launchd" ]]; then
            if [[ $EUID -eq 0 ]]; then
                echo -e "     ${CYAN}sudo launchctl load /Library/LaunchDaemons/io.flydb.flydb.plist${RESET}"
            else
                echo -e "     ${CYAN}launchctl load ~/Library/LaunchAgents/io.flydb.flydb.plist${RESET}"
            fi
        fi
        ((step_num++))
    fi

    # Step: Connect with CLI
    echo ""
    echo -e "  ${YELLOW}${step_num}. Connect with the CLI client:${RESET}"
    echo ""
    if [[ "$in_path" == true ]]; then
        echo -e "     ${CYAN}fsql${RESET}"
    else
        echo -e "     ${CYAN}${bin_dir}/fsql${RESET}"
    fi
    ((step_num++))

    # Step: Cluster-specific instructions
    if [[ "$SERVER_ROLE" == "cluster" ]]; then
        echo ""
        echo -e "  ${YELLOW}${step_num}. Cluster Setup:${RESET}"
        echo ""

        if [[ "$CLUSTER_BOOTSTRAP" == "true" ]] || [[ -z "$CLUSTER_PEERS" ]]; then
            # Bootstrap mode - this is the first node
            local this_host
            this_host=$(hostname 2>/dev/null || echo "localhost")
            echo -e "     ${GREEN}This node is bootstrapped as the cluster leader.${RESET}"
            echo ""
            echo -e "     ${DIM}To add more nodes to this cluster, run on other machines:${RESET}"
            echo ""
            if [[ "$in_path" == true ]]; then
                echo -e "     ${CYAN}flydb -role cluster -cluster-peers ${this_host}:${CLUSTER_PORT}${RESET}"
            else
                echo -e "     ${CYAN}${bin_dir}/flydb -role cluster -cluster-peers ${this_host}:${CLUSTER_PORT}${RESET}"
            fi
            echo ""
            echo -e "     ${DIM}Or install with:${RESET}"
            echo -e "     ${CYAN}curl -sSL https://get.flydb.dev | bash -s -- --role cluster \\${RESET}"
            echo -e "     ${CYAN}     --cluster-peers ${this_host}:${CLUSTER_PORT} --yes${RESET}"
        else
            # Join mode - connecting to existing cluster
            echo -e "     ${BLUE}This node will join the cluster via: ${CLUSTER_PEERS}${RESET}"
            echo ""
            echo -e "     ${DIM}The node will automatically:${RESET}"
            echo -e "     ${DIM}• Discover other cluster members${RESET}"
            echo -e "     ${DIM}• Sync data from the cluster${RESET}"
            echo -e "     ${DIM}• Participate in leader elections${RESET}"
        fi
        ((step_num++))

        echo ""
        echo -e "  ${YELLOW}${step_num}. Monitor cluster status:${RESET}"
        echo ""
        if [[ "$in_path" == true ]]; then
            echo -e "     ${CYAN}fsql -c \"SHOW CLUSTER STATUS\"${RESET}"
        else
            echo -e "     ${CYAN}${bin_dir}/fsql -c \"SHOW CLUSTER STATUS\"${RESET}"
        fi
        ((step_num++))
    else
        # Non-cluster mode - show cluster setup as optional
        echo ""
        echo -e "  ${YELLOW}${step_num}. Set up a cluster (optional):${RESET}"
        echo ""
        echo -e "     ${DIM}# Bootstrap first cluster node:${RESET}"
        if [[ "$in_path" == true ]]; then
            echo -e "     ${CYAN}flydb -role cluster${RESET}  ${DIM}# Becomes leader${RESET}"
        else
            echo -e "     ${CYAN}${bin_dir}/flydb -role cluster${RESET}  ${DIM}# Becomes leader${RESET}"
        fi
        echo ""
        echo -e "     ${DIM}# Join existing cluster:${RESET}"
        if [[ "$in_path" == true ]]; then
            echo -e "     ${CYAN}flydb -role cluster -cluster-peers node1:9998${RESET}"
        else
            echo -e "     ${CYAN}${bin_dir}/flydb -role cluster -cluster-peers node1:9998${RESET}"
        fi
        ((step_num++))
    fi

    echo ""
    separator 60
    echo ""
    echo -e "  ${DIM}Documentation:${RESET}  https://flydb.dev/docs"
    echo -e "  ${DIM}GitHub:${RESET}         https://github.com/${GITHUB_REPO}"
    echo -e "  ${DIM}Issues:${RESET}         https://github.com/${GITHUB_REPO}/issues"
    echo ""
}

# =============================================================================
# Argument Parsing
# =============================================================================

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --prefix)
                if [[ -n "${2:-}" ]]; then
                    PREFIX=$(validate_path "$2")
                    shift 2
                else
                    print_error "--prefix requires a path argument"
                    exit 1
                fi
                ;;
            --prefix=*)
                PREFIX=$(validate_path "${1#*=}")
                shift
                ;;
            --version)
                if [[ -n "${2:-}" ]]; then
                    SPECIFIC_VERSION="$2"
                    shift 2
                else
                    print_error "--version requires a version argument"
                    exit 1
                fi
                ;;
            --version=*)
                SPECIFIC_VERSION="${1#*=}"
                shift
                ;;
            --no-service)
                INSTALL_SERVICE=false
                shift
                ;;
            --no-config)
                CREATE_CONFIG=false
                shift
                ;;
            --init-db)
                INIT_DATABASE=true
                shift
                ;;
            --from-source)
                INSTALL_MODE="source"
                shift
                ;;
            --from-binary)
                INSTALL_MODE="binary"
                shift
                ;;
            --yes|-y)
                AUTO_CONFIRM=true
                INTERACTIVE=false
                shift
                ;;
            --uninstall)
                UNINSTALL=true
                shift
                ;;
            # Server configuration options
            --role)
                if [[ -n "${2:-}" ]]; then
                    case "$2" in
                        standalone|master|slave|cluster)
                            SERVER_ROLE="$2"
                            ;;
                        *)
                            print_error "Invalid role: $2. Must be standalone, master, slave, or cluster"
                            exit 1
                            ;;
                    esac
                    shift 2
                else
                    print_error "--role requires an argument"
                    exit 1
                fi
                ;;
            --role=*)
                local role="${1#*=}"
                case "$role" in
                    standalone|master|slave|cluster)
                        SERVER_ROLE="$role"
                        ;;
                    *)
                        print_error "Invalid role: $role. Must be standalone, master, slave, or cluster"
                        exit 1
                        ;;
                esac
                shift
                ;;
            --port)
                if [[ -n "${2:-}" ]] && validate_port "$2"; then
                    PORT="$2"
                    shift 2
                else
                    print_error "--port requires a valid port number (1-65535)"
                    exit 1
                fi
                ;;
            --port=*)
                local port="${1#*=}"
                if validate_port "$port"; then
                    PORT="$port"
                else
                    print_error "--port requires a valid port number (1-65535)"
                    exit 1
                fi
                shift
                ;;
            --repl-port)
                if [[ -n "${2:-}" ]] && validate_port "$2"; then
                    REPL_PORT="$2"
                    shift 2
                else
                    print_error "--repl-port requires a valid port number (1-65535)"
                    exit 1
                fi
                ;;
            --repl-port=*)
                local port="${1#*=}"
                if validate_port "$port"; then
                    REPL_PORT="$port"
                else
                    print_error "--repl-port requires a valid port number (1-65535)"
                    exit 1
                fi
                shift
                ;;
            --cluster-port)
                if [[ -n "${2:-}" ]] && validate_port "$2"; then
                    CLUSTER_PORT="$2"
                    shift 2
                else
                    print_error "--cluster-port requires a valid port number (1-65535)"
                    exit 1
                fi
                ;;
            --cluster-port=*)
                local port="${1#*=}"
                if validate_port "$port"; then
                    CLUSTER_PORT="$port"
                else
                    print_error "--cluster-port requires a valid port number (1-65535)"
                    exit 1
                fi
                shift
                ;;
            --master-addr)
                if [[ -n "${2:-}" ]]; then
                    MASTER_ADDR="$2"
                    shift 2
                else
                    print_error "--master-addr requires a host:port argument"
                    exit 1
                fi
                ;;
            --master-addr=*)
                MASTER_ADDR="${1#*=}"
                shift
                ;;
            --data-dir)
                if [[ -n "${2:-}" ]]; then
                    DATA_DIR=$(validate_path "$2")
                    shift 2
                else
                    print_error "--data-dir requires a path argument"
                    exit 1
                fi
                ;;
            --data-dir=*)
                DATA_DIR=$(validate_path "${1#*=}")
                shift
                ;;
            # Cluster options
            --cluster-bootstrap)
                CLUSTER_BOOTSTRAP="true"
                shift
                ;;
            --cluster-peers)
                if [[ -n "${2:-}" ]]; then
                    CLUSTER_PEERS="$2"
                    CLUSTER_BOOTSTRAP="false"  # Joining existing cluster
                    shift 2
                else
                    print_error "--cluster-peers requires a comma-separated list of addresses"
                    exit 1
                fi
                ;;
            --cluster-peers=*)
                CLUSTER_PEERS="${1#*=}"
                CLUSTER_BOOTSTRAP="false"  # Joining existing cluster
                shift
                ;;
            --replication-mode)
                if [[ -n "${2:-}" ]]; then
                    case "$2" in
                        async|semi_sync|sync)
                            REPLICATION_MODE="$2"
                            ;;
                        *)
                            print_error "Invalid replication mode: $2. Must be async, semi_sync, or sync"
                            exit 1
                            ;;
                    esac
                    shift 2
                else
                    print_error "--replication-mode requires an argument"
                    exit 1
                fi
                ;;
            --replication-mode=*)
                local mode="${1#*=}"
                case "$mode" in
                    async|semi_sync|sync)
                        REPLICATION_MODE="$mode"
                        ;;
                    *)
                        print_error "Invalid replication mode: $mode. Must be async, semi_sync, or sync"
                        exit 1
                        ;;
                esac
                shift
                ;;
            --heartbeat-interval)
                if [[ -n "${2:-}" ]] && [[ "$2" =~ ^[0-9]+$ ]]; then
                    HEARTBEAT_INTERVAL="$2"
                    shift 2
                else
                    print_error "--heartbeat-interval requires a number in milliseconds"
                    exit 1
                fi
                ;;
            --heartbeat-interval=*)
                local val="${1#*=}"
                if [[ "$val" =~ ^[0-9]+$ ]]; then
                    HEARTBEAT_INTERVAL="$val"
                else
                    print_error "--heartbeat-interval requires a number in milliseconds"
                    exit 1
                fi
                shift
                ;;
            --heartbeat-timeout)
                if [[ -n "${2:-}" ]] && [[ "$2" =~ ^[0-9]+$ ]]; then
                    HEARTBEAT_TIMEOUT="$2"
                    shift 2
                else
                    print_error "--heartbeat-timeout requires a number in milliseconds"
                    exit 1
                fi
                ;;
            --heartbeat-timeout=*)
                local val="${1#*=}"
                if [[ "$val" =~ ^[0-9]+$ ]]; then
                    HEARTBEAT_TIMEOUT="$val"
                else
                    print_error "--heartbeat-timeout requires a number in milliseconds"
                    exit 1
                fi
                shift
                ;;
            --election-timeout)
                if [[ -n "${2:-}" ]] && [[ "$2" =~ ^[0-9]+$ ]]; then
                    ELECTION_TIMEOUT="$2"
                    shift 2
                else
                    print_error "--election-timeout requires a number in milliseconds"
                    exit 1
                fi
                ;;
            --election-timeout=*)
                local val="${1#*=}"
                if [[ "$val" =~ ^[0-9]+$ ]]; then
                    ELECTION_TIMEOUT="$val"
                else
                    print_error "--election-timeout requires a number in milliseconds"
                    exit 1
                fi
                shift
                ;;
            --min-quorum)
                if [[ -n "${2:-}" ]] && [[ "$2" =~ ^[0-9]+$ ]]; then
                    MIN_QUORUM="$2"
                    shift 2
                else
                    print_error "--min-quorum requires a number"
                    exit 1
                fi
                ;;
            --min-quorum=*)
                local val="${1#*=}"
                if [[ "$val" =~ ^[0-9]+$ ]]; then
                    MIN_QUORUM="$val"
                else
                    print_error "--min-quorum requires a number"
                    exit 1
                fi
                shift
                ;;
            --partition-count)
                if [[ -n "${2:-}" ]] && [[ "$2" =~ ^[0-9]+$ ]]; then
                    PARTITION_COUNT="$2"
                    shift 2
                else
                    print_error "--partition-count requires a number"
                    exit 1
                fi
                ;;
            --partition-count=*)
                local val="${1#*=}"
                if [[ "$val" =~ ^[0-9]+$ ]]; then
                    PARTITION_COUNT="$val"
                else
                    print_error "--partition-count requires a number"
                    exit 1
                fi
                shift
                ;;
            --replication-factor)
                if [[ -n "${2:-}" ]] && [[ "$2" =~ ^[0-9]+$ ]]; then
                    REPLICATION_FACTOR="$2"
                    shift 2
                else
                    print_error "--replication-factor requires a number"
                    exit 1
                fi
                ;;
            --replication-factor=*)
                local val="${1#*=}"
                if [[ "$val" =~ ^[0-9]+$ ]]; then
                    REPLICATION_FACTOR="$val"
                else
                    print_error "--replication-factor requires a number"
                    exit 1
                fi
                shift
                ;;
            # Security options
            --encryption)
                ENCRYPTION_ENABLED="true"
                shift
                ;;
            --no-encryption)
                ENCRYPTION_ENABLED="false"
                shift
                ;;
            --encryption-passphrase)
                if [[ -n "${2:-}" ]]; then
                    ENCRYPTION_PASSPHRASE="$2"
                    ENCRYPTION_ENABLED="true"
                    shift 2
                else
                    print_error "--encryption-passphrase requires an argument"
                    exit 1
                fi
                ;;
            --encryption-passphrase=*)
                ENCRYPTION_PASSPHRASE="${1#*=}"
                ENCRYPTION_ENABLED="true"
                shift
                ;;
            # Logging options
            --log-level)
                if [[ -n "${2:-}" ]]; then
                    case "$2" in
                        debug|info|warn|error)
                            LOG_LEVEL="$2"
                            ;;
                        *)
                            print_error "Invalid log level: $2. Must be debug, info, warn, or error"
                            exit 1
                            ;;
                    esac
                    shift 2
                else
                    print_error "--log-level requires an argument"
                    exit 1
                fi
                ;;
            --log-level=*)
                local level="${1#*=}"
                case "$level" in
                    debug|info|warn|error)
                        LOG_LEVEL="$level"
                        ;;
                    *)
                        print_error "Invalid log level: $level. Must be debug, info, warn, or error"
                        exit 1
                        ;;
                esac
                shift
                ;;
            --log-json)
                LOG_JSON="true"
                shift
                ;;
            # Storage options
            --buffer-pool-size)
                if [[ -n "${2:-}" ]] && [[ "$2" =~ ^[0-9]+$ ]]; then
                    BUFFER_POOL_SIZE="$2"
                    shift 2
                else
                    print_error "--buffer-pool-size requires a number"
                    exit 1
                fi
                ;;
            --buffer-pool-size=*)
                local val="${1#*=}"
                if [[ "$val" =~ ^[0-9]+$ ]]; then
                    BUFFER_POOL_SIZE="$val"
                else
                    print_error "--buffer-pool-size requires a number"
                    exit 1
                fi
                shift
                ;;
            --checkpoint-secs)
                if [[ -n "${2:-}" ]] && [[ "$2" =~ ^[0-9]+$ ]]; then
                    CHECKPOINT_SECS="$2"
                    shift 2
                else
                    print_error "--checkpoint-secs requires a number"
                    exit 1
                fi
                ;;
            --checkpoint-secs=*)
                local val="${1#*=}"
                if [[ "$val" =~ ^[0-9]+$ ]]; then
                    CHECKPOINT_SECS="$val"
                else
                    print_error "--checkpoint-secs requires a number"
                    exit 1
                fi
                shift
                ;;
            --help|-h)
                print_help
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                echo ""
                echo "Use --help to see available options"
                exit 1
                ;;
        esac
    done

    # If any argument was provided, assume non-interactive unless it's just --uninstall
    if [[ -n "$PREFIX" ]] || [[ -n "$SPECIFIC_VERSION" ]] || \
       [[ "$INSTALL_SERVICE" == false ]] || [[ "$CREATE_CONFIG" == false ]] || \
       [[ "$INIT_DATABASE" == true ]] || [[ "$AUTO_CONFIRM" == true ]] || \
       [[ "$INSTALL_MODE" != "auto" ]] || [[ "$SERVER_ROLE" != "standalone" ]]; then
        INTERACTIVE=false
    fi
}

# =============================================================================
# Cleanup Handler
# =============================================================================

cleanup() {
    local exit_code=$?

    # Stop any running spinner
    spinner_stop

    # Stop sudo keepalive background process
    stop_sudo_keepalive

    # Clean up temporary directory
    cleanup_temp_dir

    # Only rollback if we were in the middle of installation and there was an error
    if [[ "$INSTALL_STARTED" == true ]] && [[ $exit_code -ne 0 ]]; then
        rollback
    fi
}

# =============================================================================
# Main
# =============================================================================

main() {
    # Set up signal handlers
    trap cleanup EXIT
    trap 'echo ""; print_warning "Installation cancelled"; exit 130' INT TERM

    # Print banner
    print_banner

    # Parse command-line arguments
    parse_args "$@"

    # Handle uninstall
    if [[ "$UNINSTALL" == true ]]; then
        run_uninstall
        exit 0
    fi

    # Detect system
    detect_os
    detect_arch
    detect_init_system

    # Detect installation mode (source vs binary)
    detect_install_mode

    # Set default prefix if not specified
    if [[ -z "$PREFIX" ]]; then
        PREFIX=$(get_default_prefix)
    fi

    # Run interactive wizard or show summary
    if [[ "$INTERACTIVE" == true ]]; then
        run_interactive_wizard
    else
        print_installation_summary

        if [[ "$AUTO_CONFIRM" != true ]]; then
            echo ""
            if ! prompt_yes_no "Proceed with installation?"; then
                print_info "Installation cancelled"
                exit 0
            fi
            echo ""
        fi
    fi

    # Check prerequisites
    check_prerequisites

    # Obtain sudo credentials upfront if needed (before any spinners)
    if ! obtain_sudo_if_needed; then
        exit 1
    fi

    # Build or download binaries based on installation mode
    if [[ "$RESOLVED_INSTALL_MODE" == "source" ]]; then
        # Build from source
        build_binaries
        # Install binaries from local build
        install_binaries
    else
        # Download pre-built binaries
        download_binaries
        # Install downloaded binaries
        install_downloaded_binaries
    fi

    # Create config file
    create_config_file

    # Install service
    if [[ "$INIT_SYSTEM" == "systemd" ]]; then
        install_systemd_service
    elif [[ "$INIT_SYSTEM" == "launchd" ]]; then
        install_launchd_service
    fi

    # Verify installation
    verify_installation

    # Print post-install instructions
    print_post_install
}

main "$@"
