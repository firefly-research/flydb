#!/bin/bash
#
# FlyDB Uninstallation Script
# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0
#
# A comprehensive uninstaller that removes all FlyDB components installed
# by install.sh, with an interactive and informative user experience.
#
# Usage:
#   Interactive:     ./uninstall.sh
#   Non-interactive: ./uninstall.sh --yes
#   Dry run:         ./uninstall.sh --dry-run
#   Keep config:     ./uninstall.sh --keep-config
#   Remove data:     ./uninstall.sh --remove-data
#
# Components removed:
#   - Binaries: flydb, flydb-shell, flydb-dump, fsql, fdump
#   - Services: systemd (flydb.service), launchd (io.flydb.flydb.plist)
#   - Configuration: /etc/flydb, ~/.config/flydb
#   - Data (optional): /var/lib/flydb, ~/.local/share/flydb
#

set -euo pipefail

readonly SCRIPT_VERSION="01.26.17"
readonly GITHUB_REPO="fireflyresearch/flydb"

# =============================================================================
# Configuration
# =============================================================================

PREFIX=""
AUTO_CONFIRM=false
DRY_RUN=false
KEEP_CONFIG=false
REMOVE_DATA=false
VERBOSE=false

# System detection
OS=""
INIT_SYSTEM=""

# Discovered components (arrays)
declare -a FOUND_BINARIES=()
declare -a FOUND_SERVICES=()
declare -a FOUND_CONFIGS=()
declare -a FOUND_DATA=()

# Removal tracking
declare -a REMOVED_ITEMS=()
declare -a FAILED_ITEMS=()
declare -a SKIPPED_ITEMS=()

# Sudo handling
SUDO_OBTAINED=false
SUDO_KEEPALIVE_PID=""

# =============================================================================
# Terminal Colors and Icons
# =============================================================================

setup_colors() {
    if [[ -t 1 ]] && [[ "${TERM:-}" != "dumb" ]]; then
        RED='\033[0;31m'
        GREEN='\033[0;32m'
        YELLOW='\033[0;33m'
        BLUE='\033[0;34m'
        MAGENTA='\033[0;35m'
        CYAN='\033[0;36m'
        WHITE='\033[0;37m'
        BOLD='\033[1m'
        DIM='\033[2m'
        ITALIC='\033[3m'
        RESET='\033[0m'
    else
        RED='' GREEN='' YELLOW='' BLUE='' MAGENTA='' CYAN='' WHITE=''
        BOLD='' DIM='' ITALIC='' RESET=''
    fi
}

# Icons (Unicode)
ICON_SUCCESS="✓"
ICON_ERROR="✗"
ICON_WARNING="⚠"
ICON_INFO="ℹ"
ICON_ARROW="→"
ICON_BULLET="•"
ICON_TRASH="🗑"
ICON_FOLDER="📁"
ICON_FILE="📄"
ICON_GEAR="⚙"
ICON_SHIELD="🛡"

# =============================================================================
# Output Functions
# =============================================================================

print_banner() {
    # Clear screen for a clean start (only in interactive mode)
    if [[ -t 1 ]] && [[ "$AUTO_CONFIRM" != true ]]; then
        clear 2>/dev/null || true
    fi

    echo ""
    echo -e "${RED}${BOLD}       _____.__            .______."
    echo -e "     _/ ____\\  | ___.__. __| _/\\_ |__"
    echo -e "     \\   __\\|  |<   |  |/ __ |  | __ \\"
    echo -e "      |  |  |  |_\\___  / /_/ |  | \\_\\ \\"
    echo -e "      |__|  |____/ ____\\____ |  |___  /"
    echo -e "                 \\/         \\/      \\/${RESET}"
    echo ""
    echo -e "  ${YELLOW}${BOLD}FlyDB Uninstaller${RESET} ${DIM}v${SCRIPT_VERSION}${RESET}"
    echo ""
    echo -e "  ${DIM}This tool will safely remove FlyDB from your system.${RESET}"
    echo -e "  ${DIM}You will be prompted before any destructive actions.${RESET}"
    echo ""
}

separator() {
    local width="${1:-60}"
    printf '%*s\n' "$width" '' | tr ' ' '─'
}

print_step() {
    echo ""
    echo -e "${CYAN}${BOLD}==> $1${RESET}"
}

print_substep() {
    echo -e "    ${ICON_ARROW} $1"
}

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
    echo -e "${BLUE}${ICON_INFO}${RESET} ${BLUE}$1${RESET}"
}

print_dim() {
    echo -e "${DIM}$1${RESET}"
}

# =============================================================================
# Spinner Animation
# =============================================================================

SPINNER_PID=""
SPINNER_MSG=""
spinner_frames=("⠋" "⠙" "⠹" "⠸" "⠼" "⠴" "⠦" "⠧" "⠇" "⠏")

spinner_start() {
    SPINNER_MSG="$1"
    if [[ ! -t 1 ]]; then
        echo "  $SPINNER_MSG..."
        return
    fi
    (
        set +e
        local i=0
        while true; do
            printf "\r  ${CYAN}${spinner_frames[$i]}${RESET} %s" "$SPINNER_MSG"
            i=$(( (i + 1) % ${#spinner_frames[@]} ))
            sleep 0.08
        done
    ) &
    SPINNER_PID=$!
    disown "$SPINNER_PID" 2>/dev/null || true
}

spinner_stop() {
    if [[ -n "$SPINNER_PID" ]] && kill -0 "$SPINNER_PID" 2>/dev/null; then
        kill "$SPINNER_PID" 2>/dev/null || true
        wait "$SPINNER_PID" 2>/dev/null || true
        printf "\r%*s\r" $((${#SPINNER_MSG} + 10)) ""
    fi
    SPINNER_PID=""
}

spinner_success() {
    spinner_stop
    echo -e "  ${GREEN}${ICON_SUCCESS}${RESET} $1"
    REMOVED_ITEMS+=("$1")
}

spinner_error() {
    spinner_stop
    echo -e "  ${RED}${ICON_ERROR}${RESET} $1"
    FAILED_ITEMS+=("$1")
}

spinner_skip() {
    spinner_stop
    echo -e "  ${YELLOW}${ICON_WARNING}${RESET} $1 ${DIM}(skipped)${RESET}"
    SKIPPED_ITEMS+=("$1")
}

# =============================================================================
# Interactive Prompts
# =============================================================================

prompt_yes_no() {
    local prompt_text="$1"
    local default="${2:-y}"
    local result=""

    spinner_stop

    local hint
    if [[ "$default" == "y" ]]; then
        hint="${GREEN}Y${RESET}/n"
    else
        hint="y/${RED}N${RESET}"
    fi

    echo -en "  ${BOLD}${prompt_text}${RESET} [${hint}]: "
    read -r result

    if [[ -z "$result" ]]; then
        result="$default"
    fi

    # Convert to lowercase (portable)
    result=$(echo "$result" | tr '[:upper:]' '[:lower:]')

    case "$result" in
        y|yes) return 0 ;;
        *) return 1 ;;
    esac
}

prompt_confirm_dangerous() {
    local prompt_text="$1"
    local confirm_word="${2:-DELETE}"

    spinner_stop

    echo ""
    echo -e "  ${RED}${BOLD}⚠ DANGER: $prompt_text${RESET}"
    echo -e "  ${DIM}This action cannot be undone.${RESET}"
    echo ""
    echo -en "  Type '${RED}${confirm_word}${RESET}' to confirm: "

    local result=""
    read -r result

    if [[ "$result" == "$confirm_word" ]]; then
        return 0
    else
        return 1
    fi
}

# =============================================================================
# System Detection
# =============================================================================

detect_os() {
    case "$(uname -s)" in
        Linux*)  OS="linux" ;;
        Darwin*) OS="darwin" ;;
        *)       OS="unknown" ;;
    esac
}

detect_init_system() {
    if [[ "$OS" == "darwin" ]]; then
        INIT_SYSTEM="launchd"
    elif command -v systemctl &>/dev/null && systemctl --version &>/dev/null; then
        INIT_SYSTEM="systemd"
    elif command -v rc-service &>/dev/null; then
        INIT_SYSTEM="openrc"
    else
        INIT_SYSTEM="none"
    fi
}

# =============================================================================
# Sudo Handling
# =============================================================================

get_sudo_cmd() {
    local target_path="$1"
    if [[ -w "$target_path" ]] || [[ -w "$(dirname "$target_path")" ]]; then
        echo ""
    elif [[ $EUID -eq 0 ]]; then
        echo ""
    else
        echo "sudo"
    fi
}

stop_sudo_keepalive() {
    if [[ -n "$SUDO_KEEPALIVE_PID" ]] && kill -0 "$SUDO_KEEPALIVE_PID" 2>/dev/null; then
        kill "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
        wait "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
        SUDO_KEEPALIVE_PID=""
    fi
}

obtain_sudo_if_needed() {
    if [[ "$SUDO_OBTAINED" == true ]] || [[ "$DRY_RUN" == true ]]; then
        return 0
    fi

    local needs_sudo=false

    # Check if any discovered items require sudo
    local all_items=()
    if [[ ${#FOUND_BINARIES[@]} -gt 0 ]]; then
        all_items+=("${FOUND_BINARIES[@]}")
    fi
    if [[ ${#FOUND_SERVICES[@]} -gt 0 ]]; then
        all_items+=("${FOUND_SERVICES[@]}")
    fi
    if [[ ${#FOUND_CONFIGS[@]} -gt 0 ]] && [[ "$KEEP_CONFIG" != true ]]; then
        all_items+=("${FOUND_CONFIGS[@]}")
    fi
    if [[ ${#FOUND_DATA[@]} -gt 0 ]] && [[ "$REMOVE_DATA" == true ]]; then
        all_items+=("${FOUND_DATA[@]}")
    fi

    for item in "${all_items[@]+"${all_items[@]}"}"; do
        if [[ -n "$item" ]] && [[ ! -w "$(dirname "$item")" ]]; then
            needs_sudo=true
            break
        fi
    done

    if [[ "$needs_sudo" == true ]] && [[ $EUID -ne 0 ]]; then
        echo ""
        print_info "Some operations require elevated privileges"
        echo -e "  ${DIM}You may be prompted for your password.${RESET}"
        echo ""

        if sudo -v; then
            SUDO_OBTAINED=true
            # Keep sudo alive in background
            local parent_pid=$$
            (
                set +e
                while true; do
                    sleep 50
                    kill -0 "$parent_pid" 2>/dev/null || exit 0
                    sudo -n true 2>/dev/null || true
                done
            ) &
            SUDO_KEEPALIVE_PID=$!
            disown "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
        else
            print_error "Failed to obtain sudo privileges"
            echo -e "  ${DIM}Try running with: sudo ./uninstall.sh${RESET}"
            return 1
        fi
    fi
    return 0
}

# =============================================================================
# Discovery Functions
# =============================================================================

# Get directory size in human-readable format
get_dir_size() {
    local dir="$1"
    if [[ -d "$dir" ]]; then
        du -sh "$dir" 2>/dev/null | cut -f1 || echo "?"
    else
        echo "0"
    fi
}

# Check if FlyDB service is currently running
check_service_running() {
    if [[ "$INIT_SYSTEM" == "systemd" ]]; then
        systemctl is-active --quiet flydb 2>/dev/null && return 0
    elif [[ "$INIT_SYSTEM" == "launchd" ]]; then
        launchctl list 2>/dev/null | grep -q "io.flydb.flydb" && return 0
    fi
    # Also check for running process
    pgrep -x flydb &>/dev/null && return 0
    return 1
}

find_installations() {
    print_step "Scanning for FlyDB components..."

    # Binary locations to check (matches install.sh)
    local bin_locations=("/usr/local/bin" "/usr/bin" "$HOME/.local/bin")
    if [[ -n "$PREFIX" ]]; then
        bin_locations=("${PREFIX}/bin" "${bin_locations[@]}")
    fi

    # All binaries that install.sh creates
    local binary_names=("flydb" "flydb-shell" "flydb-dump" "flydb-discover" "fsql" "fdump")

    for dir in "${bin_locations[@]}"; do
        for bin in "${binary_names[@]}"; do
            if [[ -x "$dir/$bin" ]] || [[ -L "$dir/$bin" ]]; then
                FOUND_BINARIES+=("$dir/$bin")
            fi
        done
    done

    # Service files (matches install.sh naming)
    if [[ "$INIT_SYSTEM" == "systemd" ]]; then
        if [[ -f "/etc/systemd/system/flydb.service" ]]; then
            FOUND_SERVICES+=("/etc/systemd/system/flydb.service")
        fi
    elif [[ "$INIT_SYSTEM" == "launchd" ]]; then
        if [[ -f "/Library/LaunchDaemons/io.flydb.flydb.plist" ]]; then
            FOUND_SERVICES+=("/Library/LaunchDaemons/io.flydb.flydb.plist")
        fi
        if [[ -f "$HOME/Library/LaunchAgents/io.flydb.flydb.plist" ]]; then
            FOUND_SERVICES+=("$HOME/Library/LaunchAgents/io.flydb.flydb.plist")
        fi
    fi

    # Config directories (matches install.sh paths)
    if [[ -d "/etc/flydb" ]]; then
        FOUND_CONFIGS+=("/etc/flydb")
    fi
    if [[ -d "$HOME/.config/flydb" ]]; then
        FOUND_CONFIGS+=("$HOME/.config/flydb")
    fi

    # Data directories (matches install.sh paths)
    if [[ -d "/var/lib/flydb" ]]; then
        FOUND_DATA+=("/var/lib/flydb")
    fi
    if [[ -d "$HOME/.local/share/flydb" ]]; then
        FOUND_DATA+=("$HOME/.local/share/flydb")
    fi
}

print_found_items() {
    local total=$((${#FOUND_BINARIES[@]} + ${#FOUND_SERVICES[@]} + ${#FOUND_CONFIGS[@]} + ${#FOUND_DATA[@]}))

    if [[ $total -eq 0 ]]; then
        echo ""
        print_warning "No FlyDB installation found on this system"
        echo ""
        echo -e "  ${DIM}Searched locations:${RESET}"
        echo -e "  ${DIM}${ICON_BULLET} Binaries: /usr/local/bin, /usr/bin, ~/.local/bin${RESET}"
        echo -e "  ${DIM}${ICON_BULLET} Services: systemd, launchd${RESET}"
        echo -e "  ${DIM}${ICON_BULLET} Config: /etc/flydb, ~/.config/flydb${RESET}"
        echo -e "  ${DIM}${ICON_BULLET} Data: /var/lib/flydb, ~/.local/share/flydb${RESET}"
        echo ""
        return 1
    fi

    echo ""
    separator 60
    echo -e "${BOLD}  Components to be removed:${RESET}"
    separator 60
    echo ""

    # Binaries section
    if [[ ${#FOUND_BINARIES[@]} -gt 0 ]]; then
        echo -e "  ${CYAN}${ICON_FILE} Binaries${RESET} ${DIM}(${#FOUND_BINARIES[@]} files)${RESET}"
        for item in "${FOUND_BINARIES[@]}"; do
            local bin_name
            bin_name=$(basename "$item")
            if [[ -L "$item" ]]; then
                echo -e "      ${RED}${ICON_TRASH}${RESET} $item ${DIM}(symlink)${RESET}"
            else
                echo -e "      ${RED}${ICON_TRASH}${RESET} $item"
            fi
        done
        echo ""
    fi

    # Services section
    if [[ ${#FOUND_SERVICES[@]} -gt 0 ]]; then
        local service_status=""
        if check_service_running; then
            service_status="${YELLOW}(running)${RESET}"
        else
            service_status="${DIM}(stopped)${RESET}"
        fi
        echo -e "  ${CYAN}${ICON_GEAR} Services${RESET} ${DIM}(${#FOUND_SERVICES[@]} files)${RESET} $service_status"
        for item in "${FOUND_SERVICES[@]}"; do
            echo -e "      ${RED}${ICON_TRASH}${RESET} $item"
        done
        echo ""
    fi

    # Configuration section
    if [[ ${#FOUND_CONFIGS[@]} -gt 0 ]]; then
        if [[ "$KEEP_CONFIG" == true ]]; then
            echo -e "  ${CYAN}${ICON_FOLDER} Configuration${RESET} ${DIM}(${#FOUND_CONFIGS[@]} directories)${RESET} ${GREEN}(will be preserved)${RESET}"
            for item in "${FOUND_CONFIGS[@]}"; do
                echo -e "      ${GREEN}${ICON_SUCCESS}${RESET} $item ${DIM}(keeping)${RESET}"
            done
        else
            echo -e "  ${CYAN}${ICON_FOLDER} Configuration${RESET} ${DIM}(${#FOUND_CONFIGS[@]} directories)${RESET}"
            for item in "${FOUND_CONFIGS[@]}"; do
                echo -e "      ${RED}${ICON_TRASH}${RESET} $item"
            done
        fi
        echo ""
    fi

    # Data section
    if [[ ${#FOUND_DATA[@]} -gt 0 ]]; then
        if [[ "$REMOVE_DATA" == true ]]; then
            echo -e "  ${RED}${ICON_FOLDER} Data Directories${RESET} ${DIM}(${#FOUND_DATA[@]} directories)${RESET} ${RED}${BOLD}(WILL BE DELETED!)${RESET}"
            for item in "${FOUND_DATA[@]}"; do
                local size
                size=$(get_dir_size "$item")
                echo -e "      ${RED}${ICON_TRASH}${RESET} $item ${DIM}($size)${RESET}"
            done
        else
            echo -e "  ${CYAN}${ICON_FOLDER} Data Directories${RESET} ${DIM}(${#FOUND_DATA[@]} directories)${RESET} ${GREEN}(will be preserved)${RESET}"
            for item in "${FOUND_DATA[@]}"; do
                local size
                size=$(get_dir_size "$item")
                echo -e "      ${GREEN}${ICON_SUCCESS}${RESET} $item ${DIM}($size - keeping)${RESET}"
            done
        fi
        echo ""
    fi

    separator 60
    return 0
}

# =============================================================================
# Service Control
# =============================================================================

stop_running_service() {
    if ! check_service_running; then
        return 0
    fi

    print_step "Stopping FlyDB service..."

    if [[ "$DRY_RUN" == true ]]; then
        print_substep "[DRY RUN] Would stop FlyDB service"
        return 0
    fi

    if [[ "$INIT_SYSTEM" == "systemd" ]]; then
        local sudo_cmd
        sudo_cmd=$(get_sudo_cmd "/etc/systemd/system")
        spinner_start "Stopping systemd service"
        $sudo_cmd systemctl stop flydb 2>/dev/null || true
        sleep 1
        if ! systemctl is-active --quiet flydb 2>/dev/null; then
            spinner_success "Stopped flydb.service"
        else
            spinner_error "Failed to stop flydb.service"
            return 1
        fi
    elif [[ "$INIT_SYSTEM" == "launchd" ]]; then
        spinner_start "Stopping launchd service"
        for plist in "/Library/LaunchDaemons/io.flydb.flydb.plist" "$HOME/Library/LaunchAgents/io.flydb.flydb.plist"; do
            if [[ -f "$plist" ]]; then
                if [[ "$plist" == *"LaunchDaemons"* ]]; then
                    sudo launchctl unload "$plist" 2>/dev/null || true
                else
                    launchctl unload "$plist" 2>/dev/null || true
                fi
            fi
        done
        spinner_success "Stopped launchd service"
    fi

    # Kill any remaining processes
    if pgrep -x flydb &>/dev/null; then
        spinner_start "Stopping remaining FlyDB processes"
        pkill -x flydb 2>/dev/null || true
        sleep 1
        if ! pgrep -x flydb &>/dev/null; then
            spinner_success "Stopped all FlyDB processes"
        else
            spinner_error "Some processes may still be running"
        fi
    fi

    return 0
}

# =============================================================================
# Removal Functions
# =============================================================================

remove_binaries() {
    if [[ ${#FOUND_BINARIES[@]} -eq 0 ]]; then return 0; fi

    print_step "Removing binaries..."

    for binary in "${FOUND_BINARIES[@]}"; do
        local sudo_cmd
        sudo_cmd=$(get_sudo_cmd "$binary")
        local bin_name
        bin_name=$(basename "$binary")

        if [[ "$DRY_RUN" == true ]]; then
            echo -e "  ${DIM}[DRY RUN]${RESET} Would remove: $binary"
        else
            spinner_start "Removing $bin_name"
            if $sudo_cmd rm -f "$binary" 2>/dev/null; then
                spinner_success "Removed $binary"
            else
                spinner_error "Failed to remove $binary"
            fi
        fi
    done
}

remove_services() {
    if [[ ${#FOUND_SERVICES[@]} -eq 0 ]]; then return 0; fi

    print_step "Removing service files..."

    for service in "${FOUND_SERVICES[@]}"; do
        local sudo_cmd
        sudo_cmd=$(get_sudo_cmd "$service")
        local service_name
        service_name=$(basename "$service")

        if [[ "$DRY_RUN" == true ]]; then
            echo -e "  ${DIM}[DRY RUN]${RESET} Would remove: $service"
        else
            # Disable service first
            if [[ "$INIT_SYSTEM" == "systemd" ]]; then
                spinner_start "Disabling systemd service"
                $sudo_cmd systemctl disable flydb 2>/dev/null || true
                spinner_success "Disabled flydb.service"
            fi

            spinner_start "Removing $service_name"
            if $sudo_cmd rm -f "$service" 2>/dev/null; then
                spinner_success "Removed $service"
            else
                spinner_error "Failed to remove $service"
            fi

            # Reload systemd daemon
            if [[ "$INIT_SYSTEM" == "systemd" ]]; then
                spinner_start "Reloading systemd daemon"
                $sudo_cmd systemctl daemon-reload 2>/dev/null || true
                spinner_success "Reloaded systemd"
            fi
        fi
    done
}

remove_configs() {
    if [[ ${#FOUND_CONFIGS[@]} -eq 0 ]]; then return 0; fi
    if [[ "$KEEP_CONFIG" == true ]]; then
        print_step "Preserving configuration..."
        for config in "${FOUND_CONFIGS[@]}"; do
            echo -e "  ${GREEN}${ICON_SUCCESS}${RESET} Kept $config"
            SKIPPED_ITEMS+=("$config")
        done
        return 0
    fi

    print_step "Removing configuration..."

    for config in "${FOUND_CONFIGS[@]}"; do
        local sudo_cmd
        sudo_cmd=$(get_sudo_cmd "$config")

        if [[ "$DRY_RUN" == true ]]; then
            echo -e "  ${DIM}[DRY RUN]${RESET} Would remove: $config"
        else
            spinner_start "Removing $(basename "$config")"
            if $sudo_cmd rm -rf "$config" 2>/dev/null; then
                spinner_success "Removed $config"
            else
                spinner_error "Failed to remove $config"
            fi
        fi
    done
}

remove_data() {
    if [[ ${#FOUND_DATA[@]} -eq 0 ]]; then return 0; fi
    if [[ "$REMOVE_DATA" != true ]]; then
        # Data is preserved by default - don't print anything here
        return 0
    fi

    print_step "Removing data directories..."
    echo ""
    print_warning "This will permanently delete all FlyDB databases!"
    echo ""

    for data_dir in "${FOUND_DATA[@]}"; do
        local sudo_cmd
        sudo_cmd=$(get_sudo_cmd "$data_dir")
        local size
        size=$(get_dir_size "$data_dir")

        if [[ "$DRY_RUN" == true ]]; then
            echo -e "  ${DIM}[DRY RUN]${RESET} Would remove: $data_dir ($size)"
        else
            spinner_start "Removing $data_dir ($size)"
            if $sudo_cmd rm -rf "$data_dir" 2>/dev/null; then
                spinner_success "Removed $data_dir"
            else
                spinner_error "Failed to remove $data_dir"
            fi
        fi
    done
}

# =============================================================================
# Argument Parsing
# =============================================================================

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --prefix)
                if [[ -n "${2:-}" ]]; then
                    PREFIX="$2"
                    shift 2
                else
                    print_error "--prefix requires a path argument"
                    exit 1
                fi
                ;;
            -y|--yes)
                AUTO_CONFIRM=true
                shift
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --remove-data|--purge)
                REMOVE_DATA=true
                shift
                ;;
            --keep-config|--no-config)
                KEEP_CONFIG=true
                shift
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                echo -e "  ${DIM}Use --help for usage information${RESET}"
                exit 1
                ;;
        esac
    done
}

show_help() {
    setup_colors
    echo ""
    echo -e "${RED}${BOLD}FlyDB Uninstallation Script${RESET} ${DIM}v${SCRIPT_VERSION}${RESET}"
    echo ""
    echo -e "${BOLD}USAGE:${RESET}"
    echo "    ./uninstall.sh [OPTIONS]"
    echo "    curl -fsSL https://raw.githubusercontent.com/${GITHUB_REPO}/main/uninstall.sh | bash"
    echo ""
    echo -e "${BOLD}OPTIONS:${RESET}"
    echo -e "    ${BOLD}-y, --yes${RESET}           Skip all confirmation prompts"
    echo -e "    ${BOLD}--dry-run${RESET}           Preview what would be removed (no changes made)"
    echo -e "    ${BOLD}--keep-config${RESET}       Preserve configuration files"
    echo -e "    ${BOLD}--remove-data${RESET}       Also remove data directories ${RED}(DELETES DATABASES!)${RESET}"
    echo -e "    ${BOLD}--prefix PATH${RESET}       Look for installation in specific prefix"
    echo -e "    ${BOLD}-v, --verbose${RESET}       Show verbose output"
    echo -e "    ${BOLD}-h, --help${RESET}          Show this help message"
    echo ""
    echo -e "${BOLD}COMPONENTS REMOVED:${RESET}"
    echo -e "    ${ICON_BULLET} Binaries: flydb, flydb-shell, flydb-dump, fsql, fdump"
    echo -e "    ${ICON_BULLET} Services: systemd (flydb.service), launchd (io.flydb.flydb.plist)"
    echo -e "    ${ICON_BULLET} Configuration: /etc/flydb, ~/.config/flydb"
    echo -e "    ${ICON_BULLET} Data (optional): /var/lib/flydb, ~/.local/share/flydb"
    echo ""
    echo -e "${BOLD}EXAMPLES:${RESET}"
    echo -e "    ${DIM}# Interactive uninstallation${RESET}"
    echo "    ./uninstall.sh"
    echo ""
    echo -e "    ${DIM}# Non-interactive, keep config${RESET}"
    echo "    ./uninstall.sh --yes --keep-config"
    echo ""
    echo -e "    ${DIM}# Preview what would be removed${RESET}"
    echo "    ./uninstall.sh --dry-run"
    echo ""
    echo -e "    ${DIM}# Complete removal including data (DANGEROUS)${RESET}"
    echo "    ./uninstall.sh --yes --remove-data"
    echo ""
}

# =============================================================================
# Completion Summary
# =============================================================================

print_completion() {
    echo ""
    separator 60

    if [[ "$DRY_RUN" == true ]]; then
        echo ""
        echo -e "  ${YELLOW}${BOLD}${ICON_INFO} Dry Run Complete${RESET}"
        echo ""
        echo -e "  ${DIM}No files were actually removed.${RESET}"
        echo -e "  ${DIM}Run without --dry-run to perform the uninstallation.${RESET}"
        echo ""
    else
        local removed_count=${#REMOVED_ITEMS[@]}
        local failed_count=${#FAILED_ITEMS[@]}
        local skipped_count=${#SKIPPED_ITEMS[@]}

        echo ""
        if [[ $failed_count -eq 0 ]]; then
            echo -e "  ${GREEN}${BOLD}${ICON_SUCCESS} Uninstallation Complete${RESET}"
        else
            echo -e "  ${YELLOW}${BOLD}${ICON_WARNING} Uninstallation Completed with Errors${RESET}"
        fi
        echo ""

        # Summary stats
        echo -e "  ${DIM}Summary:${RESET}"
        if [[ $removed_count -gt 0 ]]; then
            echo -e "    ${GREEN}${ICON_SUCCESS}${RESET} Removed: $removed_count items"
        fi
        if [[ $skipped_count -gt 0 ]]; then
            echo -e "    ${YELLOW}${ICON_WARNING}${RESET} Skipped: $skipped_count items"
        fi
        if [[ $failed_count -gt 0 ]]; then
            echo -e "    ${RED}${ICON_ERROR}${RESET} Failed: $failed_count items"
        fi
        echo ""

        # Show preserved data directories
        if [[ ${#FOUND_DATA[@]} -gt 0 ]] && [[ "$REMOVE_DATA" != true ]]; then
            echo -e "  ${BOLD}Data Preserved:${RESET}"
            for data_dir in "${FOUND_DATA[@]}"; do
                if [[ -d "$data_dir" ]]; then
                    local size
                    size=$(get_dir_size "$data_dir")
                    echo -e "    ${GREEN}${ICON_SUCCESS}${RESET} $data_dir ${DIM}($size)${RESET}"
                fi
            done
            echo ""
            echo -e "  ${DIM}To remove data directories:${RESET}"
            echo -e "  ${CYAN}./uninstall.sh --remove-data --yes${RESET}"
            echo ""
        fi

        # Show preserved config
        if [[ ${#FOUND_CONFIGS[@]} -gt 0 ]] && [[ "$KEEP_CONFIG" == true ]]; then
            echo -e "  ${BOLD}Configuration Preserved:${RESET}"
            for config in "${FOUND_CONFIGS[@]}"; do
                if [[ -d "$config" ]]; then
                    echo -e "    ${GREEN}${ICON_SUCCESS}${RESET} $config"
                fi
            done
            echo ""
        fi

        # Failed items details
        if [[ $failed_count -gt 0 ]]; then
            echo -e "  ${RED}${BOLD}Failed to remove:${RESET}"
            for item in "${FAILED_ITEMS[@]}"; do
                echo -e "    ${RED}${ICON_ERROR}${RESET} $item"
            done
            echo ""
            echo -e "  ${DIM}Try running with sudo: sudo ./uninstall.sh${RESET}"
            echo ""
        fi
    fi

    separator 60
    echo ""
    echo -e "  ${DIM}Thank you for using FlyDB!${RESET}"
    echo -e "  ${DIM}Feedback: https://github.com/${GITHUB_REPO}/issues${RESET}"
    echo ""
}

# =============================================================================
# Cleanup
# =============================================================================

cleanup() {
    local exit_code=$?
    spinner_stop
    stop_sudo_keepalive
    exit $exit_code
}

# =============================================================================
# Main Entry Point
# =============================================================================

main() {
    # Initialize
    setup_colors
    trap cleanup EXIT
    trap 'echo ""; print_warning "Uninstallation cancelled by user"; exit 130' INT TERM

    # Parse arguments first (--help should work without banner)
    parse_args "$@"

    # Show banner
    print_banner

    # Show mode indicators
    if [[ "$DRY_RUN" == true ]]; then
        echo -e "  ${YELLOW}${BOLD}${ICON_WARNING} DRY RUN MODE${RESET} ${DIM}- No files will be removed${RESET}"
        echo ""
    fi

    if [[ "$VERBOSE" == true ]]; then
        echo -e "  ${DIM}Verbose mode enabled${RESET}"
    fi

    # Detect system
    detect_os
    detect_init_system

    if [[ "$VERBOSE" == true ]]; then
        echo -e "  ${DIM}OS: $OS, Init: $INIT_SYSTEM${RESET}"
    fi

    # Find installations
    find_installations

    # Display what was found
    if ! print_found_items; then
        exit 0
    fi

    # Confirm with user
    if [[ "$AUTO_CONFIRM" != true ]]; then
        echo ""
        if ! prompt_yes_no "Proceed with uninstallation?"; then
            echo ""
            print_info "Uninstallation cancelled"
            echo ""
            exit 0
        fi
    fi

    # Get sudo if needed
    if ! obtain_sudo_if_needed; then
        exit 1
    fi

    # Stop running services first
    stop_running_service

    # Remove components in order
    remove_services
    remove_binaries
    remove_configs
    remove_data

    # Show completion summary
    print_completion
}

main "$@"

