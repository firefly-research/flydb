/*
 * Copyright (c) 2026 Firefly Software Solutions Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
Package banner provides the startup banner display for FlyDB.

Banner Display Overview:
========================

This package handles the visual branding displayed when FlyDB starts.
It uses Go's embed directive to include the ASCII art banner at compile time,
ensuring the banner file is always available without external dependencies.

Go Embed Directive:
===================

The //go:embed directive is a Go 1.16+ feature that embeds file contents
directly into the compiled binary. This approach has several benefits:

  1. No external file dependencies at runtime
  2. Faster startup (no file I/O needed)
  3. Simpler deployment (single binary)
  4. Guaranteed file availability

ANSI Color Codes:
=================

The package uses ANSI escape sequences for terminal colors. These codes
are widely supported in modern terminals (Linux, macOS, Windows 10+).

Format: \033[<code>m

Common codes used:
  - 31: Red foreground
  - 32: Green foreground
  - 33: Yellow foreground
  - 0:  Reset all attributes
  - 1:  Bold text

Example: "\033[31mRed Text\033[0m" prints "Red Text" in red.

Usage:
======

Simply call banner.Print() at application startup:

	func main() {
	    banner.Print()
	    // ... rest of initialization
	}
*/
package banner

import (
	_ "embed" // Required for the //go:embed directive
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"

	"flydb/internal/config"
)

// banner contains the ASCII art logo loaded from banner.txt at compile time.
// The //go:embed directive instructs the Go compiler to read banner.txt
// and store its contents in this variable as a string.
//
//go:embed banner.txt
var banner string

// ANSI escape codes for terminal text formatting.
// These constants provide readable names for common ANSI codes,
// making the code more maintainable and self-documenting.
const (
	// AnsiRed sets the foreground color to red.
	// Used for the main banner logo to create visual impact.
	AnsiRed = "\033[31m"

	// AnsiGreen sets the foreground color to green.
	// Used for copyright and license information.
	AnsiGreen = "\033[32m"

	// AnsiYellow sets the foreground color to yellow.
	// Available for warning messages or highlights.
	AnsiYellow = "\033[33m"

	// AnsiCyan sets the foreground color to cyan.
	// Used for section headers and informational text.
	AnsiCyan = "\033[36m"

	// AnsiReset clears all text formatting and returns to default.
	// Always use this after colored text to prevent color bleeding.
	AnsiReset = "\033[0m"

	// AnsiBold enables bold text rendering.
	// Combined with colors for emphasis on important text.
	AnsiBold = "\033[1m"

	// AnsiDim enables dim/faint text rendering.
	// Used for less important information.
	AnsiDim = "\033[2m"

	// AnsiNormal resets text weight to normal (non-bold).
	// Alias for AnsiReset for semantic clarity.
	AnsiNormal = "\033[0m"
)

// Version information for the FlyDB application.
// These constants are used in the banner display and can be
// referenced elsewhere in the application for version reporting.
const (
	Version   = "01.26.14"
	Copyright = "(c)2026 Firefly Software Solutions Inc"
	License   = "Licensed under Apache 2.0"
)

// Print displays the startup banner with version and copyright information.
// This function should be called once at application startup to provide
// visual branding and version information to the user.
//
// The banner includes:
//   - ASCII art logo (from banner.txt)
//   - Application name and version
//   - Copyright notice
//   - License information
//
// Colors are applied using ANSI escape codes for visual appeal.
// The function prints to stdout and does not return any value.
func Print() {
	// Print the ASCII art logo in red for visual impact.
	fmt.Println(AnsiRed + banner + AnsiReset)

	// Print the application name and version in bold red.
	fmt.Println(AnsiRed + AnsiBold + ":: FlyDB ::                     (v" + Version + ")" + AnsiReset)

	// Print copyright and license in bold green.
	fmt.Println(AnsiGreen + AnsiBold + Copyright + AnsiReset)
	fmt.Println(AnsiGreen + AnsiBold + License + AnsiReset)

	// Print a blank line for visual separation from subsequent output.
	fmt.Println()
}

// PrintLogSeparator prints a visual separator before logs start.
// This helps users distinguish between configuration display and log output.
func PrintLogSeparator() {
	printLogSeparator(os.Stdout)
}

func printLogSeparator(w io.Writer) {
	const lineWidth = 78
	arrow := "v"
	text := " LOGS START HERE "
	padding := (lineWidth - len(text) - 4) / 2 // 4 for arrows on each side
	if padding < 0 {
		padding = 0
	}
	line := strings.Repeat("-", padding)
	fmt.Fprintf(w, "  %s%s %s%s%s %s%s%s\n",
		AnsiYellow, arrow+arrow+line,
		AnsiBold, text, AnsiReset+AnsiYellow,
		line+arrow+arrow, AnsiReset, "")
	fmt.Fprintln(w)
}

// PrintServerWithConfig prints the server banner with comprehensive configuration display.
// This provides a clear overview of all settings including cluster, replication, and security.
// Inspired by FlyMQ's polished startup experience.
func PrintServerWithConfig(cfg *config.Config) {
	PrintServerWithConfigTo(os.Stdout, cfg)
}

// PrintServerWithConfigTo writes the server banner with configuration to the specified writer.
func PrintServerWithConfigTo(w io.Writer, cfg *config.Config) {
	// Print ASCII banner
	fmt.Fprintln(w)
	fmt.Fprintln(w, AnsiRed+banner+AnsiReset)
	fmt.Fprintln(w, AnsiRed+AnsiBold+":: FlyDB Server ::              (v"+Version+")"+AnsiReset)
	fmt.Fprintln(w, AnsiDim+"  High-Performance Embedded SQL Database"+AnsiReset)
	fmt.Fprintln(w)

	// Configuration source
	printConfigSource(w, cfg)

	// Print compact configuration
	printCompactConfig(w, cfg)

	// Footer
	fmt.Fprintln(w, AnsiDim+"  "+Copyright+AnsiReset)
	fmt.Fprintln(w)

	// Log separator
	printLogSeparator(w)
}

// Helper functions for configuration display

func printConfigSource(w io.Writer, cfg *config.Config) {
	fmt.Fprint(w, "  "+AnsiDim+"Config: "+AnsiReset)
	if cfg.ConfigFile != "" {
		fmt.Fprintln(w, AnsiYellow+cfg.ConfigFile+AnsiReset)
	} else {
		fmt.Fprintln(w, AnsiDim+"defaults + environment"+AnsiReset)
	}
	fmt.Fprintln(w)
}

func printCompactConfig(w io.Writer, cfg *config.Config) {
	const lineWidth = 78

	// === SERVER ===
	printSectionHeader(w, "Server", lineWidth)

	// Row 1: Basic network info
	col1 := fmtKV("Port", fmt.Sprintf("%s:%d%s", AnsiGreen, cfg.Port, AnsiReset))
	col2 := fmtKV("Role", cfg.Role)
	col3 := fmtKV("Log", cfg.LogLevel)
	printRow3(w, col1, col2, col3)

	// Row 2: Storage
	col1 = fmtKV("Data", cfg.DataDir)
	col2 = fmtKV("Checkpoint", fmt.Sprintf("%ds", cfg.CheckpointSecs))
	col3 = ""
	printRow3(w, col1, col2, col3)

	fmt.Fprintln(w)

	// === CLUSTER ===
	printSectionHeader(w, "Cluster", lineWidth)
	printClusterInfo(w, cfg)

	fmt.Fprintln(w)

	// === SECURITY ===
	printSectionHeader(w, "Security", lineWidth)
	printSecurityInfo(w, cfg)

	fmt.Fprintln(w)

	// === FEATURES ===
	printSectionHeader(w, "Features", lineWidth)
	printFeaturesInfo(w, cfg)

	fmt.Fprintln(w)

	// === PERFORMANCE ===
	printSectionHeader(w, "Performance", lineWidth)
	printPerformanceInfo(w, cfg)

	fmt.Fprintln(w)
}

func printSectionHeader(w io.Writer, title string, width int) {
	titleLen := len(title) + 4 // "[ title ]"
	leftPad := 2
	rightPad := width - leftPad - titleLen
	if rightPad < 0 {
		rightPad = 0
	}
	fmt.Fprintf(w, "  %s[ %s%s%s ]%s%s\n",
		AnsiDim+strings.Repeat("-", leftPad),
		AnsiReset+AnsiCyan+AnsiBold, title, AnsiReset+AnsiDim,
		strings.Repeat("-", rightPad),
		AnsiReset)
}

func fmtKV(key, value string) string {
	return fmt.Sprintf("%s%s:%s %s", AnsiDim, key, AnsiReset, value)
}

func fmtEnabled(name string, enabled bool) string {
	if enabled {
		return AnsiGreen + name + AnsiReset
	}
	return AnsiDim + name + AnsiReset
}

func printRow3(w io.Writer, col1, col2, col3 string) {
	fmt.Fprintf(w, "  %-32s %-26s %s\n", col1, col2, col3)
}

func printRow2(w io.Writer, col1, col2 string) {
	fmt.Fprintf(w, "  %-40s %s\n", col1, col2)
}


func printClusterInfo(w io.Writer, cfg *config.Config) {
	isClusterMode := cfg.Role == "cluster"
	hasPeers := len(cfg.ClusterPeers) > 0

	if !isClusterMode {
		// Standalone mode
		col1 := fmtKV("Mode", AnsiYellow+"standalone"+AnsiReset)
		col2 := AnsiDim + "(single server)" + AnsiReset
		printRow2(w, col1, col2)
		return
	}

	// Cluster mode
	var modeStr string
	if hasPeers {
		modeStr = AnsiGreen + "cluster" + AnsiReset
	} else {
		modeStr = AnsiYellow + "cluster (bootstrap)" + AnsiReset
	}

	col1 := fmtKV("Mode", modeStr)
	col2 := fmtKV("Cluster Port", fmt.Sprintf("%d", cfg.ClusterPort))
	var col3 string
	if hasPeers {
		col3 = fmtKV("Peers", fmt.Sprintf("%d", len(cfg.ClusterPeers)))
	} else {
		col3 = fmtKV("Peers", AnsiDim+"none"+AnsiReset)
	}
	printRow3(w, col1, col2, col3)

	// Replication row
	col1 = fmtKV("Replication", cfg.ReplicationMode)
	col2 = fmtKV("Partitions", fmt.Sprintf("%d", cfg.PartitionCount))
	col3 = fmtKV("Replicas", fmt.Sprintf("%d", cfg.ReplicationFactor))
	printRow3(w, col1, col2, col3)

	// Raft configuration
	if cfg.EnableRaft {
		col1 = fmtKV("Consensus", AnsiGreen+"Raft"+AnsiReset)
		col2 = fmtKV("Election", fmt.Sprintf("%dms", cfg.RaftElectionTimeout))
		col3 = fmtKV("Heartbeat", fmt.Sprintf("%dms", cfg.RaftHeartbeatInterval))
		printRow3(w, col1, col2, col3)
	}
}

func printSecurityInfo(w io.Writer, cfg *config.Config) {
	// Build security status items
	var items []string

	// TLS
	if cfg.TLSEnabled {
		if cfg.TLSAutoGen {
			items = append(items, fmtKV("TLS", AnsiGreen+"enabled (auto-gen)"+AnsiReset))
		} else {
			items = append(items, fmtKV("TLS", AnsiGreen+"enabled"+AnsiReset))
		}
	} else {
		items = append(items, fmtKV("TLS", AnsiYellow+"off"+AnsiReset))
	}

	// Encryption
	if cfg.EncryptionEnabled {
		items = append(items, fmtKV("Encryption", AnsiGreen+"AES-256-GCM"+AnsiReset))
	} else {
		items = append(items, fmtKV("Encryption", AnsiYellow+"off"+AnsiReset))
	}

	// Print in columns
	if len(items) >= 2 {
		printRow2(w, items[0], items[1])
	}
}

func printFeaturesInfo(w io.Writer, cfg *config.Config) {
	var enabled []string
	var disabled []string

	// Compression
	if cfg.EnableCompression {
		enabled = append(enabled, fmt.Sprintf("Compression(%s)", strings.ToUpper(cfg.CompressionAlgorithm)))
	} else {
		disabled = append(disabled, "Compression")
	}

	// Zero-copy
	if cfg.EnableZeroCopy {
		enabled = append(enabled, "Zero-Copy")
	} else {
		disabled = append(disabled, "Zero-Copy")
	}

	// Service Discovery
	if cfg.DiscoveryEnabled {
		enabled = append(enabled, "mDNS Discovery")
	}

	// Print enabled features
	if len(enabled) > 0 {
		fmt.Fprintf(w, "  %sEnabled:%s  %s%s%s\n", AnsiDim, AnsiReset, AnsiGreen, strings.Join(enabled, ", "), AnsiReset)
	}
	if len(disabled) > 0 {
		fmt.Fprintf(w, "  %sDisabled:%s %s\n", AnsiDim, AnsiReset, AnsiDim+strings.Join(disabled, ", ")+AnsiReset)
	}
}

func printPerformanceInfo(w io.Writer, cfg *config.Config) {
	// Row 1: Buffer pool and checkpoint
	col1 := fmtKV("Buffer Pool", formatBytes(int64(cfg.BufferPoolSizeBytes)))
	col2 := fmtKV("Checkpoint", fmt.Sprintf("%ds", cfg.CheckpointSecs))
	col3 := ""
	printRow3(w, col1, col2, col3)

	// Row 2: System info
	col1 = fmtKV("CPUs", fmt.Sprintf("%d", runtime.NumCPU()))
	col2 = fmtKV("GOMAXPROCS", fmt.Sprintf("%d", runtime.GOMAXPROCS(0)))
	col3 = ""
	printRow3(w, col1, col2, col3)
}

func formatBytes(bytes int64) string {
	if bytes == 0 {
		return "auto"
	}
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.0f%cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

