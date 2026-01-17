/*
 * Copyright (c) 2026 FlyDB Authors.
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
flydb-discover - FlyDB Node Discovery Tool

This tool discovers FlyDB nodes on the local network using mDNS (Bonjour/Avahi).
It can be used by install.sh to find existing cluster nodes for joining.

Usage:
    flydb-discover                    # Discover nodes (5 second timeout)
    flydb-discover --timeout 10       # Custom timeout in seconds
    flydb-discover --json             # Output as JSON
    flydb-discover --quiet            # Only output addresses (for scripting)
*/
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"flydb/internal/cluster"
)

const (
	version   = "1.0.0"
	copyright = "Copyright (c) 2026 FlyDB Authors"
)

// ANSI color codes
const (
	reset  = "\033[0m"
	bold   = "\033[1m"
	dim    = "\033[2m"
	red    = "\033[31m"
	green  = "\033[32m"
	yellow = "\033[33m"
	cyan   = "\033[36m"
)

func main() {
	timeout := flag.Int("timeout", 5, "Discovery timeout in seconds")
	jsonOutput := flag.Bool("json", false, "Output as JSON")
	quiet := flag.Bool("quiet", false, "Only output cluster addresses (for scripting)")
	help := flag.Bool("help", false, "Show help")
	showVersion := flag.Bool("version", false, "Show version information")
	flag.BoolVar(help, "h", false, "Show help")
	flag.BoolVar(showVersion, "v", false, "Show version information")

	flag.Parse()

	if *help {
		printUsage()
		os.Exit(0)
	}

	if *showVersion {
		printVersion()
		os.Exit(0)
	}

	// Suppress mDNS library logging (it logs IPv6 errors that are not critical)
	log.SetOutput(io.Discard)

	// Show banner and welcome message unless in quiet/json mode
	if !*quiet && !*jsonOutput {
		printBanner()
	}

	// Create discovery service (not advertising, just discovering)
	discovery := cluster.NewDiscoveryService(cluster.DiscoveryConfig{
		NodeID:  "discover-client",
		Enabled: false, // Don't advertise, just discover
	})

	// Show scanning message unless in quiet mode
	if !*quiet && !*jsonOutput {
		fmt.Printf("%s%sℹ%s Scanning for FlyDB nodes on the network (timeout: %ds)...\n\n",
			cyan, bold, reset, *timeout)
	}

	// Discover nodes
	nodes, err := discovery.DiscoverNodes(time.Duration(*timeout) * time.Second)
	if err != nil {
		if !*quiet {
			fmt.Fprintf(os.Stderr, "%s%s✗%s Discovery failed: %v\n", red, bold, reset, err)
		}
		os.Exit(1)
	}

	if len(nodes) == 0 {
		if !*quiet && !*jsonOutput {
			fmt.Printf("%s%s⚠%s No FlyDB nodes found on the network.\n\n", yellow, bold, reset)
			fmt.Printf("%s%sTROUBLESHOOTING%s\n\n", bold, cyan, reset)
			fmt.Printf("%s  Common issues:%s\n", dim, reset)
			fmt.Printf("    %s•%s FlyDB nodes are not running with discovery enabled\n", yellow, reset)
			fmt.Printf("    %s•%s mDNS/Bonjour is blocked by firewall (UDP port 5353)\n", yellow, reset)
			fmt.Printf("    %s•%s Nodes are on a different network segment\n\n", yellow, reset)
			fmt.Printf("%s  Try:%s\n", dim, reset)
			fmt.Printf("    %sflydb-discover --timeout 10%s   # Increase timeout\n\n", green, reset)
		}
		os.Exit(0)
	}

	if *jsonOutput {
		outputJSON(nodes)
	} else if *quiet {
		outputQuiet(nodes)
	} else {
		outputHuman(nodes)
	}
}

func printBanner() {
	fmt.Println()
	fmt.Printf("%s%s", cyan, bold)
	fmt.Println("  ███████╗██╗  ██╗   ██╗██████╗ ██████╗ ")
	fmt.Println("  ██╔════╝██║  ╚██╗ ██╔╝██╔══██╗██╔══██╗")
	fmt.Println("  █████╗  ██║   ╚████╔╝ ██║  ██║██████╔╝")
	fmt.Println("  ██╔══╝  ██║    ╚██╔╝  ██║  ██║██╔══██╗")
	fmt.Println("  ██║     ███████╗██║   ██████╔╝██████╔╝")
	fmt.Println("  ╚═╝     ╚══════╝╚═╝   ╚═════╝ ╚═════╝ ")
	fmt.Printf("%s\n", reset)
	fmt.Printf("  %s%sFlyDB Discover%s %sv%s%s\n", green, bold, reset, dim, version, reset)
	fmt.Printf("  %sNetwork Node Discovery Tool%s\n\n", dim, reset)
}

