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

	"flydb/internal/banner"
	"flydb/internal/cluster"
	"flydb/pkg/cli"
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
		cli.PrintInfo("Scanning for FlyDB nodes on the network (timeout: %ds)...", *timeout)
		fmt.Println()
	}

	// Discover nodes
	nodes, err := discovery.DiscoverNodes(time.Duration(*timeout) * time.Second)
	if err != nil {
		if !*quiet {
			cli.PrintError("Discovery failed: %v", err)
		}
		os.Exit(1)
	}

	if len(nodes) == 0 {
		if !*quiet && !*jsonOutput {
			cli.PrintWarning("No FlyDB nodes found on the network.")
			fmt.Println()
			fmt.Printf("  %s%sTROUBLESHOOTING:%s\n", cli.Bold, cli.Cyan, cli.Reset)
			fmt.Printf("    %sCommon issues:%s\n", cli.Dim, cli.Reset)
			fmt.Printf("      %s•%s FlyDB nodes are not running with discovery enabled\n", cli.Yellow, cli.Reset)
			fmt.Printf("      %s•%s mDNS/Bonjour is blocked by firewall (UDP port 5353)\n", cli.Yellow, cli.Reset)
			fmt.Printf("      %s•%s Nodes are on a different network segment\n", cli.Yellow, cli.Reset)
			fmt.Println()
			fmt.Printf("    %sTry:%s\n", cli.Dim, cli.Reset)
			fmt.Printf("      %sflydb-discover --timeout 10%s   # Increase timeout\n", cli.Green, cli.Reset)
			fmt.Println()
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
	banner.Print()
	fmt.Printf("  %s%sFlyDB Discover%s %sv%s%s\n", cli.BrightGreen, cli.Bold, cli.Reset, cli.Dim, banner.Version, cli.Reset)
	fmt.Printf("  %sNetwork Node Discovery Tool%s\n\n", cli.Dim, cli.Reset)
}

func printVersion() {
	fmt.Printf("flydb-discover version %s\n", banner.Version)
	fmt.Printf("%s\n", banner.Copyright)
}

func printUsage() {
	// Print banner
	printBanner()

	// Description
	fmt.Printf("  %sDiscovers FlyDB nodes on the local network using mDNS (Bonjour/Avahi).%s\n", cli.Dim, cli.Reset)
	fmt.Printf("  %sUseful for finding existing cluster nodes to join.%s\n\n", cli.Dim, cli.Reset)

	// Usage
	fmt.Printf("  %s%sUSAGE:%s\n", cli.Bold, cli.Cyan, cli.Reset)
	fmt.Println("    flydb-discover [options]")
	fmt.Println()

	// Options
	fmt.Printf("  %s%sOPTIONS:%s\n", cli.Bold, cli.Cyan, cli.Reset)
	fmt.Printf("    %s--timeout%s <n>     Discovery timeout in seconds (default: 5)\n", cli.Green, cli.Reset)
	fmt.Printf("    %s--json%s            Output results as JSON\n", cli.Green, cli.Reset)
	fmt.Printf("    %s--quiet%s, %s-q%s       Only output addresses (for scripting)\n", cli.Green, cli.Reset, cli.Green, cli.Reset)
	fmt.Printf("    %s--version%s, %s-v%s     Show version information\n", cli.Green, cli.Reset, cli.Green, cli.Reset)
	fmt.Printf("    %s--help%s, %s-h%s        Show this help message\n", cli.Green, cli.Reset, cli.Green, cli.Reset)
	fmt.Println()

	// Examples
	fmt.Printf("  %s%sEXAMPLES:%s\n", cli.Bold, cli.Cyan, cli.Reset)
	fmt.Println("    " + cli.Dimmed("# Discover nodes with default timeout"))
	fmt.Println("    flydb-discover")
	fmt.Println()
	fmt.Println("    " + cli.Dimmed("# Get JSON output for automation"))
	fmt.Println("    flydb-discover --json")
	fmt.Println()

	// Network requirements
	fmt.Printf("  %s%sNETWORK REQUIREMENTS:%s\n", cli.Bold, cli.Cyan, cli.Reset)
	fmt.Printf("    %s•%s mDNS uses UDP port 5353 (multicast)\n", cli.Yellow, cli.Reset)
	fmt.Printf("    %s•%s Nodes must be on the same network segment\n", cli.Yellow, cli.Reset)
	fmt.Println()
}

func outputJSON(nodes []*cluster.DiscoveredNode) {
	type nodeOutput struct {
		NodeID      string `json:"node_id"`
		ClusterID   string `json:"cluster_id,omitempty"`
		ClusterAddr string `json:"cluster_addr"`
		RaftAddr    string `json:"raft_addr,omitempty"`
		HTTPAddr    string `json:"http_addr,omitempty"`
		Version     string `json:"version,omitempty"`
	}

	output := make([]nodeOutput, len(nodes))
	for i, n := range nodes {
		output[i] = nodeOutput{
			NodeID:      n.NodeID,
			ClusterID:   n.ClusterID,
			ClusterAddr: n.ClusterAddr,
			RaftAddr:    n.RaftAddr,
			HTTPAddr:    n.HTTPAddr,
			Version:     n.Version,
		}
	}

	data, _ := json.MarshalIndent(output, "", "  ")
	fmt.Println(string(data))
}

func outputQuiet(nodes []*cluster.DiscoveredNode) {
	addrs := make([]string, len(nodes))
	for i, n := range nodes {
		addrs[i] = n.ClusterAddr
	}
	fmt.Println(strings.Join(addrs, ","))
}

func outputHuman(nodes []*cluster.DiscoveredNode) {
	cli.PrintSuccess("Found %d FlyDB node(s)", len(nodes))
	fmt.Println()

	for i, n := range nodes {
		// Node header with index and ID
		fmt.Printf("  %s[%d]%s %s%s%s\n",
			cli.Dim, i+1, cli.Reset,
			cli.Bold+cli.Cyan, n.NodeID, cli.Reset)

		// Cluster address (always present)
		fmt.Printf("      %sCluster Address:%s %s%s%s\n",
			cli.Dim, cli.Reset,
			cli.Green, n.ClusterAddr, cli.Reset)

		// Raft address (optional)
		if n.RaftAddr != "" {
			fmt.Printf("      %sRaft Address:%s    %s\n",
				cli.Dim, cli.Reset, n.RaftAddr)
		}

		// HTTP address (optional)
		if n.HTTPAddr != "" {
			fmt.Printf("      %sHTTP Address:%s    %s\n",
				cli.Dim, cli.Reset, n.HTTPAddr)
		}

		// Cluster ID (optional)
		if n.ClusterID != "" {
			fmt.Printf("      %sCluster ID:%s      %s\n",
				cli.Dim, cli.Reset, n.ClusterID)
		}

		// Version (optional)
		if n.Version != "" {
			fmt.Printf("      %sVersion:%s         %s\n",
				cli.Dim, cli.Reset, n.Version)
		}

		fmt.Println()
	}

	// Helpful tip
	fmt.Printf("  %sTip: Use --json for machine-readable output%s\n\n", cli.Dim, cli.Reset)
}
