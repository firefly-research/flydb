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
Package wizard provides an interactive initialization wizard for FlyDB daemon.

The wizard runs when the daemon is started without any command-line arguments,
guiding users through the configuration of the server operative mode and settings.

Operative Modes:
================

  - Standalone: Single server mode for development or small deployments
  - Master: Leader node that accepts writes and replicates to followers
  - Slave: Follower node that receives replication from a master

Interactive Flow:
=================

 1. Display welcome message and mode selection
 2. Prompt for mode-specific configuration
 3. Validate inputs and display summary
 4. Return configuration for server startup
*/
package wizard

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"flydb/internal/banner"
	"flydb/pkg/cli"
)

// Config holds the configuration collected by the wizard.
type Config struct {
	Port          string
	BinaryPort    string
	ReplPort      string
	Role          string
	MasterAddr    string
	DBPath        string
	LogLevel      string
	LogJSON       bool
	AdminPassword string // Admin password for first-time setup (empty = generate random)
	IsFirstSetup  bool   // True if this is a first-time setup requiring admin initialization
}

// DefaultConfig returns the default configuration values.
func DefaultConfig() Config {
	return Config{
		Port:          "8888",
		BinaryPort:    "8889",
		ReplPort:      "9999",
		Role:          "master",
		MasterAddr:    "",
		DBPath:        "flydb.wal",
		LogLevel:      "info",
		LogJSON:       false,
		AdminPassword: "",
		IsFirstSetup:  false,
	}
}

// ANSI color codes for terminal output.
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorCyan   = "\033[36m"
	colorBold   = "\033[1m"
)

// Run executes the interactive wizard and returns the collected configuration.
// If the user cancels (Ctrl+C), it returns nil.
// This is a convenience wrapper that calls RunWithOptions with needsAdminSetup=false.
func Run() *Config {
	return RunWithOptions(false)
}

// RunWithOptions executes the interactive wizard with optional admin setup.
// If needsAdminSetup is true, the wizard will prompt for admin password configuration.
// If the user cancels (Ctrl+C), it returns nil.
func RunWithOptions(needsAdminSetup bool) *Config {
	reader := bufio.NewReader(os.Stdin)
	config := DefaultConfig()
	config.IsFirstSetup = needsAdminSetup

	fmt.Println()
	fmt.Println(cli.Highlight("╔════════════════════════════════════════════════════════════╗"))
	fmt.Println(cli.Highlight("║                 FlyDB Initialization Wizard                ║"))
	fmt.Println(cli.Highlight("╚════════════════════════════════════════════════════════════╝"))
	fmt.Println()
	fmt.Println("Welcome to FlyDB! This wizard will help you configure the server.")
	fmt.Printf("Press %s to accept default values shown in [brackets].\n", cli.Highlight("Enter"))
	fmt.Printf("Press %s to cancel at any time.\n", cli.Highlight("Ctrl+C"))
	fmt.Println()

	// Step 1: Select operative mode
	printStepHeader(1, "Select Operative Mode")
	fmt.Println()
	fmt.Printf("  %s) Standalone  - Single server (development/small deployments)\n", cli.Success("1"))
	fmt.Printf("  %s) Master      - Leader node (accepts writes, replicates to slaves)\n", cli.Success("2"))
	fmt.Printf("  %s) Slave       - Follower node (receives replication from master)\n", cli.Success("3"))
	fmt.Println()

	mode := promptWithDefault(reader, "Select mode [1-3]", "1")
	switch mode {
	case "1":
		config.Role = "standalone"
	case "2":
		config.Role = "master"
	case "3":
		config.Role = "slave"
	default:
		cli.PrintWarning("Invalid selection, defaulting to Standalone mode.")
		config.Role = "standalone"
	}
	fmt.Println()

	// Step 2: Configure ports with validation
	printStepHeader(2, "Configure Network Ports")
	fmt.Println()

	config.Port = promptPortWithValidation(reader, "Text protocol port", config.Port)
	config.BinaryPort = promptPortWithValidation(reader, "Binary protocol port", config.BinaryPort)

	if config.Role == "master" {
		config.ReplPort = promptPortWithValidation(reader, "Replication port", config.ReplPort)
	}
	fmt.Println()

	// Step 3: Configure slave-specific settings
	stepNum := 3
	if config.Role == "slave" {
		printStepHeader(stepNum, "Configure Replication")
		fmt.Println()
		config.MasterAddr = promptWithValidation(reader, "Master address (host:port)", "localhost:9999", validateAddress)
		fmt.Println()
		stepNum++
	}

	// Configure storage
	printStepHeader(stepNum, "Configure Storage")
	fmt.Println()
	config.DBPath = promptWithDefault(reader, "Database file path", config.DBPath)
	fmt.Println()

	// Configure logging
	stepNum++
	printStepHeader(stepNum, "Configure Logging")
	fmt.Println()
	fmt.Printf("  Log levels: %s, %s, %s, %s\n",
		cli.Success("debug"), cli.Success("info"), cli.Warning("warn"), cli.Error("error"))
	config.LogLevel = promptWithValidation(reader, "Log level", config.LogLevel, validateLogLevel)
	jsonLog := promptWithDefault(reader, "Enable JSON logging? (y/n)", "n")
	config.LogJSON = strings.ToLower(jsonLog) == "y" || strings.ToLower(jsonLog) == "yes"
	fmt.Println()

	// Configure admin password (only for first-time setup)
	if needsAdminSetup {
		stepNum++
		printStepHeader(stepNum, "Configure Admin Password")
		fmt.Println()
		cli.PrintWarning("IMPORTANT: This is the first time FlyDB is starting with this database.")
		fmt.Println("  You need to set an admin password for the 'admin' user.")
		fmt.Println()
		fmt.Println("  Options:")
		fmt.Println("    - Enter a password to set it manually")
		fmt.Println("    - Press Enter to generate a secure random password")
		fmt.Println()

		config.AdminPassword = promptPassword(reader, "Admin password (Enter for random)")
		fmt.Println()
	}

	// Display configuration summary
	printSummary(&config)

	// Confirm and start
	fmt.Println()
	confirm := promptWithDefault(reader, "Start FlyDB with this configuration? (y/n)", "y")
	if strings.ToLower(confirm) != "y" && strings.ToLower(confirm) != "yes" {
		cli.PrintWarning("Configuration cancelled. Run with --help for command-line options.")
		return nil
	}

	fmt.Println()
	return &config
}

// printStepHeader prints a formatted step header.
func printStepHeader(step int, title string) {
	fmt.Printf("%s\n", cli.Highlight(fmt.Sprintf("Step %d: %s", step, title)))
	fmt.Println(cli.Separator(60))
}

// promptPortWithValidation prompts for a port with validation.
func promptPortWithValidation(reader *bufio.Reader, prompt, defaultVal string) string {
	for {
		value := promptWithDefault(reader, prompt, defaultVal)
		if ValidatePort(value) {
			return value
		}
		cli.PrintError("Invalid port number. Please enter a value between 1 and 65535.")
	}
}

// promptWithValidation prompts for input with a custom validation function.
func promptWithValidation(reader *bufio.Reader, prompt, defaultVal string, validate func(string) bool) string {
	for {
		value := promptWithDefault(reader, prompt, defaultVal)
		if validate(value) {
			return value
		}
		cli.PrintError("Invalid input. Please try again.")
	}
}

// validateAddress validates a host:port address format.
func validateAddress(addr string) bool {
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		return false
	}
	return ValidatePort(parts[1])
}

// validateLogLevel validates a log level string.
func validateLogLevel(level string) bool {
	switch strings.ToLower(level) {
	case "debug", "info", "warn", "error":
		return true
	default:
		return false
	}
}

// promptWithDefault displays a prompt and returns user input or the default value.
func promptWithDefault(reader *bufio.Reader, prompt, defaultVal string) string {
	fmt.Printf("  %s [%s%s%s]: ", prompt, colorYellow, defaultVal, colorReset)
	input, err := reader.ReadString('\n')
	if err != nil {
		return defaultVal
	}
	input = strings.TrimSpace(input)
	if input == "" {
		return defaultVal
	}
	return input
}

// promptPassword prompts for a password input.
// Returns empty string if user presses Enter (indicating random generation).
func promptPassword(reader *bufio.Reader, prompt string) string {
	fmt.Printf("  %s: ", prompt)
	input, err := reader.ReadString('\n')
	if err != nil {
		return ""
	}
	return strings.TrimSpace(input)
}

// printSummary displays the configuration summary before confirmation.
func printSummary(config *Config) {
	fmt.Println()
	fmt.Println(cli.Highlight("Configuration Summary"))
	fmt.Println(cli.Separator(60))
	fmt.Println()

	// Role display with color
	var roleDisplay string
	switch config.Role {
	case "standalone":
		roleDisplay = cli.Success("Standalone (single server)")
	case "master":
		roleDisplay = cli.Success("Master (leader node)")
	case "slave":
		roleDisplay = cli.Warning("Slave (follower node)")
	default:
		roleDisplay = config.Role
	}

	cli.KeyValue("Operative Mode", roleDisplay, 22)
	cli.KeyValue("Text Protocol Port", config.Port, 22)
	cli.KeyValue("Binary Protocol Port", config.BinaryPort, 22)

	if config.Role == "master" {
		cli.KeyValue("Replication Port", config.ReplPort, 22)
	}

	if config.Role == "slave" {
		cli.KeyValue("Master Address", config.MasterAddr, 22)
	}

	cli.KeyValue("Database Path", config.DBPath, 22)
	cli.KeyValue("Log Level", config.LogLevel, 22)
	cli.KeyValue("JSON Logging", boolToYesNo(config.LogJSON), 22)

	if config.IsFirstSetup {
		if config.AdminPassword == "" {
			cli.KeyValue("Admin Password", cli.Warning("(will be generated)"), 22)
		} else {
			cli.KeyValue("Admin Password", cli.Success("(user-specified)"), 22)
		}
	}
	fmt.Println()
}

// boolToYesNo converts a boolean to "Yes" or "No" string.
func boolToYesNo(b bool) string {
	if b {
		return "Yes"
	}
	return "No"
}

// ValidatePort checks if a port string is a valid port number.
func ValidatePort(port string) bool {
	p, err := strconv.Atoi(port)
	if err != nil {
		return false
	}
	return p > 0 && p <= 65535
}

// PrintStartupMessage displays a message after successful wizard completion.
func PrintStartupMessage(config *Config) {
	banner.Print()
	fmt.Println()
	cli.PrintSuccess("Starting FlyDB in %s mode...", config.Role)
	fmt.Println()
}
