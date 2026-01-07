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
	"path/filepath"
	"strconv"
	"strings"

	"flydb/internal/banner"
	"flydb/internal/config"
	"flydb/pkg/cli"
)

// UserChoice represents the user's choice when an existing config is found.
type UserChoice int

const (
	// ChoiceUseExisting uses the existing configuration as-is.
	ChoiceUseExisting UserChoice = iota
	// ChoiceModify modifies the existing configuration.
	ChoiceModify
	// ChoiceCreateNew creates a new configuration from scratch.
	ChoiceCreateNew
	// ChoiceCancelled indicates the user cancelled.
	ChoiceCancelled
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
	ConfigFile    string // Path to the configuration file (if loaded or saved)
	SaveConfig    bool   // Whether to save the configuration to a file
}

// DefaultConfig returns the default configuration values.
func DefaultConfig() Config {
	return Config{
		Port:          "8888",
		BinaryPort:    "8889",
		ReplPort:      "9999",
		Role:          "standalone",
		MasterAddr:    "",
		DBPath:        "flydb.wal",
		LogLevel:      "info",
		LogJSON:       false,
		AdminPassword: "",
		IsFirstSetup:  false,
		ConfigFile:    "",
		SaveConfig:    false,
	}
}

// FromConfig creates a wizard Config from a config.Config.
func FromConfig(cfg *config.Config) Config {
	return Config{
		Port:          strconv.Itoa(cfg.Port),
		BinaryPort:    strconv.Itoa(cfg.BinaryPort),
		ReplPort:      strconv.Itoa(cfg.ReplPort),
		Role:          cfg.Role,
		MasterAddr:    cfg.MasterAddr,
		DBPath:        cfg.DBPath,
		LogLevel:      cfg.LogLevel,
		LogJSON:       cfg.LogJSON,
		AdminPassword: cfg.AdminPassword,
		IsFirstSetup:  false,
		ConfigFile:    cfg.ConfigFile,
		SaveConfig:    false,
	}
}

// ToConfig converts a wizard Config to a config.Config.
func (c *Config) ToConfig() *config.Config {
	port, _ := strconv.Atoi(c.Port)
	binaryPort, _ := strconv.Atoi(c.BinaryPort)
	replPort, _ := strconv.Atoi(c.ReplPort)

	return &config.Config{
		Port:          port,
		BinaryPort:    binaryPort,
		ReplPort:      replPort,
		Role:          c.Role,
		MasterAddr:    c.MasterAddr,
		DBPath:        c.DBPath,
		LogLevel:      c.LogLevel,
		LogJSON:       c.LogJSON,
		AdminPassword: c.AdminPassword,
		ConfigFile:    c.ConfigFile,
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

// LoadExistingConfig attempts to load an existing configuration file.
// Returns the loaded config and the file path, or nil if no config file was found.
func LoadExistingConfig() (*config.Config, string) {
	// Check for config file from environment variable first
	if envPath := os.Getenv(config.EnvConfigFile); envPath != "" {
		mgr := config.NewManager()
		if err := mgr.LoadFromFile(envPath); err == nil {
			cfg := mgr.Get()
			return cfg, envPath
		}
	}

	// Try to find config file in standard locations
	configPath := config.FindConfigFile()
	if configPath == "" {
		return nil, ""
	}

	mgr := config.NewManager()
	if err := mgr.LoadFromFile(configPath); err != nil {
		return nil, ""
	}

	cfg := mgr.Get()
	return cfg, configPath
}

// DisplayExistingConfig displays the current configuration in a formatted way.
func DisplayExistingConfig(cfg *config.Config, configPath string) {
	fmt.Println()
	fmt.Println(cli.Success("  ✓ Found existing configuration"))
	fmt.Println()
	fmt.Printf("    %s %s\n", cli.Dimmed("File:"), configPath)
	fmt.Println()
	fmt.Println(cli.Highlight("  Current Settings"))
	fmt.Println("  " + cli.Separator(56))
	fmt.Println()

	// Role display with color and icon
	var roleDisplay string
	var roleIcon string
	switch cfg.Role {
	case "standalone":
		roleDisplay = "Standalone"
		roleIcon = "◉"
	case "master":
		roleDisplay = "Master"
		roleIcon = "★"
	case "slave":
		roleDisplay = "Slave"
		roleIcon = "◎"
	default:
		roleDisplay = cfg.Role
		roleIcon = "○"
	}

	fmt.Printf("    %s %-20s %s %s\n", cli.Dimmed("Role:"), roleDisplay, cli.Dimmed(roleIcon), cli.Dimmed(getRoleDescription(cfg.Role)))
	fmt.Printf("    %s %-20s %s\n", cli.Dimmed("Ports:"), fmt.Sprintf("%d (text), %d (binary)", cfg.Port, cfg.BinaryPort), cli.Dimmed(fmt.Sprintf("repl: %d", cfg.ReplPort)))

	if cfg.Role == "slave" && cfg.MasterAddr != "" {
		fmt.Printf("    %s %s\n", cli.Dimmed("Master:"), cfg.MasterAddr)
	}

	fmt.Printf("    %s %s\n", cli.Dimmed("Database:"), cfg.DBPath)
	fmt.Printf("    %s %s %s\n", cli.Dimmed("Logging:"), cfg.LogLevel, formatJSONLogging(cfg.LogJSON))
	fmt.Println()
}

// getRoleDescription returns a short description for the role.
func getRoleDescription(role string) string {
	switch role {
	case "standalone":
		return "(single server)"
	case "master":
		return "(leader node)"
	case "slave":
		return "(follower node)"
	default:
		return ""
	}
}

// formatJSONLogging formats the JSON logging setting.
func formatJSONLogging(enabled bool) string {
	if enabled {
		return cli.Dimmed("(JSON output)")
	}
	return ""
}

// PromptConfigChoice asks the user what they want to do with the existing configuration.
func PromptConfigChoice(reader *bufio.Reader) UserChoice {
	fmt.Println("  " + cli.Highlight("Options"))
	fmt.Println("  " + cli.Separator(56))
	fmt.Println()
	fmt.Printf("    %s  Use this configuration %s\n", cli.Success("[1]"), cli.Dimmed("(recommended)"))
	fmt.Printf("    %s  Modify settings\n", cli.Highlight("[2]"))
	fmt.Printf("    %s  Start fresh with defaults\n", cli.Highlight("[3]"))
	fmt.Printf("    %s  Cancel\n", cli.Dimmed("[4]"))
	fmt.Println()

	choice := promptWithDefault(reader, "  Your choice", "1")
	switch choice {
	case "1":
		return ChoiceUseExisting
	case "2":
		return ChoiceModify
	case "3":
		return ChoiceCreateNew
	case "4":
		return ChoiceCancelled
	default:
		return ChoiceUseExisting
	}
}

// PromptSaveConfig asks the user if they want to save the configuration to a file.
func PromptSaveConfig(reader *bufio.Reader, existingPath string) (bool, string) {
	fmt.Println()
	fmt.Println("  " + cli.Highlight("Save Configuration"))
	fmt.Println("  " + cli.Separator(56))
	fmt.Println()

	save := promptWithDefault(reader, "  Save to file? (Y/n)", "y")
	if strings.ToLower(save) == "n" || strings.ToLower(save) == "no" {
		return false, ""
	}

	// Determine default save path
	defaultPath := existingPath
	if defaultPath == "" {
		// Use current directory by default
		defaultPath = "./flydb.conf"
	}

	savePath := promptWithDefault(reader, "  File path", defaultPath)

	// Expand home directory if needed
	if strings.HasPrefix(savePath, "~/") {
		home, err := os.UserHomeDir()
		if err == nil {
			savePath = filepath.Join(home, savePath[2:])
		}
	}

	return true, savePath
}

// SaveConfigToFile saves the wizard configuration to a TOML file.
func SaveConfigToFile(wizardCfg *Config, path string) error {
	cfg := wizardCfg.ToConfig()
	return cfg.SaveToFile(path)
}

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
	wizardCfg := DefaultConfig()
	wizardCfg.IsFirstSetup = needsAdminSetup

	// Try to load existing configuration
	existingCfg, configPath := LoadExistingConfig()

	// Print header
	printWizardHeader()

	// If existing config found, display it and ask what to do
	if existingCfg != nil {
		DisplayExistingConfig(existingCfg, configPath)

		choice := PromptConfigChoice(reader)
		switch choice {
		case ChoiceUseExisting:
			// Use existing config as-is
			wizardCfg = FromConfig(existingCfg)
			wizardCfg.IsFirstSetup = needsAdminSetup
			fmt.Println()

			// Still need to handle admin setup if required
			if needsAdminSetup {
				printAdminPasswordPrompt(reader, &wizardCfg)
			}

			// Confirm and start
			fmt.Println()
			confirm := promptWithDefault(reader, "  Ready to start FlyDB? (Y/n)", "y")
			if strings.ToLower(confirm) != "y" && strings.ToLower(confirm) != "yes" && confirm != "" {
				fmt.Println()
				cli.PrintWarning("Cancelled. Use 'flydb --help' for command-line options.")
				return nil
			}
			fmt.Println()
			return &wizardCfg

		case ChoiceModify:
			// Pre-populate with existing values
			wizardCfg = FromConfig(existingCfg)
			wizardCfg.IsFirstSetup = needsAdminSetup
			fmt.Println()

		case ChoiceCreateNew:
			// Start fresh with defaults
			fmt.Println()

		case ChoiceCancelled:
			fmt.Println()
			cli.PrintWarning("Cancelled. Use 'flydb --help' for command-line options.")
			return nil
		}
	} else {
		printWelcomeMessage()
	}

	// Run the configuration wizard steps
	wizardCfg = *runConfigurationSteps(reader, &wizardCfg, needsAdminSetup)

	// Ask about saving configuration
	save, savePath := PromptSaveConfig(reader, configPath)
	if save {
		wizardCfg.SaveConfig = true
		wizardCfg.ConfigFile = savePath
	}

	// Display configuration summary
	printSummary(&wizardCfg)

	// Confirm and start
	fmt.Println()
	confirm := promptWithDefault(reader, "  Ready to start FlyDB? (Y/n)", "y")
	if strings.ToLower(confirm) != "y" && strings.ToLower(confirm) != "yes" && confirm != "" {
		fmt.Println()
		cli.PrintWarning("Cancelled. Use 'flydb --help' for command-line options.")
		return nil
	}

	// Save configuration if requested
	if wizardCfg.SaveConfig && wizardCfg.ConfigFile != "" {
		if err := SaveConfigToFile(&wizardCfg, wizardCfg.ConfigFile); err != nil {
			cli.PrintWarning("Failed to save configuration: %v", err)
		} else {
			fmt.Println()
			cli.PrintSuccess("Configuration saved to %s", wizardCfg.ConfigFile)
		}
	}

	fmt.Println()
	return &wizardCfg
}

// printWizardHeader prints the wizard header with banner and version info.
func printWizardHeader() {
	// Print the main FlyDB banner
	banner.Print()

	// Print wizard subtitle
	fmt.Println()
	fmt.Println("  " + cli.Highlight("Interactive Configuration Wizard"))
	fmt.Println("  " + cli.Separator(40))
	fmt.Println()
}

// printWelcomeMessage prints the welcome message for new configurations.
func printWelcomeMessage() {
	fmt.Println("  Welcome! This wizard will help you configure FlyDB.")
	fmt.Println()
	fmt.Printf("  %s Press %s to accept defaults shown in %s\n", cli.Dimmed("•"), cli.Highlight("Enter"), cli.Dimmed("[brackets]"))
	fmt.Printf("  %s Press %s to cancel\n", cli.Dimmed("•"), cli.Highlight("Ctrl+C"))
	fmt.Println()
}

// printAdminPasswordPrompt prompts for admin password setup.
func printAdminPasswordPrompt(reader *bufio.Reader, cfg *Config) {
	fmt.Println()
	fmt.Println("  " + cli.Highlight("Admin Password Setup"))
	fmt.Println("  " + cli.Separator(56))
	fmt.Println()
	cli.PrintWarning("  First-time database initialization detected.")
	fmt.Println()
	fmt.Printf("  %s Enter a password, or press %s for auto-generated\n", cli.Dimmed("•"), cli.Highlight("Enter"))
	fmt.Println()
	cfg.AdminPassword = promptPassword(reader, "  Admin password")
}

// runConfigurationSteps runs the interactive configuration steps.
func runConfigurationSteps(reader *bufio.Reader, cfg *Config, needsAdminSetup bool) *Config {
	// Step 1: Select operative mode
	printStepHeader(1, "Server Role")
	fmt.Println()
	fmt.Printf("    %s  Standalone %s\n", cli.Success("[1]"), cli.Dimmed("- single server, development/small deployments"))
	fmt.Printf("    %s  Master     %s\n", cli.Highlight("[2]"), cli.Dimmed("- leader node, accepts writes"))
	fmt.Printf("    %s  Slave      %s\n", cli.Highlight("[3]"), cli.Dimmed("- follower node, read replicas"))
	fmt.Println()

	// Determine default mode selection based on current role
	defaultMode := "1"
	switch cfg.Role {
	case "standalone":
		defaultMode = "1"
	case "master":
		defaultMode = "2"
	case "slave":
		defaultMode = "3"
	}

	mode := promptWithDefault(reader, "  Select role", defaultMode)
	switch mode {
	case "1":
		cfg.Role = "standalone"
	case "2":
		cfg.Role = "master"
	case "3":
		cfg.Role = "slave"
	default:
		cfg.Role = "standalone"
	}
	fmt.Println()

	// Step 2: Configure ports with validation
	printStepHeader(2, "Network Ports")
	fmt.Println()

	cfg.Port = promptPortWithValidation(reader, "  Text protocol port", cfg.Port)
	cfg.BinaryPort = promptPortWithValidation(reader, "  Binary protocol port", cfg.BinaryPort)

	if cfg.Role == "master" {
		cfg.ReplPort = promptPortWithValidation(reader, "  Replication port", cfg.ReplPort)
	}
	fmt.Println()

	// Step 3: Configure slave-specific settings
	stepNum := 3
	if cfg.Role == "slave" {
		printStepHeader(stepNum, "Replication")
		fmt.Println()
		defaultMaster := cfg.MasterAddr
		if defaultMaster == "" {
			defaultMaster = "localhost:9999"
		}
		cfg.MasterAddr = promptWithValidation(reader, "  Master address (host:port)", defaultMaster, validateAddress)
		fmt.Println()
		stepNum++
	}

	// Configure storage
	printStepHeader(stepNum, "Storage")
	fmt.Println()
	cfg.DBPath = promptWithDefault(reader, "  Database file path", cfg.DBPath)
	fmt.Println()

	// Configure logging
	stepNum++
	printStepHeader(stepNum, "Logging")
	fmt.Println()
	fmt.Printf("    Available levels: %s %s %s %s\n",
		cli.Success("debug"), cli.Highlight("info"), cli.Warning("warn"), cli.Error("error"))
	fmt.Println()
	cfg.LogLevel = promptWithValidation(reader, "  Log level", cfg.LogLevel, validateLogLevel)
	defaultJSON := "n"
	if cfg.LogJSON {
		defaultJSON = "y"
	}
	jsonLog := promptWithDefault(reader, "  JSON output? (y/n)", defaultJSON)
	cfg.LogJSON = strings.ToLower(jsonLog) == "y" || strings.ToLower(jsonLog) == "yes"
	fmt.Println()

	// Configure admin password (only for first-time setup)
	if needsAdminSetup {
		stepNum++
		printStepHeader(stepNum, "Admin Password")
		printAdminPasswordPrompt(reader, cfg)
	}

	return cfg
}

// printStepHeader prints a formatted step header.
func printStepHeader(step int, title string) {
	fmt.Printf("  %s %s\n", cli.Highlight(fmt.Sprintf("Step %d:", step)), cli.Highlight(title))
	fmt.Println("  " + cli.Separator(56))
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
	fmt.Printf("%s [%s%s%s]: ", prompt, colorYellow, defaultVal, colorReset)
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
	fmt.Printf("%s: ", prompt)
	input, err := reader.ReadString('\n')
	if err != nil {
		return ""
	}
	return strings.TrimSpace(input)
}

// printSummary displays the configuration summary before confirmation.
func printSummary(cfg *Config) {
	fmt.Println()
	fmt.Println("  " + cli.Highlight("Configuration Summary"))
	fmt.Println("  " + cli.Separator(56))
	fmt.Println()

	// Role display
	roleDisplay := cfg.Role
	roleDesc := getRoleDescription(cfg.Role)
	fmt.Printf("    %s %s %s\n", cli.Dimmed("Role:"), roleDisplay, cli.Dimmed(roleDesc))

	// Ports
	ports := fmt.Sprintf("%s (text), %s (binary)", cfg.Port, cfg.BinaryPort)
	if cfg.Role == "master" {
		ports += fmt.Sprintf(", %s (repl)", cfg.ReplPort)
	}
	fmt.Printf("    %s %s\n", cli.Dimmed("Ports:"), ports)

	if cfg.Role == "slave" {
		fmt.Printf("    %s %s\n", cli.Dimmed("Master:"), cfg.MasterAddr)
	}

	fmt.Printf("    %s %s\n", cli.Dimmed("Database:"), cfg.DBPath)
	fmt.Printf("    %s %s\n", cli.Dimmed("Logging:"), cfg.LogLevel+formatJSONLogging(cfg.LogJSON))

	if cfg.IsFirstSetup {
		if cfg.AdminPassword == "" {
			fmt.Printf("    %s %s\n", cli.Dimmed("Admin:"), cli.Warning("auto-generated"))
		} else {
			fmt.Printf("    %s %s\n", cli.Dimmed("Admin:"), cli.Success("user-specified"))
		}
	}

	if cfg.SaveConfig && cfg.ConfigFile != "" {
		fmt.Printf("    %s %s\n", cli.Dimmed("Config:"), cli.Success(cfg.ConfigFile))
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
