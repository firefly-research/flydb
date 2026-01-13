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
 3. Configure encryption and passphrase
 4. Validate inputs and display summary
 5. Return configuration for server startup

Encryption Passphrase:
======================

When encryption is enabled (the default), the wizard handles passphrase configuration:

  - If FLYDB_ENCRYPTION_PASSPHRASE environment variable is set, it is used automatically
  - Otherwise, the user is prompted to enter a passphrase interactively
  - User can skip by pressing Enter (must set env var before starting)
  - The passphrase is stored in memory only, never written to config files
*/
package wizard

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"flydb/internal/auth"
	"flydb/internal/banner"
	"flydb/internal/config"
	"flydb/internal/storage"
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
	Port                 string // Server port for client connections (binary protocol)
	ReplPort             string
	ClusterPort          string
	Role                 string
	MasterAddr           string
	DBPath               string
	DataDir              string // Directory for multi-database storage
	MultiDBEnabled       bool   // Enable multi-database mode
	EncryptionEnabled    bool   // Enable data-at-rest encryption
	EncryptionPassphrase string // Passphrase for encryption (not stored in config file)
	GeneratedPassphrase  string // Auto-generated passphrase (for display only)
	LogLevel             string
	LogJSON              bool
	AdminPassword        string // Admin password for first-time setup (empty = generate random)
	IsFirstSetup         bool   // True if this is a first-time setup requiring admin initialization
	ConfigFile           string // Path to the configuration file (if loaded or saved)
	SaveConfig           bool   // Whether to save the configuration to a file

	// Cluster configuration
	ClusterPeers      []string // List of peer addresses for cluster mode
	HeartbeatInterval int      // Heartbeat interval in milliseconds
	HeartbeatTimeout  int      // Heartbeat timeout in milliseconds
	ElectionTimeout   int      // Election timeout in milliseconds
	MinQuorum         int      // Minimum nodes for quorum (0 = auto)
	EnablePreVote     bool     // Enable pre-vote protocol
	PartitionCount    int      // Number of data partitions (power of 2)
	ReplicationFactor int      // Number of replicas per partition

	// Replication configuration
	ReplicationMode   string // async, semi_sync, or sync
	SyncTimeout       int    // Timeout for sync replication in ms
	MaxReplicationLag int    // Max acceptable replication lag in ms

	// Raft consensus configuration (01.26.13+)
	EnableRaft            bool // Enable Raft consensus (replaces Bully)
	RaftElectionTimeout   int  // Raft election timeout in ms
	RaftHeartbeatInterval int  // Raft heartbeat interval in ms

	// Compression configuration (01.26.13+)
	EnableCompression    bool   // Enable compression for WAL and replication
	CompressionAlgorithm string // gzip, lz4, snappy, or zstd
	CompressionMinSize   int    // Minimum size to compress (bytes)

	// Performance configuration (01.26.13+)
	EnableZeroCopy      bool // Enable zero-copy buffer pooling
	BufferPoolSizeBytes int  // Buffer pool size in bytes (0 = auto)
}

// DefaultConfig returns the default configuration values.
// Note: Encryption is enabled by default for security. Users must provide a passphrase
// via the FLYDB_ENCRYPTION_PASSPHRASE environment variable.
func DefaultConfig() Config {
	return Config{
		Port:              "8889", // Default port for binary protocol
		ReplPort:          "9999",
		ClusterPort:       "9998",
		Role:              "standalone",
		MasterAddr:        "",
		DBPath:            "flydb.fdb",
		DataDir:           config.GetDefaultDataDir(),
		MultiDBEnabled:    true, // Multi-database mode is always enabled
		EncryptionEnabled: true, // Encryption enabled by default for security
		LogLevel:          "info",
		LogJSON:           false,
		AdminPassword:     "",
		IsFirstSetup:      false,
		ConfigFile:        "",
		SaveConfig:        false,

		// Cluster defaults
		ClusterPeers:      []string{},
		HeartbeatInterval: 500,  // 500ms
		HeartbeatTimeout:  2000, // 2s
		ElectionTimeout:   1000, // 1s
		MinQuorum:         0,    // auto-calculate
		EnablePreVote:     true,
		PartitionCount:    256,  // Number of data partitions
		ReplicationFactor: 3,    // Number of replicas per partition

		// Replication defaults
		ReplicationMode:   "async",
		SyncTimeout:       5000,  // 5s
		MaxReplicationLag: 10000, // 10s

		// Raft consensus defaults (01.26.13+)
		EnableRaft:            true, // Enable Raft by default
		RaftElectionTimeout:   1000, // 1s
		RaftHeartbeatInterval: 150,  // 150ms

		// Compression defaults (01.26.13+)
		EnableCompression:    false,  // Disabled by default
		CompressionAlgorithm: "gzip", // Default to gzip
		CompressionMinSize:   256,    // 256 bytes minimum

		// Performance defaults (01.26.13+)
		EnableZeroCopy:      true, // Enabled by default
		BufferPoolSizeBytes: 0,    // Auto-size
	}
}

// FromConfig creates a wizard Config from a config.Config.
func FromConfig(cfg *config.Config) Config {
	return Config{
		Port:                 strconv.Itoa(cfg.Port),
		ReplPort:             strconv.Itoa(cfg.ReplPort),
		ClusterPort:          strconv.Itoa(cfg.ClusterPort),
		Role:                 cfg.Role,
		MasterAddr:           cfg.MasterAddr,
		DBPath:               cfg.DBPath,
		DataDir:              cfg.DataDir,
		MultiDBEnabled:       cfg.DataDir != "",
		EncryptionEnabled:    cfg.EncryptionEnabled,
		EncryptionPassphrase: cfg.EncryptionPassphrase,
		LogLevel:             cfg.LogLevel,
		LogJSON:              cfg.LogJSON,
		AdminPassword:        cfg.AdminPassword,
		IsFirstSetup:         false,
		ConfigFile:           cfg.ConfigFile,
		SaveConfig:           false,

		// Cluster configuration
		ClusterPeers:      cfg.ClusterPeers,
		HeartbeatInterval: cfg.HeartbeatInterval,
		HeartbeatTimeout:  cfg.HeartbeatTimeout,
		ElectionTimeout:   cfg.ElectionTimeout,
		MinQuorum:         cfg.MinQuorum,
		EnablePreVote:     cfg.EnablePreVote,
		PartitionCount:    cfg.PartitionCount,
		ReplicationFactor: cfg.ReplicationFactor,

		// Replication configuration
		ReplicationMode:   cfg.ReplicationMode,
		SyncTimeout:       cfg.SyncTimeout,
		MaxReplicationLag: cfg.MaxReplicationLag,

		// Raft consensus configuration (01.26.13+)
		EnableRaft:            cfg.EnableRaft,
		RaftElectionTimeout:   cfg.RaftElectionTimeout,
		RaftHeartbeatInterval: cfg.RaftHeartbeatInterval,

		// Compression configuration (01.26.13+)
		EnableCompression:    cfg.EnableCompression,
		CompressionAlgorithm: cfg.CompressionAlgorithm,
		CompressionMinSize:   cfg.CompressionMinSize,

		// Performance configuration (01.26.13+)
		EnableZeroCopy:      cfg.EnableZeroCopy,
		BufferPoolSizeBytes: cfg.BufferPoolSizeBytes,
	}
}

// ToConfig converts a wizard Config to a config.Config.
func (c *Config) ToConfig() *config.Config {
	port, _ := strconv.Atoi(c.Port)
	replPort, _ := strconv.Atoi(c.ReplPort)
	clusterPort, _ := strconv.Atoi(c.ClusterPort)

	dataDir := c.DataDir
	if !c.MultiDBEnabled {
		dataDir = ""
	}

	return &config.Config{
		Port:                 port,
		ReplPort:             replPort,
		ClusterPort:          clusterPort,
		Role:                 c.Role,
		MasterAddr:           c.MasterAddr,
		DBPath:               c.DBPath,
		DataDir:              dataDir,
		EncryptionEnabled:    c.EncryptionEnabled,
		EncryptionPassphrase: c.EncryptionPassphrase,
		LogLevel:             c.LogLevel,
		LogJSON:              c.LogJSON,
		AdminPassword:        c.AdminPassword,
		ConfigFile:           c.ConfigFile,

		// Cluster configuration
		ClusterPeers:      c.ClusterPeers,
		HeartbeatInterval: c.HeartbeatInterval,
		HeartbeatTimeout:  c.HeartbeatTimeout,
		ElectionTimeout:   c.ElectionTimeout,
		MinQuorum:         c.MinQuorum,
		EnablePreVote:     c.EnablePreVote,
		PartitionCount:    c.PartitionCount,
		ReplicationFactor: c.ReplicationFactor,

		// Replication configuration
		ReplicationMode:   c.ReplicationMode,
		SyncTimeout:       c.SyncTimeout,
		MaxReplicationLag: c.MaxReplicationLag,

		// Raft consensus configuration (01.26.13+)
		EnableRaft:            c.EnableRaft,
		RaftElectionTimeout:   c.RaftElectionTimeout,
		RaftHeartbeatInterval: c.RaftHeartbeatInterval,

		// Compression configuration (01.26.13+)
		EnableCompression:    c.EnableCompression,
		CompressionAlgorithm: c.CompressionAlgorithm,
		CompressionMinSize:   c.CompressionMinSize,

		// Performance configuration (01.26.13+)
		EnableZeroCopy:      c.EnableZeroCopy,
		BufferPoolSizeBytes: c.BufferPoolSizeBytes,
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
	switch cfg.Role {
	case "standalone":
		roleDisplay = "Standalone"
	case "master":
		roleDisplay = "Master"
	case "slave":
		roleDisplay = "Slave"
	case "cluster":
		roleDisplay = "Cluster"
	default:
		roleDisplay = cfg.Role
	}

	fmt.Printf("    %-16s %s %s\n", cli.Dimmed("Role:"), roleDisplay, cli.Dimmed(getRoleDescription(cfg.Role)))
	fmt.Printf("    %-16s %d\n", cli.Dimmed("Port:"), cfg.Port)
	if cfg.Role == "master" || cfg.Role == "cluster" {
		fmt.Printf("    %-16s %d\n", cli.Dimmed("Repl Port:"), cfg.ReplPort)
	}
	if cfg.Role == "cluster" {
		fmt.Printf("    %-16s %d\n", cli.Dimmed("Cluster Port:"), cfg.ClusterPort)
		if len(cfg.ClusterPeers) > 0 {
			fmt.Printf("    %-16s %v\n", cli.Dimmed("Cluster Peers:"), cfg.ClusterPeers)
		}
		fmt.Printf("    %-16s %s\n", cli.Dimmed("Replication:"), cfg.ReplicationMode)
	}

	if cfg.Role == "slave" && cfg.MasterAddr != "" {
		fmt.Printf("    %-16s %s\n", cli.Dimmed("Master:"), cfg.MasterAddr)
	}

	// Storage
	fmt.Printf("    %-16s %s\n", cli.Dimmed("Data Directory:"), cfg.DataDir)

	// Encryption
	fmt.Printf("    %-16s %s\n", cli.Dimmed("Encryption:"), formatEncryptionWithPassphrase(cfg.EncryptionEnabled, cfg.EncryptionPassphrase))

	// Logging
	fmt.Printf("    %-16s %s%s\n", cli.Dimmed("Log Level:"), cfg.LogLevel, formatJSONLogging(cfg.LogJSON))
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
	case "cluster":
		return "(distributed cluster)"
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

// formatEncryption formats the encryption setting.
func formatEncryption(enabled bool) string {
	if enabled {
		return cli.Success("enabled")
	}
	return cli.Dimmed("disabled")
}

// formatEncryptionWithPassphrase formats the encryption setting with passphrase status.
func formatEncryptionWithPassphrase(enabled bool, passphrase string) string {
	if !enabled {
		return cli.Dimmed("disabled")
	}
	if passphrase != "" {
		return cli.Success("enabled") + " " + cli.Success("(passphrase set)")
	}
	return cli.Success("enabled") + " " + cli.Warning("(passphrase required)")
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

			// Handle encryption passphrase if encryption is enabled
			if wizardCfg.EncryptionEnabled {
				envPassphrase := os.Getenv(config.EnvEncryptionPassphrase)
				if envPassphrase != "" {
					wizardCfg.EncryptionPassphrase = envPassphrase
					fmt.Printf("    %s Encryption passphrase loaded from %s\n", cli.Success("✓"), cli.Highlight("FLYDB_ENCRYPTION_PASSPHRASE"))
					fmt.Println()
				} else {
					// Check if this is first-time setup (no data exists yet)
					isFirstTimeSetup := !storage.DataDirectoryHasData(wizardCfg.DataDir)
					if isFirstTimeSetup {
						// Auto-generate passphrase for first-time setup
						generatedPassphrase, err := auth.GenerateSecurePassword(24)
						if err != nil {
							cli.PrintError("Failed to generate passphrase: %s", err.Error())
							return nil
						}
						wizardCfg.EncryptionPassphrase = generatedPassphrase
						wizardCfg.GeneratedPassphrase = generatedPassphrase

						fmt.Println()
						fmt.Println("  " + cli.Warning("═══════════════════════════════════════════════════════════"))
						fmt.Println("  " + cli.Warning("  FIRST-TIME SETUP: Encryption Passphrase Generated"))
						fmt.Println("  " + cli.Warning("═══════════════════════════════════════════════════════════"))
						fmt.Println()
						fmt.Printf("    %s %s\n", cli.Dimmed("Passphrase:"), cli.Highlight(generatedPassphrase))
						fmt.Println()
						fmt.Println("  " + cli.Warning("IMPORTANT: Save this passphrase securely!"))
						fmt.Println("  " + cli.Warning("Data cannot be recovered without it!"))
						fmt.Println()
						fmt.Printf("    %s\n", cli.Dimmed("Set this environment variable before starting:"))
						fmt.Printf("    %s\n", cli.Info("export FLYDB_ENCRYPTION_PASSPHRASE=\""+generatedPassphrase+"\""))
						fmt.Println()
						fmt.Println("  " + cli.Warning("═══════════════════════════════════════════════════════════"))
						fmt.Println()
					} else {
						// Existing data but no passphrase - MUST prompt for it
						fmt.Println()
						cli.PrintError("Encryption is enabled but no passphrase was provided.")
						fmt.Println()
						fmt.Println("  " + cli.Warning("Existing encrypted data was found in: ") + cli.Highlight(wizardCfg.DataDir))
						fmt.Println()
						fmt.Println("  " + cli.Highlight("You MUST provide the original passphrase to access your data."))
						fmt.Println()
						fmt.Println("  " + cli.Dimmed("Options:"))
						fmt.Println("    1. Set the environment variable and restart:")
						fmt.Println("       " + cli.Info("export FLYDB_ENCRYPTION_PASSPHRASE=\"your-passphrase\""))
						fmt.Println()
						fmt.Println("    2. Enter the passphrase now:")
						fmt.Println()
						passphrase := promptPassword(reader, "  Encryption passphrase")
						if passphrase == "" {
							fmt.Println()
							cli.PrintError("Passphrase is required to access encrypted data.")
							fmt.Println()
							fmt.Println("  " + cli.Warning("If you have lost your passphrase, your data cannot be recovered."))
							fmt.Println()
							return nil
						}
						wizardCfg.EncryptionPassphrase = passphrase
						fmt.Println()
						fmt.Printf("    %s Passphrase set\n", cli.Success("✓"))
						fmt.Println()
					}
				}
			}

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
	fmt.Printf("    %s  Cluster    %s\n", cli.Highlight("[4]"), cli.Dimmed("- distributed cluster with automatic failover"))
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
	case "cluster":
		defaultMode = "4"
	}

	mode := promptWithDefault(reader, "  Select role", defaultMode)
	switch mode {
	case "1":
		cfg.Role = "standalone"
	case "2":
		cfg.Role = "master"
	case "3":
		cfg.Role = "slave"
	case "4":
		cfg.Role = "cluster"
	default:
		cfg.Role = "standalone"
	}
	fmt.Println()

	// Step 2: Configure ports with validation
	printStepHeader(2, "Network Ports")
	fmt.Println()

	cfg.Port = promptPortWithValidation(reader, "  Server port (binary protocol)", cfg.Port)

	if cfg.Role == "master" || cfg.Role == "cluster" {
		cfg.ReplPort = promptPortWithValidation(reader, "  Replication port", cfg.ReplPort)
	}
	if cfg.Role == "cluster" {
		cfg.ClusterPort = promptPortWithValidation(reader, "  Cluster port", cfg.ClusterPort)
	}
	fmt.Println()

	// Step 3: Configure role-specific settings
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

	// Configure cluster-specific settings
	if cfg.Role == "cluster" {
		printStepHeader(stepNum, "Cluster Configuration")
		fmt.Println()
		fmt.Printf("    %s Enter peer addresses for cluster nodes\n", cli.Dimmed("•"))
		fmt.Printf("    %s Format: host:port (comma-separated for multiple)\n", cli.Dimmed("•"))
		fmt.Printf("    %s Example: node2:9998,node3:9998\n", cli.Dimmed("•"))
		fmt.Println()

		// Get existing peers as default
		defaultPeers := ""
		if len(cfg.ClusterPeers) > 0 {
			defaultPeers = strings.Join(cfg.ClusterPeers, ",")
		}
		peersInput := promptWithDefault(reader, "  Cluster peers", defaultPeers)
		if peersInput != "" {
			peers := strings.Split(peersInput, ",")
			cfg.ClusterPeers = make([]string, 0, len(peers))
			for _, peer := range peers {
				peer = strings.TrimSpace(peer)
				if peer != "" {
					cfg.ClusterPeers = append(cfg.ClusterPeers, peer)
				}
			}
		}
		fmt.Println()
		stepNum++

		// Replication mode
		printStepHeader(stepNum, "Replication Mode")
		fmt.Println()
		fmt.Printf("    %s  Async     %s\n", cli.Success("[1]"), cli.Dimmed("- best performance, eventual consistency"))
		fmt.Printf("    %s  Semi-sync %s\n", cli.Highlight("[2]"), cli.Dimmed("- balanced, at least one replica confirms"))
		fmt.Printf("    %s  Sync      %s\n", cli.Highlight("[3]"), cli.Dimmed("- strongest consistency, all replicas confirm"))
		fmt.Println()

		defaultReplMode := "1"
		switch cfg.ReplicationMode {
		case "async":
			defaultReplMode = "1"
		case "semi_sync":
			defaultReplMode = "2"
		case "sync":
			defaultReplMode = "3"
		}

		replMode := promptWithDefault(reader, "  Select replication mode", defaultReplMode)
		switch replMode {
		case "1":
			cfg.ReplicationMode = "async"
		case "2":
			cfg.ReplicationMode = "semi_sync"
		case "3":
			cfg.ReplicationMode = "sync"
		default:
			cfg.ReplicationMode = "async"
		}
		fmt.Println()
		stepNum++

		// Advanced cluster settings
		printStepHeader(stepNum, "Advanced Cluster Settings")
		fmt.Println()
		fmt.Printf("    %s Press Enter to accept defaults (recommended)\n", cli.Dimmed("•"))
		fmt.Println()

		// Heartbeat interval
		defaultHeartbeat := strconv.Itoa(cfg.HeartbeatInterval)
		heartbeatStr := promptWithDefault(reader, "  Heartbeat interval (ms)", defaultHeartbeat)
		if hb, err := strconv.Atoi(heartbeatStr); err == nil && hb >= 100 {
			cfg.HeartbeatInterval = hb
		}

		// Heartbeat timeout
		defaultTimeout := strconv.Itoa(cfg.HeartbeatTimeout)
		timeoutStr := promptWithDefault(reader, "  Heartbeat timeout (ms)", defaultTimeout)
		if to, err := strconv.Atoi(timeoutStr); err == nil && to >= cfg.HeartbeatInterval*2 {
			cfg.HeartbeatTimeout = to
		}

		// Election timeout
		defaultElection := strconv.Itoa(cfg.ElectionTimeout)
		electionStr := promptWithDefault(reader, "  Election timeout (ms)", defaultElection)
		if et, err := strconv.Atoi(electionStr); err == nil && et >= 500 {
			cfg.ElectionTimeout = et
		}

		// Min quorum
		defaultQuorum := strconv.Itoa(cfg.MinQuorum)
		quorumStr := promptWithDefault(reader, "  Min quorum (0=auto)", defaultQuorum)
		if q, err := strconv.Atoi(quorumStr); err == nil && q >= 0 {
			cfg.MinQuorum = q
		}

		// Pre-vote
		defaultPreVote := "y"
		if !cfg.EnablePreVote {
			defaultPreVote = "n"
		}
		preVoteStr := promptWithDefault(reader, "  Enable pre-vote protocol? (y/n)", defaultPreVote)
		cfg.EnablePreVote = strings.ToLower(preVoteStr) == "y" || strings.ToLower(preVoteStr) == "yes"

		// Partition count
		defaultPartitions := strconv.Itoa(cfg.PartitionCount)
		partitionsStr := promptWithDefault(reader, "  Partition count (power of 2)", defaultPartitions)
		if p, err := strconv.Atoi(partitionsStr); err == nil && p >= 16 && p <= 4096 {
			// Ensure it's a power of 2
			if p&(p-1) == 0 {
				cfg.PartitionCount = p
			}
		}

		// Replication factor
		defaultReplFactor := strconv.Itoa(cfg.ReplicationFactor)
		replFactorStr := promptWithDefault(reader, "  Replication factor (1-5)", defaultReplFactor)
		if rf, err := strconv.Atoi(replFactorStr); err == nil && rf >= 1 && rf <= 5 {
			cfg.ReplicationFactor = rf
		}

		fmt.Println()
		stepNum++
	}

	// Configure storage
	printStepHeader(stepNum, "Storage")
	fmt.Println()

	// FlyDB always uses multi-database mode - ask for data directory
	defaultDataDir := config.GetDefaultDataDir()
	fmt.Printf("    %s FlyDB stores each database in a separate directory\n", cli.Dimmed("•"))
	fmt.Printf("    %s Supports CREATE DATABASE, DROP DATABASE, USE commands\n", cli.Dimmed("•"))
	fmt.Printf("    %s Default location: %s\n", cli.Dimmed("•"), cli.Info(defaultDataDir))
	fmt.Println()

	if cfg.DataDir == "" {
		cfg.DataDir = defaultDataDir
	}
	cfg.DataDir = promptWithDefault(reader, "  Data directory for databases", cfg.DataDir)
	cfg.MultiDBEnabled = true // Always enabled
	fmt.Println()

	// Configure encryption
	stepNum++
	printStepHeader(stepNum, "Data-at-Rest Encryption")
	fmt.Println()
	fmt.Printf("    %s Encrypts WAL data on disk using AES-256-GCM\n", cli.Dimmed("•"))
	fmt.Printf("    %s %s\n", cli.Dimmed("•"), cli.Warning("Encryption is enabled by default for security"))
	fmt.Printf("    %s %s\n", cli.Dimmed("•"), cli.Warning("Keep your passphrase safe - data cannot be recovered without it!"))
	fmt.Println()
	defaultEncryption := "y" // Encryption enabled by default
	if !cfg.EncryptionEnabled {
		defaultEncryption = "n"
	}
	encryptionChoice := promptWithDefault(reader, "  Enable encryption? (y/n)", defaultEncryption)
	cfg.EncryptionEnabled = strings.ToLower(encryptionChoice) == "y" || strings.ToLower(encryptionChoice) == "yes"
	fmt.Println()

	// If encryption is enabled, prompt for passphrase
	if cfg.EncryptionEnabled {
		// Check if passphrase is already set via environment variable
		envPassphrase := os.Getenv(config.EnvEncryptionPassphrase)
		if envPassphrase != "" {
			cfg.EncryptionPassphrase = envPassphrase
			fmt.Printf("    %s Passphrase detected from %s environment variable\n", cli.Success("✓"), cli.Highlight("FLYDB_ENCRYPTION_PASSPHRASE"))
			fmt.Println()
		} else {
			// Prompt for passphrase
			fmt.Printf("    %s Enter a passphrase, or press %s for auto-generated\n", cli.Dimmed("•"), cli.Highlight("Enter"))
			fmt.Println()
			passphrase := promptPassword(reader, "  Encryption passphrase")
			if passphrase != "" {
				cfg.EncryptionPassphrase = passphrase
				fmt.Println()
				fmt.Printf("    %s Passphrase set successfully\n", cli.Success("✓"))
			} else {
				// Auto-generate passphrase
				generatedPassphrase, err := auth.GenerateSecurePassword(24)
				if err != nil {
					fmt.Println()
					cli.PrintError("  Failed to generate passphrase: %s", err.Error())
					fmt.Println()
				} else {
					cfg.EncryptionPassphrase = generatedPassphrase
					cfg.GeneratedPassphrase = generatedPassphrase // Store for display
					fmt.Println()
					fmt.Printf("    %s Passphrase auto-generated\n", cli.Success("✓"))
				}
			}
			fmt.Println()
		}
	}

	// Configure performance options (01.26.13+)
	stepNum++
	printStepHeader(stepNum, "Performance Options (01.26.13+)")
	fmt.Println()
	fmt.Printf("    %s These features are new in FlyDB 01.26.13\n", cli.Dimmed("•"))
	fmt.Println()

	// Raft consensus (for cluster mode)
	if cfg.Role == "cluster" {
		fmt.Printf("    %s\n", cli.Highlight("Consensus Algorithm"))
		fmt.Printf("    %s Raft provides stronger consistency than Bully\n", cli.Dimmed("•"))
		fmt.Println()
		fmt.Printf("    %s  Raft   %s\n", cli.Success("[1]"), cli.Dimmed("(recommended) - Strong consistency, pre-vote protocol"))
		fmt.Printf("    %s  Bully  %s\n", cli.Highlight("[2]"), cli.Dimmed("(legacy) - Simple leader election based on node ID"))
		fmt.Println()

		defaultConsensus := "1"
		if !cfg.EnableRaft {
			defaultConsensus = "2"
		}
		consensusChoice := promptWithDefault(reader, "  Select consensus algorithm", defaultConsensus)
		cfg.EnableRaft = consensusChoice != "2"
		fmt.Println()
	}

	// Compression
	fmt.Printf("    %s\n", cli.Highlight("Compression"))
	fmt.Printf("    %s Compress WAL entries and replication traffic\n", cli.Dimmed("•"))
	fmt.Println()

	defaultCompression := "n"
	if cfg.EnableCompression {
		defaultCompression = "y"
	}
	compressionChoice := promptWithDefault(reader, "  Enable compression? (y/n)", defaultCompression)
	cfg.EnableCompression = strings.ToLower(compressionChoice) == "y" || strings.ToLower(compressionChoice) == "yes"

	if cfg.EnableCompression {
		fmt.Println()
		fmt.Printf("    %s  gzip   %s\n", cli.Success("[1]"), cli.Dimmed("Good compression ratio, moderate speed"))
		fmt.Printf("    %s  lz4    %s\n", cli.Highlight("[2]"), cli.Dimmed("Very fast, lower compression ratio"))
		fmt.Printf("    %s  snappy %s\n", cli.Highlight("[3]"), cli.Dimmed("Fast, balanced for real-time use"))
		fmt.Printf("    %s  zstd   %s\n", cli.Highlight("[4]"), cli.Dimmed("Best compression ratio"))
		fmt.Println()

		defaultAlg := "1"
		switch cfg.CompressionAlgorithm {
		case "lz4":
			defaultAlg = "2"
		case "snappy":
			defaultAlg = "3"
		case "zstd":
			defaultAlg = "4"
		}
		algChoice := promptWithDefault(reader, "  Select algorithm", defaultAlg)
		switch algChoice {
		case "2":
			cfg.CompressionAlgorithm = "lz4"
		case "3":
			cfg.CompressionAlgorithm = "snappy"
		case "4":
			cfg.CompressionAlgorithm = "zstd"
		default:
			cfg.CompressionAlgorithm = "gzip"
		}
	}
	fmt.Println()

	// Zero-copy buffer pooling
	fmt.Printf("    %s\n", cli.Highlight("Zero-Copy Buffer Pooling"))
	fmt.Printf("    %s Reduces memory allocations and GC pressure\n", cli.Dimmed("•"))
	fmt.Println()

	defaultZeroCopy := "y"
	if !cfg.EnableZeroCopy {
		defaultZeroCopy = "n"
	}
	zeroCopyChoice := promptWithDefault(reader, "  Enable zero-copy? (y/n)", defaultZeroCopy)
	cfg.EnableZeroCopy = strings.ToLower(zeroCopyChoice) == "y" || strings.ToLower(zeroCopyChoice) == "yes"
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
	fmt.Printf("    %-16s %s %s\n", cli.Dimmed("Role:"), roleDisplay, cli.Dimmed(roleDesc))

	// Ports
	fmt.Printf("    %-16s %s\n", cli.Dimmed("Port:"), cfg.Port)
	if cfg.Role == "master" || cfg.Role == "cluster" {
		fmt.Printf("    %-16s %s\n", cli.Dimmed("Repl Port:"), cfg.ReplPort)
	}
	if cfg.Role == "cluster" {
		fmt.Printf("    %-16s %s\n", cli.Dimmed("Cluster Port:"), cfg.ClusterPort)
	}

	if cfg.Role == "slave" {
		fmt.Printf("    %-16s %s\n", cli.Dimmed("Master:"), cfg.MasterAddr)
	}

	// Cluster configuration
	if cfg.Role == "cluster" {
		fmt.Println()
		fmt.Println("  " + cli.Highlight("Cluster Settings"))
		fmt.Println("  " + cli.Separator(56))
		fmt.Println()
		if len(cfg.ClusterPeers) > 0 {
			fmt.Printf("    %-16s %v\n", cli.Dimmed("Peers:"), cfg.ClusterPeers)
		} else {
			fmt.Printf("    %-16s %s\n", cli.Dimmed("Peers:"), cli.Warning("none configured"))
		}
		fmt.Printf("    %-16s %s\n", cli.Dimmed("Replication:"), cfg.ReplicationMode)
		fmt.Printf("    %-16s %dms\n", cli.Dimmed("Heartbeat:"), cfg.HeartbeatInterval)
		fmt.Printf("    %-16s %dms\n", cli.Dimmed("HB Timeout:"), cfg.HeartbeatTimeout)
		fmt.Printf("    %-16s %dms\n", cli.Dimmed("Election:"), cfg.ElectionTimeout)
		quorumDisplay := "auto"
		if cfg.MinQuorum > 0 {
			quorumDisplay = strconv.Itoa(cfg.MinQuorum)
		}
		fmt.Printf("    %-16s %s\n", cli.Dimmed("Min Quorum:"), quorumDisplay)
		fmt.Printf("    %-16s %v\n", cli.Dimmed("Pre-vote:"), cfg.EnablePreVote)
		fmt.Printf("    %-16s %d\n", cli.Dimmed("Partitions:"), cfg.PartitionCount)
		fmt.Printf("    %-16s %d\n", cli.Dimmed("Repl Factor:"), cfg.ReplicationFactor)
	}

	// Storage
	fmt.Println()
	fmt.Printf("    %-16s %s\n", cli.Dimmed("Data Directory:"), cfg.DataDir)

	// Encryption
	encStatus := formatEncryptionWithPassphrase(cfg.EncryptionEnabled, cfg.EncryptionPassphrase)
	fmt.Printf("    %-16s %s\n", cli.Dimmed("Encryption:"), encStatus)

	// Show generated passphrase if applicable
	if cfg.GeneratedPassphrase != "" {
		fmt.Println()
		fmt.Println("  " + cli.Warning("═══════════════════════════════════════════════════════"))
		fmt.Println("  " + cli.Warning("  IMPORTANT: Save this encryption passphrase securely!"))
		fmt.Println("  " + cli.Warning("═══════════════════════════════════════════════════════"))
		fmt.Println()
		fmt.Printf("    %s %s\n", cli.Dimmed("Passphrase:"), cli.Highlight(cfg.GeneratedPassphrase))
		fmt.Println()
		fmt.Printf("    %s\n", cli.Dimmed("Set this environment variable before starting:"))
		fmt.Printf("    %s\n", cli.Info("export FLYDB_ENCRYPTION_PASSPHRASE=\""+cfg.GeneratedPassphrase+"\""))
		fmt.Println()
		fmt.Println("  " + cli.Warning("═══════════════════════════════════════════════════════"))
	}

	// Performance options (01.26.13+)
	fmt.Println()
	fmt.Println("  " + cli.Highlight("Performance (01.26.13+)"))
	fmt.Println("  " + cli.Separator(56))
	fmt.Println()

	if cfg.Role == "cluster" {
		consensusAlg := "Raft"
		if !cfg.EnableRaft {
			consensusAlg = "Bully (legacy)"
		}
		fmt.Printf("    %-16s %s\n", cli.Dimmed("Consensus:"), consensusAlg)
	}

	if cfg.EnableCompression {
		fmt.Printf("    %-16s %s (%s)\n", cli.Dimmed("Compression:"), cli.Success("enabled"), cfg.CompressionAlgorithm)
	} else {
		fmt.Printf("    %-16s %s\n", cli.Dimmed("Compression:"), cli.Dimmed("disabled"))
	}

	if cfg.EnableZeroCopy {
		fmt.Printf("    %-16s %s\n", cli.Dimmed("Zero-Copy:"), cli.Success("enabled"))
	} else {
		fmt.Printf("    %-16s %s\n", cli.Dimmed("Zero-Copy:"), cli.Dimmed("disabled"))
	}

	// Logging
	fmt.Println()
	fmt.Printf("    %-16s %s%s\n", cli.Dimmed("Log Level:"), cfg.LogLevel, formatJSONLogging(cfg.LogJSON))

	// Admin password
	if cfg.IsFirstSetup {
		if cfg.AdminPassword == "" {
			fmt.Printf("    %-16s %s\n", cli.Dimmed("Admin Password:"), cli.Warning("auto-generated (will be displayed at startup)"))
		} else {
			fmt.Printf("    %-16s %s\n", cli.Dimmed("Admin Password:"), cli.Success("user-specified"))
		}
	}

	// Config file
	if cfg.SaveConfig && cfg.ConfigFile != "" {
		fmt.Printf("    %-16s %s\n", cli.Dimmed("Config File:"), cli.Success(cfg.ConfigFile))
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
