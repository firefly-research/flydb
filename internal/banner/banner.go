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

	// AnsiReset clears all text formatting and returns to default.
	// Always use this after colored text to prevent color bleeding.
	AnsiReset = "\033[0m"

	// AnsiBold enables bold text rendering.
	// Combined with colors for emphasis on important text.
	AnsiBold = "\033[1m"

	// AnsiNormal resets text weight to normal (non-bold).
	// Alias for AnsiReset for semantic clarity.
	AnsiNormal = "\033[0m"
)

// Version information for the FlyDB application.
// These constants are used in the banner display and can be
// referenced elsewhere in the application for version reporting.
const (
	Version   = "01.26.12"
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
