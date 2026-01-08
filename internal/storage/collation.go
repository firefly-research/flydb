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

package storage

import (
	"strings"
	"unicode"

	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

// Collator provides string comparison based on collation rules.
type Collator interface {
	// Compare compares two strings according to collation rules.
	// Returns -1 if a < b, 0 if a == b, 1 if a > b.
	Compare(a, b string) int

	// Equal returns true if two strings are equal according to collation rules.
	Equal(a, b string) bool
}

// DefaultCollator uses standard Go string comparison (byte-wise).
type DefaultCollator struct{}

// Compare implements Collator.
func (c *DefaultCollator) Compare(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// Equal implements Collator.
func (c *DefaultCollator) Equal(a, b string) bool {
	return a == b
}

// BinaryCollator uses strict byte-wise comparison.
type BinaryCollator struct{}

// Compare implements Collator.
func (c *BinaryCollator) Compare(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// Equal implements Collator.
func (c *BinaryCollator) Equal(a, b string) bool {
	return a == b
}

// NocaseCollator uses case-insensitive comparison.
type NocaseCollator struct{}

// Compare implements Collator.
func (c *NocaseCollator) Compare(a, b string) int {
	aLower := strings.ToLower(a)
	bLower := strings.ToLower(b)
	if aLower < bLower {
		return -1
	}
	if aLower > bLower {
		return 1
	}
	return 0
}

// Equal implements Collator.
func (c *NocaseCollator) Equal(a, b string) bool {
	return strings.EqualFold(a, b)
}

// UnicodeCollator uses Unicode collation with locale support.
type UnicodeCollator struct {
	collator *collate.Collator
	locale   string
}

// NewUnicodeCollator creates a new Unicode collator for the given locale.
func NewUnicodeCollator(locale string) *UnicodeCollator {
	tag := language.Make(locale)
	if tag == language.Und {
		tag = language.English
	}
	return &UnicodeCollator{
		collator: collate.New(tag, collate.Loose),
		locale:   locale,
	}
}

// Compare implements Collator.
func (c *UnicodeCollator) Compare(a, b string) int {
	return c.collator.CompareString(a, b)
}

// Equal implements Collator.
func (c *UnicodeCollator) Equal(a, b string) bool {
	return c.collator.CompareString(a, b) == 0
}

// GetCollator returns a Collator for the given collation type and locale.
func GetCollator(collationType Collation, locale string) Collator {
	switch collationType {
	case CollationBinary:
		return &BinaryCollator{}
	case CollationCaseInsensitive:
		return &NocaseCollator{}
	case CollationUnicode:
		return NewUnicodeCollator(locale)
	default:
		return &DefaultCollator{}
	}
}

// NormalizeForCollation normalizes a string for the given collation.
func NormalizeForCollation(s string, collationType Collation) string {
	switch collationType {
	case CollationCaseInsensitive:
		return strings.ToLower(s)
	case CollationUnicode:
		// Normalize Unicode to NFC form
		return strings.Map(func(r rune) rune {
			if unicode.IsSpace(r) {
				return ' '
			}
			return r
		}, s)
	default:
		return s
	}
}

