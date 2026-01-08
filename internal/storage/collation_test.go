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
	"testing"
)

func TestDefaultCollator(t *testing.T) {
	c := &DefaultCollator{}

	// Test Compare
	if c.Compare("abc", "abd") >= 0 {
		t.Error("Expected abc < abd")
	}
	if c.Compare("abc", "abc") != 0 {
		t.Error("Expected abc == abc")
	}
	if c.Compare("abd", "abc") <= 0 {
		t.Error("Expected abd > abc")
	}

	// Test Equal
	if !c.Equal("abc", "abc") {
		t.Error("Expected abc == abc")
	}
	if c.Equal("abc", "ABC") {
		t.Error("Expected abc != ABC for default collation")
	}
}

func TestNocaseCollator(t *testing.T) {
	c := &NocaseCollator{}

	// Test case-insensitive comparison
	if c.Compare("ABC", "abc") != 0 {
		t.Error("Expected ABC == abc (case-insensitive)")
	}
	if c.Compare("abc", "ABD") >= 0 {
		t.Error("Expected abc < ABD (case-insensitive)")
	}

	// Test Equal
	if !c.Equal("Hello", "HELLO") {
		t.Error("Expected Hello == HELLO (case-insensitive)")
	}
	if !c.Equal("hello", "HeLLo") {
		t.Error("Expected hello == HeLLo (case-insensitive)")
	}
}

func TestBinaryCollator(t *testing.T) {
	c := &BinaryCollator{}

	// Binary comparison is byte-wise
	if c.Compare("ABC", "abc") >= 0 {
		t.Error("Expected ABC < abc in binary comparison")
	}
	if !c.Equal("abc", "abc") {
		t.Error("Expected abc == abc")
	}
	if c.Equal("abc", "ABC") {
		t.Error("Expected abc != ABC in binary comparison")
	}
}

func TestUnicodeCollator(t *testing.T) {
	c := NewUnicodeCollator("en_US")

	// Basic comparison
	if c.Compare("abc", "abd") >= 0 {
		t.Error("Expected abc < abd")
	}
	if c.Compare("abc", "abc") != 0 {
		t.Error("Expected abc == abc")
	}

	// Unicode collation should handle accented characters
	// In many locales, "café" and "cafe" are considered close
	if c.Compare("cafe", "cafz") >= 0 {
		t.Error("Expected cafe < cafz")
	}
}

func TestUnicodeCollatorGerman(t *testing.T) {
	c := NewUnicodeCollator("de_DE")

	// German collation: ä should sort near a
	// This is a basic test - actual behavior depends on locale
	if c.Compare("a", "z") >= 0 {
		t.Error("Expected a < z in German collation")
	}
}

func TestGetCollator(t *testing.T) {
	tests := []struct {
		collation Collation
		locale    string
		wantType  string
	}{
		{CollationDefault, "en_US", "*storage.DefaultCollator"},
		{CollationBinary, "en_US", "*storage.BinaryCollator"},
		{CollationCaseInsensitive, "en_US", "*storage.NocaseCollator"},
		{CollationUnicode, "en_US", "*storage.UnicodeCollator"},
		{CollationUnicode, "de_DE", "*storage.UnicodeCollator"},
	}

	for _, tt := range tests {
		c := GetCollator(tt.collation, tt.locale)
		if c == nil {
			t.Errorf("GetCollator(%s, %s) returned nil", tt.collation, tt.locale)
		}
	}
}

func TestNormalizeForCollation(t *testing.T) {
	// Nocase normalization
	result := NormalizeForCollation("Hello World", CollationCaseInsensitive)
	if result != "hello world" {
		t.Errorf("Expected 'hello world', got '%s'", result)
	}

	// Default normalization (no change)
	result = NormalizeForCollation("Hello World", CollationDefault)
	if result != "Hello World" {
		t.Errorf("Expected 'Hello World', got '%s'", result)
	}

	// Binary normalization (no change)
	result = NormalizeForCollation("Hello World", CollationBinary)
	if result != "Hello World" {
		t.Errorf("Expected 'Hello World', got '%s'", result)
	}
}

