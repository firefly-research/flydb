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

func TestUTF8Encoder(t *testing.T) {
	e := &UTF8Encoder{}

	// Test Encode/Decode
	original := "Hello, 世界! 🌍"
	encoded, err := e.Encode(original)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := e.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded != original {
		t.Errorf("Expected %q, got %q", original, decoded)
	}

	// Test Validate
	if err := e.Validate("Hello, World!"); err != nil {
		t.Errorf("Validate failed for valid string: %v", err)
	}

	// Test Name
	if e.Name() != "UTF8" {
		t.Errorf("Expected name UTF8, got %s", e.Name())
	}
}

func TestASCIIEncoder(t *testing.T) {
	e := &ASCIIEncoder{}

	// Test valid ASCII
	original := "Hello, World!"
	encoded, err := e.Encode(original)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := e.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded != original {
		t.Errorf("Expected %q, got %q", original, decoded)
	}

	// Test invalid ASCII (non-ASCII character)
	_, err = e.Encode("Hello, 世界!")
	if err == nil {
		t.Error("Expected error for non-ASCII string")
	}

	// Test Validate
	if err := e.Validate("Hello"); err != nil {
		t.Errorf("Validate failed for valid ASCII: %v", err)
	}

	if err := e.Validate("Héllo"); err == nil {
		t.Error("Expected error for non-ASCII character")
	}

	// Test Name
	if e.Name() != "ASCII" {
		t.Errorf("Expected name ASCII, got %s", e.Name())
	}
}

func TestLatin1Encoder(t *testing.T) {
	e := &Latin1Encoder{}

	// Test valid Latin-1
	original := "Hello, café!"
	encoded, err := e.Encode(original)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := e.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded != original {
		t.Errorf("Expected %q, got %q", original, decoded)
	}

	// Test Validate
	if err := e.Validate("café"); err != nil {
		t.Errorf("Validate failed for valid Latin-1: %v", err)
	}

	if err := e.Validate("世界"); err == nil {
		t.Error("Expected error for non-Latin-1 characters")
	}

	// Test Name
	if e.Name() != "LATIN1" {
		t.Errorf("Expected name LATIN1, got %s", e.Name())
	}
}

func TestUTF16Encoder(t *testing.T) {
	e := &UTF16Encoder{}

	// Test Encode/Decode
	original := "Hello, 世界!"
	encoded, err := e.Encode(original)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := e.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded != original {
		t.Errorf("Expected %q, got %q", original, decoded)
	}

	// Test invalid decode (odd bytes)
	_, err = e.Decode([]byte{0x00, 0x48, 0x00})
	if err == nil {
		t.Error("Expected error for odd number of bytes")
	}

	// Test Name
	if e.Name() != "UTF16" {
		t.Errorf("Expected name UTF16, got %s", e.Name())
	}
}

func TestGetEncoder(t *testing.T) {
	tests := []struct {
		encoding CharacterEncoding
		wantName string
	}{
		{EncodingUTF8, "UTF8"},
		{EncodingLatin1, "LATIN1"},
		{EncodingASCII, "ASCII"},
		{EncodingUTF16, "UTF16"},
		{EncodingDefault, "UTF8"},
	}

	for _, tt := range tests {
		e := GetEncoder(tt.encoding)
		if e.Name() != tt.wantName {
			t.Errorf("GetEncoder(%s) returned encoder with name %s, want %s",
				tt.encoding, e.Name(), tt.wantName)
		}
	}
}

func TestValidateStringForEncoding(t *testing.T) {
	// UTF-8 accepts everything
	if err := ValidateStringForEncoding("Hello, 世界! 🌍", EncodingUTF8); err != nil {
		t.Errorf("UTF-8 should accept all valid strings: %v", err)
	}

	// ASCII rejects non-ASCII
	if err := ValidateStringForEncoding("Hello", EncodingASCII); err != nil {
		t.Errorf("ASCII should accept ASCII strings: %v", err)
	}
	if err := ValidateStringForEncoding("Héllo", EncodingASCII); err == nil {
		t.Error("ASCII should reject non-ASCII characters")
	}

	// Latin-1 rejects characters > 255
	if err := ValidateStringForEncoding("café", EncodingLatin1); err != nil {
		t.Errorf("Latin-1 should accept Latin-1 strings: %v", err)
	}
	if err := ValidateStringForEncoding("世界", EncodingLatin1); err == nil {
		t.Error("Latin-1 should reject non-Latin-1 characters")
	}
}

