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
	"fmt"
	"unicode/utf16"
	"unicode/utf8"

	"golang.org/x/text/encoding/charmap"
)

// Encoder provides encoding/decoding for different character encodings.
type Encoder interface {
	// Encode converts a Go string to bytes in the target encoding.
	Encode(s string) ([]byte, error)

	// Decode converts bytes from the target encoding to a Go string.
	Decode(b []byte) (string, error)

	// Validate checks if a string is valid for this encoding.
	Validate(s string) error

	// Name returns the encoding name.
	Name() string
}

// UTF8Encoder handles UTF-8 encoding (Go's native string encoding).
type UTF8Encoder struct{}

// Encode implements Encoder.
func (e *UTF8Encoder) Encode(s string) ([]byte, error) {
	return []byte(s), nil
}

// Decode implements Encoder.
func (e *UTF8Encoder) Decode(b []byte) (string, error) {
	if !utf8.Valid(b) {
		return "", fmt.Errorf("invalid UTF-8 sequence")
	}
	return string(b), nil
}

// Validate implements Encoder.
func (e *UTF8Encoder) Validate(s string) error {
	if !utf8.ValidString(s) {
		return fmt.Errorf("string contains invalid UTF-8 sequences")
	}
	return nil
}

// Name implements Encoder.
func (e *UTF8Encoder) Name() string {
	return "UTF8"
}

// Latin1Encoder handles ISO-8859-1 (Latin-1) encoding.
type Latin1Encoder struct{}

// Encode implements Encoder.
func (e *Latin1Encoder) Encode(s string) ([]byte, error) {
	encoder := charmap.ISO8859_1.NewEncoder()
	return encoder.Bytes([]byte(s))
}

// Decode implements Encoder.
func (e *Latin1Encoder) Decode(b []byte) (string, error) {
	decoder := charmap.ISO8859_1.NewDecoder()
	result, err := decoder.Bytes(b)
	if err != nil {
		return "", err
	}
	return string(result), nil
}

// Validate implements Encoder.
func (e *Latin1Encoder) Validate(s string) error {
	for _, r := range s {
		if r > 255 {
			return fmt.Errorf("character U+%04X is not valid in Latin-1 encoding", r)
		}
	}
	return nil
}

// Name implements Encoder.
func (e *Latin1Encoder) Name() string {
	return "LATIN1"
}

// ASCIIEncoder handles ASCII encoding.
type ASCIIEncoder struct{}

// Encode implements Encoder.
func (e *ASCIIEncoder) Encode(s string) ([]byte, error) {
	for _, r := range s {
		if r > 127 {
			return nil, fmt.Errorf("character U+%04X is not valid ASCII", r)
		}
	}
	return []byte(s), nil
}

// Decode implements Encoder.
func (e *ASCIIEncoder) Decode(b []byte) (string, error) {
	for _, c := range b {
		if c > 127 {
			return "", fmt.Errorf("byte 0x%02X is not valid ASCII", c)
		}
	}
	return string(b), nil
}

// Validate implements Encoder.
func (e *ASCIIEncoder) Validate(s string) error {
	for _, r := range s {
		if r > 127 {
			return fmt.Errorf("character U+%04X is not valid ASCII", r)
		}
	}
	return nil
}

// Name implements Encoder.
func (e *ASCIIEncoder) Name() string {
	return "ASCII"
}

// UTF16Encoder handles UTF-16 encoding.
type UTF16Encoder struct{}

// Encode implements Encoder.
func (e *UTF16Encoder) Encode(s string) ([]byte, error) {
	runes := []rune(s)
	u16 := utf16.Encode(runes)
	result := make([]byte, len(u16)*2)
	for i, v := range u16 {
		result[i*2] = byte(v >> 8)
		result[i*2+1] = byte(v)
	}
	return result, nil
}

// Decode implements Encoder.
func (e *UTF16Encoder) Decode(b []byte) (string, error) {
	if len(b)%2 != 0 {
		return "", fmt.Errorf("invalid UTF-16: odd number of bytes")
	}
	u16 := make([]uint16, len(b)/2)
	for i := range u16 {
		u16[i] = uint16(b[i*2])<<8 | uint16(b[i*2+1])
	}
	return string(utf16.Decode(u16)), nil
}

// Validate implements Encoder.
func (e *UTF16Encoder) Validate(s string) error {
	// All valid Go strings can be encoded as UTF-16
	return nil
}

// Name implements Encoder.
func (e *UTF16Encoder) Name() string {
	return "UTF16"
}

// GetEncoder returns an Encoder for the given character encoding.
func GetEncoder(encoding CharacterEncoding) Encoder {
	switch encoding {
	case EncodingLatin1:
		return &Latin1Encoder{}
	case EncodingASCII:
		return &ASCIIEncoder{}
	case EncodingUTF16:
		return &UTF16Encoder{}
	default:
		return &UTF8Encoder{}
	}
}

// ValidateStringForEncoding validates that a string can be stored with the given encoding.
func ValidateStringForEncoding(s string, encoding CharacterEncoding) error {
	encoder := GetEncoder(encoding)
	return encoder.Validate(s)
}

