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
Package storage provides encryption utilities for data at rest.

Encryption Overview:
====================

FlyDB supports AES-256-GCM encryption for WAL entries. This provides:
  - Confidentiality: Data is encrypted and unreadable without the key
  - Integrity: GCM mode provides authenticated encryption
  - Nonce uniqueness: Each encryption uses a random nonce

Key Management:
===============

Keys can be provided in two ways:
  1. Direct 32-byte key: For production use with external key management
  2. Passphrase: Derived using PBKDF2 with SHA-256 (for development/testing)

Performance Considerations:
===========================

  - AES-GCM is hardware-accelerated on modern CPUs (AES-NI)
  - Each record is encrypted independently for random access
  - Nonce is prepended to ciphertext (12 bytes overhead per record)
  - GCM tag adds 16 bytes overhead per record
*/
package storage

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"

	"golang.org/x/crypto/pbkdf2"
)

// EncryptionConfig holds the configuration for WAL encryption.
type EncryptionConfig struct {
	// Enabled indicates whether encryption is enabled.
	Enabled bool

	// Key is the 32-byte AES-256 encryption key.
	// If empty and Passphrase is set, the key is derived from the passphrase.
	Key []byte

	// Passphrase is used to derive the encryption key if Key is not set.
	// The key is derived using PBKDF2 with SHA-256.
	Passphrase string

	// Salt is used for key derivation from passphrase.
	// If empty, a default salt is used (not recommended for production).
	Salt []byte
}

// DefaultSalt is used when no salt is provided for key derivation.
// In production, always use a unique salt per database.
var DefaultSalt = []byte("flydb-default-salt-v1")

// KeyDerivationIterations is the number of PBKDF2 iterations.
// Higher values are more secure but slower.
const KeyDerivationIterations = 100000

// Encryptor provides encryption and decryption for WAL entries.
type Encryptor struct {
	gcm cipher.AEAD
}

// NewEncryptor creates a new Encryptor with the given configuration.
// Returns nil if encryption is disabled.
//
// Parameters:
//   - config: Encryption configuration
//
// Returns the Encryptor, or an error if the key is invalid.
func NewEncryptor(config EncryptionConfig) (*Encryptor, error) {
	if !config.Enabled {
		return nil, nil
	}

	key := config.Key
	if len(key) == 0 && config.Passphrase != "" {
		// Derive key from passphrase
		salt := config.Salt
		if len(salt) == 0 {
			salt = DefaultSalt
		}
		key = pbkdf2.Key([]byte(config.Passphrase), salt, KeyDerivationIterations, 32, sha256.New)
	}

	if len(key) != 32 {
		return nil, errors.New("encryption key must be 32 bytes (256 bits)")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	return &Encryptor{gcm: gcm}, nil
}

// Encrypt encrypts the plaintext using AES-256-GCM.
// The nonce is prepended to the ciphertext.
//
// Parameters:
//   - plaintext: The data to encrypt
//
// Returns the ciphertext (nonce + encrypted data + tag), or an error.
func (e *Encryptor) Encrypt(plaintext []byte) ([]byte, error) {
	// Generate a random nonce
	nonce := make([]byte, e.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	// Encrypt and prepend nonce
	ciphertext := e.gcm.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// Decrypt decrypts the ciphertext using AES-256-GCM.
// Expects the nonce to be prepended to the ciphertext.
//
// Parameters:
//   - ciphertext: The encrypted data (nonce + encrypted data + tag)
//
// Returns the plaintext, or an error if decryption fails.
func (e *Encryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < e.gcm.NonceSize() {
		return nil, errors.New("ciphertext too short")
	}

	nonce := ciphertext[:e.gcm.NonceSize()]
	ciphertext = ciphertext[e.gcm.NonceSize():]

	return e.gcm.Open(nil, nonce, ciphertext, nil)
}

