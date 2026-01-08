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
	"encoding/binary"
	"errors"
	"os"
	"strings"
	"testing"
)

func setupTestWAL(t *testing.T) (*WAL, string, func()) {
	tmpDir, err := os.MkdirTemp("", "flydb_wal_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	walPath := tmpDir + "/test.fdb"
	wal, err := OpenWAL(walPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to open WAL: %v", err)
	}

	cleanup := func() {
		wal.Close()
		os.RemoveAll(tmpDir)
	}

	return wal, walPath, cleanup
}

func TestWALWriteAndReplay(t *testing.T) {
	wal, _, cleanup := setupTestWAL(t)
	defer cleanup()

	// Write some records
	err := wal.Write(OpPut, "key1", []byte("value1"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	err = wal.Write(OpPut, "key2", []byte("value2"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	err = wal.Write(OpDelete, "key1", nil)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Replay and verify
	data := make(map[string][]byte)
	err = wal.Replay(0, func(op byte, key string, value []byte) {
		if op == OpPut {
			data[key] = value
		} else if op == OpDelete {
			delete(data, key)
		}
	})
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	// key1 should be deleted, key2 should exist
	if _, ok := data["key1"]; ok {
		t.Error("key1 should have been deleted")
	}
	if string(data["key2"]) != "value2" {
		t.Errorf("Expected 'value2', got '%s'", string(data["key2"]))
	}
}

func TestWALSize(t *testing.T) {
	wal, _, cleanup := setupTestWAL(t)
	defer cleanup()

	// Initial size should be WALHeaderSize (8 bytes for the header)
	size, err := wal.Size()
	if err != nil {
		t.Fatalf("Size failed: %v", err)
	}
	if size != WALHeaderSize {
		t.Errorf("Expected initial size %d (header only), got %d", WALHeaderSize, size)
	}

	// Write a record
	err = wal.Write(OpPut, "key", []byte("value"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Size should be > header size
	size, err = wal.Size()
	if err != nil {
		t.Fatalf("Size failed: %v", err)
	}
	if size <= WALHeaderSize {
		t.Errorf("Expected size > %d after write, got %d", WALHeaderSize, size)
	}
}

func TestWALReplayFromOffset(t *testing.T) {
	wal, walPath, cleanup := setupTestWAL(t)
	defer cleanup()

	// Write first record
	err := wal.Write(OpPut, "key1", []byte("value1"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Get offset after first record
	offset, err := wal.Size()
	if err != nil {
		t.Fatalf("Size failed: %v", err)
	}

	// Write second record
	err = wal.Write(OpPut, "key2", []byte("value2"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Close and reopen to ensure data is flushed
	wal.Close()
	wal, err = OpenWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}

	// Replay from offset - should only see key2
	var keys []string
	err = wal.Replay(offset, func(op byte, key string, value []byte) {
		keys = append(keys, key)
	})
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if len(keys) != 1 || keys[0] != "key2" {
		t.Errorf("Expected only key2, got %v", keys)
	}
}

func setupEncryptedTestWAL(t *testing.T, passphrase string) (*WAL, string, func()) {
	tmpDir, err := os.MkdirTemp("", "flydb_wal_enc_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	walPath := tmpDir + "/test_encrypted.fdb"
	config := EncryptionConfig{
		Enabled:    true,
		Passphrase: passphrase,
	}
	wal, err := OpenWALWithEncryption(walPath, config)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to open encrypted WAL: %v", err)
	}

	cleanup := func() {
		wal.Close()
		os.RemoveAll(tmpDir)
	}

	return wal, walPath, cleanup
}

func TestEncryptedWALWriteAndReplay(t *testing.T) {
	passphrase := "test-passphrase-123"
	wal, _, cleanup := setupEncryptedTestWAL(t, passphrase)
	defer cleanup()

	// Verify encryption is enabled
	if !wal.IsEncrypted() {
		t.Fatal("Expected WAL to be encrypted")
	}

	// Write some records
	err := wal.Write(OpPut, "key1", []byte("value1"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	err = wal.Write(OpPut, "key2", []byte("value2"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	err = wal.Write(OpDelete, "key1", nil)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Replay and verify
	records := make(map[string][]byte)
	err = wal.Replay(0, func(op byte, key string, value []byte) {
		if op == OpPut {
			records[key] = value
		} else if op == OpDelete {
			delete(records, key)
		}
	})
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	// After replay: key1 should be deleted, key2 should exist
	if _, exists := records["key1"]; exists {
		t.Error("key1 should have been deleted")
	}
	if string(records["key2"]) != "value2" {
		t.Errorf("Expected key2=value2, got %s", string(records["key2"]))
	}
}

func TestEncryptedWALPersistence(t *testing.T) {
	passphrase := "test-passphrase-456"
	tmpDir, err := os.MkdirTemp("", "flydb_wal_enc_persist_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := tmpDir + "/test_encrypted.fdb"
	config := EncryptionConfig{
		Enabled:    true,
		Passphrase: passphrase,
	}

	// Write records
	wal, err := OpenWALWithEncryption(walPath, config)
	if err != nil {
		t.Fatalf("Failed to open encrypted WAL: %v", err)
	}

	err = wal.Write(OpPut, "persistent_key", []byte("persistent_value"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	wal.Close()

	// Reopen with same passphrase and verify
	wal, err = OpenWALWithEncryption(walPath, config)
	if err != nil {
		t.Fatalf("Failed to reopen encrypted WAL: %v", err)
	}
	defer wal.Close()

	var foundKey string
	var foundValue []byte
	err = wal.Replay(0, func(op byte, key string, value []byte) {
		foundKey = key
		foundValue = value
	})
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if foundKey != "persistent_key" {
		t.Errorf("Expected key 'persistent_key', got '%s'", foundKey)
	}
	if string(foundValue) != "persistent_value" {
		t.Errorf("Expected value 'persistent_value', got '%s'", string(foundValue))
	}
}

func TestEncryptedWALWrongPassphrase(t *testing.T) {
	passphrase := "correct-passphrase"
	tmpDir, err := os.MkdirTemp("", "flydb_wal_enc_wrong_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := tmpDir + "/test_encrypted.fdb"
	config := EncryptionConfig{
		Enabled:    true,
		Passphrase: passphrase,
	}

	// Write records with correct passphrase
	wal, err := OpenWALWithEncryption(walPath, config)
	if err != nil {
		t.Fatalf("Failed to open encrypted WAL: %v", err)
	}

	err = wal.Write(OpPut, "secret_key", []byte("secret_value"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	wal.Close()

	// Try to read with wrong passphrase
	wrongConfig := EncryptionConfig{
		Enabled:    true,
		Passphrase: "wrong-passphrase",
	}
	wal, err = OpenWALWithEncryption(walPath, wrongConfig)
	if err != nil {
		t.Fatalf("Failed to reopen encrypted WAL: %v", err)
	}
	defer wal.Close()

	// Replay should fail due to decryption error
	err = wal.Replay(0, func(op byte, key string, value []byte) {
		t.Error("Should not have successfully decrypted with wrong passphrase")
	})
	if err == nil {
		t.Error("Expected decryption error with wrong passphrase")
	}
}

func TestEncryptedWALWithDirectKey(t *testing.T) {
	// Use a direct 32-byte key instead of passphrase
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	tmpDir, err := os.MkdirTemp("", "flydb_wal_enc_key_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := tmpDir + "/test_encrypted.fdb"
	config := EncryptionConfig{
		Enabled: true,
		Key:     key,
	}

	wal, err := OpenWALWithEncryption(walPath, config)
	if err != nil {
		t.Fatalf("Failed to open encrypted WAL: %v", err)
	}

	err = wal.Write(OpPut, "key_test", []byte("value_test"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	wal.Close()

	// Reopen and verify
	wal, err = OpenWALWithEncryption(walPath, config)
	if err != nil {
		t.Fatalf("Failed to reopen encrypted WAL: %v", err)
	}
	defer wal.Close()

	var foundValue []byte
	err = wal.Replay(0, func(op byte, key string, value []byte) {
		foundValue = value
	})
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if string(foundValue) != "value_test" {
		t.Errorf("Expected 'value_test', got '%s'", string(foundValue))
	}
}

func TestEncryptionConfigValidation(t *testing.T) {
	// Test with invalid key length
	config := EncryptionConfig{
		Enabled: true,
		Key:     []byte("too-short"),
	}

	_, err := NewEncryptor(config)
	if err == nil {
		t.Error("Expected error for invalid key length")
	}
}

func TestUnencryptedWALIsNotEncrypted(t *testing.T) {
	wal, _, cleanup := setupTestWAL(t)
	defer cleanup()

	if wal.IsEncrypted() {
		t.Error("Expected unencrypted WAL")
	}
}

func TestEncryptionMismatchUnencryptedDBWithEncryptionEnabled(t *testing.T) {
	// Create an unencrypted WAL file
	tmpFile, err := os.CreateTemp("", "wal_mismatch_test_*.fdb")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	walPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(walPath)

	// Create unencrypted WAL and write some data
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	err = wal.Write(OpPut, "key1", []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	wal.Close()

	// Try to open with encryption enabled - should fail with clear error
	encConfig := EncryptionConfig{
		Enabled:    true,
		Passphrase: "test-passphrase",
	}
	_, err = OpenWALWithEncryption(walPath, encConfig)
	if err == nil {
		t.Fatal("Expected error when opening unencrypted DB with encryption enabled")
	}

	// Check that it's an EncryptionMismatchError
	var mismatchErr *EncryptionMismatchError
	if !errors.As(err, &mismatchErr) {
		t.Fatalf("Expected EncryptionMismatchError, got: %T: %v", err, err)
	}

	if mismatchErr.DatabaseEncrypted {
		t.Error("Expected DatabaseEncrypted=false")
	}
	if !mismatchErr.ConfigEncrypted {
		t.Error("Expected ConfigEncrypted=true")
	}

	// Check error message contains helpful guidance
	if !strings.Contains(err.Error(), "encryption mismatch") {
		t.Errorf("Error should mention 'encryption mismatch': %v", err)
	}
	if !strings.Contains(err.Error(), "encryption_enabled = false") {
		t.Errorf("Error should suggest disabling encryption: %v", err)
	}
}

func TestEncryptionMismatchEncryptedDBWithEncryptionDisabled(t *testing.T) {
	// Create an encrypted WAL file
	tmpFile, err := os.CreateTemp("", "wal_mismatch_test_*.fdb")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	walPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(walPath)

	// Create encrypted WAL and write some data
	encConfig := EncryptionConfig{
		Enabled:    true,
		Passphrase: "test-passphrase",
	}
	wal, err := OpenWALWithEncryption(walPath, encConfig)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	err = wal.Write(OpPut, "key1", []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	wal.Close()

	// Try to open without encryption - should fail with clear error
	noEncConfig := EncryptionConfig{
		Enabled: false,
	}
	_, err = OpenWALWithEncryption(walPath, noEncConfig)
	if err == nil {
		t.Fatal("Expected error when opening encrypted DB without encryption")
	}

	// Check that it's an EncryptionMismatchError
	var mismatchErr *EncryptionMismatchError
	if !errors.As(err, &mismatchErr) {
		t.Fatalf("Expected EncryptionMismatchError, got: %T: %v", err, err)
	}

	if !mismatchErr.DatabaseEncrypted {
		t.Error("Expected DatabaseEncrypted=true")
	}
	if mismatchErr.ConfigEncrypted {
		t.Error("Expected ConfigEncrypted=false")
	}

	// Check error message contains helpful guidance
	if !strings.Contains(err.Error(), "encryption mismatch") {
		t.Errorf("Error should mention 'encryption mismatch': %v", err)
	}
	if !strings.Contains(err.Error(), "FLYDB_ENCRYPTION_PASSPHRASE") {
		t.Errorf("Error should suggest setting passphrase: %v", err)
	}
}

func TestWALHeaderValidation(t *testing.T) {
	// Create a new WAL file
	tmpFile, err := os.CreateTemp("", "wal_header_test_*.fdb")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	walPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(walPath)

	// Create unencrypted WAL
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	wal.Close()

	// Read the header and verify it
	f, err := os.Open(walPath)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer f.Close()

	header := make([]byte, WALHeaderSize)
	n, err := f.Read(header)
	if err != nil {
		t.Fatalf("Failed to read header: %v", err)
	}
	if n != WALHeaderSize {
		t.Fatalf("Expected %d bytes, got %d", WALHeaderSize, n)
	}

	// Check magic number
	magic := binary.BigEndian.Uint32(header[0:4])
	if magic != WALMagic {
		t.Errorf("Expected magic 0x%X, got 0x%X", WALMagic, magic)
	}

	// Check version
	if header[4] != WALVersion {
		t.Errorf("Expected version %d, got %d", WALVersion, header[4])
	}

	// Check flags (should be 0 for unencrypted)
	if header[5] != 0 {
		t.Errorf("Expected flags 0 for unencrypted, got %d", header[5])
	}
}

func TestWALHeaderEncryptedFlag(t *testing.T) {
	// Create a new encrypted WAL file
	tmpFile, err := os.CreateTemp("", "wal_header_enc_test_*.fdb")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	walPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(walPath)

	// Create encrypted WAL
	encConfig := EncryptionConfig{
		Enabled:    true,
		Passphrase: "test-passphrase",
	}
	wal, err := OpenWALWithEncryption(walPath, encConfig)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	wal.Close()

	// Read the header and verify it
	f, err := os.Open(walPath)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer f.Close()

	header := make([]byte, WALHeaderSize)
	n, err := f.Read(header)
	if err != nil {
		t.Fatalf("Failed to read header: %v", err)
	}
	if n != WALHeaderSize {
		t.Fatalf("Expected %d bytes, got %d", WALHeaderSize, n)
	}

	// Check magic number
	magic := binary.BigEndian.Uint32(header[0:4])
	if magic != WALMagic {
		t.Errorf("Expected magic 0x%X, got 0x%X", WALMagic, magic)
	}

	// Check flags (should have encrypted bit set)
	if header[5] != WALFlagEncrypted {
		t.Errorf("Expected flags %d for encrypted, got %d", WALFlagEncrypted, header[5])
	}
}
