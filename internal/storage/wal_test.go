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
	"os"
	"testing"
)

func setupTestWAL(t *testing.T) (*WAL, string, func()) {
	tmpDir, err := os.MkdirTemp("", "flydb_wal_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	walPath := tmpDir + "/test.wal"
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

	// Initial size should be 0
	size, err := wal.Size()
	if err != nil {
		t.Fatalf("Size failed: %v", err)
	}
	if size != 0 {
		t.Errorf("Expected initial size 0, got %d", size)
	}

	// Write a record
	err = wal.Write(OpPut, "key", []byte("value"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Size should be > 0
	size, err = wal.Size()
	if err != nil {
		t.Fatalf("Size failed: %v", err)
	}
	if size == 0 {
		t.Error("Expected size > 0 after write")
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

	walPath := tmpDir + "/test_encrypted.wal"
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

	walPath := tmpDir + "/test_encrypted.wal"
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

	walPath := tmpDir + "/test_encrypted.wal"
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

	walPath := tmpDir + "/test_encrypted.wal"
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

