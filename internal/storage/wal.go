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
Write-Ahead Log (WAL) Implementation
=====================================

The WAL provides durability for FlyDB by persisting all operations to disk
before they are applied to the in-memory store. This ensures that committed
data survives crashes and restarts.

How WAL Works:
==============

	1. Before any write operation (Put/Delete), the operation is appended to the WAL
	2. The WAL is an append-only file - records are never modified or deleted
	3. On startup, the WAL is replayed to rebuild the in-memory state
	4. The WAL can be replayed from any offset for replication

WAL Record Format:
==================

Each record in the WAL has the following binary format:

	┌─────────┬───────────┬─────────────┬─────────────┬─────────────┐
	│ Op (1B) │ KeyLen(4B)│ Key (var)   │ ValLen (4B) │ Value (var) │
	└─────────┴───────────┴─────────────┴─────────────┴─────────────┘

	- Op: Operation type (1 = Put, 2 = Delete)
	- KeyLen: Length of the key in bytes (big-endian uint32)
	- Key: The key bytes
	- ValLen: Length of the value in bytes (big-endian uint32)
	- Value: The value bytes (empty for Delete operations)

Example WAL Contents:
=====================

	Record 1: PUT "user:alice" -> {"name":"Alice"}
	Record 2: PUT "user:bob" -> {"name":"Bob"}
	Record 3: DELETE "user:alice"
	Record 4: PUT "user:alice" -> {"name":"Alice Smith"}

After replay, only "user:bob" and "user:alice" (with updated value) exist.

Replication:
============

The WAL is also used for leader-follower replication:

	1. Followers track their current WAL offset
	2. When syncing, followers request records from their offset
	3. The leader streams WAL records to followers
	4. Followers apply records and update their offset

This provides eventual consistency between leader and followers.

Thread Safety:
==============

The WAL uses a mutex to ensure thread-safe writes. Multiple goroutines
can safely call Write() concurrently.

Durability Considerations:
==========================

Currently, the WAL does not call fsync() after each write. This means
that in the event of a power failure, some recent writes may be lost.
A production system would offer configurable durability levels:

  - fsync after every write (safest, slowest)
  - fsync periodically (balanced)
  - no fsync (fastest, least durable)
*/
package storage

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"sync"
)

// Operation type constants for WAL records.
const (
	// OpPut represents a Put operation in the WAL.
	// The record contains both key and value.
	OpPut byte = 1

	// OpDelete represents a Delete operation in the WAL.
	// The record contains only the key (value is empty).
	OpDelete byte = 2
)

// WAL (Write-Ahead Log) provides durability for the database.
// All modifications are appended to the WAL file before being applied
// to the in-memory store.
//
// The WAL is an append-only file that records all Put and Delete operations.
// On startup, the WAL is replayed to rebuild the in-memory state.
// The WAL is also used for replication - followers can replay from their
// last known offset to catch up with the leader.
//
// Encryption:
// When encryption is enabled, each WAL record is encrypted using AES-256-GCM
// before being written to disk. The encrypted record format is:
//
//	┌──────────────┬─────────────────────────────────────────────────────┐
//	│ EncLen (4B)  │ Encrypted Payload (nonce + ciphertext + tag)        │
//	└──────────────┴─────────────────────────────────────────────────────┘
//
// Thread Safety: All methods are safe for concurrent use.
type WAL struct {
	// file is the underlying WAL file handle.
	file *os.File

	// mu protects concurrent writes to the WAL.
	mu sync.Mutex

	// encryptor handles encryption/decryption of WAL entries.
	// If nil, encryption is disabled.
	encryptor *Encryptor
}

// OpenWAL opens or creates a WAL file at the specified path.
// The file is opened in append mode, so new records are always
// added to the end.
//
// Parameters:
//   - path: Path to the WAL file (created if it doesn't exist)
//
// Returns the WAL instance, or an error if the file cannot be opened.
//
// Example:
//
//	wal, err := storage.OpenWAL("/var/lib/flydb/data.wal")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer wal.Close()
func OpenWAL(path string) (*WAL, error) {
	return OpenWALWithEncryption(path, EncryptionConfig{Enabled: false})
}

// OpenWALWithEncryption opens or creates a WAL file with optional encryption.
// When encryption is enabled, all WAL entries are encrypted using AES-256-GCM.
//
// Parameters:
//   - path: Path to the WAL file (created if it doesn't exist)
//   - config: Encryption configuration
//
// Returns the WAL instance, or an error if the file cannot be opened
// or the encryption configuration is invalid.
//
// Example:
//
//	config := storage.EncryptionConfig{
//	    Enabled:    true,
//	    Passphrase: "my-secret-passphrase",
//	}
//	wal, err := storage.OpenWALWithEncryption("/var/lib/flydb/data.wal", config)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer wal.Close()
func OpenWALWithEncryption(path string, config EncryptionConfig) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	var encryptor *Encryptor
	if config.Enabled {
		encryptor, err = NewEncryptor(config)
		if err != nil {
			f.Close()
			return nil, err
		}
	}

	return &WAL{file: f, encryptor: encryptor}, nil
}

// IsEncrypted returns true if the WAL is using encryption.
func (w *WAL) IsEncrypted() bool {
	return w.encryptor != nil
}

// Write appends an operation to the WAL file.
// This method is thread-safe and blocks until the write completes.
//
// Unencrypted Record Format:
//
//	┌─────────┬───────────┬─────────────┬─────────────┬─────────────┐
//	│ Op (1B) │ KeyLen(4B)│ Key (var)   │ ValLen (4B) │ Value (var) │
//	└─────────┴───────────┴─────────────┴─────────────┴─────────────┘
//
// Encrypted Record Format:
//
//	┌──────────────┬─────────────────────────────────────────────────────┐
//	│ EncLen (4B)  │ Encrypted Payload (nonce + ciphertext + tag)        │
//	└──────────────┴─────────────────────────────────────────────────────┘
//
// Parameters:
//   - op: Operation type (OpPut or OpDelete)
//   - key: The key for this operation
//   - value: The value (can be nil for Delete operations)
//
// Returns an error if the write fails.
func (w *WAL) Write(op byte, key string, value []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Calculate buffer size: Op(1) + KeyLen(4) + Key + ValueLen(4) + Value
	buf := make([]byte, 1+4+len(key)+4+len(value))

	// Write operation type.
	buf[0] = op

	// Write key length and key.
	binary.BigEndian.PutUint32(buf[1:], uint32(len(key)))
	copy(buf[5:], []byte(key))

	// Write value length and value.
	offset := 5 + len(key)
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(value)))
	copy(buf[offset+4:], value)

	// If encryption is enabled, encrypt the record
	if w.encryptor != nil {
		encrypted, err := w.encryptor.Encrypt(buf)
		if err != nil {
			return err
		}

		// Write encrypted record with length prefix
		encBuf := make([]byte, 4+len(encrypted))
		binary.BigEndian.PutUint32(encBuf, uint32(len(encrypted)))
		copy(encBuf[4:], encrypted)

		_, err = w.file.Write(encBuf)
		return err
	}

	// Write the entire record atomically.
	_, err := w.file.Write(buf)
	return err
}

// Close closes the underlying WAL file.
// After Close is called, no other methods should be called on this WAL.
//
// Returns an error if the file cannot be closed.
func (w *WAL) Close() error {
	return w.file.Close()
}

// Size returns the current size of the WAL file in bytes.
// This is used for replication - followers track their offset
// and request records from that position.
//
// Returns the file size in bytes, or an error if the stat fails.
func (w *WAL) Size() (int64, error) {
	info, err := w.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// Replay reads the WAL from startOffset and invokes fn for each record found.
// This is used for two purposes:
//
//  1. Startup Recovery: Replay from offset 0 to rebuild in-memory state
//  2. Replication: Replay from follower's last offset to catch up
//
// The callback function is invoked for each record with:
//   - op: The operation type (OpPut or OpDelete)
//   - key: The key from the record
//   - value: The value from the record (empty for Delete)
//
// Parameters:
//   - startOffset: Byte offset to start reading from
//   - fn: Callback function invoked for each record
//
// Returns an error if reading fails (EOF is not an error).
//
// Example:
//
//	// Replay entire WAL to rebuild state
//	err := wal.Replay(0, func(op byte, key string, value []byte) {
//	    if op == storage.OpPut {
//	        data[key] = value
//	    } else {
//	        delete(data, key)
//	    }
//	})
func (w *WAL) Replay(startOffset int64, fn func(op byte, key string, value []byte)) error {
	// Seek to the starting position.
	w.file.Seek(startOffset, 0)
	reader := bufio.NewReader(w.file)

	// Use encrypted or unencrypted replay based on configuration
	if w.encryptor != nil {
		return w.replayEncrypted(reader, fn)
	}
	return w.replayUnencrypted(reader, fn)
}

// replayUnencrypted reads unencrypted WAL records.
func (w *WAL) replayUnencrypted(reader *bufio.Reader, fn func(op byte, key string, value []byte)) error {
	for {
		// Read Operation Byte.
		op, err := reader.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Read Key Length.
		var keyLen uint32
		if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
			return err
		}

		// Read Key.
		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, keyBuf); err != nil {
			return err
		}

		// Read Value Length.
		var valLen uint32
		if err := binary.Read(reader, binary.BigEndian, &valLen); err != nil {
			return err
		}

		// Read Value.
		valBuf := make([]byte, valLen)
		if _, err := io.ReadFull(reader, valBuf); err != nil {
			return err
		}

		// Invoke callback with the record.
		fn(op, string(keyBuf), valBuf)
	}
	return nil
}

// replayEncrypted reads encrypted WAL records.
func (w *WAL) replayEncrypted(reader *bufio.Reader, fn func(op byte, key string, value []byte)) error {
	for {
		// Read encrypted record length
		var encLen uint32
		if err := binary.Read(reader, binary.BigEndian, &encLen); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Read encrypted payload
		encBuf := make([]byte, encLen)
		if _, err := io.ReadFull(reader, encBuf); err != nil {
			return err
		}

		// Decrypt the record
		plaintext, err := w.encryptor.Decrypt(encBuf)
		if err != nil {
			return err
		}

		// Parse the decrypted record
		if len(plaintext) < 9 { // Minimum: Op(1) + KeyLen(4) + ValLen(4)
			return io.ErrUnexpectedEOF
		}

		op := plaintext[0]
		keyLen := binary.BigEndian.Uint32(plaintext[1:5])

		if len(plaintext) < int(5+keyLen+4) {
			return io.ErrUnexpectedEOF
		}

		key := string(plaintext[5 : 5+keyLen])
		valLen := binary.BigEndian.Uint32(plaintext[5+keyLen : 9+keyLen])

		if len(plaintext) < int(9+keyLen+valLen) {
			return io.ErrUnexpectedEOF
		}

		value := plaintext[9+keyLen : 9+keyLen+valLen]

		// Invoke callback with the record.
		fn(op, key, value)
	}
	return nil
}
