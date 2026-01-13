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
Package compression provides configurable compression for FlyDB.

Compression Overview:
=====================

This module implements configurable compression for:
- WAL entries to reduce disk I/O
- Replication traffic to reduce network bandwidth
- Batch operations for better compression ratios

Supported Algorithms:
=====================

1. LZ4: Fast compression/decompression, moderate ratio
2. Snappy: Very fast, lower ratio, good for real-time
3. Zstd: Best ratio, configurable speed/ratio tradeoff

Batch Compression:
==================

Batching multiple entries before compression improves ratios:
1. Collect entries into a batch
2. Compress the entire batch
3. Store/transmit compressed batch
4. Decompress and split on read
*/
package compression

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

// Algorithm represents a compression algorithm
type Algorithm int

const (
	AlgorithmNone Algorithm = iota
	AlgorithmGzip
	AlgorithmLZ4
	AlgorithmSnappy
	AlgorithmZstd
)

func (a Algorithm) String() string {
	switch a {
	case AlgorithmNone:
		return "none"
	case AlgorithmGzip:
		return "gzip"
	case AlgorithmLZ4:
		return "lz4"
	case AlgorithmSnappy:
		return "snappy"
	case AlgorithmZstd:
		return "zstd"
	default:
		return "unknown"
	}
}

// ParseAlgorithm parses a compression algorithm from string
func ParseAlgorithm(s string) (Algorithm, error) {
	switch s {
	case "none", "":
		return AlgorithmNone, nil
	case "gzip":
		return AlgorithmGzip, nil
	case "lz4":
		return AlgorithmLZ4, nil
	case "snappy":
		return AlgorithmSnappy, nil
	case "zstd":
		return AlgorithmZstd, nil
	default:
		return AlgorithmNone, fmt.Errorf("unknown compression algorithm: %s", s)
	}
}

// Level represents compression level
type Level int

const (
	LevelFastest Level = 1
	LevelDefault Level = 5
	LevelBest    Level = 9
)

// Config holds compression configuration
type Config struct {
	Algorithm        Algorithm `json:"algorithm"`
	Level            Level     `json:"level"`
	MinSize          int       `json:"min_size"`           // Minimum size to compress
	BatchSize        int       `json:"batch_size"`         // Number of entries per batch
	BatchTimeout     int       `json:"batch_timeout_ms"`   // Max wait time for batch (ms)
	DictionaryEnable bool      `json:"dictionary_enable"`  // Use dictionary compression
}

// DefaultConfig returns sensible defaults
func DefaultConfig() Config {
	return Config{
		Algorithm:        AlgorithmGzip,
		Level:            LevelDefault,
		MinSize:          256,
		BatchSize:        100,
		BatchTimeout:     10,
		DictionaryEnable: false,
	}
}

// Errors
var (
	ErrDataTooSmall     = errors.New("data too small to compress")
	ErrInvalidHeader    = errors.New("invalid compression header")
	ErrUnsupportedAlgo  = errors.New("unsupported compression algorithm")
	ErrDecompressFailed = errors.New("decompression failed")
)

// Compressor provides compression/decompression operations
type Compressor struct {
	config     Config
	gzipPool   sync.Pool
	bufferPool sync.Pool
}

// NewCompressor creates a new compressor
func NewCompressor(config Config) *Compressor {
	return &Compressor{
		config: config,
		gzipPool: sync.Pool{
			New: func() interface{} {
				return gzip.NewWriter(nil)
			},
		},
		bufferPool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
}

// Compress compresses data using the configured algorithm
func (c *Compressor) Compress(data []byte) ([]byte, error) {
	if len(data) < c.config.MinSize {
		return data, ErrDataTooSmall
	}

	switch c.config.Algorithm {
	case AlgorithmNone:
		return data, nil
	case AlgorithmGzip:
		return c.compressGzip(data)
	case AlgorithmLZ4:
		return c.compressLZ4(data)
	case AlgorithmSnappy:
		return c.compressSnappy(data)
	case AlgorithmZstd:
		return c.compressZstd(data)
	default:
		return nil, ErrUnsupportedAlgo
	}
}

// Decompress decompresses data
func (c *Compressor) Decompress(data []byte, algorithm Algorithm) ([]byte, error) {
	switch algorithm {
	case AlgorithmNone:
		return data, nil
	case AlgorithmGzip:
		return c.decompressGzip(data)
	case AlgorithmLZ4:
		return c.decompressLZ4(data)
	case AlgorithmSnappy:
		return c.decompressSnappy(data)
	case AlgorithmZstd:
		return c.decompressZstd(data)
	default:
		return nil, ErrUnsupportedAlgo
	}
}

// compressGzip compresses using gzip
func (c *Compressor) compressGzip(data []byte) ([]byte, error) {
	buf := c.bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer c.bufferPool.Put(buf)

	w := c.gzipPool.Get().(*gzip.Writer)
	w.Reset(buf)
	defer c.gzipPool.Put(w)

	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}

	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

// decompressGzip decompresses gzip data
func (c *Compressor) decompressGzip(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return io.ReadAll(r)
}

// compressLZ4 compresses using LZ4 (placeholder - uses gzip for now)
func (c *Compressor) compressLZ4(data []byte) ([]byte, error) {
	// TODO: Implement LZ4 compression when lz4 package is added
	return c.compressGzip(data)
}

// decompressLZ4 decompresses LZ4 data
func (c *Compressor) decompressLZ4(data []byte) ([]byte, error) {
	// TODO: Implement LZ4 decompression when lz4 package is added
	return c.decompressGzip(data)
}

// compressSnappy compresses using Snappy (placeholder - uses gzip for now)
func (c *Compressor) compressSnappy(data []byte) ([]byte, error) {
	// TODO: Implement Snappy compression when snappy package is added
	return c.compressGzip(data)
}

// decompressSnappy decompresses Snappy data
func (c *Compressor) decompressSnappy(data []byte) ([]byte, error) {
	// TODO: Implement Snappy decompression when snappy package is added
	return c.decompressGzip(data)
}

// compressZstd compresses using Zstd (placeholder - uses gzip for now)
func (c *Compressor) compressZstd(data []byte) ([]byte, error) {
	// TODO: Implement Zstd compression when zstd package is added
	return c.compressGzip(data)
}

// decompressZstd decompresses Zstd data
func (c *Compressor) decompressZstd(data []byte) ([]byte, error) {
	// TODO: Implement Zstd decompression when zstd package is added
	return c.decompressGzip(data)
}

// BatchCompressor handles batch compression for better ratios
type BatchCompressor struct {
	compressor *Compressor
	mu         sync.Mutex
	entries    [][]byte
	totalSize  int
}

// NewBatchCompressor creates a new batch compressor
func NewBatchCompressor(config Config) *BatchCompressor {
	return &BatchCompressor{
		compressor: NewCompressor(config),
		entries:    make([][]byte, 0, config.BatchSize),
	}
}

// Add adds an entry to the batch
func (bc *BatchCompressor) Add(entry []byte) {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	bc.entries = append(bc.entries, entry)
	bc.totalSize += len(entry)
}

// Flush compresses and returns the batch
func (bc *BatchCompressor) Flush() ([]byte, error) {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if len(bc.entries) == 0 {
		return nil, nil
	}

	// Encode batch: [count][len1][data1][len2][data2]...
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(len(bc.entries)))

	for _, entry := range bc.entries {
		binary.Write(buf, binary.BigEndian, uint32(len(entry)))
		buf.Write(entry)
	}

	// Compress the batch
	compressed, err := bc.compressor.Compress(buf.Bytes())
	if err != nil && err != ErrDataTooSmall {
		return nil, err
	}
	if err == ErrDataTooSmall {
		compressed = buf.Bytes()
	}

	// Reset batch
	bc.entries = bc.entries[:0]
	bc.totalSize = 0

	return compressed, nil
}

// DecompressBatch decompresses a batch and returns individual entries
func (bc *BatchCompressor) DecompressBatch(data []byte, algorithm Algorithm) ([][]byte, error) {
	// Decompress
	decompressed, err := bc.compressor.Decompress(data, algorithm)
	if err != nil {
		return nil, err
	}

	// Decode batch
	r := bytes.NewReader(decompressed)
	var count uint32
	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return nil, ErrInvalidHeader
	}

	entries := make([][]byte, 0, count)
	for i := uint32(0); i < count; i++ {
		var length uint32
		if err := binary.Read(r, binary.BigEndian, &length); err != nil {
			return nil, err
		}

		entry := make([]byte, length)
		if _, err := io.ReadFull(r, entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// Size returns the current batch size
func (bc *BatchCompressor) Size() int {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	return len(bc.entries)
}

// TotalBytes returns the total uncompressed bytes in the batch
func (bc *BatchCompressor) TotalBytes() int {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	return bc.totalSize
}

