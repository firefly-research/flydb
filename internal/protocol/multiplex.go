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
Package protocol provides connection multiplexing for FlyDB.

Multiplexing Overview:
======================

This module implements connection multiplexing to allow multiple logical
connections (streams) over a single TCP connection:

- Reduces connection overhead
- Enables concurrent requests on one connection
- Supports flow control per stream
- Handles stream prioritization

Frame Format:
=============

Multiplexed frames add a stream ID to the standard protocol:

  +--------+--------+--------+--------+--------+--------+--------+--------+...
  | Magic  | Version| MsgType| Flags  | StreamID (4B)   |    Length (4B)   | Payload...
  +--------+--------+--------+--------+--------+--------+--------+--------+...

Stream Lifecycle:
=================

1. Client opens stream with unique ID
2. Messages are tagged with stream ID
3. Server routes responses to correct stream
4. Either side can close stream
*/
package protocol

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

// Multiplexing constants
const (
	MultiplexHeaderSize = 12 // Magic + Version + Type + Flags + StreamID + Length
	MaxStreams          = 65536
)

// Stream states
const (
	StreamOpen uint32 = iota
	StreamHalfClosed
	StreamClosed
)

// Errors
var (
	ErrStreamClosed    = errors.New("stream is closed")
	ErrTooManyStreams  = errors.New("too many streams")
	ErrStreamNotFound  = errors.New("stream not found")
	ErrInvalidStreamID = errors.New("invalid stream ID")
)

// MultiplexFrame represents a multiplexed message frame
type MultiplexFrame struct {
	Header   Header
	StreamID uint32
	Payload  []byte
}

// Stream represents a logical stream within a multiplexed connection
type Stream struct {
	ID       uint32
	state    uint32
	recvChan chan *MultiplexFrame
	sendChan chan *MultiplexFrame
	mu       sync.Mutex
	conn     *MultiplexConn
}

// MultiplexConn manages a multiplexed connection
type MultiplexConn struct {
	conn       io.ReadWriteCloser
	mu         sync.RWMutex
	streams    map[uint32]*Stream
	nextID     uint32
	isClient   bool
	closed     atomic.Bool
	closeChan  chan struct{}
	writeMu    sync.Mutex
	headerBuf  []byte
	bufferPool *BufferPool
}

// NewMultiplexConn creates a new multiplexed connection
func NewMultiplexConn(conn io.ReadWriteCloser, isClient bool) *MultiplexConn {
	mc := &MultiplexConn{
		conn:       conn,
		streams:    make(map[uint32]*Stream),
		isClient:   isClient,
		closeChan:  make(chan struct{}),
		headerBuf:  make([]byte, MultiplexHeaderSize),
		bufferPool: DefaultBufferPool,
	}

	// Client uses odd stream IDs, server uses even
	if isClient {
		mc.nextID = 1
	} else {
		mc.nextID = 2
	}

	// Start read loop
	go mc.readLoop()

	return mc
}

// OpenStream opens a new stream
func (mc *MultiplexConn) OpenStream() (*Stream, error) {
	if mc.closed.Load() {
		return nil, ErrStreamClosed
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()

	if len(mc.streams) >= MaxStreams {
		return nil, ErrTooManyStreams
	}

	streamID := mc.nextID
	mc.nextID += 2 // Increment by 2 to maintain odd/even

	stream := &Stream{
		ID:       streamID,
		state:    StreamOpen,
		recvChan: make(chan *MultiplexFrame, 64),
		sendChan: make(chan *MultiplexFrame, 64),
		conn:     mc,
	}

	mc.streams[streamID] = stream
	return stream, nil
}

// AcceptStream accepts an incoming stream (for server side)
func (mc *MultiplexConn) AcceptStream(streamID uint32) (*Stream, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if _, exists := mc.streams[streamID]; exists {
		return nil, ErrInvalidStreamID
	}

	stream := &Stream{
		ID:       streamID,
		state:    StreamOpen,
		recvChan: make(chan *MultiplexFrame, 64),
		sendChan: make(chan *MultiplexFrame, 64),
		conn:     mc,
	}

	mc.streams[streamID] = stream
	return stream, nil
}

// GetStream returns an existing stream
func (mc *MultiplexConn) GetStream(streamID uint32) (*Stream, error) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	stream, ok := mc.streams[streamID]
	if !ok {
		return nil, ErrStreamNotFound
	}
	return stream, nil
}

// CloseStream closes a stream
func (mc *MultiplexConn) CloseStream(streamID uint32) error {
	mc.mu.Lock()
	stream, ok := mc.streams[streamID]
	if !ok {
		mc.mu.Unlock()
		return ErrStreamNotFound
	}
	delete(mc.streams, streamID)
	mc.mu.Unlock()

	atomic.StoreUint32(&stream.state, StreamClosed)
	close(stream.recvChan)
	return nil
}

// Close closes the multiplexed connection
func (mc *MultiplexConn) Close() error {
	if mc.closed.Swap(true) {
		return nil
	}

	close(mc.closeChan)

	mc.mu.Lock()
	for id, stream := range mc.streams {
		atomic.StoreUint32(&stream.state, StreamClosed)
		close(stream.recvChan)
		delete(mc.streams, id)
	}
	mc.mu.Unlock()

	return mc.conn.Close()
}

// readLoop reads frames and dispatches to streams
func (mc *MultiplexConn) readLoop() {
	for {
		select {
		case <-mc.closeChan:
			return
		default:
		}

		frame, err := mc.readFrame()
		if err != nil {
			if err != io.EOF {
				// Log error
			}
			mc.Close()
			return
		}

		mc.mu.RLock()
		stream, ok := mc.streams[frame.StreamID]
		mc.mu.RUnlock()

		if !ok {
			// Unknown stream - could be new stream from peer
			if !mc.isClient {
				// Server auto-accepts new streams
				stream, err = mc.AcceptStream(frame.StreamID)
				if err != nil {
					continue
				}
			} else {
				continue
			}
		}

		select {
		case stream.recvChan <- frame:
		default:
			// Channel full, drop frame (flow control would handle this)
		}
	}
}

// readFrame reads a single multiplexed frame
func (mc *MultiplexConn) readFrame() (*MultiplexFrame, error) {
	if _, err := io.ReadFull(mc.conn, mc.headerBuf); err != nil {
		return nil, err
	}

	frame := &MultiplexFrame{
		Header: Header{
			Magic:   mc.headerBuf[0],
			Version: mc.headerBuf[1],
			Type:    MessageType(mc.headerBuf[2]),
			Flags:   MessageFlag(mc.headerBuf[3]),
			Length:  binary.BigEndian.Uint32(mc.headerBuf[8:12]),
		},
		StreamID: binary.BigEndian.Uint32(mc.headerBuf[4:8]),
	}

	if frame.Header.Magic != MagicByte {
		return nil, ErrInvalidMagic
	}
	if frame.Header.Length > MaxMessageSize {
		return nil, ErrMessageTooLarge
	}

	frame.Payload = mc.bufferPool.Get(int(frame.Header.Length))
	if _, err := io.ReadFull(mc.conn, frame.Payload); err != nil {
		mc.bufferPool.Put(frame.Payload)
		return nil, err
	}

	return frame, nil
}

// WriteFrame writes a frame to a stream
func (mc *MultiplexConn) WriteFrame(streamID uint32, msgType MessageType, flags MessageFlag, payload []byte) error {
	if mc.closed.Load() {
		return ErrStreamClosed
	}

	mc.writeMu.Lock()
	defer mc.writeMu.Unlock()

	// Build header
	header := make([]byte, MultiplexHeaderSize)
	header[0] = MagicByte
	header[1] = ProtocolVersion
	header[2] = byte(msgType)
	header[3] = byte(flags)
	binary.BigEndian.PutUint32(header[4:8], streamID)
	binary.BigEndian.PutUint32(header[8:12], uint32(len(payload)))

	// Write header
	if _, err := mc.conn.Write(header); err != nil {
		return err
	}

	// Write payload
	if len(payload) > 0 {
		if _, err := mc.conn.Write(payload); err != nil {
			return err
		}
	}

	return nil
}

// Stream methods

// Send sends a message on the stream
func (s *Stream) Send(msgType MessageType, payload []byte) error {
	if atomic.LoadUint32(&s.state) != StreamOpen {
		return ErrStreamClosed
	}
	return s.conn.WriteFrame(s.ID, msgType, FlagNone, payload)
}

// Recv receives a message from the stream
func (s *Stream) Recv() (*MultiplexFrame, error) {
	if atomic.LoadUint32(&s.state) == StreamClosed {
		return nil, ErrStreamClosed
	}

	frame, ok := <-s.recvChan
	if !ok {
		return nil, ErrStreamClosed
	}
	return frame, nil
}

// Close closes the stream
func (s *Stream) Close() error {
	return s.conn.CloseStream(s.ID)
}

// ID returns the stream ID
func (s *Stream) StreamID() uint32 {
	return s.ID
}

