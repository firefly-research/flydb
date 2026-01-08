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
Package protocol implements the FlyDB binary wire protocol.

Protocol Overview:
==================

The binary protocol provides efficient communication between clients and the
FlyDB server. It uses a simple message framing format with type-length-value
encoding for optimal performance.

Message Format:
===============

	+--------+--------+--------+--------+--------+--------+...
	| Magic  | Version| MsgType| Flags  |    Length (4B)   | Payload...
	+--------+--------+--------+--------+--------+--------+...

	- Magic (1 byte): Protocol magic number (0xFD for FlyDB)
	- Version (1 byte): Protocol version (currently 0x01)
	- MsgType (1 byte): Message type identifier
	- Flags (1 byte): Message flags (compression, etc.)
	- Length (4 bytes): Payload length in big-endian
	- Payload: Variable-length message data

Message Types:
==============

	- 0x01: Query - SQL query request
	- 0x02: QueryResult - Query response with rows
	- 0x03: Error - Error response
	- 0x04: Prepare - Prepare statement request
	- 0x05: PrepareResult - Prepare response with statement ID
	- 0x06: Execute - Execute prepared statement
	- 0x07: Deallocate - Deallocate prepared statement
	- 0x08: Auth - Authentication request
	- 0x09: AuthResult - Authentication response
	- 0x0A: Ping - Keep-alive ping
	- 0x0B: Pong - Keep-alive pong
*/
package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

// Protocol constants.
const (
	MagicByte      byte = 0xFD // FlyDB magic byte
	ProtocolVersion byte = 0x01

	// Maximum message size (16 MB)
	MaxMessageSize = 16 * 1024 * 1024

	// Header size in bytes
	HeaderSize = 8
)

// MessageType represents the type of protocol message.
type MessageType byte

// Message type constants.
const (
	// Core messages (0x01-0x0F)
	MsgQuery         MessageType = 0x01
	MsgQueryResult   MessageType = 0x02
	MsgError         MessageType = 0x03
	MsgPrepare       MessageType = 0x04
	MsgPrepareResult MessageType = 0x05
	MsgExecute       MessageType = 0x06
	MsgDeallocate    MessageType = 0x07
	MsgAuth          MessageType = 0x08
	MsgAuthResult    MessageType = 0x09
	MsgPing          MessageType = 0x0A
	MsgPong          MessageType = 0x0B

	// Cursor operations (0x10-0x1F)
	MsgCursorOpen   MessageType = 0x10
	MsgCursorFetch  MessageType = 0x11
	MsgCursorClose  MessageType = 0x12
	MsgCursorScroll MessageType = 0x13
	MsgCursorResult MessageType = 0x14

	// Metadata operations (0x20-0x2F)
	MsgGetTables      MessageType = 0x20
	MsgGetColumns     MessageType = 0x21
	MsgGetPrimaryKeys MessageType = 0x22
	MsgGetForeignKeys MessageType = 0x23
	MsgGetIndexes     MessageType = 0x24
	MsgGetTypeInfo    MessageType = 0x25
	MsgMetadataResult MessageType = 0x26

	// Transaction operations (0x30-0x3F)
	MsgBeginTx     MessageType = 0x30
	MsgCommitTx    MessageType = 0x31
	MsgRollbackTx  MessageType = 0x32
	MsgSavepoint   MessageType = 0x33
	MsgTxResult    MessageType = 0x34

	// Session operations (0x40-0x4F)
	MsgSetOption      MessageType = 0x40
	MsgGetOption      MessageType = 0x41
	MsgGetServerInfo  MessageType = 0x42
	MsgSessionResult  MessageType = 0x43

	// Database operations (0x50-0x5F)
	MsgUseDatabase       MessageType = 0x50
	MsgGetDatabases      MessageType = 0x51
	MsgDatabaseResult    MessageType = 0x52
)

// MessageFlag represents message flags.
type MessageFlag byte

// Message flag constants.
const (
	FlagNone       MessageFlag = 0x00
	FlagCompressed MessageFlag = 0x01
	FlagEncrypted  MessageFlag = 0x02
)

// Header represents a protocol message header.
type Header struct {
	Magic   byte
	Version byte
	Type    MessageType
	Flags   MessageFlag
	Length  uint32
}

// Message represents a complete protocol message.
type Message struct {
	Header  Header
	Payload []byte
}

// Common errors.
var (
	ErrInvalidMagic    = errors.New("invalid protocol magic byte")
	ErrInvalidVersion  = errors.New("unsupported protocol version")
	ErrMessageTooLarge = errors.New("message exceeds maximum size")
	ErrInvalidMessage  = errors.New("invalid message format")
)

// WriteHeader writes a message header to the writer.
func WriteHeader(w io.Writer, h Header) error {
	buf := make([]byte, HeaderSize)
	buf[0] = h.Magic
	buf[1] = h.Version
	buf[2] = byte(h.Type)
	buf[3] = byte(h.Flags)
	binary.BigEndian.PutUint32(buf[4:], h.Length)
	_, err := w.Write(buf)
	return err
}

// ReadHeader reads a message header from the reader.
func ReadHeader(r io.Reader) (Header, error) {
	buf := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return Header{}, err
	}

	h := Header{
		Magic:   buf[0],
		Version: buf[1],
		Type:    MessageType(buf[2]),
		Flags:   MessageFlag(buf[3]),
		Length:  binary.BigEndian.Uint32(buf[4:]),
	}

	if h.Magic != MagicByte {
		return Header{}, ErrInvalidMagic
	}
	if h.Version != ProtocolVersion {
		return Header{}, ErrInvalidVersion
	}
	if h.Length > MaxMessageSize {
		return Header{}, ErrMessageTooLarge
	}

	return h, nil
}

// WriteMessage writes a complete message to the writer.
func WriteMessage(w io.Writer, msgType MessageType, payload []byte) error {
	h := Header{
		Magic:   MagicByte,
		Version: ProtocolVersion,
		Type:    msgType,
		Flags:   FlagNone,
		Length:  uint32(len(payload)),
	}

	if err := WriteHeader(w, h); err != nil {
		return err
	}

	if len(payload) > 0 {
		_, err := w.Write(payload)
		return err
	}
	return nil
}

// ReadMessage reads a complete message from the reader.
func ReadMessage(r io.Reader) (*Message, error) {
	h, err := ReadHeader(r)
	if err != nil {
		return nil, err
	}

	msg := &Message{Header: h}
	if h.Length > 0 {
		msg.Payload = make([]byte, h.Length)
		if _, err := io.ReadFull(r, msg.Payload); err != nil {
			return nil, err
		}
	}

	return msg, nil
}

