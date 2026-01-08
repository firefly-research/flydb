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

package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
)

// QueryMessage represents a SQL query request.
type QueryMessage struct {
	Query string `json:"query"`
}

// Encode encodes the query message to bytes.
func (m *QueryMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeQueryMessage decodes a query message from bytes.
func DecodeQueryMessage(data []byte) (*QueryMessage, error) {
	var m QueryMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// QueryResultMessage represents a query response.
type QueryResultMessage struct {
	Success  bool            `json:"success"`
	Message  string          `json:"message,omitempty"`
	Columns  []string        `json:"columns,omitempty"`
	Rows     [][]interface{} `json:"rows,omitempty"`
	RowCount int             `json:"row_count"`
}

// Encode encodes the query result message to bytes.
func (m *QueryResultMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeQueryResultMessage decodes a query result message from bytes.
func DecodeQueryResultMessage(data []byte) (*QueryResultMessage, error) {
	var m QueryResultMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// ErrorMessage represents an error response.
type ErrorMessage struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Encode encodes the error message to bytes.
func (m *ErrorMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeErrorMessage decodes an error message from bytes.
func DecodeErrorMessage(data []byte) (*ErrorMessage, error) {
	var m ErrorMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// PrepareMessage represents a prepare statement request.
type PrepareMessage struct {
	Name  string `json:"name"`
	Query string `json:"query"`
}

// Encode encodes the prepare message to bytes.
func (m *PrepareMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodePrepareMessage decodes a prepare message from bytes.
func DecodePrepareMessage(data []byte) (*PrepareMessage, error) {
	var m PrepareMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// PrepareResultMessage represents a prepare statement response.
type PrepareResultMessage struct {
	Success    bool     `json:"success"`
	Name       string   `json:"name"`
	ParamCount int      `json:"param_count"`
	ParamTypes []string `json:"param_types,omitempty"`
}

// Encode encodes the prepare result message to bytes.
func (m *PrepareResultMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodePrepareResultMessage decodes a prepare result message from bytes.
func DecodePrepareResultMessage(data []byte) (*PrepareResultMessage, error) {
	var m PrepareResultMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// ExecuteMessage represents an execute prepared statement request.
type ExecuteMessage struct {
	Name   string        `json:"name"`
	Params []interface{} `json:"params"`
}

// Encode encodes the execute message to bytes.
func (m *ExecuteMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeExecuteMessage decodes an execute message from bytes.
func DecodeExecuteMessage(data []byte) (*ExecuteMessage, error) {
	var m ExecuteMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// DeallocateMessage represents a deallocate prepared statement request.
type DeallocateMessage struct {
	Name string `json:"name"`
}

// Encode encodes the deallocate message to bytes.
func (m *DeallocateMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeDeallocateMessage decodes a deallocate message from bytes.
func DecodeDeallocateMessage(data []byte) (*DeallocateMessage, error) {
	var m DeallocateMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// AuthMessage represents an authentication request.
type AuthMessage struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database,omitempty"` // Optional: database to connect to (default: "default")
}

// Encode encodes the auth message to bytes.
func (m *AuthMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeAuthMessage decodes an auth message from bytes.
func DecodeAuthMessage(data []byte) (*AuthMessage, error) {
	var m AuthMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// AuthResultMessage represents an authentication response.
type AuthResultMessage struct {
	Success  bool   `json:"success"`
	Message  string `json:"message,omitempty"`
	Database string `json:"database,omitempty"` // Current database after authentication
}

// Encode encodes the auth result message to bytes.
func (m *AuthResultMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeAuthResultMessage decodes an auth result message from bytes.
func DecodeAuthResultMessage(data []byte) (*AuthResultMessage, error) {
	var m AuthResultMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// BinaryEncoder provides efficient binary encoding for row data.
type BinaryEncoder struct {
	buf *bytes.Buffer
}

// NewBinaryEncoder creates a new binary encoder.
func NewBinaryEncoder() *BinaryEncoder {
	return &BinaryEncoder{buf: new(bytes.Buffer)}
}

// WriteString writes a length-prefixed string.
func (e *BinaryEncoder) WriteString(s string) error {
	if err := binary.Write(e.buf, binary.BigEndian, uint32(len(s))); err != nil {
		return err
	}
	_, err := e.buf.WriteString(s)
	return err
}

// WriteInt64 writes a 64-bit integer.
func (e *BinaryEncoder) WriteInt64(v int64) error {
	return binary.Write(e.buf, binary.BigEndian, v)
}

// WriteFloat64 writes a 64-bit float.
func (e *BinaryEncoder) WriteFloat64(v float64) error {
	return binary.Write(e.buf, binary.BigEndian, v)
}

// WriteBool writes a boolean.
func (e *BinaryEncoder) WriteBool(v bool) error {
	if v {
		return e.buf.WriteByte(1)
	}
	return e.buf.WriteByte(0)
}

// WriteBytes writes length-prefixed bytes.
func (e *BinaryEncoder) WriteBytes(b []byte) error {
	if err := binary.Write(e.buf, binary.BigEndian, uint32(len(b))); err != nil {
		return err
	}
	_, err := e.buf.Write(b)
	return err
}

// Bytes returns the encoded bytes.
func (e *BinaryEncoder) Bytes() []byte {
	return e.buf.Bytes()
}

// Reset resets the encoder for reuse.
func (e *BinaryEncoder) Reset() {
	e.buf.Reset()
}

// BinaryDecoder provides efficient binary decoding for row data.
type BinaryDecoder struct {
	r io.Reader
}

// NewBinaryDecoder creates a new binary decoder.
func NewBinaryDecoder(data []byte) *BinaryDecoder {
	return &BinaryDecoder{r: bytes.NewReader(data)}
}

// ReadString reads a length-prefixed string.
func (d *BinaryDecoder) ReadString() (string, error) {
	var length uint32
	if err := binary.Read(d.r, binary.BigEndian, &length); err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(d.r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// ReadInt64 reads a 64-bit integer.
func (d *BinaryDecoder) ReadInt64() (int64, error) {
	var v int64
	err := binary.Read(d.r, binary.BigEndian, &v)
	return v, err
}

// ReadFloat64 reads a 64-bit float.
func (d *BinaryDecoder) ReadFloat64() (float64, error) {
	var v float64
	err := binary.Read(d.r, binary.BigEndian, &v)
	return v, err
}

// ReadBool reads a boolean.
func (d *BinaryDecoder) ReadBool() (bool, error) {
	var b byte
	if err := binary.Read(d.r, binary.BigEndian, &b); err != nil {
		return false, err
	}
	return b != 0, nil
}

// ReadBytes reads length-prefixed bytes.
func (d *BinaryDecoder) ReadBytes() ([]byte, error) {
	var length uint32
	if err := binary.Read(d.r, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(d.r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
