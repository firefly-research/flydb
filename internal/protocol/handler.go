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
	"bufio"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"flydb/internal/logging"
)

// Package-level logger for the protocol component.
var log = logging.NewLogger("protocol")

// QueryExecutor is the interface for executing SQL queries.
type QueryExecutor interface {
	Execute(query string) (string, error)
}

// PreparedStatementManager is the interface for managing prepared statements.
type PreparedStatementManager interface {
	Prepare(name, query string) error
	Execute(name string, params []interface{}) (string, error)
	Deallocate(name string) error
}

// Authenticator is the interface for authentication.
type Authenticator interface {
	Authenticate(username, password string) bool
}

// BinaryHandler handles binary protocol connections.
type BinaryHandler struct {
	executor    QueryExecutor
	prepMgr     PreparedStatementManager
	auth        Authenticator
	mu          sync.RWMutex
	connections map[net.Conn]bool
}

// NewBinaryHandler creates a new binary protocol handler.
func NewBinaryHandler(executor QueryExecutor, prepMgr PreparedStatementManager, auth Authenticator) *BinaryHandler {
	return &BinaryHandler{
		executor:    executor,
		prepMgr:     prepMgr,
		auth:        auth,
		connections: make(map[net.Conn]bool),
	}
}

// HandleConnection handles a single binary protocol connection.
func (h *BinaryHandler) HandleConnection(conn net.Conn) {
	remoteAddr := conn.RemoteAddr().String()
	connStart := time.Now()

	log.Info("Binary connection established",
		"remote_addr", remoteAddr,
		"local_addr", conn.LocalAddr().String(),
	)

	h.mu.Lock()
	h.connections[conn] = true
	activeConns := len(h.connections)
	h.mu.Unlock()

	log.Debug("Active binary connections", "count", activeConns)

	defer func() {
		connDuration := time.Since(connStart)

		h.mu.Lock()
		delete(h.connections, conn)
		remainingConns := len(h.connections)
		h.mu.Unlock()

		conn.Close()
		log.Info("Binary connection terminated",
			"remote_addr", remoteAddr,
			"duration", connDuration,
			"remaining_connections", remainingConns,
		)
	}()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	authenticated := false

	for {
		msg, err := ReadMessage(reader)
		if err != nil {
			if err != io.EOF {
				log.Debug("Binary read error", "remote_addr", remoteAddr, "error", err)
				h.sendError(writer, 500, fmt.Sprintf("read error: %v", err))
			}
			return
		}

		// Get command name for logging (without sensitive data)
		cmdName := msgTypeToString(msg.Header.Type)

		// Create request context for logging
		reqCtx := logging.NewRequestContext(remoteAddr, cmdName)

		// Handle authentication first
		if !authenticated && msg.Header.Type != MsgAuth && msg.Header.Type != MsgPing {
			reqCtx.LogError(log, "authentication required")
			h.sendError(writer, 401, "authentication required")
			continue
		}

		var success bool
		switch msg.Header.Type {
		case MsgAuth:
			authenticated = h.handleAuth(writer, msg.Payload, remoteAddr)
			success = authenticated

		case MsgQuery:
			success = h.handleQuery(writer, msg.Payload, remoteAddr)

		case MsgPrepare:
			success = h.handlePrepare(writer, msg.Payload, remoteAddr)

		case MsgExecute:
			success = h.handleExecute(writer, msg.Payload, remoteAddr)

		case MsgDeallocate:
			success = h.handleDeallocate(writer, msg.Payload, remoteAddr)

		case MsgPing:
			h.handlePing(writer)
			success = true

		default:
			reqCtx.LogError(log, "unknown message type")
			h.sendError(writer, 400, "unknown message type")
			continue
		}

		// Log the completed request
		if success {
			reqCtx.LogComplete(log, "success")
		} else {
			reqCtx.LogError(log, "command failed")
		}
	}
}

// msgTypeToString converts a message type to a string for logging.
func msgTypeToString(t MessageType) string {
	switch t {
	case MsgQuery:
		return "QUERY"
	case MsgQueryResult:
		return "QUERY_RESULT"
	case MsgError:
		return "ERROR"
	case MsgPrepare:
		return "PREPARE"
	case MsgPrepareResult:
		return "PREPARE_RESULT"
	case MsgExecute:
		return "EXECUTE"
	case MsgDeallocate:
		return "DEALLOCATE"
	case MsgAuth:
		return "AUTH"
	case MsgAuthResult:
		return "AUTH_RESULT"
	case MsgPing:
		return "PING"
	case MsgPong:
		return "PONG"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", t)
	}
}

// handleAuth handles authentication messages.
func (h *BinaryHandler) handleAuth(w *bufio.Writer, payload []byte, remoteAddr string) bool {
	authMsg, err := DecodeAuthMessage(payload)
	if err != nil {
		log.Debug("Invalid auth message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid auth message")
		return false
	}

	success := h.auth.Authenticate(authMsg.Username, authMsg.Password)
	result := &AuthResultMessage{
		Success: success,
	}
	if !success {
		result.Message = "authentication failed"
		log.Warn("Binary auth failed", "remote_addr", remoteAddr, "user", authMsg.Username)
	} else {
		result.Message = "authenticated"
		log.Info("Binary auth success", "remote_addr", remoteAddr, "user", authMsg.Username)
	}

	data, _ := result.Encode()
	WriteMessage(w, MsgAuthResult, data)
	w.Flush()
	return success
}

// handleQuery handles query messages.
func (h *BinaryHandler) handleQuery(w *bufio.Writer, payload []byte, remoteAddr string) bool {
	queryMsg, err := DecodeQueryMessage(payload)
	if err != nil {
		log.Debug("Invalid query message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid query message")
		return false
	}

	log.Debug("Executing binary query", "remote_addr", remoteAddr)
	result, err := h.executor.Execute(queryMsg.Query)
	if err != nil {
		log.Debug("Binary query error", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 500, err.Error())
		return false
	}

	log.Debug("Binary query success", "remote_addr", remoteAddr)
	resultMsg := &QueryResultMessage{
		Success: true,
		Message: result,
	}
	data, _ := resultMsg.Encode()
	WriteMessage(w, MsgQueryResult, data)
	w.Flush()
	return true
}

// handlePrepare handles prepare statement messages.
func (h *BinaryHandler) handlePrepare(w *bufio.Writer, payload []byte, remoteAddr string) bool {
	prepMsg, err := DecodePrepareMessage(payload)
	if err != nil {
		log.Debug("Invalid prepare message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid prepare message")
		return false
	}

	if h.prepMgr == nil {
		h.sendError(w, 501, "prepared statements not supported")
		return false
	}

	log.Debug("Preparing statement", "remote_addr", remoteAddr, "name", prepMsg.Name)
	err = h.prepMgr.Prepare(prepMsg.Name, prepMsg.Query)
	if err != nil {
		log.Debug("Prepare error", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 500, err.Error())
		return false
	}

	resultMsg := &PrepareResultMessage{
		Success: true,
		Name:    prepMsg.Name,
	}
	data, _ := resultMsg.Encode()
	WriteMessage(w, MsgPrepareResult, data)
	w.Flush()
	return true
}

// handleExecute handles execute prepared statement messages.
func (h *BinaryHandler) handleExecute(w *bufio.Writer, payload []byte, remoteAddr string) bool {
	execMsg, err := DecodeExecuteMessage(payload)
	if err != nil {
		log.Debug("Invalid execute message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid execute message")
		return false
	}

	if h.prepMgr == nil {
		h.sendError(w, 501, "prepared statements not supported")
		return false
	}

	log.Debug("Executing prepared statement", "remote_addr", remoteAddr, "name", execMsg.Name)
	result, err := h.prepMgr.Execute(execMsg.Name, execMsg.Params)
	if err != nil {
		log.Debug("Execute error", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 500, err.Error())
		return false
	}

	resultMsg := &QueryResultMessage{
		Success: true,
		Message: result,
	}
	data, _ := resultMsg.Encode()
	WriteMessage(w, MsgQueryResult, data)
	w.Flush()
	return true
}

// handleDeallocate handles deallocate prepared statement messages.
func (h *BinaryHandler) handleDeallocate(w *bufio.Writer, payload []byte, remoteAddr string) bool {
	deallocMsg, err := DecodeDeallocateMessage(payload)
	if err != nil {
		log.Debug("Invalid deallocate message", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 400, "invalid deallocate message")
		return false
	}

	if h.prepMgr == nil {
		h.sendError(w, 501, "prepared statements not supported")
		return false
	}

	log.Debug("Deallocating statement", "remote_addr", remoteAddr, "name", deallocMsg.Name)
	err = h.prepMgr.Deallocate(deallocMsg.Name)
	if err != nil {
		log.Debug("Deallocate error", "remote_addr", remoteAddr, "error", err)
		h.sendError(w, 500, err.Error())
		return false
	}

	resultMsg := &QueryResultMessage{
		Success: true,
		Message: "DEALLOCATE OK",
	}
	data, _ := resultMsg.Encode()
	WriteMessage(w, MsgQueryResult, data)
	w.Flush()
	return true
}

// handlePing handles ping messages.
func (h *BinaryHandler) handlePing(w *bufio.Writer) {
	WriteMessage(w, MsgPong, nil)
	w.Flush()
}

// sendError sends an error message.
func (h *BinaryHandler) sendError(w *bufio.Writer, code int, message string) {
	errMsg := &ErrorMessage{
		Code:    code,
		Message: message,
	}
	data, _ := errMsg.Encode()
	WriteMessage(w, MsgError, data)
	w.Flush()
}

// Close closes all connections.
func (h *BinaryHandler) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for conn := range h.connections {
		conn.Close()
	}
}

