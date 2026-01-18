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
Package sql contains the PreparedStatementManager for query compilation and reuse.

Prepared Statements Overview:
=============================

Prepared statements allow queries to be compiled once and executed multiple times
with different parameter values. This provides several benefits:

 1. Performance: The query is parsed and validated only once
 2. Security: Parameters are properly escaped, preventing SQL injection
 3. Efficiency: Reduced parsing overhead for repeated queries

Usage:
======

 1. PREPARE: Compile a query with parameter placeholders ($1, $2, etc.)
 2. EXECUTE: Run the prepared query with specific parameter values
 3. DEALLOCATE: Remove the prepared statement when no longer needed

Example:
========

	PREPARE get_user AS SELECT * FROM users WHERE id = $1
	EXECUTE get_user USING 42
	EXECUTE get_user USING 100
	DEALLOCATE get_user
*/
package sql

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	ferrors "flydb/internal/errors"
)

// PreparedStatement represents a compiled prepared statement.
type PreparedStatement struct {
	Name       string    // Statement name
	Query      string    // Original query with placeholders
	ParamCount int       // Number of parameters
	ParsedStmt Statement // Cached parsed statement (for non-parameterized queries)
}

// PreparedStatementManager manages prepared statements.
type PreparedStatementManager struct {
	statements map[string]*PreparedStatement
	executor   *Executor
	mu         sync.RWMutex
}

// NewPreparedStatementManager creates a new prepared statement manager.
func NewPreparedStatementManager(executor *Executor) *PreparedStatementManager {
	return &PreparedStatementManager{
		statements: make(map[string]*PreparedStatement),
		executor:   executor,
	}
}

// paramRegex matches parameter placeholders like $1, $2, etc.
var paramRegex = regexp.MustCompile(`\$([0-9]+)`)

// Prepare compiles a query and stores it for later execution.
func (m *PreparedStatementManager) Prepare(name, query string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if statement already exists
	if _, exists := m.statements[name]; exists {
		return ferrors.PreparedStatementAlreadyExists(name)
	}

	// Count parameters
	matches := paramRegex.FindAllStringSubmatch(query, -1)
	paramCount := 0
	for _, match := range matches {
		var num int
		fmt.Sscanf(match[1], "%d", &num)
		if num > paramCount {
			paramCount = num
		}
	}

	// Store the prepared statement
	m.statements[name] = &PreparedStatement{
		Name:       name,
		Query:      query,
		ParamCount: paramCount,
	}

	return nil
}

// Execute runs a prepared statement with the given parameters.
func (m *PreparedStatementManager) Execute(name string, params []interface{}) (string, error) {
	m.mu.RLock()
	stmt, exists := m.statements[name]
	m.mu.RUnlock()

	if !exists {
		return "", ferrors.PreparedStatementNotFound(name)
	}

	// Validate parameter count
	if len(params) != stmt.ParamCount {
		return "", ferrors.ParameterMismatch(stmt.ParamCount, len(params))
	}

	// Substitute parameters into the query
	query := stmt.Query
	for i, param := range params {
		placeholder := fmt.Sprintf("$%d", i+1)
		value := formatParamValue(param)
		query = strings.ReplaceAll(query, placeholder, value)
	}

	// Parse and execute the query
	lexer := NewLexer(query)
	parser := NewParser(lexer)
	parsedStmt, err := parser.Parse()
	if err != nil {
		return "", ferrors.InternalError("failed to parse prepared statement").WithCause(err)
	}

	return m.executor.Execute(parsedStmt)
}

// formatParamValue formats a parameter value for substitution.
func formatParamValue(param interface{}) string {
	switch v := param.(type) {
	case string:
		// Escape single quotes in strings
		escaped := strings.ReplaceAll(v, "'", "''")
		return "'" + escaped + "'"
	case nil:
		return "NULL"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// Deallocate removes a prepared statement.
func (m *PreparedStatementManager) Deallocate(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.statements[name]; !exists {
		return ferrors.PreparedStatementNotFound(name)
	}

	delete(m.statements, name)
	return nil
}

// Get retrieves a prepared statement by name.
func (m *PreparedStatementManager) Get(name string) (*PreparedStatement, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	stmt, exists := m.statements[name]
	return stmt, exists
}

// List returns all prepared statement names.
func (m *PreparedStatementManager) List() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, 0, len(m.statements))
	for name := range m.statements {
		names = append(names, name)
	}
	return names
}

// Clear removes all prepared statements.
func (m *PreparedStatementManager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statements = make(map[string]*PreparedStatement)
}

// ExecuteWithStringParams runs a prepared statement with string parameters.
// This is a convenience method for string-based parameter passing.
func (m *PreparedStatementManager) ExecuteWithStringParams(name string, params []string) (string, error) {
	// Convert string params to interface{}
	iparams := make([]interface{}, len(params))
	for i, p := range params {
		iparams[i] = p
	}
	return m.Execute(name, iparams)
}

// validatePreparedStatement validates a prepared statement without storing it.
func validatePreparedStatement(query string) error {
	// Try to parse the query with placeholder substitution
	testQuery := paramRegex.ReplaceAllString(query, "'test'")
	lexer := NewLexer(testQuery)
	parser := NewParser(lexer)
	_, err := parser.Parse()
	if err != nil {
		return fmt.Errorf("invalid query syntax: %v", err)
	}
	return nil
}
