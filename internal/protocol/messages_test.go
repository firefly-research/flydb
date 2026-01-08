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
	"testing"
)

func TestQueryMessageEncodeDecode(t *testing.T) {
	original := &QueryMessage{Query: "SELECT * FROM users"}
	
	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	
	decoded, err := DecodeQueryMessage(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	
	if decoded.Query != original.Query {
		t.Errorf("Expected '%s', got '%s'", original.Query, decoded.Query)
	}
}

func TestQueryResultMessageEncodeDecode(t *testing.T) {
	original := &QueryResultMessage{
		Success:  true,
		Message:  "OK",
		Columns:  []string{"id", "name"},
		Rows:     [][]interface{}{{1, "Alice"}, {2, "Bob"}},
		RowCount: 2,
	}
	
	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	
	decoded, err := DecodeQueryResultMessage(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	
	if decoded.Success != original.Success {
		t.Errorf("Success mismatch")
	}
	if decoded.RowCount != original.RowCount {
		t.Errorf("RowCount mismatch: expected %d, got %d", original.RowCount, decoded.RowCount)
	}
}

func TestErrorMessageEncodeDecode(t *testing.T) {
	original := &ErrorMessage{Code: 404, Message: "Table not found"}
	
	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	
	decoded, err := DecodeErrorMessage(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	
	if decoded.Code != original.Code {
		t.Errorf("Code mismatch: expected %d, got %d", original.Code, decoded.Code)
	}
	if decoded.Message != original.Message {
		t.Errorf("Message mismatch")
	}
}

func TestAuthMessageEncodeDecode(t *testing.T) {
	original := &AuthMessage{Username: "admin", Password: "secret"}

	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeAuthMessage(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Username != original.Username {
		t.Errorf("Username mismatch")
	}
	if decoded.Password != original.Password {
		t.Errorf("Password mismatch")
	}
}

func TestAuthMessageWithDatabaseEncodeDecode(t *testing.T) {
	original := &AuthMessage{Username: "admin", Password: "secret", Database: "mydb"}

	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeAuthMessage(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Username != original.Username {
		t.Errorf("Username mismatch: expected '%s', got '%s'", original.Username, decoded.Username)
	}
	if decoded.Password != original.Password {
		t.Errorf("Password mismatch")
	}
	if decoded.Database != original.Database {
		t.Errorf("Database mismatch: expected '%s', got '%s'", original.Database, decoded.Database)
	}
}

func TestAuthResultMessageEncodeDecode(t *testing.T) {
	original := &AuthResultMessage{
		Success:  true,
		Message:  "Authentication successful",
		Database: "testdb",
	}

	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeAuthResultMessage(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Success != original.Success {
		t.Errorf("Success mismatch")
	}
	if decoded.Message != original.Message {
		t.Errorf("Message mismatch: expected '%s', got '%s'", original.Message, decoded.Message)
	}
	if decoded.Database != original.Database {
		t.Errorf("Database mismatch: expected '%s', got '%s'", original.Database, decoded.Database)
	}
}

func TestPrepareMessageEncodeDecode(t *testing.T) {
	original := &PrepareMessage{Name: "get_user", Query: "SELECT * FROM users WHERE id = $1"}
	
	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	
	decoded, err := DecodePrepareMessage(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	
	if decoded.Name != original.Name {
		t.Errorf("Name mismatch")
	}
	if decoded.Query != original.Query {
		t.Errorf("Query mismatch")
	}
}

func TestBinaryEncoderDecoder(t *testing.T) {
	encoder := NewBinaryEncoder()

	encoder.WriteString("hello")
	encoder.WriteInt64(12345)
	encoder.WriteFloat64(3.14159)
	encoder.WriteBool(true)
	encoder.WriteBytes([]byte{1, 2, 3})

	decoder := NewBinaryDecoder(encoder.Bytes())

	str, err := decoder.ReadString()
	if err != nil || str != "hello" {
		t.Errorf("String mismatch: %v, %s", err, str)
	}

	i64, err := decoder.ReadInt64()
	if err != nil || i64 != 12345 {
		t.Errorf("Int64 mismatch: %v, %d", err, i64)
	}

	f64, err := decoder.ReadFloat64()
	if err != nil || f64 != 3.14159 {
		t.Errorf("Float64 mismatch: %v, %f", err, f64)
	}

	b, err := decoder.ReadBool()
	if err != nil || !b {
		t.Errorf("Bool mismatch: %v, %v", err, b)
	}

	bytes, err := decoder.ReadBytes()
	if err != nil || len(bytes) != 3 {
		t.Errorf("Bytes mismatch: %v, %v", err, bytes)
	}
}

// ============================================================================
// SDK Message Tests for ODBC/JDBC driver support
// ============================================================================

func TestCursorOpenMessageEncodeDecode(t *testing.T) {
	original := &CursorOpenMessage{
		Query:       "SELECT * FROM users WHERE id = $1",
		CursorType:  1, // Static
		Concurrency: 0, // Read-only
		FetchSize:   100,
		Parameters:  []interface{}{42},
	}

	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeCursorOpenMessage(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Query != original.Query {
		t.Errorf("Query mismatch: expected '%s', got '%s'", original.Query, decoded.Query)
	}
	if decoded.CursorType != original.CursorType {
		t.Errorf("CursorType mismatch: expected %d, got %d", original.CursorType, decoded.CursorType)
	}
	if decoded.FetchSize != original.FetchSize {
		t.Errorf("FetchSize mismatch: expected %d, got %d", original.FetchSize, decoded.FetchSize)
	}
}

func TestCursorFetchMessageEncodeDecode(t *testing.T) {
	original := &CursorFetchMessage{
		CursorID:  "cur_12345",
		Direction: 0, // Next
		Offset:    0,
		Count:     50,
	}

	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeCursorFetchMessage(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.CursorID != original.CursorID {
		t.Errorf("CursorID mismatch")
	}
	if decoded.Count != original.Count {
		t.Errorf("Count mismatch: expected %d, got %d", original.Count, decoded.Count)
	}
}

func TestCursorResultMessageEncodeDecode(t *testing.T) {
	original := &CursorResultMessage{
		Success:     true,
		CursorID:    "cur_12345",
		Columns:     []ColumnMetadata{{Index: 0, Name: "id", Type: "INTEGER"}},
		Rows:        [][]interface{}{{1}, {2}, {3}},
		RowCount:    3,
		HasMoreRows: true,
		Position:    2,
		TotalRows:   100,
	}

	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeCursorResultMessage(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.CursorID != original.CursorID {
		t.Errorf("CursorID mismatch")
	}
	if decoded.RowCount != original.RowCount {
		t.Errorf("RowCount mismatch: expected %d, got %d", original.RowCount, decoded.RowCount)
	}
	if decoded.HasMoreRows != original.HasMoreRows {
		t.Errorf("HasMoreRows mismatch")
	}
}

func TestGetTablesMessageEncodeDecode(t *testing.T) {
	original := &GetTablesMessage{
		Catalog:    "main",
		Schema:     "public",
		TableName:  "%",
		TableTypes: []string{"TABLE", "VIEW"},
	}

	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeGetTablesMessage(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Catalog != original.Catalog {
		t.Errorf("Catalog mismatch")
	}
	if len(decoded.TableTypes) != len(original.TableTypes) {
		t.Errorf("TableTypes length mismatch")
	}
}

func TestBeginTxMessageEncodeDecode(t *testing.T) {
	original := &BeginTxMessage{
		IsolationLevel: 2, // Repeatable Read
		ReadOnly:       true,
		Deferrable:     false,
	}

	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeBeginTxMessage(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.IsolationLevel != original.IsolationLevel {
		t.Errorf("IsolationLevel mismatch: expected %d, got %d", original.IsolationLevel, decoded.IsolationLevel)
	}
	if decoded.ReadOnly != original.ReadOnly {
		t.Errorf("ReadOnly mismatch")
	}
}

func TestSessionResultMessageEncodeDecode(t *testing.T) {
	original := &SessionResultMessage{
		Success:         true,
		SessionID:       "sess_12345",
		ServerVersion:   "1.0.0",
		ProtocolVersion: 1,
		Database:        "testdb",
		Username:        "admin",
		AutoCommit:      true,
		IsolationLevel:  1,
		Capabilities:    []string{"sql", "transactions", "cursors"},
	}

	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeSessionResultMessage(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.SessionID != original.SessionID {
		t.Errorf("SessionID mismatch")
	}
	if decoded.ServerVersion != original.ServerVersion {
		t.Errorf("ServerVersion mismatch")
	}
	if len(decoded.Capabilities) != len(original.Capabilities) {
		t.Errorf("Capabilities length mismatch")
	}
}

func TestEnhancedQueryResultMessageEncodeDecode(t *testing.T) {
	original := &EnhancedQueryResultMessage{
		Success:       true,
		StatementType: "SELECT",
		AffectedRows:  0,
		Columns: []ColumnMetadata{
			{Index: 0, Name: "id", Type: "INTEGER", Nullable: false, AutoIncrement: true},
			{Index: 1, Name: "name", Type: "TEXT", Nullable: true},
		},
		Rows:        [][]interface{}{{1, "Alice"}, {2, "Bob"}},
		RowCount:    2,
		HasMoreRows: false,
		SQLSTATE:    "00000",
	}

	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeEnhancedQueryResultMessage(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.StatementType != original.StatementType {
		t.Errorf("StatementType mismatch")
	}
	if decoded.RowCount != original.RowCount {
		t.Errorf("RowCount mismatch: expected %d, got %d", original.RowCount, decoded.RowCount)
	}
	if len(decoded.Columns) != len(original.Columns) {
		t.Errorf("Columns length mismatch")
	}
}

// ============================================================================
// Database Selection Message Tests for ODBC/JDBC driver support
// ============================================================================

func TestUseDatabaseMessageEncodeDecode(t *testing.T) {
	original := &UseDatabaseMessage{
		Database: "production_db",
	}

	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeUseDatabaseMessage(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Database != original.Database {
		t.Errorf("Database mismatch: expected '%s', got '%s'", original.Database, decoded.Database)
	}
}

func TestGetDatabasesMessageEncodeDecode(t *testing.T) {
	original := &GetDatabasesMessage{
		Pattern: "test%",
	}

	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeGetDatabasesMessage(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Pattern != original.Pattern {
		t.Errorf("Pattern mismatch: expected '%s', got '%s'", original.Pattern, decoded.Pattern)
	}
}

func TestDatabaseResultMessageEncodeDecode(t *testing.T) {
	original := &DatabaseResultMessage{
		Success:   true,
		Database:  "mydb",
		Databases: []string{"default", "mydb", "testdb"},
		Message:   "Database changed successfully",
	}

	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeDatabaseResultMessage(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Success != original.Success {
		t.Errorf("Success mismatch")
	}
	if decoded.Database != original.Database {
		t.Errorf("Database mismatch: expected '%s', got '%s'", original.Database, decoded.Database)
	}
	if len(decoded.Databases) != len(original.Databases) {
		t.Errorf("Databases length mismatch: expected %d, got %d", len(original.Databases), len(decoded.Databases))
	}
	for i, db := range original.Databases {
		if decoded.Databases[i] != db {
			t.Errorf("Database[%d] mismatch: expected '%s', got '%s'", i, db, decoded.Databases[i])
		}
	}
	if decoded.Message != original.Message {
		t.Errorf("Message mismatch: expected '%s', got '%s'", original.Message, decoded.Message)
	}
}

func TestDatabaseResultMessageEmptyDatabases(t *testing.T) {
	original := &DatabaseResultMessage{
		Success:   true,
		Database:  "default",
		Databases: []string{},
		Message:   "",
	}

	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeDatabaseResultMessage(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Success != original.Success {
		t.Errorf("Success mismatch")
	}
	if len(decoded.Databases) != 0 {
		t.Errorf("Expected empty Databases, got %d items", len(decoded.Databases))
	}
}
