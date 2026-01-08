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

package sql

import (
	"os"
	"strings"
	"testing"

	"flydb/internal/auth"
	"flydb/internal/storage"
)

func setupExecutorTest(t *testing.T) (*Executor, func()) {
	tmpDir, err := os.MkdirTemp("", "flydb_executor_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	kv, err := storage.NewKVStore(tmpDir + "/test.db")
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create KVStore: %v", err)
	}

	authMgr := auth.NewAuthManager(kv)
	exec := NewExecutor(kv, authMgr)

	cleanup := func() {
		kv.Close()
		os.RemoveAll(tmpDir)
	}

	return exec, cleanup
}

func TestExecutorCreateTable(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	stmt := &CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
		},
	}

	result, err := exec.Execute(stmt)
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	if result != "CREATE TABLE OK" {
		t.Errorf("Expected 'CREATE TABLE OK', got '%s'", result)
	}
}

func TestExecutorInsertAndSelect(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create table
	exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
		},
	})

	// Insert row
	result, err := exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"1", "Alice"},
	})
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	if result != "INSERT 1" {
		t.Errorf("Expected 'INSERT 1', got '%s'", result)
	}

	// Select row
	result, err = exec.Execute(&SelectStmt{
		TableName: "users",
		Columns:   []string{"name"},
		Where:     &Condition{Column: "id", Value: "1"},
	})
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	expected := "name\nAlice\n(1 rows)"
	if result != expected {
		t.Errorf("Expected '%s', got '%s'", expected, result)
	}
}

func TestExecutorUpdate(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create table and insert row
	exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
		},
	})
	exec.Execute(&InsertStmt{TableName: "users", Values: []string{"1", "Alice"}})

	// Update row
	result, err := exec.Execute(&UpdateStmt{
		TableName: "users",
		Updates:   map[string]string{"name": "Bob"},
		Where:     &Condition{Column: "id", Value: "1"},
	})
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}
	if result != "UPDATE 1" {
		t.Errorf("Expected 'UPDATE 1', got '%s'", result)
	}

	// Verify update
	result, _ = exec.Execute(&SelectStmt{
		TableName: "users",
		Columns:   []string{"name"},
		Where:     &Condition{Column: "id", Value: "1"},
	})
	expected := "name\nBob\n(1 rows)"
	if result != expected {
		t.Errorf("Expected '%s', got '%s'", expected, result)
	}
}

func TestExecutorDelete(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create table and insert rows
	exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns:   []ColumnDef{{Name: "id", Type: "INT"}, {Name: "name", Type: "TEXT"}},
	})
	exec.Execute(&InsertStmt{TableName: "users", Values: []string{"1", "Alice"}})
	exec.Execute(&InsertStmt{TableName: "users", Values: []string{"2", "Bob"}})

	// Delete one row
	result, err := exec.Execute(&DeleteStmt{
		TableName: "users",
		Where:     &Condition{Column: "id", Value: "1"},
	})
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	if result != "DELETE 1" {
		t.Errorf("Expected 'DELETE 1', got '%s'", result)
	}
}

func TestExecutorCreateUser(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	result, err := exec.Execute(&CreateUserStmt{
		Username: "testuser",
		Password: "testpass",
	})
	if err != nil {
		t.Fatalf("CREATE USER failed: %v", err)
	}
	if result != "CREATE USER OK" {
		t.Errorf("Expected 'CREATE USER OK', got '%s'", result)
	}
}

func TestExecutorGrant(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create user first
	exec.Execute(&CreateUserStmt{Username: "alice", Password: "pass"})

	// Grant permission
	result, err := exec.Execute(&GrantStmt{
		Username:  "alice",
		TableName: "orders",
	})
	if err != nil {
		t.Fatalf("GRANT failed: %v", err)
	}
	if result != "GRANT OK" {
		t.Errorf("Expected 'GRANT OK', got '%s'", result)
	}
}

func TestExecutorPermissionDenied(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create table as admin
	exec.Execute(&CreateTableStmt{
		TableName: "secret",
		Columns:   []ColumnDef{{Name: "data", Type: "TEXT"}},
	})

	// Create user without permissions
	exec.Execute(&CreateUserStmt{Username: "bob", Password: "pass"})

	// Set user context to bob
	exec.SetUser("bob")

	// Try to select from table without permission
	_, err := exec.Execute(&SelectStmt{
		TableName: "secret",
		Columns:   []string{"data"},
	})
	if err == nil {
		t.Error("Expected permission denied error")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("Expected 'permission denied' error, got: %v", err)
	}
}

func TestExecutorJoin(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create users table
	exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns:   []ColumnDef{{Name: "id", Type: "INT"}, {Name: "name", Type: "TEXT"}},
	})
	exec.Execute(&InsertStmt{TableName: "users", Values: []string{"1", "Alice"}})

	// Create orders table
	exec.Execute(&CreateTableStmt{
		TableName: "orders",
		Columns:   []ColumnDef{{Name: "id", Type: "INT"}, {Name: "user_id", Type: "INT"}, {Name: "product", Type: "TEXT"}},
	})
	exec.Execute(&InsertStmt{TableName: "orders", Values: []string{"1", "1", "Widget"}})

	// Join query
	result, err := exec.Execute(&SelectStmt{
		TableName: "users",
		Columns:   []string{"name", "product"},
		Join: &JoinClause{
			TableName: "orders",
			On:        &Condition{Column: "users.id", Value: "orders.user_id"},
		},
	})
	if err != nil {
		t.Fatalf("JOIN failed: %v", err)
	}
	if !strings.Contains(result, "Alice") || !strings.Contains(result, "Widget") {
		t.Errorf("Expected 'Alice' and 'Widget' in result, got '%s'", result)
	}
}

func TestExecutorOrderByAndLimit(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create table and insert rows
	exec.Execute(&CreateTableStmt{
		TableName: "items",
		Columns:   []ColumnDef{{Name: "id", Type: "INT"}, {Name: "name", Type: "TEXT"}},
	})
	exec.Execute(&InsertStmt{TableName: "items", Values: []string{"3", "C"}})
	exec.Execute(&InsertStmt{TableName: "items", Values: []string{"1", "A"}})
	exec.Execute(&InsertStmt{TableName: "items", Values: []string{"2", "B"}})

	// Select with ORDER BY and LIMIT
	result, err := exec.Execute(&SelectStmt{
		TableName: "items",
		Columns:   []string{"name"},
		OrderBy:   &OrderByClause{Column: "name", Direction: "ASC"},
		Limit:     2,
	})
	if err != nil {
		t.Fatalf("SELECT with ORDER BY failed: %v", err)
	}
	lines := strings.Split(result, "\n")
	// Result format: header row + data rows + row count line "(N rows)"
	// With LIMIT 2, we expect 1 header + 2 data rows + 1 row count line = 4 lines
	if len(lines) != 4 {
		t.Errorf("Expected 4 lines (1 header + 2 data + 1 count), got %d: %s", len(lines), result)
	}
}

func TestExecutorAggregateCount(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "items",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert some rows
	for i := 1; i <= 5; i++ {
		_, err = exec.Execute(&InsertStmt{
			TableName: "items",
			Values:    []string{string(rune('0' + i)), "item" + string(rune('0'+i))},
		})
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test COUNT(*)
	result, err := exec.Execute(&SelectStmt{
		TableName: "items",
		Aggregates: []*AggregateExpr{
			{Function: "COUNT", Column: "*"},
		},
	})
	if err != nil {
		t.Fatalf("SELECT COUNT(*) failed: %v", err)
	}

	expected := "count\n5\n(1 row)"
	if result != expected {
		t.Errorf("Expected COUNT(*) = '%s', got '%s'", expected, result)
	}
}

func TestExecutorAggregateSumAvg(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create table with numeric column
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "orders",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "amount", Type: "INT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert rows with amounts: 10, 20, 30, 40
	amounts := []string{"10", "20", "30", "40"}
	for i, amt := range amounts {
		_, err = exec.Execute(&InsertStmt{
			TableName: "orders",
			Values:    []string{string(rune('1' + i)), amt},
		})
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test SUM(amount) - should be 100
	result, err := exec.Execute(&SelectStmt{
		TableName: "orders",
		Aggregates: []*AggregateExpr{
			{Function: "SUM", Column: "amount"},
		},
	})
	if err != nil {
		t.Fatalf("SELECT SUM failed: %v", err)
	}

	expected := "sum(amount)\n100.00\n(1 row)"
	if result != expected {
		t.Errorf("Expected SUM = '%s', got '%s'", expected, result)
	}

	// Test AVG(amount) - should be 25
	result, err = exec.Execute(&SelectStmt{
		TableName: "orders",
		Aggregates: []*AggregateExpr{
			{Function: "AVG", Column: "amount"},
		},
	})
	if err != nil {
		t.Fatalf("SELECT AVG failed: %v", err)
	}

	expected = "avg(amount)\n25.00\n(1 row)"
	if result != expected {
		t.Errorf("Expected AVG = '%s', got '%s'", expected, result)
	}
}

func TestExecutorAggregateMinMax(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "products",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "price", Type: "INT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert rows with prices: 50, 100, 25, 75
	prices := []string{"50", "100", "25", "75"}
	for i, price := range prices {
		_, err = exec.Execute(&InsertStmt{
			TableName: "products",
			Values:    []string{string(rune('1' + i)), price},
		})
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test MIN and MAX together
	result, err := exec.Execute(&SelectStmt{
		TableName: "products",
		Aggregates: []*AggregateExpr{
			{Function: "MIN", Column: "price"},
			{Function: "MAX", Column: "price"},
		},
	})
	if err != nil {
		t.Fatalf("SELECT MIN, MAX failed: %v", err)
	}

	expected := "min(price), max(price)\n25.00, 100.00\n(1 row)"
	if result != expected {
		t.Errorf("Expected MIN=25.00, MAX=100.00, got '%s'", result)
	}
}

func TestExecutorAggregateWithWhere(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "sales",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "category", Type: "TEXT"},
			{Name: "amount", Type: "INT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert rows
	data := [][]string{
		{"1", "electronics", "100"},
		{"2", "electronics", "200"},
		{"3", "clothing", "50"},
		{"4", "electronics", "150"},
	}
	for _, row := range data {
		_, err = exec.Execute(&InsertStmt{
			TableName: "sales",
			Values:    row,
		})
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test COUNT with WHERE - only electronics (3 rows)
	result, err := exec.Execute(&SelectStmt{
		TableName: "sales",
		Aggregates: []*AggregateExpr{
			{Function: "COUNT", Column: "*"},
		},
		Where: &Condition{Column: "category", Value: "electronics"},
	})
	if err != nil {
		t.Fatalf("SELECT COUNT with WHERE failed: %v", err)
	}

	expected := "count\n3\n(1 row)"
	if result != expected {
		t.Errorf("Expected COUNT = '%s' for electronics, got '%s'", expected, result)
	}

	// Test SUM with WHERE - electronics total = 450
	result, err = exec.Execute(&SelectStmt{
		TableName: "sales",
		Aggregates: []*AggregateExpr{
			{Function: "SUM", Column: "amount"},
		},
		Where: &Condition{Column: "category", Value: "electronics"},
	})
	if err != nil {
		t.Fatalf("SELECT SUM with WHERE failed: %v", err)
	}

	expected = "sum(amount)\n450.00\n(1 row)"
	if result != expected {
		t.Errorf("Expected SUM = '%s' for electronics, got '%s'", expected, result)
	}
}

func TestExecutorGroupBy(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "sales",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "category", Type: "TEXT"},
			{Name: "amount", Type: "INT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	testData := []struct {
		id       string
		category string
		amount   string
	}{
		{"1", "electronics", "100"},
		{"2", "electronics", "200"},
		{"3", "clothing", "50"},
		{"4", "clothing", "75"},
		{"5", "clothing", "25"},
	}

	for _, d := range testData {
		_, err := exec.Execute(&InsertStmt{
			TableName: "sales",
			Values:    []string{d.id, d.category, d.amount},
		})
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test GROUP BY with COUNT
	result, err := exec.Execute(&SelectStmt{
		TableName: "sales",
		Columns:   []string{"category"},
		Aggregates: []*AggregateExpr{
			{Function: "COUNT", Column: "*"},
		},
		GroupBy: []string{"category"},
	})
	if err != nil {
		t.Fatalf("SELECT with GROUP BY failed: %v", err)
	}

	// Result should have 1 header + 2 groups + 1 row count line = 4 lines
	lines := strings.Split(result, "\n")
	if len(lines) != 4 {
		t.Errorf("Expected 4 lines (1 header + 2 groups + 1 count), got %d: %s", len(lines), result)
	}

	// Test GROUP BY with SUM
	result, err = exec.Execute(&SelectStmt{
		TableName: "sales",
		Columns:   []string{"category"},
		Aggregates: []*AggregateExpr{
			{Function: "SUM", Column: "amount"},
		},
		GroupBy: []string{"category"},
	})
	if err != nil {
		t.Fatalf("SELECT with GROUP BY SUM failed: %v", err)
	}

	// Should have 1 header + 2 groups + 1 row count line = 4 lines
	lines = strings.Split(result, "\n")
	if len(lines) != 4 {
		t.Errorf("Expected 4 lines (1 header + 2 groups + 1 count), got %d: %s", len(lines), result)
	}
}

func TestExecutorGroupByWithHaving(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "orders",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "customer", Type: "TEXT"},
			{Name: "amount", Type: "INT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data - customer A has 3 orders, B has 2, C has 1
	testData := []struct {
		id       string
		customer string
		amount   string
	}{
		{"1", "A", "100"},
		{"2", "A", "200"},
		{"3", "A", "150"},
		{"4", "B", "300"},
		{"5", "B", "250"},
		{"6", "C", "500"},
	}

	for _, d := range testData {
		_, err := exec.Execute(&InsertStmt{
			TableName: "orders",
			Values:    []string{d.id, d.customer, d.amount},
		})
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test HAVING COUNT(*) > 1 - should return A and B only
	result, err := exec.Execute(&SelectStmt{
		TableName: "orders",
		Columns:   []string{"customer"},
		Aggregates: []*AggregateExpr{
			{Function: "COUNT", Column: "*"},
		},
		GroupBy: []string{"customer"},
		Having: &HavingClause{
			Aggregate: &AggregateExpr{Function: "COUNT", Column: "*"},
			Operator:  ">",
			Value:     "1",
		},
	})
	if err != nil {
		t.Fatalf("SELECT with HAVING failed: %v", err)
	}

	// Should have 1 header + 2 groups (A and B, not C) + 1 row count line = 4 lines
	lines := strings.Split(result, "\n")
	if len(lines) != 4 {
		t.Errorf("Expected 4 lines (1 header + 2 groups + 1 count) with HAVING, got %d: %s", len(lines), result)
	}

	// Test HAVING SUM(amount) >= 500 - should return A (450) and B (550)
	result, err = exec.Execute(&SelectStmt{
		TableName: "orders",
		Columns:   []string{"customer"},
		Aggregates: []*AggregateExpr{
			{Function: "SUM", Column: "amount"},
		},
		GroupBy: []string{"customer"},
		Having: &HavingClause{
			Aggregate: &AggregateExpr{Function: "SUM", Column: "amount"},
			Operator:  ">=",
			Value:     "500",
		},
	})
	if err != nil {
		t.Fatalf("SELECT with HAVING SUM failed: %v", err)
	}

	// Should have 1 header + 2 groups (B=550 and C=500, not A=450) + 1 row count line = 4 lines
	lines = strings.Split(result, "\n")
	if len(lines) != 4 {
		t.Errorf("Expected 4 lines (1 header + 2 groups + 1 count) with HAVING SUM >= 500, got %d: %s", len(lines), result)
	}
}


// ============================================================================
// Constraint Tests
// ============================================================================

func TestPrimaryKeyConstraint(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create table with PRIMARY KEY
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT", Constraints: []ColumnConstraint{{Type: ConstraintPrimaryKey}}},
			{Name: "name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert first row
	_, err = exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"1", "Alice"},
	})
	if err != nil {
		t.Fatalf("First INSERT failed: %v", err)
	}

	// Try to insert duplicate primary key - should fail
	_, err = exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"1", "Bob"},
	})
	if err == nil {
		t.Error("Expected error for duplicate primary key, got none")
	}
	if err != nil && !strings.Contains(err.Error(), "duplicate primary key value") {
		t.Errorf("Expected 'duplicate primary key value' error, got: %v", err)
	}

	// Insert with different primary key - should succeed
	_, err = exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"2", "Bob"},
	})
	if err != nil {
		t.Fatalf("Second INSERT with different PK failed: %v", err)
	}
}

func TestNotNullConstraint(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create table with NOT NULL constraint
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT", Constraints: []ColumnConstraint{{Type: ConstraintNotNull}}},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert with valid value
	_, err = exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"1", "Alice"},
	})
	if err != nil {
		t.Fatalf("INSERT with valid value failed: %v", err)
	}

	// Try to insert NULL - should fail
	_, err = exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"2", "NULL"},
	})
	if err == nil {
		t.Error("Expected error for NULL in NOT NULL column, got none")
	}
	if err != nil && !strings.Contains(err.Error(), "cannot be NULL") {
		t.Errorf("Expected 'cannot be NULL' error, got: %v", err)
	}

	// Try to insert empty string - should fail
	_, err = exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"3", ""},
	})
	if err == nil {
		t.Error("Expected error for empty string in NOT NULL column, got none")
	}
}

func TestUniqueConstraint(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create table with UNIQUE constraint
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "email", Type: "TEXT", Constraints: []ColumnConstraint{{Type: ConstraintUnique}}},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert first row
	_, err = exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"1", "alice@example.com"},
	})
	if err != nil {
		t.Fatalf("First INSERT failed: %v", err)
	}

	// Try to insert duplicate email - should fail
	_, err = exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"2", "alice@example.com"},
	})
	if err == nil {
		t.Error("Expected error for duplicate unique value, got none")
	}
	if err != nil && !strings.Contains(err.Error(), "duplicate value for unique column") {
		t.Errorf("Expected 'duplicate value for unique column' error, got: %v", err)
	}

	// Insert with different email - should succeed
	_, err = exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"2", "bob@example.com"},
	})
	if err != nil {
		t.Fatalf("Second INSERT with different email failed: %v", err)
	}
}

func TestForeignKeyConstraint(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create parent table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT", Constraints: []ColumnConstraint{{Type: ConstraintPrimaryKey}}},
			{Name: "name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	// Insert a user
	_, err = exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"1", "Alice"},
	})
	if err != nil {
		t.Fatalf("INSERT user failed: %v", err)
	}

	// Create child table with foreign key
	_, err = exec.Execute(&CreateTableStmt{
		TableName: "orders",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "user_id", Type: "INT", Constraints: []ColumnConstraint{
				{Type: ConstraintForeignKey, ForeignKey: &ForeignKeyRef{Table: "users", Column: "id"}},
			}},
			{Name: "amount", Type: "INT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	// Insert order with valid foreign key
	_, err = exec.Execute(&InsertStmt{
		TableName: "orders",
		Values:    []string{"1", "1", "100"},
	})
	if err != nil {
		t.Fatalf("INSERT order with valid FK failed: %v", err)
	}

	// Try to insert order with invalid foreign key - should fail
	_, err = exec.Execute(&InsertStmt{
		TableName: "orders",
		Values:    []string{"2", "999", "200"},
	})
	if err == nil {
		t.Error("Expected error for invalid foreign key, got none")
	}
	if err != nil && !strings.Contains(err.Error(), "foreign key constraint violation") {
		t.Errorf("Expected 'foreign key constraint violation' error, got: %v", err)
	}
}

func TestForeignKeyDeleteRestriction(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create parent table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT", Constraints: []ColumnConstraint{{Type: ConstraintPrimaryKey}}},
			{Name: "name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	// Insert a user
	_, err = exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"1", "Alice"},
	})
	if err != nil {
		t.Fatalf("INSERT user failed: %v", err)
	}

	// Create child table with foreign key
	_, err = exec.Execute(&CreateTableStmt{
		TableName: "orders",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "user_id", Type: "INT", Constraints: []ColumnConstraint{
				{Type: ConstraintForeignKey, ForeignKey: &ForeignKeyRef{Table: "users", Column: "id"}},
			}},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	// Insert order referencing the user
	_, err = exec.Execute(&InsertStmt{
		TableName: "orders",
		Values:    []string{"1", "1"},
	})
	if err != nil {
		t.Fatalf("INSERT order failed: %v", err)
	}

	// Try to delete the user - should fail because of FK reference
	_, err = exec.Execute(&DeleteStmt{
		TableName: "users",
		Where:     &Condition{Column: "id", Value: "1"},
	})
	if err == nil {
		t.Error("Expected error when deleting referenced row, got none")
	}
	if err != nil && !strings.Contains(err.Error(), "cannot delete") {
		t.Errorf("Expected 'cannot delete' error, got: %v", err)
	}
}

func TestSerialAutoIncrement(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create table with SERIAL column
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "items",
		Columns: []ColumnDef{
			{Name: "id", Type: "SERIAL"},
			{Name: "name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert without providing id (should auto-generate)
	_, err = exec.Execute(&InsertStmt{
		TableName: "items",
		Values:    []string{"Item1"},
	})
	if err != nil {
		t.Fatalf("First INSERT failed: %v", err)
	}

	// Insert another row
	_, err = exec.Execute(&InsertStmt{
		TableName: "items",
		Values:    []string{"Item2"},
	})
	if err != nil {
		t.Fatalf("Second INSERT failed: %v", err)
	}

	// Select and verify auto-increment values
	result, err := exec.Execute(&SelectStmt{
		TableName: "items",
		Columns:   []string{"id", "name"},
	})
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	// Should have 2 rows with ids 1 and 2
	if !strings.Contains(result, "1") || !strings.Contains(result, "2") {
		t.Errorf("Expected auto-increment ids 1 and 2, got: %s", result)
	}
}

func TestDefaultValue(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create table with DEFAULT constraint
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "settings",
		Columns: []ColumnDef{
			{Name: "key", Type: "TEXT"},
			{Name: "value", Type: "TEXT", Constraints: []ColumnConstraint{
				{Type: ConstraintDefault, DefaultValue: "default_value"},
			}},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert with explicit value
	_, err = exec.Execute(&InsertStmt{
		TableName: "settings",
		Values:    []string{"key1", "custom_value"},
	})
	if err != nil {
		t.Fatalf("INSERT with explicit value failed: %v", err)
	}

	// Select and verify
	result, err := exec.Execute(&SelectStmt{
		TableName: "settings",
		Columns:   []string{"key", "value"},
	})
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if !strings.Contains(result, "custom_value") {
		t.Errorf("Expected 'custom_value', got: %s", result)
	}
}

func TestUpdateConstraintValidation(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create table with constraints
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT", Constraints: []ColumnConstraint{{Type: ConstraintPrimaryKey}}},
			{Name: "email", Type: "TEXT", Constraints: []ColumnConstraint{
				{Type: ConstraintUnique},
				{Type: ConstraintNotNull},
			}},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert two users
	_, err = exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"1", "alice@example.com"},
	})
	if err != nil {
		t.Fatalf("First INSERT failed: %v", err)
	}

	_, err = exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"2", "bob@example.com"},
	})
	if err != nil {
		t.Fatalf("Second INSERT failed: %v", err)
	}

	// Try to update to duplicate email - should fail
	_, err = exec.Execute(&UpdateStmt{
		TableName: "users",
		Updates:   map[string]string{"email": "alice@example.com"},
		Where:     &Condition{Column: "id", Value: "2"},
	})
	if err == nil {
		t.Error("Expected error for duplicate unique value on UPDATE, got none")
	}

	// Try to update to NULL - should fail
	_, err = exec.Execute(&UpdateStmt{
		TableName: "users",
		Updates:   map[string]string{"email": "NULL"},
		Where:     &Condition{Column: "id", Value: "1"},
	})
	if err == nil {
		t.Error("Expected error for NULL in NOT NULL column on UPDATE, got none")
	}
}

func TestIntrospectTableWithConstraints(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create table with various constraints
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "products",
		Columns: []ColumnDef{
			{Name: "id", Type: "SERIAL", Constraints: []ColumnConstraint{{Type: ConstraintPrimaryKey}}},
			{Name: "name", Type: "TEXT", Constraints: []ColumnConstraint{{Type: ConstraintNotNull}}},
			{Name: "sku", Type: "TEXT", Constraints: []ColumnConstraint{{Type: ConstraintUnique}}},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Introspect the table
	result, err := exec.Execute(&InspectStmt{
		Target:     "TABLE",
		ObjectName: "products",
	})
	if err != nil {
		t.Fatalf("INSPECT TABLE failed: %v", err)
	}

	// Verify constraint information is shown
	if !strings.Contains(result, "PRIMARY KEY") {
		t.Error("Expected 'PRIMARY KEY' in introspect output")
	}
	if !strings.Contains(result, "NOT NULL") {
		t.Error("Expected 'NOT NULL' in introspect output")
	}
	if !strings.Contains(result, "UNIQUE") {
		t.Error("Expected 'UNIQUE' in introspect output")
	}
	if !strings.Contains(result, "AUTO_INCREMENT") {
		t.Error("Expected 'AUTO_INCREMENT' in introspect output")
	}
}

// Tests for DISTINCT
func TestDistinct(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create table
	exec.Execute(&CreateTableStmt{
		TableName: "items",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "category", Type: "TEXT"},
		},
	})

	// Insert duplicate categories
	exec.Execute(&InsertStmt{TableName: "items", Values: []string{"1", "electronics"}})
	exec.Execute(&InsertStmt{TableName: "items", Values: []string{"2", "electronics"}})
	exec.Execute(&InsertStmt{TableName: "items", Values: []string{"3", "books"}})
	exec.Execute(&InsertStmt{TableName: "items", Values: []string{"4", "books"}})
	exec.Execute(&InsertStmt{TableName: "items", Values: []string{"5", "clothing"}})

	// Select with DISTINCT
	result, err := exec.Execute(&SelectStmt{
		TableName: "items",
		Columns:   []string{"category"},
		Distinct:  true,
	})
	if err != nil {
		t.Fatalf("SELECT DISTINCT failed: %v", err)
	}

	// Should have 3 unique categories + header
	lines := strings.Split(result, "\n")
	// Last line is "(X rows)" so we need to count actual data rows
	if !strings.Contains(result, "(3 rows)") {
		t.Errorf("Expected 3 distinct rows, got: %s", result)
	}
	if len(lines) < 4 {
		t.Errorf("Expected at least 4 lines (header + 3 rows + count), got %d", len(lines))
	}
}

// Tests for UNION
func TestUnion(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create two tables
	exec.Execute(&CreateTableStmt{
		TableName: "employees",
		Columns: []ColumnDef{
			{Name: "name", Type: "TEXT"},
		},
	})
	exec.Execute(&CreateTableStmt{
		TableName: "contractors",
		Columns: []ColumnDef{
			{Name: "name", Type: "TEXT"},
		},
	})

	// Insert data
	exec.Execute(&InsertStmt{TableName: "employees", Values: []string{"Alice"}})
	exec.Execute(&InsertStmt{TableName: "employees", Values: []string{"Bob"}})
	exec.Execute(&InsertStmt{TableName: "contractors", Values: []string{"Charlie"}})
	exec.Execute(&InsertStmt{TableName: "contractors", Values: []string{"Bob"}}) // Duplicate

	// UNION (removes duplicates)
	result, err := exec.Execute(&UnionStmt{
		Left:  &SelectStmt{TableName: "employees", Columns: []string{"name"}},
		Right: &SelectStmt{TableName: "contractors", Columns: []string{"name"}},
		All:   false,
	})
	if err != nil {
		t.Fatalf("UNION failed: %v", err)
	}

	// Should have 3 unique names (Bob appears once)
	if !strings.Contains(result, "(3 rows)") {
		t.Errorf("Expected 3 rows in UNION result, got: %s", result)
	}

	// UNION ALL (keeps duplicates)
	result, err = exec.Execute(&UnionStmt{
		Left:  &SelectStmt{TableName: "employees", Columns: []string{"name"}},
		Right: &SelectStmt{TableName: "contractors", Columns: []string{"name"}},
		All:   true,
	})
	if err != nil {
		t.Fatalf("UNION ALL failed: %v", err)
	}

	// Should have 4 names (Bob appears twice)
	if !strings.Contains(result, "(4 rows)") {
		t.Errorf("Expected 4 rows in UNION ALL result, got: %s", result)
	}
}

// Tests for CHECK Constraint
func TestCheckConstraint(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create table with CHECK constraint
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "products",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "price", Type: "INT", Constraints: []ColumnConstraint{
				{Type: ConstraintCheck, CheckExpr: &CheckExpr{
					Column:   "price",
					Operator: ">",
					Value:    "0",
				}},
			}},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE with CHECK failed: %v", err)
	}

	// Insert valid value
	result, err := exec.Execute(&InsertStmt{
		TableName: "products",
		Values:    []string{"1", "100"},
	})
	if err != nil {
		t.Fatalf("INSERT with valid CHECK value failed: %v", err)
	}
	if result != "INSERT 1" {
		t.Errorf("Expected 'INSERT 1', got '%s'", result)
	}

	// Insert invalid value (price = 0)
	_, err = exec.Execute(&InsertStmt{
		TableName: "products",
		Values:    []string{"2", "0"},
	})
	if err == nil {
		t.Error("Expected CHECK constraint violation, got none")
	}
	if err != nil && !strings.Contains(err.Error(), "CHECK constraint violation") {
		t.Errorf("Expected CHECK constraint violation error, got: %v", err)
	}

	// Insert invalid value (price = -10)
	_, err = exec.Execute(&InsertStmt{
		TableName: "products",
		Values:    []string{"3", "-10"},
	})
	if err == nil {
		t.Error("Expected CHECK constraint violation for negative price, got none")
	}
}

// Tests for Stored Procedures
func TestStoredProcedures(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a table first
	exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
			{Name: "status", Type: "TEXT"},
		},
	})

	// Insert some data
	exec.Execute(&InsertStmt{TableName: "users", Values: []string{"1", "Alice", "active"}})
	exec.Execute(&InsertStmt{TableName: "users", Values: []string{"2", "Bob", "inactive"}})

	// Create a stored procedure
	result, err := exec.Execute(&CreateProcedureStmt{
		Name: "update_status",
		Parameters: []ProcedureParam{
			{Name: "user_id", Type: "INT"},
			{Name: "new_status", Type: "TEXT"},
		},
		BodySQL: []string{"UPDATE users SET status = '$2' WHERE id = $1"},
	})
	if err != nil {
		t.Fatalf("CREATE PROCEDURE failed: %v", err)
	}
	if result != "CREATE PROCEDURE OK" {
		t.Errorf("Expected 'CREATE PROCEDURE OK', got '%s'", result)
	}

	// Call the procedure
	result, err = exec.Execute(&CallStmt{
		ProcedureName: "update_status",
		Arguments:     []string{"1", "suspended"},
	})
	if err != nil {
		t.Fatalf("CALL failed: %v", err)
	}
	if !strings.Contains(result, "UPDATE 1") {
		t.Errorf("Expected 'UPDATE 1' in result, got '%s'", result)
	}

	// Verify the update
	result, err = exec.Execute(&SelectStmt{
		TableName: "users",
		Columns:   []string{"status"},
		Where:     &Condition{Column: "id", Value: "1"},
	})
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if !strings.Contains(result, "suspended") {
		t.Errorf("Expected 'suspended' in result, got '%s'", result)
	}

	// Drop the procedure
	result, err = exec.Execute(&DropProcedureStmt{Name: "update_status"})
	if err != nil {
		t.Fatalf("DROP PROCEDURE failed: %v", err)
	}
	if result != "DROP PROCEDURE OK" {
		t.Errorf("Expected 'DROP PROCEDURE OK', got '%s'", result)
	}

	// Verify procedure is gone
	_, err = exec.Execute(&CallStmt{
		ProcedureName: "update_status",
		Arguments:     []string{"1", "active"},
	})
	if err == nil {
		t.Error("Expected error calling dropped procedure, got none")
	}
}

// Tests for Subqueries
func TestSubqueryInWhere(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create tables
	exec.Execute(&CreateTableStmt{
		TableName: "orders",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "user_id", Type: "INT"},
			{Name: "amount", Type: "INT"},
		},
	})
	exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
		},
	})

	// Insert data
	exec.Execute(&InsertStmt{TableName: "users", Values: []string{"1", "Alice"}})
	exec.Execute(&InsertStmt{TableName: "users", Values: []string{"2", "Bob"}})
	exec.Execute(&InsertStmt{TableName: "orders", Values: []string{"1", "1", "100"}})
	exec.Execute(&InsertStmt{TableName: "orders", Values: []string{"2", "1", "200"}})
	exec.Execute(&InsertStmt{TableName: "orders", Values: []string{"3", "2", "150"}})

	// Subquery with IN
	result, err := exec.Execute(&SelectStmt{
		TableName: "orders",
		Columns:   []string{"id", "amount"},
		WhereExt: &WhereClause{
			Column:     "user_id",
			Operator:   "IN",
			IsSubquery: true,
			Subquery: &SelectStmt{
				TableName: "users",
				Columns:   []string{"id"},
				Where:     &Condition{Column: "name", Value: "Alice"},
			},
		},
	})
	if err != nil {
		t.Fatalf("SELECT with subquery failed: %v", err)
	}

	// Should return Alice's orders (2 rows)
	if !strings.Contains(result, "(2 rows)") {
		t.Errorf("Expected 2 rows for Alice's orders, got: %s", result)
	}
}

func TestAlterTableAddColumn(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert a row
	_, err = exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"1", "Alice"},
	})
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Add a new column
	result, err := exec.Execute(&AlterTableStmt{
		TableName: "users",
		Action:    AlterActionAddColumn,
		ColumnDef: &ColumnDef{
			Name: "email",
			Type: "TEXT",
		},
	})
	if err != nil {
		t.Fatalf("ALTER TABLE ADD COLUMN failed: %v", err)
	}
	if result != "ALTER TABLE OK" {
		t.Errorf("Expected 'ALTER TABLE OK', got: %s", result)
	}

	// Verify the column was added to the schema
	schema, ok := exec.catalog.GetTable("users")
	if !ok {
		t.Fatal("Table not found after ALTER")
	}
	if len(schema.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(schema.Columns))
	}
	if schema.Columns[2].Name != "email" {
		t.Errorf("Expected new column 'email', got '%s'", schema.Columns[2].Name)
	}

	// Verify existing rows have the new column
	selectResult, err := exec.Execute(&SelectStmt{
		TableName: "users",
		Columns:   []string{"id", "name", "email"},
	})
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if !strings.Contains(selectResult, "1, Alice,") {
		t.Errorf("Expected row with empty email, got: %s", selectResult)
	}
}

func TestAlterTableDropColumn(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
			{Name: "email", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert a row
	_, err = exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"1", "Alice", "alice@example.com"},
	})
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Drop a column
	result, err := exec.Execute(&AlterTableStmt{
		TableName:  "users",
		Action:     AlterActionDropColumn,
		ColumnName: "email",
	})
	if err != nil {
		t.Fatalf("ALTER TABLE DROP COLUMN failed: %v", err)
	}
	if result != "ALTER TABLE OK" {
		t.Errorf("Expected 'ALTER TABLE OK', got: %s", result)
	}

	// Verify the column was removed from the schema
	schema, ok := exec.catalog.GetTable("users")
	if !ok {
		t.Fatal("Table not found after ALTER")
	}
	if len(schema.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(schema.Columns))
	}

	// Verify existing rows don't have the dropped column
	selectResult, err := exec.Execute(&SelectStmt{
		TableName: "users",
		Columns:   []string{"id", "name"},
	})
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if !strings.Contains(selectResult, "1, Alice") {
		t.Errorf("Expected row without email, got: %s", selectResult)
	}
}

func TestAlterTableRenameColumn(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert a row
	_, err = exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"1", "Alice"},
	})
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Rename a column
	result, err := exec.Execute(&AlterTableStmt{
		TableName:     "users",
		Action:        AlterActionRenameColumn,
		ColumnName:    "name",
		NewColumnName: "full_name",
	})
	if err != nil {
		t.Fatalf("ALTER TABLE RENAME COLUMN failed: %v", err)
	}
	if result != "ALTER TABLE OK" {
		t.Errorf("Expected 'ALTER TABLE OK', got: %s", result)
	}

	// Verify the column was renamed in the schema
	schema, ok := exec.catalog.GetTable("users")
	if !ok {
		t.Fatal("Table not found after ALTER")
	}
	if schema.Columns[1].Name != "full_name" {
		t.Errorf("Expected column 'full_name', got '%s'", schema.Columns[1].Name)
	}

	// Verify existing rows have the renamed column
	selectResult, err := exec.Execute(&SelectStmt{
		TableName: "users",
		Columns:   []string{"id", "full_name"},
	})
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if !strings.Contains(selectResult, "1, Alice") {
		t.Errorf("Expected row with full_name, got: %s", selectResult)
	}
}

func TestAlterTableModifyColumn(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "age", Type: "INT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Modify a column type
	result, err := exec.Execute(&AlterTableStmt{
		TableName:     "users",
		Action:        AlterActionModifyColumn,
		ColumnName:    "age",
		NewColumnType: "BIGINT",
	})
	if err != nil {
		t.Fatalf("ALTER TABLE MODIFY COLUMN failed: %v", err)
	}
	if result != "ALTER TABLE OK" {
		t.Errorf("Expected 'ALTER TABLE OK', got: %s", result)
	}

	// Verify the column type was changed in the schema
	schema, ok := exec.catalog.GetTable("users")
	if !ok {
		t.Fatal("Table not found after ALTER")
	}
	if schema.Columns[1].Type != "BIGINT" {
		t.Errorf("Expected column type 'BIGINT', got '%s'", schema.Columns[1].Type)
	}
}

func TestAlterTableDropPrimaryKeyColumn(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a table with primary key
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT", Constraints: []ColumnConstraint{{Type: ConstraintPrimaryKey}}},
			{Name: "name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Try to drop the primary key column - should fail
	_, err = exec.Execute(&AlterTableStmt{
		TableName:  "users",
		Action:     AlterActionDropColumn,
		ColumnName: "id",
	})
	if err == nil {
		t.Error("Expected error when dropping primary key column, got nil")
	}
	if !strings.Contains(err.Error(), "primary key") {
		t.Errorf("Expected primary key error, got: %v", err)
	}
}

func TestCreateView(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
			{Name: "status", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert some data
	_, err = exec.Execute(&InsertStmt{TableName: "users", Values: []string{"1", "Alice", "active"}})
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute(&InsertStmt{TableName: "users", Values: []string{"2", "Bob", "inactive"}})
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute(&InsertStmt{TableName: "users", Values: []string{"3", "Charlie", "active"}})
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Create a view
	result, err := exec.Execute(&CreateViewStmt{
		ViewName: "active_users",
		Query: &SelectStmt{
			TableName: "users",
			Columns:   []string{"id", "name"},
			Where:     &Condition{Column: "status", Value: "active"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE VIEW failed: %v", err)
	}
	if result != "CREATE VIEW OK" {
		t.Errorf("Expected 'CREATE VIEW OK', got: %s", result)
	}

	// Verify the view was created
	view, ok := exec.catalog.GetView("active_users")
	if !ok {
		t.Fatal("View not found after CREATE VIEW")
	}
	if view.Name != "active_users" {
		t.Errorf("Expected view name 'active_users', got '%s'", view.Name)
	}
}

func TestQueryView(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
			{Name: "status", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert some data
	_, err = exec.Execute(&InsertStmt{TableName: "users", Values: []string{"1", "Alice", "active"}})
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute(&InsertStmt{TableName: "users", Values: []string{"2", "Bob", "inactive"}})
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute(&InsertStmt{TableName: "users", Values: []string{"3", "Charlie", "active"}})
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Create a view
	_, err = exec.Execute(&CreateViewStmt{
		ViewName: "active_users",
		Query: &SelectStmt{
			TableName: "users",
			Columns:   []string{"id", "name"},
			Where:     &Condition{Column: "status", Value: "active"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE VIEW failed: %v", err)
	}

	// Query the view
	result, err := exec.Execute(&SelectStmt{
		TableName: "active_users",
		Columns:   []string{"*"},
	})
	if err != nil {
		t.Fatalf("SELECT from view failed: %v", err)
	}

	// Should return 2 rows (Alice and Charlie)
	if !strings.Contains(result, "(2 rows)") {
		t.Errorf("Expected 2 rows, got: %s", result)
	}
	if !strings.Contains(result, "Alice") {
		t.Errorf("Expected Alice in results, got: %s", result)
	}
	if !strings.Contains(result, "Charlie") {
		t.Errorf("Expected Charlie in results, got: %s", result)
	}
	if strings.Contains(result, "Bob") {
		t.Errorf("Did not expect Bob in results, got: %s", result)
	}
}

func TestDropView(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create a view
	_, err = exec.Execute(&CreateViewStmt{
		ViewName: "all_users",
		Query: &SelectStmt{
			TableName: "users",
			Columns:   []string{"*"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE VIEW failed: %v", err)
	}

	// Drop the view
	result, err := exec.Execute(&DropViewStmt{ViewName: "all_users"})
	if err != nil {
		t.Fatalf("DROP VIEW failed: %v", err)
	}
	if result != "DROP VIEW OK" {
		t.Errorf("Expected 'DROP VIEW OK', got: %s", result)
	}

	// Verify the view was dropped
	_, ok := exec.catalog.GetView("all_users")
	if ok {
		t.Error("View should not exist after DROP VIEW")
	}
}

func TestViewDuplicateName(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create a view
	_, err = exec.Execute(&CreateViewStmt{
		ViewName: "my_view",
		Query: &SelectStmt{
			TableName: "users",
			Columns:   []string{"*"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE VIEW failed: %v", err)
	}

	// Try to create another view with the same name - should fail
	_, err = exec.Execute(&CreateViewStmt{
		ViewName: "my_view",
		Query: &SelectStmt{
			TableName: "users",
			Columns:   []string{"*"},
		},
	})
	if err == nil {
		t.Error("Expected error when creating duplicate view, got nil")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("Expected 'already exists' error, got: %v", err)
	}
}

// ============================================================================
// Trigger Tests
// ============================================================================

func TestTriggerCreateAndDrop(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create an audit log table
	_, err = exec.Execute(&CreateTableStmt{
		TableName: "audit_log",
		Columns: []ColumnDef{
			{Name: "action", Type: "TEXT"},
			{Name: "table_name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE audit_log failed: %v", err)
	}

	// Create a trigger
	result, err := exec.Execute(&CreateTriggerStmt{
		TriggerName: "log_insert",
		Timing:      TriggerTimingAfter,
		Event:       TriggerEventInsert,
		TableName:   "users",
		ActionSQL:   "INSERT INTO audit_log VALUES ( 'insert' , 'users' )",
	})
	if err != nil {
		t.Fatalf("CREATE TRIGGER failed: %v", err)
	}
	if result != "CREATE TRIGGER OK" {
		t.Errorf("Expected 'CREATE TRIGGER OK', got '%s'", result)
	}

	// Drop the trigger
	result, err = exec.Execute(&DropTriggerStmt{
		TriggerName: "log_insert",
		TableName:   "users",
	})
	if err != nil {
		t.Fatalf("DROP TRIGGER failed: %v", err)
	}
	if result != "DROP TRIGGER OK" {
		t.Errorf("Expected 'DROP TRIGGER OK', got '%s'", result)
	}
}

func TestTriggerDuplicateError(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create audit_log table
	_, err = exec.Execute(&CreateTableStmt{
		TableName: "audit_log",
		Columns: []ColumnDef{
			{Name: "action", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE audit_log failed: %v", err)
	}

	// Create a trigger
	_, err = exec.Execute(&CreateTriggerStmt{
		TriggerName: "log_insert",
		Timing:      TriggerTimingAfter,
		Event:       TriggerEventInsert,
		TableName:   "users",
		ActionSQL:   "INSERT INTO audit_log VALUES ( 'insert' )",
	})
	if err != nil {
		t.Fatalf("CREATE TRIGGER failed: %v", err)
	}

	// Try to create the same trigger again - should fail
	_, err = exec.Execute(&CreateTriggerStmt{
		TriggerName: "log_insert",
		Timing:      TriggerTimingAfter,
		Event:       TriggerEventInsert,
		TableName:   "users",
		ActionSQL:   "INSERT INTO audit_log VALUES ( 'insert' )",
	})
	if err == nil {
		t.Error("Expected error when creating duplicate trigger, got nil")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("Expected 'already exists' error, got: %v", err)
	}
}

func TestTriggerDropNonExistent(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Try to drop a non-existent trigger
	_, err = exec.Execute(&DropTriggerStmt{
		TriggerName: "nonexistent",
		TableName:   "users",
	})
	if err == nil {
		t.Error("Expected error when dropping non-existent trigger, got nil")
	}
	if !strings.Contains(err.Error(), "not found") && !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("Expected 'not found' or 'does not exist' error, got: %v", err)
	}
}

func TestTriggerOnNonExistentTable(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Try to create a trigger on a non-existent table
	_, err := exec.Execute(&CreateTriggerStmt{
		TriggerName: "log_insert",
		Timing:      TriggerTimingAfter,
		Event:       TriggerEventInsert,
		TableName:   "nonexistent",
		ActionSQL:   "INSERT INTO audit_log VALUES ( 'insert' )",
	})
	if err == nil {
		t.Error("Expected error when creating trigger on non-existent table, got nil")
	}
	if !strings.Contains(err.Error(), "table not found") {
		t.Errorf("Expected 'table not found' error, got: %v", err)
	}
}

func TestTriggerAfterInsert(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create users table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	// Create audit_log table
	_, err = exec.Execute(&CreateTableStmt{
		TableName: "audit_log",
		Columns: []ColumnDef{
			{Name: "action", Type: "TEXT"},
			{Name: "table_name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE audit_log failed: %v", err)
	}

	// Create AFTER INSERT trigger
	_, err = exec.Execute(&CreateTriggerStmt{
		TriggerName: "log_insert",
		Timing:      TriggerTimingAfter,
		Event:       TriggerEventInsert,
		TableName:   "users",
		ActionSQL:   "INSERT INTO audit_log VALUES ( 'insert' , 'users' )",
	})
	if err != nil {
		t.Fatalf("CREATE TRIGGER failed: %v", err)
	}

	// Insert a row into users - should trigger the audit log insert
	_, err = exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"1", "Alice"},
	})
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Check that the audit log has an entry
	result, err := exec.Execute(&SelectStmt{
		TableName: "audit_log",
		Columns:   []string{"action", "table_name"},
	})
	if err != nil {
		t.Fatalf("SELECT from audit_log failed: %v", err)
	}
	if !strings.Contains(result, "insert") || !strings.Contains(result, "users") {
		t.Errorf("Expected audit log entry with 'insert' and 'users', got: %s", result)
	}
}

func TestTriggerAfterUpdate(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create users table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	// Create audit_log table
	_, err = exec.Execute(&CreateTableStmt{
		TableName: "audit_log",
		Columns: []ColumnDef{
			{Name: "action", Type: "TEXT"},
			{Name: "table_name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE audit_log failed: %v", err)
	}

	// Insert a row first
	_, err = exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"1", "Alice"},
	})
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Create AFTER UPDATE trigger
	_, err = exec.Execute(&CreateTriggerStmt{
		TriggerName: "log_update",
		Timing:      TriggerTimingAfter,
		Event:       TriggerEventUpdate,
		TableName:   "users",
		ActionSQL:   "INSERT INTO audit_log VALUES ( 'update' , 'users' )",
	})
	if err != nil {
		t.Fatalf("CREATE TRIGGER failed: %v", err)
	}

	// Update the row - should trigger the audit log insert
	_, err = exec.Execute(&UpdateStmt{
		TableName: "users",
		Updates:   map[string]string{"name": "Bob"},
		Where:     &Condition{Column: "id", Value: "1"},
	})
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}

	// Check that the audit log has an entry
	result, err := exec.Execute(&SelectStmt{
		TableName: "audit_log",
		Columns:   []string{"action", "table_name"},
	})
	if err != nil {
		t.Fatalf("SELECT from audit_log failed: %v", err)
	}
	if !strings.Contains(result, "update") || !strings.Contains(result, "users") {
		t.Errorf("Expected audit log entry with 'update' and 'users', got: %s", result)
	}
}

func TestTriggerAfterDelete(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create users table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	// Create audit_log table
	_, err = exec.Execute(&CreateTableStmt{
		TableName: "audit_log",
		Columns: []ColumnDef{
			{Name: "action", Type: "TEXT"},
			{Name: "table_name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE audit_log failed: %v", err)
	}

	// Insert a row first
	_, err = exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"1", "Alice"},
	})
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Create AFTER DELETE trigger
	_, err = exec.Execute(&CreateTriggerStmt{
		TriggerName: "log_delete",
		Timing:      TriggerTimingAfter,
		Event:       TriggerEventDelete,
		TableName:   "users",
		ActionSQL:   "INSERT INTO audit_log VALUES ( 'delete' , 'users' )",
	})
	if err != nil {
		t.Fatalf("CREATE TRIGGER failed: %v", err)
	}

	// Delete the row - should trigger the audit log insert
	_, err = exec.Execute(&DeleteStmt{
		TableName: "users",
		Where:     &Condition{Column: "id", Value: "1"},
	})
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	// Check that the audit log has an entry
	result, err := exec.Execute(&SelectStmt{
		TableName: "audit_log",
		Columns:   []string{"action", "table_name"},
	})
	if err != nil {
		t.Fatalf("SELECT from audit_log failed: %v", err)
	}
	if !strings.Contains(result, "delete") || !strings.Contains(result, "users") {
		t.Errorf("Expected audit log entry with 'delete' and 'users', got: %s", result)
	}
}

func TestTriggerBeforeInsert(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create users table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	// Create audit_log table
	_, err = exec.Execute(&CreateTableStmt{
		TableName: "audit_log",
		Columns: []ColumnDef{
			{Name: "action", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE audit_log failed: %v", err)
	}

	// Create BEFORE INSERT trigger
	_, err = exec.Execute(&CreateTriggerStmt{
		TriggerName: "before_insert",
		Timing:      TriggerTimingBefore,
		Event:       TriggerEventInsert,
		TableName:   "users",
		ActionSQL:   "INSERT INTO audit_log VALUES ( 'before_insert' )",
	})
	if err != nil {
		t.Fatalf("CREATE TRIGGER failed: %v", err)
	}

	// Insert a row into users - should trigger the before insert
	_, err = exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"1", "Alice"},
	})
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Check that the audit log has an entry
	result, err := exec.Execute(&SelectStmt{
		TableName: "audit_log",
		Columns:   []string{"action"},
	})
	if err != nil {
		t.Fatalf("SELECT from audit_log failed: %v", err)
	}
	if !strings.Contains(result, "before_insert") {
		t.Errorf("Expected audit log entry with 'before_insert', got: %s", result)
	}
}

func TestMultipleTriggers(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create users table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	// Create audit_log table
	_, err = exec.Execute(&CreateTableStmt{
		TableName: "audit_log",
		Columns: []ColumnDef{
			{Name: "action", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE audit_log failed: %v", err)
	}

	// Create BEFORE INSERT trigger
	_, err = exec.Execute(&CreateTriggerStmt{
		TriggerName: "before_insert",
		Timing:      TriggerTimingBefore,
		Event:       TriggerEventInsert,
		TableName:   "users",
		ActionSQL:   "INSERT INTO audit_log VALUES ( 'before' )",
	})
	if err != nil {
		t.Fatalf("CREATE TRIGGER before_insert failed: %v", err)
	}

	// Create AFTER INSERT trigger
	_, err = exec.Execute(&CreateTriggerStmt{
		TriggerName: "after_insert",
		Timing:      TriggerTimingAfter,
		Event:       TriggerEventInsert,
		TableName:   "users",
		ActionSQL:   "INSERT INTO audit_log VALUES ( 'after' )",
	})
	if err != nil {
		t.Fatalf("CREATE TRIGGER after_insert failed: %v", err)
	}

	// Insert a row into users - should trigger both
	_, err = exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"1", "Alice"},
	})
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Check that the audit log has both entries
	result, err := exec.Execute(&SelectStmt{
		TableName: "audit_log",
		Columns:   []string{"action"},
	})
	if err != nil {
		t.Fatalf("SELECT from audit_log failed: %v", err)
	}
	if !strings.Contains(result, "before") {
		t.Errorf("Expected audit log entry with 'before', got: %s", result)
	}
	if !strings.Contains(result, "after") {
		t.Errorf("Expected audit log entry with 'after', got: %s", result)
	}
}

func TestDropTable(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert some data
	_, err = exec.Execute(&InsertStmt{
		TableName: "users",
		Values:    []string{"1", "Alice"},
	})
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Drop the table
	result, err := exec.Execute(&DropTableStmt{TableName: "users"})
	if err != nil {
		t.Fatalf("DROP TABLE failed: %v", err)
	}
	if result != "DROP TABLE OK" {
		t.Errorf("Expected 'DROP TABLE OK', got '%s'", result)
	}

	// Verify table no longer exists
	_, err = exec.Execute(&SelectStmt{
		TableName: "users",
		Columns:   []string{"*"},
	})
	if err == nil {
		t.Error("Expected error when selecting from dropped table")
	}
}

func TestDropTableIfExists(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Drop a non-existent table with IF EXISTS - should succeed
	result, err := exec.Execute(&DropTableStmt{TableName: "nonexistent", IfExists: true})
	if err != nil {
		t.Fatalf("DROP TABLE IF EXISTS failed: %v", err)
	}
	if result != "DROP TABLE OK" {
		t.Errorf("Expected 'DROP TABLE OK', got '%s'", result)
	}

	// Drop a non-existent table without IF EXISTS - should fail
	_, err = exec.Execute(&DropTableStmt{TableName: "nonexistent", IfExists: false})
	if err == nil {
		t.Error("Expected error when dropping non-existent table without IF EXISTS")
	}
}

func TestDropTableWithForeignKeyReference(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create parent table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT", Constraints: []ColumnConstraint{{Type: ConstraintPrimaryKey}}},
			{Name: "name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	// Create child table with foreign key
	_, err = exec.Execute(&CreateTableStmt{
		TableName: "orders",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "user_id", Type: "INT", Constraints: []ColumnConstraint{
				{Type: ConstraintForeignKey, ForeignKey: &ForeignKeyRef{Table: "users", Column: "id"}},
			}},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	// Try to drop the parent table - should fail
	_, err = exec.Execute(&DropTableStmt{TableName: "users"})
	if err == nil {
		t.Error("Expected error when dropping table with foreign key reference")
	}
	if !strings.Contains(err.Error(), "referenced by foreign key") {
		t.Errorf("Expected foreign key error, got: %v", err)
	}

	// Drop the child table first
	_, err = exec.Execute(&DropTableStmt{TableName: "orders"})
	if err != nil {
		t.Fatalf("DROP TABLE orders failed: %v", err)
	}

	// Now drop the parent table - should succeed
	_, err = exec.Execute(&DropTableStmt{TableName: "users"})
	if err != nil {
		t.Fatalf("DROP TABLE users failed: %v", err)
	}
}

func TestTruncateTable(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a table
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "logs",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "message", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert some data
	exec.Execute(&InsertStmt{TableName: "logs", Values: []string{"1", "Log 1"}})
	exec.Execute(&InsertStmt{TableName: "logs", Values: []string{"2", "Log 2"}})
	exec.Execute(&InsertStmt{TableName: "logs", Values: []string{"3", "Log 3"}})

	// Verify data exists
	result, _ := exec.Execute(&SelectStmt{TableName: "logs", Columns: []string{"*"}})
	if !strings.Contains(result, "3 rows") {
		t.Errorf("Expected 3 rows before truncate, got: %s", result)
	}

	// Truncate the table
	result, err = exec.Execute(&TruncateTableStmt{TableName: "logs"})
	if err != nil {
		t.Fatalf("TRUNCATE TABLE failed: %v", err)
	}
	if result != "TRUNCATE TABLE OK" {
		t.Errorf("Expected 'TRUNCATE TABLE OK', got '%s'", result)
	}

	// Verify table is empty but still exists
	result, err = exec.Execute(&SelectStmt{TableName: "logs", Columns: []string{"*"}})
	if err != nil {
		t.Fatalf("SELECT after TRUNCATE failed: %v", err)
	}
	if !strings.Contains(result, "0 rows") {
		t.Errorf("Expected 0 rows after truncate, got: %s", result)
	}
}

func TestTruncateTableNonExistent(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Try to truncate a non-existent table
	_, err := exec.Execute(&TruncateTableStmt{TableName: "nonexistent"})
	if err == nil {
		t.Error("Expected error when truncating non-existent table")
	}
	if !strings.Contains(err.Error(), "table not found") {
		t.Errorf("Expected 'table not found' error, got: %v", err)
	}
}

func TestTruncateTableResetsSequence(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a table with SERIAL column
	_, err := exec.Execute(&CreateTableStmt{
		TableName: "items",
		Columns: []ColumnDef{
			{Name: "id", Type: "SERIAL"},
			{Name: "name", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert some data (auto-increment should assign ids 1, 2, 3)
	exec.Execute(&InsertStmt{TableName: "items", Values: []string{"Item 1"}})
	exec.Execute(&InsertStmt{TableName: "items", Values: []string{"Item 2"}})
	exec.Execute(&InsertStmt{TableName: "items", Values: []string{"Item 3"}})

	// Truncate the table
	_, err = exec.Execute(&TruncateTableStmt{TableName: "items"})
	if err != nil {
		t.Fatalf("TRUNCATE TABLE failed: %v", err)
	}

	// Insert new data - sequence should restart from 1
	exec.Execute(&InsertStmt{TableName: "items", Values: []string{"New Item"}})

	result, _ := exec.Execute(&SelectStmt{TableName: "items", Columns: []string{"id", "name"}})
	if !strings.Contains(result, "1") {
		t.Errorf("Expected id to restart from 1 after truncate, got: %s", result)
	}
}

func TestDropTablePermissionDenied(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a table as admin
	exec.Execute(&CreateTableStmt{
		TableName: "secret",
		Columns:   []ColumnDef{{Name: "data", Type: "TEXT"}},
	})

	// Create a non-admin user
	exec.Execute(&CreateUserStmt{Username: "alice", Password: "pass"})

	// Set current user to non-admin
	exec.SetUser("alice")

	// Try to drop table - should fail
	_, err := exec.Execute(&DropTableStmt{TableName: "secret"})
	if err == nil {
		t.Error("Expected permission denied error")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("Expected 'permission denied' error, got: %v", err)
	}
}

func TestTruncateTablePermissionDenied(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a table as admin
	exec.Execute(&CreateTableStmt{
		TableName: "logs",
		Columns:   []ColumnDef{{Name: "data", Type: "TEXT"}},
	})

	// Create a non-admin user
	exec.Execute(&CreateUserStmt{Username: "alice", Password: "pass"})

	// Set current user to non-admin
	exec.SetUser("alice")

	// Try to truncate table - should fail
	_, err := exec.Execute(&TruncateTableStmt{TableName: "logs"})
	if err == nil {
		t.Error("Expected permission denied error")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("Expected 'permission denied' error, got: %v", err)
	}
}

func TestExecutorAlterUser(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a user
	result, err := exec.Execute(&CreateUserStmt{Username: "testuser", Password: "oldpass"})
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}
	if result != "CREATE USER OK" {
		t.Errorf("Expected 'CREATE USER OK', got '%s'", result)
	}

	// Alter the user's password
	result, err = exec.Execute(&AlterUserStmt{Username: "testuser", NewPassword: "newpass"})
	if err != nil {
		t.Fatalf("AlterUser failed: %v", err)
	}
	if result != "ALTER USER OK" {
		t.Errorf("Expected 'ALTER USER OK', got '%s'", result)
	}
}

func TestExecutorAlterUserNonExistent(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Try to alter a non-existent user
	_, err := exec.Execute(&AlterUserStmt{Username: "nonexistent", NewPassword: "newpass"})
	if err == nil {
		t.Error("Expected error when altering non-existent user")
	}
	if !strings.Contains(err.Error(), "user does not exist") {
		t.Errorf("Expected 'user does not exist' error, got: %v", err)
	}
}

func TestExecutorAlterUserPermissionDenied(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a user
	exec.Execute(&CreateUserStmt{Username: "alice", Password: "pass"})

	// Set current user to non-admin
	exec.SetUser("alice")

	// Try to alter user - should fail
	_, err := exec.Execute(&AlterUserStmt{Username: "alice", NewPassword: "newpass"})
	if err == nil {
		t.Error("Expected permission denied error")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("Expected 'permission denied' error, got: %v", err)
	}
}

func TestExecutorRevoke(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a table
	exec.Execute(&CreateTableStmt{
		TableName: "products",
		Columns:   []ColumnDef{{Name: "id", Type: "INT"}, {Name: "name", Type: "TEXT"}},
	})

	// Create a user
	exec.Execute(&CreateUserStmt{Username: "bob", Password: "pass"})

	// Grant permission
	result, err := exec.Execute(&GrantStmt{TableName: "products", Username: "bob"})
	if err != nil {
		t.Fatalf("Grant failed: %v", err)
	}
	if result != "GRANT OK" {
		t.Errorf("Expected 'GRANT OK', got '%s'", result)
	}

	// Revoke permission
	result, err = exec.Execute(&RevokeStmt{TableName: "products", Username: "bob"})
	if err != nil {
		t.Fatalf("Revoke failed: %v", err)
	}
	if result != "REVOKE OK" {
		t.Errorf("Expected 'REVOKE OK', got '%s'", result)
	}
}

func TestExecutorRevokeNonExistentPermission(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a user but don't grant any permissions
	exec.Execute(&CreateUserStmt{Username: "charlie", Password: "pass"})

	// Try to revoke a permission that doesn't exist
	_, err := exec.Execute(&RevokeStmt{TableName: "products", Username: "charlie"})
	if err == nil {
		t.Error("Expected error when revoking non-existent permission")
	}
	if !strings.Contains(err.Error(), "permission does not exist") {
		t.Errorf("Expected 'permission does not exist' error, got: %v", err)
	}
}

func TestExecutorRevokePermissionDenied(t *testing.T) {
	exec, cleanup := setupExecutorTest(t)
	defer cleanup()

	// Create a user
	exec.Execute(&CreateUserStmt{Username: "alice", Password: "pass"})

	// Set current user to non-admin
	exec.SetUser("alice")

	// Try to revoke - should fail
	_, err := exec.Execute(&RevokeStmt{TableName: "products", Username: "alice"})
	if err == nil {
		t.Error("Expected permission denied error")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("Expected 'permission denied' error, got: %v", err)
	}
}