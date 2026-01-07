package sql

import (
	"testing"
)

func TestParseUpdate(t *testing.T) {
	input := "UPDATE products SET price=1200 WHERE id=1"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	updateStmt, ok := stmt.(*UpdateStmt)
	if !ok {
		t.Fatalf("Expected UpdateStmt, got %T", stmt)
	}

	if updateStmt.TableName != "products" {
		t.Errorf("Expected table products, got %s", updateStmt.TableName)
	}

	if val, ok := updateStmt.Updates["price"]; !ok || val != "1200" {
		t.Errorf("Expected price=1200, got %v=%v", "price", val)
	}

	if updateStmt.Where == nil {
		t.Fatal("Expected Where clause")
	}

	if updateStmt.Where.Column != "id" || updateStmt.Where.Value != "1" {
		t.Errorf("Expected WHERE id=1, got %s=%s", updateStmt.Where.Column, updateStmt.Where.Value)
	}
}

func TestParseBegin(t *testing.T) {
	tests := []string{
		"BEGIN",
		"BEGIN TRANSACTION",
	}

	for _, input := range tests {
		lexer := NewLexer(input)
		parser := NewParser(lexer)
		stmt, err := parser.Parse()
		if err != nil {
			t.Fatalf("Parse failed for '%s': %v", input, err)
		}

		_, ok := stmt.(*BeginStmt)
		if !ok {
			t.Fatalf("Expected BeginStmt for '%s', got %T", input, stmt)
		}
	}
}

func TestParseCommit(t *testing.T) {
	input := "COMMIT"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	_, ok := stmt.(*CommitStmt)
	if !ok {
		t.Fatalf("Expected CommitStmt, got %T", stmt)
	}
}

func TestParseRollback(t *testing.T) {
	input := "ROLLBACK"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	_, ok := stmt.(*RollbackStmt)
	if !ok {
		t.Fatalf("Expected RollbackStmt, got %T", stmt)
	}
}

func TestParseCreateIndex(t *testing.T) {
	input := "CREATE INDEX idx_users_email ON users (email)"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createIndexStmt, ok := stmt.(*CreateIndexStmt)
	if !ok {
		t.Fatalf("Expected CreateIndexStmt, got %T", stmt)
	}

	if createIndexStmt.IndexName != "idx_users_email" {
		t.Errorf("Expected index name idx_users_email, got %s", createIndexStmt.IndexName)
	}

	if createIndexStmt.TableName != "users" {
		t.Errorf("Expected table name users, got %s", createIndexStmt.TableName)
	}

	if createIndexStmt.ColumnName != "email" {
		t.Errorf("Expected column name email, got %s", createIndexStmt.ColumnName)
	}
}

func TestParsePrepare(t *testing.T) {
	input := "PREPARE get_user AS SELECT * FROM users WHERE id = $1"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	prepareStmt, ok := stmt.(*PrepareStmt)
	if !ok {
		t.Fatalf("Expected PrepareStmt, got %T", stmt)
	}

	if prepareStmt.Name != "get_user" {
		t.Errorf("Expected name get_user, got %s", prepareStmt.Name)
	}

	if prepareStmt.Query != "SELECT * FROM users WHERE id = $1" {
		t.Errorf("Expected query 'SELECT * FROM users WHERE id = $1', got '%s'", prepareStmt.Query)
	}
}

func TestParseExecute(t *testing.T) {
	tests := []struct {
		input      string
		name       string
		paramCount int
	}{
		{"EXECUTE get_user USING 42", "get_user", 1},
		{"EXECUTE insert_user USING 1, 'Alice', 'alice@example.com'", "insert_user", 3},
		{"EXECUTE simple_query", "simple_query", 0},
	}

	for _, tt := range tests {
		lexer := NewLexer(tt.input)
		parser := NewParser(lexer)
		stmt, err := parser.Parse()
		if err != nil {
			t.Fatalf("Parse failed for '%s': %v", tt.input, err)
		}

		execStmt, ok := stmt.(*ExecuteStmt)
		if !ok {
			t.Fatalf("Expected ExecuteStmt for '%s', got %T", tt.input, stmt)
		}

		if execStmt.Name != tt.name {
			t.Errorf("Expected name %s, got %s", tt.name, execStmt.Name)
		}

		if len(execStmt.Params) != tt.paramCount {
			t.Errorf("Expected %d params, got %d", tt.paramCount, len(execStmt.Params))
		}
	}
}

func TestParseDeallocate(t *testing.T) {
	input := "DEALLOCATE get_user"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	deallocStmt, ok := stmt.(*DeallocateStmt)
	if !ok {
		t.Fatalf("Expected DeallocateStmt, got %T", stmt)
	}

	if deallocStmt.Name != "get_user" {
		t.Errorf("Expected name get_user, got %s", deallocStmt.Name)
	}
}

func TestParseCreateTableWithExtendedTypes(t *testing.T) {
	tests := []struct {
		input    string
		colTypes []string
	}{
		{
			"CREATE TABLE test (id INT, name TEXT, active BOOLEAN)",
			[]string{"INT", "TEXT", "BOOLEAN"},
		},
		{
			"CREATE TABLE metrics (id INT, value FLOAT, created TIMESTAMP)",
			[]string{"INT", "FLOAT", "TIMESTAMP"},
		},
		{
			"CREATE TABLE events (id UUID, data JSONB, date DATE)",
			[]string{"UUID", "JSONB", "DATE"},
		},
		{
			"CREATE TABLE files (id INT, content BLOB)",
			[]string{"INT", "BLOB"},
		},
	}

	for _, tt := range tests {
		lexer := NewLexer(tt.input)
		parser := NewParser(lexer)
		stmt, err := parser.Parse()
		if err != nil {
			t.Fatalf("Parse failed for '%s': %v", tt.input, err)
		}

		createStmt, ok := stmt.(*CreateTableStmt)
		if !ok {
			t.Fatalf("Expected CreateTableStmt for '%s', got %T", tt.input, stmt)
		}

		if len(createStmt.Columns) != len(tt.colTypes) {
			t.Errorf("Expected %d columns, got %d", len(tt.colTypes), len(createStmt.Columns))
			continue
		}

		for i, expectedType := range tt.colTypes {
			if createStmt.Columns[i].Type != expectedType {
				t.Errorf("Expected column %d type %s, got %s", i, expectedType, createStmt.Columns[i].Type)
			}
		}
	}
}


func TestParseSelectWithAggregates(t *testing.T) {
	tests := []struct {
		input     string
		aggCount  int
		functions []string
		columns   []string
	}{
		{
			input:     "SELECT COUNT(*) FROM users",
			aggCount:  1,
			functions: []string{"COUNT"},
			columns:   []string{"*"},
		},
		{
			input:     "SELECT SUM(amount) FROM orders",
			aggCount:  1,
			functions: []string{"SUM"},
			columns:   []string{"amount"},
		},
		{
			input:     "SELECT AVG(price) FROM products",
			aggCount:  1,
			functions: []string{"AVG"},
			columns:   []string{"price"},
		},
		{
			input:     "SELECT MIN(price), MAX(price) FROM products",
			aggCount:  2,
			functions: []string{"MIN", "MAX"},
			columns:   []string{"price", "price"},
		},
		{
			input:     "SELECT COUNT(*), SUM(amount), AVG(amount) FROM orders",
			aggCount:  3,
			functions: []string{"COUNT", "SUM", "AVG"},
			columns:   []string{"*", "amount", "amount"},
		},
	}

	for _, tt := range tests {
		lexer := NewLexer(tt.input)
		parser := NewParser(lexer)
		stmt, err := parser.Parse()
		if err != nil {
			t.Fatalf("Parse failed for '%s': %v", tt.input, err)
		}

		selectStmt, ok := stmt.(*SelectStmt)
		if !ok {
			t.Fatalf("Expected SelectStmt, got %T", stmt)
		}

		if len(selectStmt.Aggregates) != tt.aggCount {
			t.Errorf("Expected %d aggregates, got %d for '%s'",
				tt.aggCount, len(selectStmt.Aggregates), tt.input)
			continue
		}

		for i, agg := range selectStmt.Aggregates {
			if agg.Function != tt.functions[i] {
				t.Errorf("Expected function %s, got %s", tt.functions[i], agg.Function)
			}
			if agg.Column != tt.columns[i] {
				t.Errorf("Expected column %s, got %s", tt.columns[i], agg.Column)
			}
		}
	}
}

func TestParseSelectWithAggregatesAndWhere(t *testing.T) {
	input := "SELECT COUNT(*) FROM orders WHERE status = 'completed'"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", stmt)
	}

	if len(selectStmt.Aggregates) != 1 {
		t.Fatalf("Expected 1 aggregate, got %d", len(selectStmt.Aggregates))
	}

	if selectStmt.Aggregates[0].Function != "COUNT" {
		t.Errorf("Expected COUNT, got %s", selectStmt.Aggregates[0].Function)
	}

	if selectStmt.Where == nil {
		t.Fatal("Expected WHERE clause")
	}

	if selectStmt.Where.Column != "status" {
		t.Errorf("Expected WHERE column 'status', got '%s'", selectStmt.Where.Column)
	}
}

func TestParseSelectWithGroupBy(t *testing.T) {
	input := "SELECT category, COUNT(*) FROM products GROUP BY category"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", stmt)
	}

	if selectStmt.TableName != "products" {
		t.Errorf("Expected table 'products', got '%s'", selectStmt.TableName)
	}

	if len(selectStmt.Columns) != 1 || selectStmt.Columns[0] != "category" {
		t.Errorf("Expected column 'category', got %v", selectStmt.Columns)
	}

	if len(selectStmt.Aggregates) != 1 {
		t.Fatalf("Expected 1 aggregate, got %d", len(selectStmt.Aggregates))
	}

	if selectStmt.Aggregates[0].Function != "COUNT" {
		t.Errorf("Expected COUNT, got %s", selectStmt.Aggregates[0].Function)
	}

	if len(selectStmt.GroupBy) != 1 || selectStmt.GroupBy[0] != "category" {
		t.Errorf("Expected GROUP BY 'category', got %v", selectStmt.GroupBy)
	}
}

func TestParseSelectWithGroupByMultipleColumns(t *testing.T) {
	input := "SELECT category, region, SUM(amount) FROM sales GROUP BY category, region"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", stmt)
	}

	if len(selectStmt.GroupBy) != 2 {
		t.Fatalf("Expected 2 GROUP BY columns, got %d", len(selectStmt.GroupBy))
	}

	if selectStmt.GroupBy[0] != "category" || selectStmt.GroupBy[1] != "region" {
		t.Errorf("Expected GROUP BY 'category, region', got %v", selectStmt.GroupBy)
	}
}

func TestParseSelectWithHaving(t *testing.T) {
	input := "SELECT category, COUNT(*) FROM products GROUP BY category HAVING COUNT(*) > 5"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", stmt)
	}

	if selectStmt.Having == nil {
		t.Fatal("Expected HAVING clause")
	}

	if selectStmt.Having.Aggregate.Function != "COUNT" {
		t.Errorf("Expected HAVING COUNT, got %s", selectStmt.Having.Aggregate.Function)
	}

	if selectStmt.Having.Operator != ">" {
		t.Errorf("Expected operator '>', got '%s'", selectStmt.Having.Operator)
	}

	if selectStmt.Having.Value != "5" {
		t.Errorf("Expected value '5', got '%s'", selectStmt.Having.Value)
	}
}

func TestParseSelectWithHavingLessEqual(t *testing.T) {
	input := "SELECT category, SUM(price) FROM products GROUP BY category HAVING SUM(price) <= 1000"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", stmt)
	}

	if selectStmt.Having == nil {
		t.Fatal("Expected HAVING clause")
	}

	if selectStmt.Having.Operator != "<=" {
		t.Errorf("Expected operator '<=', got '%s'", selectStmt.Having.Operator)
	}
}


// ============================================================================
// Constraint Parsing Tests
// ============================================================================

func TestParseCreateTableWithPrimaryKey(t *testing.T) {
	input := "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("Expected CreateTableStmt, got %T", stmt)
	}

	if createStmt.TableName != "users" {
		t.Errorf("Expected table users, got %s", createStmt.TableName)
	}

	if len(createStmt.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(createStmt.Columns))
	}

	// Check id column has PRIMARY KEY constraint
	idCol := createStmt.Columns[0]
	if !idCol.IsPrimaryKey() {
		t.Error("Expected id column to have PRIMARY KEY constraint")
	}
}

func TestParseCreateTableWithNotNull(t *testing.T) {
	input := "CREATE TABLE users (id INT, name TEXT NOT NULL)"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("Expected CreateTableStmt, got %T", stmt)
	}

	// Check name column has NOT NULL constraint
	nameCol := createStmt.Columns[1]
	if !nameCol.IsNotNull() {
		t.Error("Expected name column to have NOT NULL constraint")
	}
}

func TestParseCreateTableWithUnique(t *testing.T) {
	input := "CREATE TABLE users (id INT, email TEXT UNIQUE)"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("Expected CreateTableStmt, got %T", stmt)
	}

	// Check email column has UNIQUE constraint
	emailCol := createStmt.Columns[1]
	if !emailCol.IsUnique() {
		t.Error("Expected email column to have UNIQUE constraint")
	}
}

func TestParseCreateTableWithForeignKey(t *testing.T) {
	input := "CREATE TABLE orders (id INT, user_id INT REFERENCES users(id))"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("Expected CreateTableStmt, got %T", stmt)
	}

	// Check user_id column has FOREIGN KEY constraint
	userIdCol := createStmt.Columns[1]
	fk := userIdCol.GetForeignKey()
	if fk == nil {
		t.Fatal("Expected user_id column to have FOREIGN KEY constraint")
	}

	if fk.Table != "users" {
		t.Errorf("Expected FK to reference users table, got %s", fk.Table)
	}

	if fk.Column != "id" {
		t.Errorf("Expected FK to reference id column, got %s", fk.Column)
	}
}

func TestParseCreateTableWithMultipleConstraints(t *testing.T) {
	input := "CREATE TABLE users (id INT PRIMARY KEY, email TEXT NOT NULL UNIQUE)"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("Expected CreateTableStmt, got %T", stmt)
	}

	// Check id column
	idCol := createStmt.Columns[0]
	if !idCol.IsPrimaryKey() {
		t.Error("Expected id column to have PRIMARY KEY constraint")
	}

	// Check email column has both NOT NULL and UNIQUE
	emailCol := createStmt.Columns[1]
	if !emailCol.IsNotNull() {
		t.Error("Expected email column to have NOT NULL constraint")
	}
	if !emailCol.IsUnique() {
		t.Error("Expected email column to have UNIQUE constraint")
	}
}

func TestParseCreateTableWithSerial(t *testing.T) {
	input := "CREATE TABLE items (id SERIAL, name TEXT)"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("Expected CreateTableStmt, got %T", stmt)
	}

	// Check id column is SERIAL (auto-increment)
	idCol := createStmt.Columns[0]
	if idCol.Type != "SERIAL" {
		t.Errorf("Expected id column type SERIAL, got %s", idCol.Type)
	}
	if !idCol.IsAutoIncrement() {
		t.Error("Expected id column to be auto-increment")
	}
}

func TestParseCreateTableWithDefault(t *testing.T) {
	input := "CREATE TABLE settings (key TEXT, value TEXT DEFAULT 'default')"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("Expected CreateTableStmt, got %T", stmt)
	}

	// Check value column has DEFAULT constraint
	valueCol := createStmt.Columns[1]
	defaultVal, hasDefault := valueCol.GetDefaultValue()
	if !hasDefault {
		t.Fatal("Expected value column to have DEFAULT constraint")
	}
	if defaultVal != "default" {
		t.Errorf("Expected default value 'default', got '%s'", defaultVal)
	}
}

func TestParseSelectDistinct(t *testing.T) {
	input := "SELECT DISTINCT category FROM products"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", stmt)
	}

	if !selectStmt.Distinct {
		t.Error("Expected Distinct to be true")
	}
	if selectStmt.TableName != "products" {
		t.Errorf("Expected table products, got %s", selectStmt.TableName)
	}
	if len(selectStmt.Columns) != 1 || selectStmt.Columns[0] != "category" {
		t.Errorf("Expected column 'category', got %v", selectStmt.Columns)
	}
}

func TestParseUnion(t *testing.T) {
	input := "SELECT name FROM employees UNION SELECT name FROM contractors"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	unionStmt, ok := stmt.(*UnionStmt)
	if !ok {
		t.Fatalf("Expected UnionStmt, got %T", stmt)
	}

	if unionStmt.All {
		t.Error("Expected All to be false for UNION")
	}
	if unionStmt.Left.TableName != "employees" {
		t.Errorf("Expected left table 'employees', got %s", unionStmt.Left.TableName)
	}
	if unionStmt.Right.TableName != "contractors" {
		t.Errorf("Expected right table 'contractors', got %s", unionStmt.Right.TableName)
	}
}

func TestParseUnionAll(t *testing.T) {
	input := "SELECT name FROM employees UNION ALL SELECT name FROM contractors"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	unionStmt, ok := stmt.(*UnionStmt)
	if !ok {
		t.Fatalf("Expected UnionStmt, got %T", stmt)
	}

	if !unionStmt.All {
		t.Error("Expected All to be true for UNION ALL")
	}
}

func TestParseCheckConstraint(t *testing.T) {
	input := "CREATE TABLE products (id INT, price INT CHECK (price > 0))"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("Expected CreateTableStmt, got %T", stmt)
	}

	priceCol := createStmt.Columns[1]
	checkExpr := priceCol.GetCheckConstraint()
	if checkExpr == nil {
		t.Fatal("Expected CHECK constraint on price column")
	}
	if checkExpr.Column != "price" {
		t.Errorf("Expected check column 'price', got '%s'", checkExpr.Column)
	}
	if checkExpr.Operator != ">" {
		t.Errorf("Expected operator '>', got '%s'", checkExpr.Operator)
	}
	if checkExpr.Value != "0" {
		t.Errorf("Expected value '0', got '%s'", checkExpr.Value)
	}
}

func TestParseCreateProcedure(t *testing.T) {
	input := "CREATE PROCEDURE update_status(user_id INT, status TEXT) BEGIN UPDATE users SET status = $2 WHERE id = $1; END"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	procStmt, ok := stmt.(*CreateProcedureStmt)
	if !ok {
		t.Fatalf("Expected CreateProcedureStmt, got %T", stmt)
	}

	if procStmt.Name != "update_status" {
		t.Errorf("Expected procedure name 'update_status', got '%s'", procStmt.Name)
	}
	if len(procStmt.Parameters) != 2 {
		t.Errorf("Expected 2 parameters, got %d", len(procStmt.Parameters))
	}
	if procStmt.Parameters[0].Name != "user_id" || procStmt.Parameters[0].Type != "INT" {
		t.Errorf("Expected first param 'user_id INT', got '%s %s'", procStmt.Parameters[0].Name, procStmt.Parameters[0].Type)
	}
	if len(procStmt.BodySQL) == 0 {
		t.Error("Expected at least one SQL statement in body")
	}
}

func TestParseCall(t *testing.T) {
	input := "CALL update_status(1, 'active')"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	callStmt, ok := stmt.(*CallStmt)
	if !ok {
		t.Fatalf("Expected CallStmt, got %T", stmt)
	}

	if callStmt.ProcedureName != "update_status" {
		t.Errorf("Expected procedure name 'update_status', got '%s'", callStmt.ProcedureName)
	}
	if len(callStmt.Arguments) != 2 {
		t.Errorf("Expected 2 arguments, got %d", len(callStmt.Arguments))
	}
	if callStmt.Arguments[0] != "1" {
		t.Errorf("Expected first arg '1', got '%s'", callStmt.Arguments[0])
	}
	if callStmt.Arguments[1] != "active" {
		t.Errorf("Expected second arg 'active', got '%s'", callStmt.Arguments[1])
	}
}

func TestParseDropProcedure(t *testing.T) {
	input := "DROP PROCEDURE update_status"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	dropStmt, ok := stmt.(*DropProcedureStmt)
	if !ok {
		t.Fatalf("Expected DropProcedureStmt, got %T", stmt)
	}

	if dropStmt.Name != "update_status" {
		t.Errorf("Expected procedure name 'update_status', got '%s'", dropStmt.Name)
	}
}

func TestParseAlterTableAddColumn(t *testing.T) {
	input := "ALTER TABLE users ADD COLUMN email TEXT NOT NULL"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	alterStmt, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("Expected AlterTableStmt, got %T", stmt)
	}

	if alterStmt.TableName != "users" {
		t.Errorf("Expected table 'users', got '%s'", alterStmt.TableName)
	}

	if alterStmt.Action != AlterActionAddColumn {
		t.Errorf("Expected action ADD COLUMN, got '%s'", alterStmt.Action)
	}

	if alterStmt.ColumnDef == nil {
		t.Fatal("Expected ColumnDef to be set")
	}

	if alterStmt.ColumnDef.Name != "email" {
		t.Errorf("Expected column name 'email', got '%s'", alterStmt.ColumnDef.Name)
	}

	if alterStmt.ColumnDef.Type != "TEXT" {
		t.Errorf("Expected column type 'TEXT', got '%s'", alterStmt.ColumnDef.Type)
	}

	if !alterStmt.ColumnDef.IsNotNull() {
		t.Error("Expected NOT NULL constraint")
	}
}

func TestParseAlterTableDropColumn(t *testing.T) {
	input := "ALTER TABLE users DROP COLUMN email"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	alterStmt, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("Expected AlterTableStmt, got %T", stmt)
	}

	if alterStmt.TableName != "users" {
		t.Errorf("Expected table 'users', got '%s'", alterStmt.TableName)
	}

	if alterStmt.Action != AlterActionDropColumn {
		t.Errorf("Expected action DROP COLUMN, got '%s'", alterStmt.Action)
	}

	if alterStmt.ColumnName != "email" {
		t.Errorf("Expected column name 'email', got '%s'", alterStmt.ColumnName)
	}
}

func TestParseAlterTableRenameColumn(t *testing.T) {
	input := "ALTER TABLE users RENAME COLUMN email TO email_address"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	alterStmt, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("Expected AlterTableStmt, got %T", stmt)
	}

	if alterStmt.TableName != "users" {
		t.Errorf("Expected table 'users', got '%s'", alterStmt.TableName)
	}

	if alterStmt.Action != AlterActionRenameColumn {
		t.Errorf("Expected action RENAME COLUMN, got '%s'", alterStmt.Action)
	}

	if alterStmt.ColumnName != "email" {
		t.Errorf("Expected old column name 'email', got '%s'", alterStmt.ColumnName)
	}

	if alterStmt.NewColumnName != "email_address" {
		t.Errorf("Expected new column name 'email_address', got '%s'", alterStmt.NewColumnName)
	}
}

func TestParseAlterTableModifyColumn(t *testing.T) {
	input := "ALTER TABLE users MODIFY COLUMN age BIGINT"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	alterStmt, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("Expected AlterTableStmt, got %T", stmt)
	}

	if alterStmt.TableName != "users" {
		t.Errorf("Expected table 'users', got '%s'", alterStmt.TableName)
	}

	if alterStmt.Action != AlterActionModifyColumn {
		t.Errorf("Expected action MODIFY COLUMN, got '%s'", alterStmt.Action)
	}

	if alterStmt.ColumnName != "age" {
		t.Errorf("Expected column name 'age', got '%s'", alterStmt.ColumnName)
	}

	if alterStmt.NewColumnType != "BIGINT" {
		t.Errorf("Expected new column type 'BIGINT', got '%s'", alterStmt.NewColumnType)
	}
}

func TestParseCreateView(t *testing.T) {
	input := "CREATE VIEW active_users AS SELECT id, name FROM users WHERE status = 'active'"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	viewStmt, ok := stmt.(*CreateViewStmt)
	if !ok {
		t.Fatalf("Expected CreateViewStmt, got %T", stmt)
	}

	if viewStmt.ViewName != "active_users" {
		t.Errorf("Expected view name 'active_users', got '%s'", viewStmt.ViewName)
	}

	if viewStmt.Query == nil {
		t.Fatal("Expected Query to be set")
	}

	if viewStmt.Query.TableName != "users" {
		t.Errorf("Expected table 'users', got '%s'", viewStmt.Query.TableName)
	}

	if len(viewStmt.Query.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(viewStmt.Query.Columns))
	}
}

func TestParseDropView(t *testing.T) {
	input := "DROP VIEW active_users"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	dropStmt, ok := stmt.(*DropViewStmt)
	if !ok {
		t.Fatalf("Expected DropViewStmt, got %T", stmt)
	}

	if dropStmt.ViewName != "active_users" {
		t.Errorf("Expected view name 'active_users', got '%s'", dropStmt.ViewName)
	}
}

// ============================================================================
// Trigger Parser Tests
// ============================================================================

func TestParseCreateTriggerAfterInsert(t *testing.T) {
	input := "CREATE TRIGGER log_insert AFTER INSERT ON users FOR EACH ROW EXECUTE INSERT INTO audit_log VALUES ( 'insert' , 'users' )"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	triggerStmt, ok := stmt.(*CreateTriggerStmt)
	if !ok {
		t.Fatalf("Expected CreateTriggerStmt, got %T", stmt)
	}

	if triggerStmt.TriggerName != "log_insert" {
		t.Errorf("Expected trigger name 'log_insert', got '%s'", triggerStmt.TriggerName)
	}

	if triggerStmt.Timing != TriggerTimingAfter {
		t.Errorf("Expected timing AFTER, got '%s'", triggerStmt.Timing)
	}

	if triggerStmt.Event != TriggerEventInsert {
		t.Errorf("Expected event INSERT, got '%s'", triggerStmt.Event)
	}

	if triggerStmt.TableName != "users" {
		t.Errorf("Expected table 'users', got '%s'", triggerStmt.TableName)
	}

	if triggerStmt.ActionSQL == "" {
		t.Error("Expected ActionSQL to be set")
	}
}

func TestParseCreateTriggerBeforeUpdate(t *testing.T) {
	input := "CREATE TRIGGER validate_update BEFORE UPDATE ON products FOR EACH ROW EXECUTE SELECT validate_product ( )"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	triggerStmt, ok := stmt.(*CreateTriggerStmt)
	if !ok {
		t.Fatalf("Expected CreateTriggerStmt, got %T", stmt)
	}

	if triggerStmt.TriggerName != "validate_update" {
		t.Errorf("Expected trigger name 'validate_update', got '%s'", triggerStmt.TriggerName)
	}

	if triggerStmt.Timing != TriggerTimingBefore {
		t.Errorf("Expected timing BEFORE, got '%s'", triggerStmt.Timing)
	}

	if triggerStmt.Event != TriggerEventUpdate {
		t.Errorf("Expected event UPDATE, got '%s'", triggerStmt.Event)
	}

	if triggerStmt.TableName != "products" {
		t.Errorf("Expected table 'products', got '%s'", triggerStmt.TableName)
	}
}

func TestParseCreateTriggerAfterDelete(t *testing.T) {
	input := "CREATE TRIGGER log_delete AFTER DELETE ON orders FOR EACH ROW EXECUTE INSERT INTO audit VALUES ( 'deleted' )"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	triggerStmt, ok := stmt.(*CreateTriggerStmt)
	if !ok {
		t.Fatalf("Expected CreateTriggerStmt, got %T", stmt)
	}

	if triggerStmt.Event != TriggerEventDelete {
		t.Errorf("Expected event DELETE, got '%s'", triggerStmt.Event)
	}
}

func TestParseDropTrigger(t *testing.T) {
	input := "DROP TRIGGER log_insert ON users"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	dropStmt, ok := stmt.(*DropTriggerStmt)
	if !ok {
		t.Fatalf("Expected DropTriggerStmt, got %T", stmt)
	}

	if dropStmt.TriggerName != "log_insert" {
		t.Errorf("Expected trigger name 'log_insert', got '%s'", dropStmt.TriggerName)
	}

	if dropStmt.TableName != "users" {
		t.Errorf("Expected table 'users', got '%s'", dropStmt.TableName)
	}
}

func TestParseDropTable(t *testing.T) {
	input := "DROP TABLE users"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	dropStmt, ok := stmt.(*DropTableStmt)
	if !ok {
		t.Fatalf("Expected DropTableStmt, got %T", stmt)
	}

	if dropStmt.TableName != "users" {
		t.Errorf("Expected table name 'users', got '%s'", dropStmt.TableName)
	}

	if dropStmt.IfExists {
		t.Error("Expected IfExists to be false")
	}
}

func TestParseDropTableIfExists(t *testing.T) {
	input := "DROP TABLE IF EXISTS temp_data"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	dropStmt, ok := stmt.(*DropTableStmt)
	if !ok {
		t.Fatalf("Expected DropTableStmt, got %T", stmt)
	}

	if dropStmt.TableName != "temp_data" {
		t.Errorf("Expected table name 'temp_data', got '%s'", dropStmt.TableName)
	}

	if !dropStmt.IfExists {
		t.Error("Expected IfExists to be true")
	}
}

func TestParseTruncateTable(t *testing.T) {
	input := "TRUNCATE TABLE logs"
	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	truncateStmt, ok := stmt.(*TruncateTableStmt)
	if !ok {
		t.Fatalf("Expected TruncateTableStmt, got %T", stmt)
	}

	if truncateStmt.TableName != "logs" {
		t.Errorf("Expected table name 'logs', got '%s'", truncateStmt.TableName)
	}
}

func TestParseAlterUser(t *testing.T) {
	tests := []struct {
		input       string
		username    string
		newPassword string
	}{
		{"ALTER USER alice IDENTIFIED BY 'newpassword'", "alice", "newpassword"},
		{"ALTER USER admin IDENTIFIED BY 'secure123'", "admin", "secure123"},
		{"ALTER USER bob IDENTIFIED BY secret", "bob", "secret"},
	}

	for _, tt := range tests {
		lexer := NewLexer(tt.input)
		parser := NewParser(lexer)
		stmt, err := parser.Parse()
		if err != nil {
			t.Fatalf("Parse failed for '%s': %v", tt.input, err)
		}

		alterUserStmt, ok := stmt.(*AlterUserStmt)
		if !ok {
			t.Fatalf("Expected AlterUserStmt, got %T", stmt)
		}

		if alterUserStmt.Username != tt.username {
			t.Errorf("Expected username '%s', got '%s'", tt.username, alterUserStmt.Username)
		}

		if alterUserStmt.NewPassword != tt.newPassword {
			t.Errorf("Expected password '%s', got '%s'", tt.newPassword, alterUserStmt.NewPassword)
		}
	}
}

func TestParseRevoke(t *testing.T) {
	tests := []struct {
		input     string
		tableName string
		username  string
	}{
		{"REVOKE ON products FROM alice", "products", "alice"},
		{"REVOKE ON orders FROM bob", "orders", "bob"},
		{"REVOKE ON users FROM admin", "users", "admin"},
	}

	for _, tt := range tests {
		lexer := NewLexer(tt.input)
		parser := NewParser(lexer)
		stmt, err := parser.Parse()
		if err != nil {
			t.Fatalf("Parse failed for '%s': %v", tt.input, err)
		}

		revokeStmt, ok := stmt.(*RevokeStmt)
		if !ok {
			t.Fatalf("Expected RevokeStmt, got %T", stmt)
		}

		if revokeStmt.TableName != tt.tableName {
			t.Errorf("Expected table '%s', got '%s'", tt.tableName, revokeStmt.TableName)
		}

		if revokeStmt.Username != tt.username {
			t.Errorf("Expected username '%s', got '%s'", tt.username, revokeStmt.Username)
		}
	}
}