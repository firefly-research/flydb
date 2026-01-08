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
Package sql contains the Parser component for SQL syntax analysis.

Parser Overview:
================

The Parser is the second stage of the SQL processing pipeline. It takes
a stream of tokens from the Lexer and builds an Abstract Syntax Tree (AST)
that represents the structure of the SQL statement.

Parsing Technique:
==================

FlyDB uses a Recursive Descent Parser, which is a top-down parsing
technique where each grammar rule is implemented as a function.

The parser maintains two tokens:
  - cur: The current token being processed
  - peek: The next token (lookahead)

This lookahead allows the parser to make decisions based on what's
coming next without consuming the token.

Grammar (Simplified BNF):
=========================

	statement     := create_table | create_user | grant | insert
	              | update | delete | select

	create_table  := CREATE TABLE ident ( column_defs )
	column_defs   := column_def (, column_def)*
	column_def    := ident type

	create_user   := CREATE USER ident IDENTIFIED BY string

	grant         := GRANT [SELECT] ON ident [where_clause] TO ident

	insert        := INSERT INTO ident VALUES ( values )
	values        := value (, value)*

	update        := UPDATE ident SET assignments [where_clause]
	assignments   := assignment (, assignment)*
	assignment    := ident = value

	delete        := DELETE FROM ident [where_clause]

	select        := SELECT columns FROM ident [join_clause]
	                 [where_clause] [order_clause] [limit_clause]
	columns       := column (, column)*
	column        := ident | *

	join_clause   := JOIN ident ON condition
	where_clause  := WHERE condition
	order_clause  := ORDER BY ident [ASC|DESC]
	limit_clause  := LIMIT number

	condition     := ident = value

Error Handling:
===============

The parser returns descriptive error messages when it encounters
unexpected tokens. Each parsing function validates the expected
token types and returns an error if the input doesn't match.

Usage Example:
==============

	lexer := sql.NewLexer("SELECT name FROM users WHERE id = 1")
	parser := sql.NewParser(lexer)
	stmt, err := parser.Parse()
	if err != nil {
	    log.Fatal(err)
	}
	// stmt is now a *SelectStmt that can be executed
*/
package sql

import (
	"errors"
	"fmt"
	"strings"
)

// Parser transforms a stream of tokens into an Abstract Syntax Tree (AST).
// It uses a recursive descent parsing technique with one token of lookahead.
//
// The parser maintains two tokens:
//   - cur: The current token being examined
//   - peek: The next token (lookahead for decision making)
type Parser struct {
	lexer *Lexer // The lexer providing tokens
	cur   Token  // Current token
	peek  Token  // Next token (lookahead)
}

// NewParser creates a new Parser instance for the given Lexer.
// It initializes the parser by reading the first two tokens to
// populate both cur and peek.
//
// Parameters:
//   - lexer: The Lexer to read tokens from
//
// Returns a Parser ready to parse statements.
func NewParser(lexer *Lexer) *Parser {
	p := &Parser{lexer: lexer}
	// Read two tokens to initialize cur and peek.
	p.nextToken()
	p.nextToken()
	return p
}

// nextToken advances the parser to the next token.
// The current token becomes the previous peek, and a new token
// is read from the lexer into peek.
func (p *Parser) nextToken() {
	p.cur = p.peek
	p.peek = p.lexer.NextToken()
}

// Parse parses the input and returns the corresponding Statement AST.
// This is the main entry point for parsing SQL statements.
//
// The parser examines the first keyword to determine the statement type,
// then delegates to the appropriate parsing function.
//
// Supported statements:
//   - CREATE TABLE: Creates a new table
//   - CREATE USER: Creates a new user
//   - CREATE INDEX: Creates an index
//   - GRANT: Assigns permissions
//   - INSERT: Inserts a row
//   - UPDATE: Updates rows
//   - DELETE: Deletes rows
//   - SELECT: Queries data
//   - BEGIN: Starts a transaction
//   - COMMIT: Commits a transaction
//   - ROLLBACK: Rolls back a transaction
//
// Returns the parsed Statement AST, or an error if parsing fails.
func (p *Parser) Parse() (Statement, error) {
	if p.cur.Type == TokenKeyword {
		switch p.cur.Value {
		case "CREATE":
			// Distinguish between CREATE TABLE, CREATE USER, CREATE INDEX, CREATE PROCEDURE, CREATE VIEW, CREATE TRIGGER, CREATE DATABASE, and CREATE ROLE
			// by looking at the next token.
			if p.peek.Value == "TABLE" {
				return p.parseCreate()
			} else if p.peek.Value == "USER" {
				return p.parseCreateUser()
			} else if p.peek.Value == "INDEX" {
				return p.parseCreateIndex()
			} else if p.peek.Value == "PROCEDURE" {
				return p.parseCreateProcedure()
			} else if p.peek.Value == "VIEW" {
				return p.parseCreateView()
			} else if p.peek.Value == "TRIGGER" {
				return p.parseCreateTrigger()
			} else if p.peek.Value == "DATABASE" {
				return p.parseCreateDatabase()
			} else if p.peek.Value == "ROLE" {
				return p.parseCreateRole()
			}
			return nil, errors.New("expected TABLE, USER, INDEX, PROCEDURE, VIEW, TRIGGER, DATABASE, or ROLE after CREATE")
		case "CALL":
			return p.parseCall()
		case "DROP":
			return p.parseDrop()
		case "GRANT":
			return p.parseGrant()
		case "REVOKE":
			return p.parseRevoke()
		case "INSERT":
			return p.parseInsert()
		case "UPDATE":
			return p.parseUpdate()
		case "DELETE":
			return p.parseDelete()
		case "SELECT":
			return p.parseSelectOrUnion()
		case "BEGIN":
			return p.parseBegin()
		case "COMMIT":
			return &CommitStmt{}, nil
		case "ROLLBACK":
			return p.parseRollback()
		case "SAVEPOINT":
			return p.parseSavepoint()
		case "RELEASE":
			return p.parseReleaseSavepoint()
		case "PREPARE":
			return p.parsePrepare()
		case "EXECUTE":
			return p.parseExecute()
		case "DEALLOCATE":
			return p.parseDeallocate()
		case "INSPECT":
			return p.parseInspect()
		case "ALTER":
			return p.parseAlter()
		case "TRUNCATE":
			return p.parseTruncate()
		case "USE":
			return p.parseUse()
		}
	}
	return nil, errors.New("unexpected token: " + p.cur.Value)
}

// parseCreateUser parses a CREATE USER statement.
// Syntax: CREATE USER <username> IDENTIFIED BY '<password>'
//
// Example: CREATE USER alice IDENTIFIED BY 'secret123'
//
// Returns a CreateUserStmt AST node.
func (p *Parser) parseCreateUser() (*CreateUserStmt, error) {
	// Skip CREATE and USER keywords (already validated).
	p.nextToken() // Skip CREATE
	p.nextToken() // Skip USER

	// Parse the username.
	if p.cur.Type != TokenIdent {
		return nil, errors.New("expected username")
	}
	username := p.cur.Value

	// Expect IDENTIFIED BY keywords.
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "IDENTIFIED" {
		return nil, errors.New("expected IDENTIFIED")
	}
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "BY" {
		return nil, errors.New("expected BY")
	}

	// Parse the password (can be string or identifier).
	p.nextToken()
	if p.cur.Type != TokenString && p.cur.Type != TokenIdent {
		return nil, errors.New("expected password")
	}
	password := p.cur.Value

	return &CreateUserStmt{Username: username, Password: password}, nil
}

// parseCreateRole parses a CREATE ROLE statement.
// Syntax: CREATE ROLE <role_name> [WITH DESCRIPTION '<description>']
//
// Examples:
//   - CREATE ROLE analyst
//   - CREATE ROLE data_scientist WITH DESCRIPTION 'Data science team role'
//
// Returns a CreateRoleStmt AST node.
func (p *Parser) parseCreateRole() (*CreateRoleStmt, error) {
	// Skip CREATE and ROLE keywords
	p.nextToken() // Skip CREATE
	p.nextToken() // Skip ROLE

	// Parse the role name
	if p.cur.Type != TokenIdent && p.cur.Type != TokenKeyword {
		return nil, errors.New("expected role name")
	}
	roleName := p.cur.Value

	// Parse optional WITH DESCRIPTION clause
	var description string
	if p.peek.Type == TokenKeyword && p.peek.Value == "WITH" {
		p.nextToken() // Skip WITH
		if !p.expectPeek(TokenKeyword) || p.cur.Value != "DESCRIPTION" {
			return nil, errors.New("expected DESCRIPTION after WITH")
		}
		if !p.expectPeek(TokenString) {
			return nil, errors.New("expected description string")
		}
		description = p.cur.Value
	}

	return &CreateRoleStmt{RoleName: roleName, Description: description}, nil
}

// parseGrant parses a GRANT statement.
// Supports two syntaxes:
//
// 1. Privilege grant: GRANT [SELECT|INSERT|UPDATE|DELETE|ALL] ON <table> [WHERE <col>=<val>] TO <user>
// 2. Role grant: GRANT <role> TO <user> [ON DATABASE <database>]
//
// Examples:
//   - GRANT SELECT ON products TO alice
//   - GRANT ON orders WHERE user_id = 'alice' TO alice
//   - GRANT reader TO alice
//   - GRANT writer TO bob ON DATABASE sales
//
// Returns a GrantStmt or GrantRoleStmt AST node.
func (p *Parser) parseGrant() (Statement, error) {
	// Look ahead to determine if this is a privilege grant or role grant
	// If next token is ON, it's a privilege grant
	// If next token is ROLE, it's a role grant
	// If next token is a role name followed by TO, it's a role grant

	// Check for privilege keywords
	if p.peek.Type == TokenKeyword {
		switch p.peek.Value {
		case "SELECT", "INSERT", "UPDATE", "DELETE", "ALL":
			p.nextToken() // Skip the privilege keyword
			// Fall through to privilege grant parsing
		case "ON":
			// Privilege grant without explicit privilege (legacy syntax)
			// Fall through to privilege grant parsing
		case "ROLE":
			// Explicit ROLE keyword - skip it and parse role grant
			p.nextToken() // Skip ROLE keyword
			return p.parseGrantRole()
		default:
			// Could be a role name - check if followed by TO
			return p.parseGrantRole()
		}
	} else if p.peek.Type == TokenIdent {
		// Could be a role name - check if followed by TO
		return p.parseGrantRole()
	}

	// Parse privilege grant
	// Expect ON keyword.
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "ON" {
		return nil, errors.New("expected ON")
	}

	// Parse the table name.
	if !p.expectPeek(TokenIdent) {
		return nil, errors.New("expected table name")
	}
	tableName := p.cur.Value

	// Parse optional WHERE clause for RLS.
	var where *Condition
	if p.peek.Type == TokenKeyword && p.peek.Value == "WHERE" {
		p.nextToken() // Skip WHERE
		if !p.expectPeek(TokenIdent) {
			return nil, errors.New("expected column in WHERE")
		}
		col := p.cur.Value
		if !p.expectPeek(TokenEqual) {
			return nil, errors.New("expected =")
		}
		p.nextToken()
		val := p.cur.Value
		where = &Condition{Column: col, Value: val}
	}

	// Expect TO keyword and username.
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "TO" {
		return nil, errors.New("expected TO")
	}
	if !p.expectPeek(TokenIdent) {
		return nil, errors.New("expected username")
	}
	username := p.cur.Value

	return &GrantStmt{TableName: tableName, Username: username, Where: where}, nil
}

// parseGrantRole parses a GRANT role TO user statement.
// Syntax: GRANT [ROLE] <role> TO <user> [ON DATABASE <database>]
// Note: The ROLE keyword is optional and may have been consumed by parseGrant.
func (p *Parser) parseGrantRole() (*GrantRoleStmt, error) {
	// Parse role name - it should be in peek position
	if p.peek.Type != TokenIdent && p.peek.Type != TokenKeyword {
		return nil, errors.New("expected role name")
	}
	p.nextToken() // Move to role name
	roleName := p.cur.Value

	// Expect TO keyword
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "TO" {
		return nil, errors.New("expected TO after role name")
	}

	// Parse username
	if !p.expectPeek(TokenIdent) {
		return nil, errors.New("expected username after TO")
	}
	username := p.cur.Value

	// Parse optional ON DATABASE clause
	var database string
	if p.peek.Type == TokenKeyword && p.peek.Value == "ON" {
		p.nextToken() // Skip ON
		if !p.expectPeek(TokenKeyword) || p.cur.Value != "DATABASE" {
			return nil, errors.New("expected DATABASE after ON")
		}
		if !p.expectPeek(TokenIdent) {
			return nil, errors.New("expected database name")
		}
		database = p.cur.Value
	}

	return &GrantRoleStmt{RoleName: roleName, Username: username, Database: database}, nil
}

// parseRevoke parses a REVOKE statement.
// Supports two syntaxes:
//
// 1. Privilege revoke: REVOKE ON <table> FROM <user>
// 2. Role revoke: REVOKE <role> FROM <user> [ON DATABASE <database>]
//
// Examples:
//   - REVOKE ON products FROM alice
//   - REVOKE reader FROM alice
//   - REVOKE writer FROM bob ON DATABASE sales
//
// Returns a RevokeStmt or RevokeRoleStmt AST node.
func (p *Parser) parseRevoke() (Statement, error) {
	// Check if next token is ON (privilege revoke), ROLE (role revoke), or a role name
	if p.peek.Type == TokenKeyword {
		switch p.peek.Value {
		case "ON":
			// Privilege revoke
			p.nextToken() // Skip ON

			// Parse the table name
			if !p.expectPeek(TokenIdent) {
				return nil, errors.New("expected table name after ON")
			}
			tableName := p.cur.Value

			// Expect FROM keyword and username
			if !p.expectPeek(TokenKeyword) || p.cur.Value != "FROM" {
				return nil, errors.New("expected FROM after table name")
			}
			if !p.expectPeek(TokenIdent) {
				return nil, errors.New("expected username after FROM")
			}
			username := p.cur.Value

			return &RevokeStmt{TableName: tableName, Username: username}, nil
		case "ROLE":
			// Explicit ROLE keyword - skip it and parse role revoke
			p.nextToken() // Skip ROLE keyword
			return p.parseRevokeRole()
		}
	}

	// Role revoke (without explicit ROLE keyword)
	return p.parseRevokeRole()
}

// parseRevokeRole parses a REVOKE role FROM user statement.
// Syntax: REVOKE [ROLE] <role> FROM <user> [ON DATABASE <database>]
// Note: The ROLE keyword is optional and may have been consumed by parseRevoke.
func (p *Parser) parseRevokeRole() (*RevokeRoleStmt, error) {
	// Parse role name - it should be in peek position
	if p.peek.Type != TokenIdent && p.peek.Type != TokenKeyword {
		return nil, errors.New("expected role name")
	}
	p.nextToken() // Move to role name
	roleName := p.cur.Value

	// Expect FROM keyword
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "FROM" {
		return nil, errors.New("expected FROM after role name")
	}

	// Parse username
	if !p.expectPeek(TokenIdent) {
		return nil, errors.New("expected username after FROM")
	}
	username := p.cur.Value

	// Parse optional ON DATABASE clause
	var database string
	if p.peek.Type == TokenKeyword && p.peek.Value == "ON" {
		p.nextToken() // Skip ON
		if !p.expectPeek(TokenKeyword) || p.cur.Value != "DATABASE" {
			return nil, errors.New("expected DATABASE after ON")
		}
		if !p.expectPeek(TokenIdent) {
			return nil, errors.New("expected database name")
		}
		database = p.cur.Value
	}

	return &RevokeRoleStmt{RoleName: roleName, Username: username, Database: database}, nil
}

// parseCreate parses a CREATE TABLE statement.
// Syntax: CREATE TABLE <name> (
//
//	<col1> <type1> [constraints],
//	<col2> <type2> [constraints],
//	[table_constraints]
//
// )
//
// Examples:
//
//	CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)
//	CREATE TABLE orders (id SERIAL PRIMARY KEY, user_id INT REFERENCES users(id))
//
// Supported types: INT, TEXT, SERIAL, and others
// Supported constraints: PRIMARY KEY, FOREIGN KEY/REFERENCES, NOT NULL, UNIQUE, AUTO_INCREMENT, DEFAULT
//
// Returns a CreateTableStmt AST node.
func (p *Parser) parseCreate() (*CreateTableStmt, error) {
	// Expect TABLE keyword.
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "TABLE" {
		return nil, errors.New("expected TABLE")
	}

	// Parse optional IF NOT EXISTS clause.
	ifNotExists := false
	if p.peek.Type == TokenKeyword && p.peek.Value == "IF" {
		p.nextToken() // consume IF
		if !p.expectPeek(TokenKeyword) || p.cur.Value != "NOT" {
			return nil, errors.New("expected NOT after IF")
		}
		if !p.expectPeek(TokenKeyword) || p.cur.Value != "EXISTS" {
			return nil, errors.New("expected EXISTS after IF NOT")
		}
		ifNotExists = true
	}

	// Parse the table name.
	if !p.expectPeek(TokenIdent) {
		return nil, errors.New("expected table name")
	}
	stmt := &CreateTableStmt{TableName: p.cur.Value, IfNotExists: ifNotExists}

	// Expect opening parenthesis.
	if !p.expectPeek(TokenLParen) {
		return nil, errors.New("expected (")
	}

	// Parse column definitions and table constraints.
	for {
		// Check for table-level constraints (PRIMARY KEY, FOREIGN KEY, CONSTRAINT, UNIQUE)
		if p.peek.Type == TokenKeyword {
			switch p.peek.Value {
			case "PRIMARY", "FOREIGN", "CONSTRAINT", "UNIQUE", "CHECK":
				constraint, err := p.parseTableConstraint()
				if err != nil {
					return nil, err
				}
				stmt.Constraints = append(stmt.Constraints, *constraint)
				if p.peek.Type == TokenComma {
					p.nextToken()
					continue
				}
				break
			}
		}

		// Column names can be identifiers or keywords (e.g., "date", "timestamp")
		// since these are valid column names even though they're also type names.
		if p.peek.Type != TokenIdent && p.peek.Type != TokenKeyword {
			break // End of column definitions
		}

		// Check if this looks like a table constraint keyword
		if p.peek.Type == TokenKeyword {
			switch p.peek.Value {
			case "PRIMARY", "FOREIGN", "CONSTRAINT", "UNIQUE", "CHECK":
				constraint, err := p.parseTableConstraint()
				if err != nil {
					return nil, err
				}
				stmt.Constraints = append(stmt.Constraints, *constraint)
				if p.peek.Type == TokenComma {
					p.nextToken()
					continue
				}
				break
			}
		}

		p.nextToken()
		colName := p.cur.Value

		// Expect type (must be a keyword like INT, TEXT, DATE, etc.)
		if !p.expectPeek(TokenKeyword) {
			return nil, errors.New("expected column type")
		}
		colType := p.cur.Value

		// Parse column constraints
		constraints, err := p.parseColumnConstraints()
		if err != nil {
			return nil, err
		}

		stmt.Columns = append(stmt.Columns, ColumnDef{
			Name:        colName,
			Type:        colType,
			Constraints: constraints,
		})

		if p.peek.Type == TokenComma {
			p.nextToken()
		} else {
			break
		}
	}

	if !p.expectPeek(TokenRParen) {
		return nil, errors.New("expected )")
	}
	return stmt, nil
}

// parseColumnConstraints parses column-level constraints after the column type.
// Supported constraints: PRIMARY KEY, NOT NULL, UNIQUE, AUTO_INCREMENT, DEFAULT, REFERENCES
func (p *Parser) parseColumnConstraints() ([]ColumnConstraint, error) {
	var constraints []ColumnConstraint

	for p.peek.Type == TokenKeyword {
		switch p.peek.Value {
		case "PRIMARY":
			p.nextToken() // consume PRIMARY
			if !p.expectPeek(TokenKeyword) || p.cur.Value != "KEY" {
				return nil, errors.New("expected KEY after PRIMARY")
			}
			constraints = append(constraints, ColumnConstraint{Type: ConstraintPrimaryKey})

		case "NOT":
			p.nextToken() // consume NOT
			if !p.expectPeek(TokenKeyword) || p.cur.Value != "NULL" {
				return nil, errors.New("expected NULL after NOT")
			}
			constraints = append(constraints, ColumnConstraint{Type: ConstraintNotNull})

		case "UNIQUE":
			p.nextToken() // consume UNIQUE
			constraints = append(constraints, ColumnConstraint{Type: ConstraintUnique})

		case "AUTO_INCREMENT":
			p.nextToken() // consume AUTO_INCREMENT
			constraints = append(constraints, ColumnConstraint{Type: ConstraintAutoIncrement})

		case "DEFAULT":
			p.nextToken() // consume DEFAULT
			p.nextToken() // get the default value
			if p.cur.Type != TokenString && p.cur.Type != TokenNumber && p.cur.Type != TokenKeyword {
				return nil, errors.New("expected default value after DEFAULT")
			}
			constraints = append(constraints, ColumnConstraint{
				Type:         ConstraintDefault,
				DefaultValue: p.cur.Value,
			})

		case "REFERENCES":
			p.nextToken() // consume REFERENCES
			// Parse referenced table name
			if !p.expectPeek(TokenIdent) {
				return nil, errors.New("expected table name after REFERENCES")
			}
			refTable := p.cur.Value

			// Expect (column)
			if !p.expectPeek(TokenLParen) {
				return nil, errors.New("expected ( after table name in REFERENCES")
			}
			if !p.expectPeek(TokenIdent) {
				return nil, errors.New("expected column name in REFERENCES")
			}
			refColumn := p.cur.Value
			if !p.expectPeek(TokenRParen) {
				return nil, errors.New("expected ) after column name in REFERENCES")
			}

			constraints = append(constraints, ColumnConstraint{
				Type: ConstraintForeignKey,
				ForeignKey: &ForeignKeyRef{
					Table:  refTable,
					Column: refColumn,
				},
			})

		case "CHECK":
			p.nextToken() // consume CHECK
			checkExpr, err := p.parseCheckExpression()
			if err != nil {
				return nil, err
			}
			constraints = append(constraints, ColumnConstraint{
				Type:      ConstraintCheck,
				CheckExpr: checkExpr,
			})

		default:
			// Not a constraint keyword, stop parsing constraints
			return constraints, nil
		}
	}

	return constraints, nil
}

// parseCheckExpression parses a CHECK constraint expression.
// Syntax: CHECK (<column> <operator> <value>)
//
//	CHECK (<column> IN (<value1>, <value2>, ...))
//	CHECK (<column> BETWEEN <min> AND <max>)
func (p *Parser) parseCheckExpression() (*CheckExpr, error) {
	if !p.expectPeek(TokenLParen) {
		return nil, errors.New("expected ( after CHECK")
	}

	// Parse the column name
	if !p.expectPeek(TokenIdent) {
		return nil, errors.New("expected column name in CHECK")
	}
	column := p.cur.Value

	checkExpr := &CheckExpr{Column: column}

	// Parse the operator
	switch p.peek.Type {
	case TokenEqual:
		checkExpr.Operator = "="
		p.nextToken()
	case TokenLessThan:
		p.nextToken()
		if p.peek.Type == TokenEqual {
			p.nextToken()
			checkExpr.Operator = "<="
		} else if p.peek.Type == TokenGreaterThan {
			p.nextToken()
			checkExpr.Operator = "<>"
		} else {
			checkExpr.Operator = "<"
		}
	case TokenGreaterThan:
		p.nextToken()
		if p.peek.Type == TokenEqual {
			p.nextToken()
			checkExpr.Operator = ">="
		} else {
			checkExpr.Operator = ">"
		}
	case TokenKeyword:
		switch p.peek.Value {
		case "IN":
			p.nextToken() // consume IN
			checkExpr.Operator = "IN"
			// Parse value list
			if !p.expectPeek(TokenLParen) {
				return nil, errors.New("expected ( after IN")
			}
			for {
				p.nextToken()
				if p.cur.Type == TokenString || p.cur.Type == TokenNumber || p.cur.Type == TokenIdent {
					checkExpr.Values = append(checkExpr.Values, p.cur.Value)
				} else {
					return nil, errors.New("expected value in IN list")
				}
				if p.peek.Type == TokenComma {
					p.nextToken()
				} else {
					break
				}
			}
			if !p.expectPeek(TokenRParen) {
				return nil, errors.New("expected ) after IN list")
			}
		case "BETWEEN":
			p.nextToken() // consume BETWEEN
			checkExpr.Operator = "BETWEEN"
			// Parse min value
			p.nextToken()
			if p.cur.Type != TokenString && p.cur.Type != TokenNumber {
				return nil, errors.New("expected min value after BETWEEN")
			}
			checkExpr.MinValue = p.cur.Value
			// Expect AND
			if !p.expectPeek(TokenKeyword) || p.cur.Value != "AND" {
				return nil, errors.New("expected AND in BETWEEN")
			}
			// Parse max value
			p.nextToken()
			if p.cur.Type != TokenString && p.cur.Type != TokenNumber {
				return nil, errors.New("expected max value after AND")
			}
			checkExpr.MaxValue = p.cur.Value
		default:
			return nil, fmt.Errorf("unexpected keyword in CHECK: %s", p.peek.Value)
		}
	default:
		return nil, errors.New("expected operator in CHECK")
	}

	// For simple comparisons, parse the value
	if checkExpr.Operator != "IN" && checkExpr.Operator != "BETWEEN" {
		p.nextToken()
		if p.cur.Type != TokenString && p.cur.Type != TokenNumber && p.cur.Type != TokenIdent {
			return nil, errors.New("expected value in CHECK")
		}
		checkExpr.Value = p.cur.Value
	}

	// Check for AND/OR
	if p.peek.Type == TokenKeyword && (p.peek.Value == "AND" || p.peek.Value == "OR") {
		op := p.peek.Value
		p.nextToken() // consume AND/OR

		// Parse the next column
		if !p.expectPeek(TokenIdent) {
			return nil, errors.New("expected column name after AND/OR in CHECK")
		}
		nextColumn := p.cur.Value

		// Create a nested check expression
		nestedCheck := &CheckExpr{Column: nextColumn}

		// Parse the operator for the nested expression
		switch p.peek.Type {
		case TokenEqual:
			nestedCheck.Operator = "="
			p.nextToken()
		case TokenLessThan:
			p.nextToken()
			if p.peek.Type == TokenEqual {
				p.nextToken()
				nestedCheck.Operator = "<="
			} else {
				nestedCheck.Operator = "<"
			}
		case TokenGreaterThan:
			p.nextToken()
			if p.peek.Type == TokenEqual {
				p.nextToken()
				nestedCheck.Operator = ">="
			} else {
				nestedCheck.Operator = ">"
			}
		default:
			return nil, errors.New("expected operator in CHECK")
		}

		// Parse the value
		p.nextToken()
		if p.cur.Type != TokenString && p.cur.Type != TokenNumber && p.cur.Type != TokenIdent {
			return nil, errors.New("expected value in CHECK")
		}
		nestedCheck.Value = p.cur.Value

		if op == "AND" {
			checkExpr.And = nestedCheck
		} else {
			checkExpr.Or = nestedCheck
		}
	}

	if !p.expectPeek(TokenRParen) {
		return nil, errors.New("expected ) after CHECK expression")
	}

	return checkExpr, nil
}

// parseTableConstraint parses table-level constraints.
// Supported: PRIMARY KEY (col1, col2), FOREIGN KEY (col) REFERENCES table(col), UNIQUE (col1, col2)
func (p *Parser) parseTableConstraint() (*TableConstraint, error) {
	constraint := &TableConstraint{}

	// Check for optional CONSTRAINT name
	if p.peek.Type == TokenKeyword && p.peek.Value == "CONSTRAINT" {
		p.nextToken() // consume CONSTRAINT
		if !p.expectPeek(TokenIdent) {
			return nil, errors.New("expected constraint name after CONSTRAINT")
		}
		constraint.Name = p.cur.Value
	}

	// Parse constraint type
	p.nextToken()
	switch p.cur.Value {
	case "PRIMARY":
		if !p.expectPeek(TokenKeyword) || p.cur.Value != "KEY" {
			return nil, errors.New("expected KEY after PRIMARY")
		}
		constraint.Type = ConstraintPrimaryKey

		// Parse column list
		if !p.expectPeek(TokenLParen) {
			return nil, errors.New("expected ( after PRIMARY KEY")
		}
		cols, err := p.parseColumnList()
		if err != nil {
			return nil, err
		}
		constraint.Columns = cols

	case "FOREIGN":
		if !p.expectPeek(TokenKeyword) || p.cur.Value != "KEY" {
			return nil, errors.New("expected KEY after FOREIGN")
		}
		constraint.Type = ConstraintForeignKey

		// Parse column list
		if !p.expectPeek(TokenLParen) {
			return nil, errors.New("expected ( after FOREIGN KEY")
		}
		cols, err := p.parseColumnList()
		if err != nil {
			return nil, err
		}
		constraint.Columns = cols

		// Expect REFERENCES
		if !p.expectPeek(TokenKeyword) || p.cur.Value != "REFERENCES" {
			return nil, errors.New("expected REFERENCES after FOREIGN KEY columns")
		}

		// Parse referenced table
		if !p.expectPeek(TokenIdent) {
			return nil, errors.New("expected table name after REFERENCES")
		}
		refTable := p.cur.Value

		// Parse referenced column
		if !p.expectPeek(TokenLParen) {
			return nil, errors.New("expected ( after referenced table name")
		}
		if !p.expectPeek(TokenIdent) {
			return nil, errors.New("expected column name in REFERENCES")
		}
		refColumn := p.cur.Value
		if !p.expectPeek(TokenRParen) {
			return nil, errors.New("expected ) after referenced column")
		}

		constraint.ForeignKey = &ForeignKeyRef{
			Table:  refTable,
			Column: refColumn,
		}

	case "UNIQUE":
		constraint.Type = ConstraintUnique

		// Parse column list
		if !p.expectPeek(TokenLParen) {
			return nil, errors.New("expected ( after UNIQUE")
		}
		cols, err := p.parseColumnList()
		if err != nil {
			return nil, err
		}
		constraint.Columns = cols

	default:
		return nil, fmt.Errorf("unexpected constraint type: %s", p.cur.Value)
	}

	return constraint, nil
}

// parseColumnList parses a comma-separated list of column names within parentheses.
// Assumes the opening parenthesis has already been consumed.
func (p *Parser) parseColumnList() ([]string, error) {
	var columns []string

	for {
		if !p.expectPeek(TokenIdent) {
			return nil, errors.New("expected column name")
		}
		columns = append(columns, p.cur.Value)

		if p.peek.Type == TokenComma {
			p.nextToken()
		} else {
			break
		}
	}

	if !p.expectPeek(TokenRParen) {
		return nil, errors.New("expected )")
	}

	return columns, nil
}

// parseInsert parses an INSERT statement.
// Syntax:
//   INSERT INTO <table> VALUES (<val1>, <val2>, ...)
//   INSERT INTO <table> (<col1>, <col2>) VALUES (<val1>, <val2>)
//   INSERT INTO <table> VALUES (<val1>, <val2>), (<val3>, <val4>)
//   INSERT INTO <table> (<col1>, <col2>) VALUES (<val1>, <val2>) ON CONFLICT DO NOTHING
//   INSERT INTO <table> (<col1>, <col2>) VALUES (<val1>, <val2>) ON CONFLICT DO UPDATE SET <col>=<val>
//
// Examples:
//   INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')
//   INSERT INTO users (id, name) VALUES (1, 'Alice')
//   INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')
//
// Returns an InsertStmt AST node.
func (p *Parser) parseInsert() (*InsertStmt, error) {
	// Expect INTO keyword.
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "INTO" {
		return nil, errors.New("expected INTO")
	}

	// Parse the table name.
	if !p.expectPeek(TokenIdent) {
		return nil, errors.New("expected table name")
	}
	stmt := &InsertStmt{TableName: p.cur.Value}

	// Check if next token is ( for column list or VALUES for value list
	p.nextToken()

	// Check for optional column list: INSERT INTO table (col1, col2) VALUES ...
	if p.cur.Type == TokenLParen {
		// Parse column list
		for {
			p.nextToken()
			if p.cur.Type == TokenIdent {
				stmt.Columns = append(stmt.Columns, p.cur.Value)
			} else {
				return nil, errors.New("expected column name")
			}

			// Check for more columns
			if p.peek.Type == TokenComma {
				p.nextToken()
			} else {
				break
			}
		}

		// Expect closing parenthesis
		if !p.expectPeek(TokenRParen) {
			return nil, errors.New("expected )")
		}

		// Now expect VALUES keyword
		if !p.expectPeek(TokenKeyword) || p.cur.Value != "VALUES" {
			return nil, errors.New("expected VALUES")
		}
	} else if p.cur.Type == TokenKeyword && p.cur.Value == "VALUES" {
		// No column list, just VALUES
	} else {
		return nil, errors.New("expected ( or VALUES")
	}

	// Parse value rows (can be multiple)
	for {
		// Expect opening parenthesis for values
		if !p.expectPeek(TokenLParen) {
			return nil, errors.New("expected (")
		}

		// Parse the value list for this row
		var rowValues []string
		for {
			p.nextToken()
			if p.cur.Type == TokenString || p.cur.Type == TokenNumber || p.cur.Type == TokenIdent ||
				(p.cur.Type == TokenKeyword && (p.cur.Value == "NULL" || p.cur.Value == "TRUE" || p.cur.Value == "FALSE")) {
				rowValues = append(rowValues, p.cur.Value)
			} else {
				return nil, errors.New("expected value")
			}

			// Check for more values
			if p.peek.Type == TokenComma {
				p.nextToken()
			} else {
				break
			}
		}

		// Expect closing parenthesis
		if !p.expectPeek(TokenRParen) {
			return nil, errors.New("expected )")
		}

		// Add this row to MultiValues
		stmt.MultiValues = append(stmt.MultiValues, rowValues)

		// Check for more rows (comma-separated)
		if p.peek.Type == TokenComma {
			p.nextToken()
		} else {
			break
		}
	}

	// For backward compatibility, set Values to the first row
	if len(stmt.MultiValues) > 0 {
		stmt.Values = stmt.MultiValues[0]
	}

	// Check for ON CONFLICT clause
	if p.peek.Type == TokenKeyword && p.peek.Value == "ON" {
		p.nextToken() // consume ON
		if !p.expectPeek(TokenKeyword) || p.cur.Value != "CONFLICT" {
			return nil, errors.New("expected CONFLICT after ON")
		}

		stmt.OnConflict = &OnConflictClause{}

		// Expect DO keyword
		if !p.expectPeek(TokenKeyword) || p.cur.Value != "DO" {
			return nil, errors.New("expected DO after CONFLICT")
		}

		// Check for NOTHING or UPDATE
		p.nextToken()
		if p.cur.Type == TokenKeyword && p.cur.Value == "NOTHING" {
			stmt.OnConflict.DoNothing = true
		} else if p.cur.Type == TokenKeyword && p.cur.Value == "UPDATE" {
			stmt.OnConflict.DoUpdate = true
			stmt.OnConflict.Updates = make(map[string]string)

			// Expect SET keyword
			if !p.expectPeek(TokenKeyword) || p.cur.Value != "SET" {
				return nil, errors.New("expected SET after UPDATE")
			}

			// Parse column assignments
			for {
				if !p.expectPeek(TokenIdent) {
					return nil, errors.New("expected column name")
				}
				col := p.cur.Value

				if !p.expectPeek(TokenEqual) {
					return nil, errors.New("expected =")
				}

				p.nextToken()
				if p.cur.Type != TokenString && p.cur.Type != TokenNumber && p.cur.Type != TokenIdent {
					return nil, errors.New("expected value")
				}
				stmt.OnConflict.Updates[col] = p.cur.Value

				// Check for more assignments
				if p.peek.Type == TokenComma {
					p.nextToken()
				} else {
					break
				}
			}
		} else {
			return nil, errors.New("expected NOTHING or UPDATE after DO")
		}
	}

	return stmt, nil
}

// parseUpdate parses an UPDATE statement.
// Syntax: UPDATE <table> SET <col1>=<val1>, <col2>=<val2> [WHERE <col>=<val>]
//
// Example: UPDATE products SET price=1200 WHERE id=1
//
// Multiple column assignments can be separated by commas.
// The WHERE clause is optional.
//
// Returns an UpdateStmt AST node.
func (p *Parser) parseUpdate() (*UpdateStmt, error) {
	// Parse the table name.
	if !p.expectPeek(TokenIdent) {
		return nil, errors.New("expected table name")
	}
	tableName := p.cur.Value

	// Expect SET keyword.
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "SET" {
		return nil, errors.New("expected SET")
	}

	// Parse column assignments.
	updates := make(map[string]string)
	for {
		// Parse column name.
		if !p.expectPeek(TokenIdent) {
			return nil, errors.New("expected column name")
		}
		col := p.cur.Value

		// Expect equals sign.
		if !p.expectPeek(TokenEqual) {
			return nil, errors.New("expected =")
		}

		// Parse the new value.
		// Currently supports simple values (strings, numbers, identifiers).
		// A production parser would support expressions here.
		p.nextToken()
		updates[col] = p.cur.Value

		// Check for more assignments.
		if p.peek.Type == TokenComma {
			p.nextToken()
		} else {
			break
		}
	}

	// Parse optional WHERE clause.
	var where *Condition
	if p.peek.Type == TokenKeyword && p.peek.Value == "WHERE" {
		p.nextToken() // WHERE

		if !p.expectPeek(TokenIdent) {
			return nil, errors.New("expected column in WHERE")
		}
		col := p.cur.Value

		if !p.expectPeek(TokenEqual) {
			return nil, errors.New("expected =")
		}

		p.nextToken()
		val := p.cur.Value
		where = &Condition{Column: col, Value: val}
	}

	return &UpdateStmt{TableName: tableName, Updates: updates, Where: where}, nil
}

// parseDelete parses a DELETE statement.
// Syntax: DELETE FROM <table> [WHERE <col>=<val>]
//
// Example: DELETE FROM users WHERE id=5
//
// The WHERE clause is optional. Without it, all rows are deleted.
//
// Returns a DeleteStmt AST node.
func (p *Parser) parseDelete() (*DeleteStmt, error) {
	// Expect FROM keyword.
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "FROM" {
		return nil, errors.New("expected FROM")
	}

	// Parse the table name.
	if !p.expectPeek(TokenIdent) {
		return nil, errors.New("expected table name")
	}
	tableName := p.cur.Value

	// Parse optional WHERE clause.
	var where *Condition
	if p.peek.Type == TokenKeyword && p.peek.Value == "WHERE" {
		p.nextToken() // Skip WHERE

		if !p.expectPeek(TokenIdent) {
			return nil, errors.New("expected column in WHERE")
		}
		col := p.cur.Value

		if !p.expectPeek(TokenEqual) {
			return nil, errors.New("expected =")
		}

		p.nextToken()
		val := p.cur.Value
		where = &Condition{Column: col, Value: val}
	}

	return &DeleteStmt{TableName: tableName, Where: where}, nil
}

// parseSelect parses a SELECT statement.
// Syntax: SELECT [DISTINCT] <columns> FROM <table>
//
//	[JOIN <table2> ON <condition>]
//	[WHERE <condition>]
//	[ORDER BY <column> [ASC|DESC]]
//	[LIMIT <n>]
//
// Examples:
//   - SELECT name, email FROM users
//   - SELECT DISTINCT category FROM products
//   - SELECT * FROM products WHERE id = 1
//   - SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id
//   - SELECT name FROM products ORDER BY price DESC LIMIT 10
//   - SELECT COUNT(*), SUM(amount) FROM orders
//
// Returns a SelectStmt AST node.
func (p *Parser) parseSelect() (*SelectStmt, error) {
	stmt := &SelectStmt{}

	// Check for DISTINCT keyword
	if p.peek.Type == TokenKeyword && p.peek.Value == "DISTINCT" {
		p.nextToken() // consume DISTINCT
		stmt.Distinct = true
	}

	// Parse the column list.
	// Columns can be identifiers, qualified names (table.column), *, or aggregate functions.
	for {
		if p.peek.Type == TokenIdent || p.peek.Type == TokenKeyword {
			// Check if we've reached FROM.
			if p.peek.Value == "FROM" {
				break
			}

			// Check for aggregate functions: COUNT, SUM, AVG, MIN, MAX
			if p.peek.Type == TokenKeyword && isAggregateFunction(p.peek.Value) {
				agg, err := p.parseAggregate()
				if err != nil {
					return nil, err
				}
				stmt.Aggregates = append(stmt.Aggregates, agg)
			} else if p.peek.Type == TokenKeyword && isScalarFunction(p.peek.Value) {
				// Check for scalar functions: UPPER, LOWER, LENGTH, etc.
				fn, err := p.parseScalarFunction()
				if err != nil {
					return nil, err
				}
				stmt.Functions = append(stmt.Functions, fn)
			} else {
				p.nextToken()
				stmt.Columns = append(stmt.Columns, p.cur.Value)
			}
		} else {
			return nil, fmt.Errorf("expected column or FROM, got %v", p.peek.Value)
		}

		// Check for more columns or FROM.
		if p.peek.Type == TokenComma {
			p.nextToken()
		} else if p.peek.Value == "FROM" {
			break
		} else {
			return nil, errors.New("expected comma or FROM")
		}
	}

	// Expect FROM keyword and table name.
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "FROM" {
		return nil, errors.New("expected FROM")
	}
	if !p.expectPeek(TokenIdent) {
		return nil, errors.New("expected table name")
	}
	stmt.TableName = p.cur.Value

	// Parse optional JOIN clause.
	// Supports: JOIN, INNER JOIN, LEFT [OUTER] JOIN, RIGHT [OUTER] JOIN, FULL [OUTER] JOIN
	if p.peek.Type == TokenKeyword && (p.peek.Value == "JOIN" || p.peek.Value == "INNER" ||
		p.peek.Value == "LEFT" || p.peek.Value == "RIGHT" || p.peek.Value == "FULL") {

		joinType := JoinTypeInner // Default to INNER JOIN

		// Determine join type
		if p.peek.Value == "LEFT" {
			joinType = JoinTypeLeft
			p.nextToken() // consume LEFT
			// Skip optional OUTER keyword
			if p.peek.Type == TokenKeyword && p.peek.Value == "OUTER" {
				p.nextToken()
			}
		} else if p.peek.Value == "RIGHT" {
			joinType = JoinTypeRight
			p.nextToken() // consume RIGHT
			// Skip optional OUTER keyword
			if p.peek.Type == TokenKeyword && p.peek.Value == "OUTER" {
				p.nextToken()
			}
		} else if p.peek.Value == "FULL" {
			joinType = JoinTypeFull
			p.nextToken() // consume FULL
			// Skip optional OUTER keyword
			if p.peek.Type == TokenKeyword && p.peek.Value == "OUTER" {
				p.nextToken()
			}
		} else if p.peek.Value == "INNER" {
			p.nextToken() // consume INNER
		}

		// Now expect JOIN keyword
		if p.peek.Type != TokenKeyword || p.peek.Value != "JOIN" {
			return nil, errors.New("expected JOIN keyword")
		}
		p.nextToken() // Skip JOIN

		// Parse the join table name.
		if !p.expectPeek(TokenIdent) {
			return nil, errors.New("expected table name in JOIN")
		}
		joinTable := p.cur.Value

		// Expect ON keyword.
		if !p.expectPeek(TokenKeyword) || p.cur.Value != "ON" {
			return nil, errors.New("expected ON")
		}

		// Parse the join condition: left_col = right_col
		if !p.expectPeek(TokenIdent) {
			return nil, errors.New("expected column in ON")
		}
		leftCol := p.cur.Value

		if !p.expectPeek(TokenEqual) {
			return nil, errors.New("expected =")
		}
		p.nextToken()
		rightCol := p.cur.Value // Can be identifier or value

		stmt.Join = &JoinClause{
			JoinType:  joinType,
			TableName: joinTable,
			On:        &Condition{Column: leftCol, Value: rightCol},
		}
	}

	// Parse optional WHERE clause.
	if p.peek.Type == TokenKeyword && p.peek.Value == "WHERE" {
		p.nextToken() // Skip WHERE

		whereClause, err := p.parseWhereClause()
		if err != nil {
			return nil, err
		}

		// For backward compatibility, also set the simple Where if it's a simple condition
		if whereClause != nil && !whereClause.IsSubquery && whereClause.Operator == "=" {
			stmt.Where = &Condition{Column: whereClause.Column, Value: whereClause.Value}
		}
		stmt.WhereExt = whereClause
	}

	// Parse optional GROUP BY clause.
	if p.peek.Type == TokenKeyword && p.peek.Value == "GROUP" {
		p.nextToken() // Skip GROUP

		if !p.expectPeek(TokenKeyword) || p.cur.Value != "BY" {
			return nil, errors.New("expected BY after GROUP")
		}

		// Parse column list for GROUP BY
		for {
			if !p.expectPeek(TokenIdent) {
				return nil, errors.New("expected column in GROUP BY")
			}
			stmt.GroupBy = append(stmt.GroupBy, p.cur.Value)

			// Check for more columns
			if p.peek.Type == TokenComma {
				p.nextToken()
			} else {
				break
			}
		}
	}

	// Parse optional HAVING clause.
	if p.peek.Type == TokenKeyword && p.peek.Value == "HAVING" {
		p.nextToken() // Skip HAVING

		// Parse aggregate function
		if p.peek.Type != TokenKeyword || !isAggregateFunction(p.peek.Value) {
			return nil, errors.New("expected aggregate function in HAVING")
		}
		agg, err := p.parseAggregate()
		if err != nil {
			return nil, err
		}

		// Parse comparison operator
		var operator string
		switch p.peek.Type {
		case TokenEqual:
			operator = "="
		case TokenLessThan:
			operator = "<"
		case TokenGreaterThan:
			operator = ">"
		case TokenLessEqual:
			operator = "<="
		case TokenGreaterEqual:
			operator = ">="
		default:
			return nil, errors.New("expected comparison operator in HAVING")
		}
		p.nextToken()

		// Parse value
		p.nextToken()
		if p.cur.Type != TokenNumber && p.cur.Type != TokenString {
			return nil, errors.New("expected value in HAVING")
		}
		value := p.cur.Value

		stmt.Having = &HavingClause{
			Aggregate: agg,
			Operator:  operator,
			Value:     value,
		}
	}

	// Parse optional ORDER BY clause.
	if p.peek.Type == TokenKeyword && p.peek.Value == "ORDER" {
		p.nextToken() // Skip ORDER

		if !p.expectPeek(TokenKeyword) || p.cur.Value != "BY" {
			return nil, errors.New("expected BY")
		}
		if !p.expectPeek(TokenIdent) {
			return nil, errors.New("expected column in ORDER BY")
		}
		col := p.cur.Value

		// Parse optional direction (default is ASC).
		dir := "ASC"
		if p.peek.Type == TokenKeyword && (p.peek.Value == "ASC" || p.peek.Value == "DESC") {
			p.nextToken()
			dir = p.cur.Value
		}
		stmt.OrderBy = &OrderByClause{Column: col, Direction: dir}
	}

	// Parse optional LIMIT clause.
	if p.peek.Type == TokenKeyword && p.peek.Value == "LIMIT" {
		p.nextToken() // Skip LIMIT

		if !p.expectPeek(TokenNumber) {
			return nil, errors.New("expected number in LIMIT")
		}
		fmt.Sscanf(p.cur.Value, "%d", &stmt.Limit)
	}

	// Parse optional OFFSET clause.
	if p.peek.Type == TokenKeyword && p.peek.Value == "OFFSET" {
		p.nextToken() // Skip OFFSET

		if !p.expectPeek(TokenNumber) {
			return nil, errors.New("expected number in OFFSET")
		}
		fmt.Sscanf(p.cur.Value, "%d", &stmt.Offset)
	}

	return stmt, nil
}

// parseSelectOrUnion parses a SELECT statement and checks for UNION, INTERSECT, or EXCEPT.
// If a set operation is found, it parses the right side and returns the appropriate statement.
// Otherwise, it returns the SelectStmt directly.
func (p *Parser) parseSelectOrUnion() (Statement, error) {
	left, err := p.parseSelect()
	if err != nil {
		return nil, err
	}

	// Check for set operation keywords: UNION, INTERSECT, EXCEPT
	if p.peek.Type == TokenKeyword {
		switch p.peek.Value {
		case "UNION":
			return p.parseUnion(left)
		case "INTERSECT":
			return p.parseIntersect(left)
		case "EXCEPT":
			return p.parseExcept(left)
		}
	}

	return left, nil
}

// parseUnion parses a UNION operation.
func (p *Parser) parseUnion(left *SelectStmt) (*UnionStmt, error) {
	p.nextToken() // consume UNION

	// Check for ALL keyword
	all := false
	if p.peek.Type == TokenKeyword && p.peek.Value == "ALL" {
		p.nextToken() // consume ALL
		all = true
	}

	// Expect SELECT keyword for the right side
	if p.peek.Type != TokenKeyword || p.peek.Value != "SELECT" {
		return nil, errors.New("expected SELECT after UNION")
	}
	p.nextToken() // consume SELECT

	// Parse the right SELECT statement
	right, err := p.parseSelect()
	if err != nil {
		return nil, fmt.Errorf("error parsing UNION right side: %v", err)
	}

	union := &UnionStmt{
		Left:  left,
		Right: right,
		All:   all,
	}

	// Check for additional UNIONs (chaining)
	for p.peek.Type == TokenKeyword && p.peek.Value == "UNION" {
		p.nextToken() // consume UNION

		chainAll := false
		if p.peek.Type == TokenKeyword && p.peek.Value == "ALL" {
			p.nextToken() // consume ALL
			chainAll = true
		}

		if p.peek.Type != TokenKeyword || p.peek.Value != "SELECT" {
			return nil, errors.New("expected SELECT after UNION")
		}
		p.nextToken() // consume SELECT

		nextRight, err := p.parseSelect()
		if err != nil {
			return nil, fmt.Errorf("error parsing chained UNION: %v", err)
		}

		// Chain the union
		union.NextUnion = &UnionStmt{
			Left:  union.Right,
			Right: nextRight,
			All:   chainAll,
		}
		union.Right = nextRight
	}

	return union, nil
}

// parseIntersect parses an INTERSECT operation.
func (p *Parser) parseIntersect(left *SelectStmt) (*IntersectStmt, error) {
	p.nextToken() // consume INTERSECT

	// Check for ALL keyword
	all := false
	if p.peek.Type == TokenKeyword && p.peek.Value == "ALL" {
		p.nextToken() // consume ALL
		all = true
	}

	// Expect SELECT keyword for the right side
	if p.peek.Type != TokenKeyword || p.peek.Value != "SELECT" {
		return nil, errors.New("expected SELECT after INTERSECT")
	}
	p.nextToken() // consume SELECT

	// Parse the right SELECT statement
	right, err := p.parseSelect()
	if err != nil {
		return nil, fmt.Errorf("error parsing INTERSECT right side: %v", err)
	}

	return &IntersectStmt{
		Left:  left,
		Right: right,
		All:   all,
	}, nil
}

// parseExcept parses an EXCEPT operation.
func (p *Parser) parseExcept(left *SelectStmt) (*ExceptStmt, error) {
	p.nextToken() // consume EXCEPT

	// Check for ALL keyword
	all := false
	if p.peek.Type == TokenKeyword && p.peek.Value == "ALL" {
		p.nextToken() // consume ALL
		all = true
	}

	// Expect SELECT keyword for the right side
	if p.peek.Type != TokenKeyword || p.peek.Value != "SELECT" {
		return nil, errors.New("expected SELECT after EXCEPT")
	}
	p.nextToken() // consume SELECT

	// Parse the right SELECT statement
	right, err := p.parseSelect()
	if err != nil {
		return nil, fmt.Errorf("error parsing EXCEPT right side: %v", err)
	}

	return &ExceptStmt{
		Left:  left,
		Right: right,
		All:   all,
	}, nil
}

// expectPeek checks if the next token is of the expected type.
// If it matches, the parser advances to that token and returns true.
// If it doesn't match, the parser stays at the current position and returns false.
//
// This is a common pattern in recursive descent parsers for optional
// or conditional token consumption.
//
// Parameters:
//   - t: The expected token type
//
// Returns true if the peek token matches and was consumed, false otherwise.
func (p *Parser) expectPeek(t TokenType) bool {
	if p.peek.Type == t {
		p.nextToken()
		return true
	}
	return false
}

// parseBegin parses a BEGIN statement.
// Syntax: BEGIN [TRANSACTION]
//
// Example: BEGIN
//
// Returns a BeginStmt AST node.
func (p *Parser) parseBegin() (*BeginStmt, error) {
	// Skip optional TRANSACTION keyword
	if p.peek.Type == TokenKeyword && p.peek.Value == "TRANSACTION" {
		p.nextToken()
	}
	return &BeginStmt{}, nil
}

// parseRollback parses a ROLLBACK statement.
// Syntax: ROLLBACK | ROLLBACK TO [SAVEPOINT] <name>
//
// Examples:
//   - ROLLBACK
//   - ROLLBACK TO savepoint1
//   - ROLLBACK TO SAVEPOINT savepoint1
//
// Returns a RollbackStmt AST node.
func (p *Parser) parseRollback() (*RollbackStmt, error) {
	// Check for ROLLBACK TO
	if p.peek.Type == TokenKeyword && p.peek.Value == "TO" {
		p.nextToken() // consume TO

		// Skip optional SAVEPOINT keyword
		if p.peek.Type == TokenKeyword && p.peek.Value == "SAVEPOINT" {
			p.nextToken()
		}

		// Parse savepoint name
		if !p.expectPeek(TokenIdent) {
			return nil, errors.New("expected savepoint name after ROLLBACK TO")
		}
		return &RollbackStmt{ToSavepoint: p.cur.Value}, nil
	}

	return &RollbackStmt{}, nil
}

// parseSavepoint parses a SAVEPOINT statement.
// Syntax: SAVEPOINT <name>
//
// Example: SAVEPOINT sp1
//
// Returns a SavepointStmt AST node.
func (p *Parser) parseSavepoint() (*SavepointStmt, error) {
	// Parse savepoint name
	if !p.expectPeek(TokenIdent) {
		return nil, errors.New("expected savepoint name")
	}
	return &SavepointStmt{Name: p.cur.Value}, nil
}

// parseReleaseSavepoint parses a RELEASE SAVEPOINT statement.
// Syntax: RELEASE [SAVEPOINT] <name>
//
// Examples:
//   - RELEASE sp1
//   - RELEASE SAVEPOINT sp1
//
// Returns a ReleaseSavepointStmt AST node.
func (p *Parser) parseReleaseSavepoint() (*ReleaseSavepointStmt, error) {
	// Skip optional SAVEPOINT keyword
	if p.peek.Type == TokenKeyword && p.peek.Value == "SAVEPOINT" {
		p.nextToken()
	}

	// Parse savepoint name
	if !p.expectPeek(TokenIdent) {
		return nil, errors.New("expected savepoint name after RELEASE")
	}
	return &ReleaseSavepointStmt{Name: p.cur.Value}, nil
}

// parseCreateIndex parses a CREATE INDEX statement.
// Syntax: CREATE INDEX [IF NOT EXISTS] <name> ON <table> (<column>)
//
// Examples:
//   CREATE INDEX idx_users_email ON users (email)
//   CREATE INDEX IF NOT EXISTS idx_users_email ON users (email)
//
// Returns a CreateIndexStmt AST node.
func (p *Parser) parseCreateIndex() (*CreateIndexStmt, error) {
	// Skip CREATE and INDEX keywords
	p.nextToken() // Skip CREATE
	p.nextToken() // Skip INDEX

	// Parse optional IF NOT EXISTS clause
	ifNotExists := false
	if p.cur.Type == TokenKeyword && p.cur.Value == "IF" {
		p.nextToken() // consume IF
		if p.cur.Type != TokenKeyword || p.cur.Value != "NOT" {
			return nil, errors.New("expected NOT after IF")
		}
		p.nextToken() // consume NOT
		if p.cur.Type != TokenKeyword || p.cur.Value != "EXISTS" {
			return nil, errors.New("expected EXISTS after IF NOT")
		}
		ifNotExists = true
		p.nextToken() // move to index name
	}

	// Parse the index name
	if p.cur.Type != TokenIdent {
		return nil, errors.New("expected index name")
	}
	indexName := p.cur.Value

	// Expect ON keyword
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "ON" {
		return nil, errors.New("expected ON")
	}

	// Parse the table name
	if !p.expectPeek(TokenIdent) {
		return nil, errors.New("expected table name")
	}
	tableName := p.cur.Value

	// Expect opening parenthesis
	if !p.expectPeek(TokenLParen) {
		return nil, errors.New("expected (")
	}

	// Parse the column name
	if !p.expectPeek(TokenIdent) {
		return nil, errors.New("expected column name")
	}
	columnName := p.cur.Value

	// Expect closing parenthesis
	if !p.expectPeek(TokenRParen) {
		return nil, errors.New("expected )")
	}

	return &CreateIndexStmt{
		IndexName:   indexName,
		TableName:   tableName,
		ColumnName:  columnName,
		IfNotExists: ifNotExists,
	}, nil
}

// parsePrepare parses a PREPARE statement.
// Syntax: PREPARE <name> AS <query>
//
// Example: PREPARE get_user AS SELECT * FROM users WHERE id = $1
//
// Returns a PrepareStmt AST node.
func (p *Parser) parsePrepare() (*PrepareStmt, error) {
	// Skip PREPARE keyword
	p.nextToken()

	// Parse the statement name
	if p.cur.Type != TokenIdent {
		return nil, errors.New("expected statement name after PREPARE")
	}
	name := p.cur.Value

	// Expect AS keyword
	p.nextToken()
	if p.cur.Type != TokenKeyword || p.cur.Value != "AS" {
		return nil, errors.New("expected AS after statement name")
	}

	// Collect the rest of the tokens as the query string
	p.nextToken()
	query := ""
	for p.cur.Type != TokenEOF {
		if query != "" {
			query += " "
		}
		query += p.cur.Value
		p.nextToken()
	}

	if query == "" {
		return nil, errors.New("expected query after AS")
	}

	return &PrepareStmt{
		Name:  name,
		Query: query,
	}, nil
}

// parseExecute parses an EXECUTE statement.
// Syntax: EXECUTE <name> [USING <param1>, <param2>, ...]
//
// Example: EXECUTE get_user USING 42
//
// Returns an ExecuteStmt AST node.
func (p *Parser) parseExecute() (*ExecuteStmt, error) {
	// Skip EXECUTE keyword
	p.nextToken()

	// Parse the statement name
	if p.cur.Type != TokenIdent {
		return nil, errors.New("expected statement name after EXECUTE")
	}
	name := p.cur.Value

	stmt := &ExecuteStmt{
		Name:   name,
		Params: []string{},
	}

	// Check for optional USING clause
	p.nextToken()
	if p.cur.Type == TokenKeyword && p.cur.Value == "USING" {
		p.nextToken()

		// Parse parameter values
		for {
			var value string
			switch p.cur.Type {
			case TokenNumber, TokenIdent:
				value = p.cur.Value
			case TokenString:
				value = p.cur.Value
			case TokenKeyword:
				// Handle TRUE/FALSE as boolean values
				if p.cur.Value == "TRUE" || p.cur.Value == "FALSE" {
					value = p.cur.Value
				} else if p.cur.Value == "NULL" {
					value = "NULL"
				} else {
					return nil, fmt.Errorf("unexpected keyword in USING clause: %s", p.cur.Value)
				}
			default:
				return nil, errors.New("expected parameter value")
			}
			stmt.Params = append(stmt.Params, value)

			p.nextToken()
			if p.cur.Type != TokenComma {
				break
			}
			p.nextToken() // Skip comma
		}
	}

	return stmt, nil
}

// parseDeallocate parses a DEALLOCATE statement.
// Syntax: DEALLOCATE <name>
//
// Example: DEALLOCATE get_user
//
// Returns a DeallocateStmt AST node.
func (p *Parser) parseDeallocate() (*DeallocateStmt, error) {
	// Skip DEALLOCATE keyword
	p.nextToken()

	// Parse the statement name
	if p.cur.Type != TokenIdent {
		return nil, errors.New("expected statement name after DEALLOCATE")
	}
	name := p.cur.Value

	return &DeallocateStmt{
		Name: name,
	}, nil
}


// isAggregateFunction checks if a keyword is an aggregate function.
func isAggregateFunction(keyword string) bool {
	switch keyword {
	case "COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT", "STRING_AGG":
		return true
	}
	return false
}

// isScalarFunction checks if a keyword is a scalar function.
func isScalarFunction(keyword string) bool {
	switch keyword {
	// String functions
	case "UPPER", "LOWER", "LENGTH", "LEN", "CONCAT", "SUBSTRING", "SUBSTR",
		"TRIM", "LTRIM", "RTRIM", "REPLACE", "LEFT", "RIGHT", "REVERSE", "REPEAT":
		return true
	// Numeric functions
	case "ABS", "ROUND", "CEIL", "CEILING", "FLOOR", "MOD", "POWER", "POW", "SQRT":
		return true
	// Date/Time functions
	case "NOW", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
		"DATE_ADD", "DATE_SUB", "DATEDIFF", "DATEADD", "EXTRACT",
		"YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND":
		return true
	// NULL handling functions
	case "COALESCE", "NULLIF", "IFNULL", "NVL", "ISNULL":
		return true
	// Type conversion
	case "CAST", "CONVERT":
		return true
	}
	return false
}

// parseAggregate parses an aggregate function call.
// Syntax: <function>(<column>) or <function>(*)
//
// Examples:
//   - COUNT(*)
//   - SUM(amount)
//   - AVG(price)
//
// Returns an AggregateExpr AST node.
func (p *Parser) parseAggregate() (*AggregateExpr, error) {
	// Get the function name
	p.nextToken()
	funcName := p.cur.Value

	// Expect opening parenthesis
	if !p.expectPeek(TokenLParen) {
		return nil, fmt.Errorf("expected ( after %s", funcName)
	}

	// Parse the column or *
	p.nextToken()
	var column string
	if p.cur.Type == TokenIdent {
		column = p.cur.Value
	} else if p.cur.Type == TokenKeyword && p.cur.Value == "*" {
		column = "*"
	} else if p.cur.Value == "*" {
		column = "*"
	} else {
		return nil, fmt.Errorf("expected column name or * in %s()", funcName)
	}

	// For GROUP_CONCAT/STRING_AGG, check for separator
	separator := ","
	if funcName == "GROUP_CONCAT" || funcName == "STRING_AGG" {
		if p.peek.Type == TokenComma {
			p.nextToken() // Skip comma
			p.nextToken() // Get separator
			if p.cur.Type == TokenString {
				separator = p.cur.Value
			}
		}
	}

	// Expect closing parenthesis
	if !p.expectPeek(TokenRParen) {
		return nil, fmt.Errorf("expected ) after %s(%s", funcName, column)
	}

	return &AggregateExpr{
		Function:  funcName,
		Column:    column,
		Separator: separator,
	}, nil
}

// parseScalarFunction parses a scalar function call.
// Syntax: <function>(<arg1>, <arg2>, ...)
//
// Examples:
//   - UPPER(name)
//   - CONCAT(first_name, ' ', last_name)
//   - SUBSTRING(description, 1, 100)
//
// Returns a FunctionExpr AST node.
func (p *Parser) parseScalarFunction() (*FunctionExpr, error) {
	// Get the function name
	p.nextToken()
	funcName := p.cur.Value

	// Expect opening parenthesis
	if !p.expectPeek(TokenLParen) {
		return nil, fmt.Errorf("expected ( after %s", funcName)
	}

	// Parse arguments
	var args []string
	for {
		p.nextToken()

		// Check for closing parenthesis (empty args or end of args)
		if p.cur.Type == TokenRParen {
			break
		}

		// Parse argument (can be column name, string literal, or number)
		var arg string
		switch p.cur.Type {
		case TokenIdent:
			arg = p.cur.Value
		case TokenString:
			arg = "'" + p.cur.Value + "'" // Keep quotes to identify as literal
		case TokenNumber:
			arg = p.cur.Value
		case TokenKeyword:
			// Handle NULL or other keywords
			arg = p.cur.Value
		default:
			return nil, fmt.Errorf("unexpected token in function arguments: %v", p.cur.Value)
		}
		args = append(args, arg)

		// Check for comma or closing parenthesis
		if p.peek.Type == TokenComma {
			p.nextToken() // Skip comma
		} else if p.peek.Type == TokenRParen {
			p.nextToken() // Move to closing paren
			break
		} else {
			return nil, fmt.Errorf("expected , or ) in function call, got %v", p.peek.Value)
		}
	}

	return &FunctionExpr{
		Function:  funcName,
		Arguments: args,
	}, nil
}

// parseInspect parses an INSPECT statement.
// Syntax: INSPECT <target> [<object_name>]
//
// Supported targets:
//   - USERS: List all database users
//   - TABLES: List all tables with their schemas
//   - TABLE <name>: Detailed info for a specific table
//   - INDEXES: List all indexes
//   - SERVER: Show server/daemon information
//   - STATUS: Show overall database status and statistics
//
// Examples:
//
//	INSPECT USERS
//	INSPECT TABLES
//	INSPECT TABLE employees
//	INSPECT SERVER
//	INSPECT STATUS
//
// Returns an InspectStmt AST node.
func (p *Parser) parseInspect() (*InspectStmt, error) {
	// Expect the target
	// These are parsed as identifiers to avoid conflicts with table names
	p.nextToken()
	if p.cur.Type != TokenIdent && p.cur.Type != TokenKeyword {
		return nil, errors.New("expected USERS, TABLES, TABLE, INDEXES, SERVER, STATUS, ROLES, or ROLE after INSPECT")
	}

	// Normalize to uppercase for case-insensitive matching
	target := strings.ToUpper(p.cur.Value)
	switch target {
	case "USERS", "TABLES", "INDEXES", "SERVER", "STATUS", "DATABASES", "ROLES", "PRIVILEGES":
		// These targets don't take an object name
		return &InspectStmt{Target: target}, nil
	case "TABLE":
		// This target requires an object name
		p.nextToken()
		if p.cur.Type != TokenIdent && p.cur.Type != TokenKeyword {
			return nil, fmt.Errorf("expected table name after INSPECT TABLE")
		}
		objectName := p.cur.Value
		return &InspectStmt{Target: target, ObjectName: objectName}, nil
	case "DATABASE":
		// INSPECT DATABASE <name> - inspect a specific database
		p.nextToken()
		if p.cur.Type != TokenIdent && p.cur.Type != TokenKeyword {
			return nil, fmt.Errorf("expected database name after INSPECT DATABASE")
		}
		objectName := p.cur.Value
		return &InspectStmt{Target: target, ObjectName: objectName}, nil
	case "ROLE":
		// INSPECT ROLE <name> - inspect a specific role
		p.nextToken()
		if p.cur.Type != TokenIdent && p.cur.Type != TokenKeyword {
			return nil, fmt.Errorf("expected role name after INSPECT ROLE")
		}
		objectName := p.cur.Value
		return &InspectStmt{Target: target, ObjectName: objectName}, nil
	case "USER":
		// INSPECT USER <name> [ROLES|PRIVILEGES] - inspect a specific user
		p.nextToken()
		if p.cur.Type != TokenIdent && p.cur.Type != TokenKeyword {
			return nil, fmt.Errorf("expected username after INSPECT USER")
		}
		objectName := p.cur.Value
		// Check for optional ROLES or PRIVILEGES suffix
		if p.peek.Type == TokenIdent || p.peek.Type == TokenKeyword {
			suffix := strings.ToUpper(p.peek.Value)
			if suffix == "ROLES" {
				p.nextToken()
				return &InspectStmt{Target: "USER_ROLES", ObjectName: objectName}, nil
			} else if suffix == "PRIVILEGES" {
				p.nextToken()
				return &InspectStmt{Target: "USER_PRIVILEGES", ObjectName: objectName}, nil
			}
		}
		return &InspectStmt{Target: target, ObjectName: objectName}, nil
	default:
		return nil, fmt.Errorf("unknown INSPECT target: %s (expected USERS, TABLES, TABLE, INDEXES, SERVER, STATUS, DATABASES, ROLES, ROLE, or USER)", target)
	}
}

// parseWhereClause parses a WHERE clause with support for subqueries.
// Syntax:
//
//	<column> = <value>
//	<column> IN (<value1>, <value2>, ...)
//	<column> IN (SELECT ...)
//	EXISTS (SELECT ...)
//
// Returns a WhereClause AST node.
func (p *Parser) parseWhereClause() (*WhereClause, error) {
	// Check for EXISTS keyword
	if p.peek.Type == TokenKeyword && p.peek.Value == "EXISTS" {
		p.nextToken() // consume EXISTS
		if !p.expectPeek(TokenLParen) {
			return nil, errors.New("expected ( after EXISTS")
		}
		// Parse the subquery
		if p.peek.Type != TokenKeyword || p.peek.Value != "SELECT" {
			return nil, errors.New("expected SELECT in EXISTS subquery")
		}
		p.nextToken() // consume SELECT
		subquery, err := p.parseSelect()
		if err != nil {
			return nil, fmt.Errorf("error parsing EXISTS subquery: %v", err)
		}
		if !p.expectPeek(TokenRParen) {
			return nil, errors.New("expected ) after EXISTS subquery")
		}
		return &WhereClause{
			Operator:   "EXISTS",
			Subquery:   subquery,
			IsSubquery: true,
		}, nil
	}

	// Parse column name
	if !p.expectPeek(TokenIdent) {
		return nil, errors.New("expected column in WHERE")
	}
	col := p.cur.Value

	// Parse operator
	var operator string
	switch p.peek.Type {
	case TokenEqual:
		operator = "="
		p.nextToken()
	case TokenLessThan:
		operator = "<"
		p.nextToken()
	case TokenGreaterThan:
		operator = ">"
		p.nextToken()
	case TokenLessEqual:
		operator = "<="
		p.nextToken()
	case TokenGreaterEqual:
		operator = ">="
		p.nextToken()
	case TokenKeyword:
		switch p.peek.Value {
		case "IN":
			operator = "IN"
			p.nextToken()
		case "LIKE":
			operator = "LIKE"
			p.nextToken()
		case "IS":
			p.nextToken() // consume IS
			if p.peek.Type == TokenKeyword && p.peek.Value == "NOT" {
				p.nextToken() // consume NOT
				if p.peek.Type == TokenKeyword && p.peek.Value == "NULL" {
					operator = "IS NOT NULL"
					p.nextToken()
				} else {
					return nil, errors.New("expected NULL after IS NOT")
				}
			} else if p.peek.Type == TokenKeyword && p.peek.Value == "NULL" {
				operator = "IS NULL"
				p.nextToken()
			} else {
				return nil, errors.New("expected NULL or NOT NULL after IS")
			}
		case "NOT":
			p.nextToken() // consume NOT
			if p.peek.Type == TokenKeyword && p.peek.Value == "IN" {
				operator = "NOT IN"
				p.nextToken()
			} else if p.peek.Type == TokenKeyword && p.peek.Value == "LIKE" {
				operator = "NOT LIKE"
				p.nextToken()
			} else {
				return nil, errors.New("expected IN or LIKE after NOT")
			}
		case "BETWEEN":
			operator = "BETWEEN"
			p.nextToken()
		default:
			return nil, fmt.Errorf("unexpected keyword in WHERE: %s", p.peek.Value)
		}
	default:
		return nil, errors.New("expected operator in WHERE")
	}

	// Handle IN and NOT IN with subquery or value list
	if operator == "IN" || operator == "NOT IN" {
		if !p.expectPeek(TokenLParen) {
			return nil, errors.New("expected ( after IN")
		}

		// Check if it's a subquery
		if p.peek.Type == TokenKeyword && p.peek.Value == "SELECT" {
			p.nextToken() // consume SELECT
			subquery, err := p.parseSelect()
			if err != nil {
				return nil, fmt.Errorf("error parsing IN subquery: %v", err)
			}
			if !p.expectPeek(TokenRParen) {
				return nil, errors.New("expected ) after IN subquery")
			}
			return &WhereClause{
				Column:     col,
				Operator:   operator,
				Subquery:   subquery,
				IsSubquery: true,
			}, nil
		}

		// Parse value list
		var values []string
		for {
			p.nextToken()
			if p.cur.Type == TokenString || p.cur.Type == TokenNumber || p.cur.Type == TokenIdent {
				values = append(values, p.cur.Value)
			} else {
				return nil, errors.New("expected value in IN list")
			}
			if p.peek.Type == TokenComma {
				p.nextToken()
			} else {
				break
			}
		}
		if !p.expectPeek(TokenRParen) {
			return nil, errors.New("expected ) after IN list")
		}
		return &WhereClause{
			Column:   col,
			Operator: operator,
			Values:   values,
		}, nil
	}

	// Handle IS NULL and IS NOT NULL (no value needed)
	if operator == "IS NULL" || operator == "IS NOT NULL" {
		return &WhereClause{
			Column:   col,
			Operator: operator,
		}, nil
	}

	// Handle BETWEEN: column BETWEEN low AND high
	if operator == "BETWEEN" {
		p.nextToken()
		if p.cur.Type != TokenString && p.cur.Type != TokenNumber && p.cur.Type != TokenIdent {
			return nil, errors.New("expected low value in BETWEEN")
		}
		lowVal := p.cur.Value

		// Expect AND keyword
		if !p.expectPeek(TokenKeyword) || p.cur.Value != "AND" {
			return nil, errors.New("expected AND in BETWEEN")
		}

		p.nextToken()
		if p.cur.Type != TokenString && p.cur.Type != TokenNumber && p.cur.Type != TokenIdent {
			return nil, errors.New("expected high value in BETWEEN")
		}
		highVal := p.cur.Value

		return &WhereClause{
			Column:      col,
			Operator:    operator,
			BetweenLow:  lowVal,
			BetweenHigh: highVal,
		}, nil
	}

	// Parse simple value (for =, <, >, <=, >=, LIKE, NOT LIKE)
	p.nextToken()
	if p.cur.Type != TokenString && p.cur.Type != TokenNumber && p.cur.Type != TokenIdent {
		return nil, errors.New("expected value in WHERE")
	}
	val := p.cur.Value

	return &WhereClause{
		Column:   col,
		Operator: operator,
		Value:    val,
	}, nil
}

// parseCreateProcedure parses a CREATE PROCEDURE statement.
// Syntax: CREATE [OR REPLACE] PROCEDURE [IF NOT EXISTS] <name>([<param1> <type1>, ...]) BEGIN <statements> END
func (p *Parser) parseCreateProcedure() (*CreateProcedureStmt, error) {
	p.nextToken() // Skip CREATE

	// Check for OR REPLACE
	orReplace := false
	if p.cur.Type == TokenKeyword && p.cur.Value == "OR" {
		p.nextToken() // consume OR
		if p.cur.Type != TokenKeyword || p.cur.Value != "REPLACE" {
			return nil, errors.New("expected REPLACE after OR")
		}
		orReplace = true
		p.nextToken() // move past REPLACE
	}

	p.nextToken() // Skip PROCEDURE

	// Check for IF NOT EXISTS
	ifNotExists := false
	if p.cur.Type == TokenKeyword && p.cur.Value == "IF" {
		p.nextToken() // consume IF
		if p.cur.Type != TokenKeyword || p.cur.Value != "NOT" {
			return nil, errors.New("expected NOT after IF")
		}
		p.nextToken() // consume NOT
		if p.cur.Type != TokenKeyword || p.cur.Value != "EXISTS" {
			return nil, errors.New("expected EXISTS after IF NOT")
		}
		ifNotExists = true
		p.nextToken() // move to procedure name
	}

	// Parse procedure name
	if p.cur.Type != TokenIdent {
		return nil, errors.New("expected procedure name")
	}
	stmt := &CreateProcedureStmt{Name: p.cur.Value, IfNotExists: ifNotExists, OrReplace: orReplace}

	// Parse parameters
	if !p.expectPeek(TokenLParen) {
		return nil, errors.New("expected ( after procedure name")
	}

	// Parse parameter list
	for p.peek.Type != TokenRParen {
		p.nextToken()
		if p.cur.Type != TokenIdent {
			return nil, errors.New("expected parameter name")
		}
		paramName := p.cur.Value

		p.nextToken()
		if p.cur.Type != TokenKeyword && p.cur.Type != TokenIdent {
			return nil, errors.New("expected parameter type")
		}
		paramType := p.cur.Value

		stmt.Parameters = append(stmt.Parameters, ProcedureParam{
			Name: paramName,
			Type: paramType,
		})

		if p.peek.Type == TokenComma {
			p.nextToken()
		}
	}

	if !p.expectPeek(TokenRParen) {
		return nil, errors.New("expected ) after parameters")
	}

	// Expect BEGIN
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "BEGIN" {
		return nil, errors.New("expected BEGIN")
	}

	// Parse body statements until END
	// For simplicity, we'll collect raw SQL strings
	for {
		p.nextToken()
		if p.cur.Type == TokenKeyword && p.cur.Value == "END" {
			break
		}
		if p.cur.Type == TokenEOF {
			return nil, errors.New("expected END")
		}

		// Collect tokens until semicolon
		var sqlParts []string
		for p.cur.Type != TokenEOF && p.cur.Value != ";" {
			sqlParts = append(sqlParts, p.cur.Value)
			if p.peek.Type == TokenKeyword && p.peek.Value == "END" {
				break
			}
			if p.peek.Value == ";" {
				p.nextToken()
				break
			}
			p.nextToken()
		}

		if len(sqlParts) > 0 {
			stmt.BodySQL = append(stmt.BodySQL, strings.Join(sqlParts, " "))
		}
	}

	return stmt, nil
}

// parseCall parses a CALL statement.
// Syntax: CALL <procedure_name>([<arg1>, <arg2>, ...])
func (p *Parser) parseCall() (*CallStmt, error) {
	p.nextToken() // Skip CALL

	// Parse procedure name
	if p.cur.Type != TokenIdent {
		return nil, errors.New("expected procedure name")
	}
	stmt := &CallStmt{ProcedureName: p.cur.Value}

	// Parse arguments
	if !p.expectPeek(TokenLParen) {
		return nil, errors.New("expected ( after procedure name")
	}

	// Parse argument list
	for p.peek.Type != TokenRParen {
		p.nextToken()
		if p.cur.Type == TokenString || p.cur.Type == TokenNumber || p.cur.Type == TokenIdent {
			stmt.Arguments = append(stmt.Arguments, p.cur.Value)
		} else if p.cur.Type == TokenKeyword && (p.cur.Value == "NULL" || p.cur.Value == "TRUE" || p.cur.Value == "FALSE") {
			stmt.Arguments = append(stmt.Arguments, p.cur.Value)
		} else {
			return nil, errors.New("expected argument value")
		}

		if p.peek.Type == TokenComma {
			p.nextToken()
		}
	}

	if !p.expectPeek(TokenRParen) {
		return nil, errors.New("expected ) after arguments")
	}

	return stmt, nil
}

// parseDrop parses a DROP statement.
// Syntax: DROP TABLE|INDEX|PROCEDURE|VIEW|TRIGGER|DATABASE [IF EXISTS] <name> ...
func (p *Parser) parseDrop() (Statement, error) {
	p.nextToken() // Skip DROP

	if p.cur.Type != TokenKeyword {
		return nil, errors.New("expected TABLE, INDEX, PROCEDURE, VIEW, TRIGGER, DATABASE, ROLE, or USER after DROP")
	}

	switch p.cur.Value {
	case "TABLE":
		// Parse optional IF EXISTS
		ifExists := false
		p.nextToken()
		if p.cur.Type == TokenKeyword && p.cur.Value == "IF" {
			p.nextToken()
			if p.cur.Type != TokenKeyword || p.cur.Value != "EXISTS" {
				return nil, errors.New("expected EXISTS after IF")
			}
			ifExists = true
			p.nextToken()
		}
		if p.cur.Type != TokenIdent {
			return nil, errors.New("expected table name")
		}
		return &DropTableStmt{TableName: p.cur.Value, IfExists: ifExists}, nil
	case "INDEX":
		// Parse optional IF EXISTS
		ifExists := false
		p.nextToken()
		if p.cur.Type == TokenKeyword && p.cur.Value == "IF" {
			p.nextToken()
			if p.cur.Type != TokenKeyword || p.cur.Value != "EXISTS" {
				return nil, errors.New("expected EXISTS after IF")
			}
			ifExists = true
			p.nextToken()
		}
		if p.cur.Type != TokenIdent {
			return nil, errors.New("expected index name")
		}
		indexName := p.cur.Value

		// Expect ON keyword
		if !p.expectPeek(TokenKeyword) || p.cur.Value != "ON" {
			return nil, errors.New("expected ON after index name")
		}

		// Parse table name
		if !p.expectPeek(TokenIdent) {
			return nil, errors.New("expected table name after ON")
		}
		tableName := p.cur.Value

		return &DropIndexStmt{IndexName: indexName, TableName: tableName, IfExists: ifExists}, nil
	case "PROCEDURE":
		// Parse optional IF EXISTS
		ifExists := false
		p.nextToken()
		if p.cur.Type == TokenKeyword && p.cur.Value == "IF" {
			p.nextToken()
			if p.cur.Type != TokenKeyword || p.cur.Value != "EXISTS" {
				return nil, errors.New("expected EXISTS after IF")
			}
			ifExists = true
			p.nextToken()
		}
		if p.cur.Type != TokenIdent {
			return nil, errors.New("expected procedure name")
		}
		return &DropProcedureStmt{Name: p.cur.Value, IfExists: ifExists}, nil
	case "VIEW":
		// Parse optional IF EXISTS
		ifExists := false
		p.nextToken()
		if p.cur.Type == TokenKeyword && p.cur.Value == "IF" {
			p.nextToken()
			if p.cur.Type != TokenKeyword || p.cur.Value != "EXISTS" {
				return nil, errors.New("expected EXISTS after IF")
			}
			ifExists = true
			p.nextToken()
		}
		if p.cur.Type != TokenIdent {
			return nil, errors.New("expected view name")
		}
		return &DropViewStmt{ViewName: p.cur.Value, IfExists: ifExists}, nil
	case "TRIGGER":
		// Parse optional IF EXISTS
		ifExists := false
		p.nextToken()
		if p.cur.Type == TokenKeyword && p.cur.Value == "IF" {
			p.nextToken()
			if p.cur.Type != TokenKeyword || p.cur.Value != "EXISTS" {
				return nil, errors.New("expected EXISTS after IF")
			}
			ifExists = true
			p.nextToken()
		}
		if p.cur.Type != TokenIdent {
			return nil, errors.New("expected trigger name")
		}
		triggerName := p.cur.Value

		// Expect ON keyword
		if !p.expectPeek(TokenKeyword) || p.cur.Value != "ON" {
			return nil, errors.New("expected ON after trigger name")
		}

		// Parse table name
		if !p.expectPeek(TokenIdent) {
			return nil, errors.New("expected table name after ON")
		}
		tableName := p.cur.Value

		return &DropTriggerStmt{TriggerName: triggerName, TableName: tableName, IfExists: ifExists}, nil
	case "DATABASE":
		// Parse optional IF EXISTS
		ifExists := false
		p.nextToken()
		if p.cur.Type == TokenKeyword && p.cur.Value == "IF" {
			p.nextToken()
			if p.cur.Type != TokenKeyword || p.cur.Value != "EXISTS" {
				return nil, errors.New("expected EXISTS after IF")
			}
			ifExists = true
			p.nextToken()
		}
		if p.cur.Type != TokenIdent && p.cur.Type != TokenKeyword {
			return nil, errors.New("expected database name")
		}
		return &DropDatabaseStmt{DatabaseName: p.cur.Value, IfExists: ifExists}, nil
	case "ROLE":
		// Parse optional IF EXISTS
		ifExists := false
		p.nextToken()
		if p.cur.Type == TokenKeyword && p.cur.Value == "IF" {
			p.nextToken()
			if p.cur.Type != TokenKeyword || p.cur.Value != "EXISTS" {
				return nil, errors.New("expected EXISTS after IF")
			}
			ifExists = true
			p.nextToken()
		}
		if p.cur.Type != TokenIdent && p.cur.Type != TokenKeyword {
			return nil, errors.New("expected role name")
		}
		return &DropRoleStmt{RoleName: p.cur.Value, IfExists: ifExists}, nil
	case "USER":
		// Parse optional IF EXISTS
		ifExists := false
		p.nextToken()
		if p.cur.Type == TokenKeyword && p.cur.Value == "IF" {
			p.nextToken()
			if p.cur.Type != TokenKeyword || p.cur.Value != "EXISTS" {
				return nil, errors.New("expected EXISTS after IF")
			}
			ifExists = true
			p.nextToken()
		}
		if p.cur.Type != TokenIdent {
			return nil, errors.New("expected username")
		}
		return &DropUserStmt{Username: p.cur.Value, IfExists: ifExists}, nil
	default:
		return nil, errors.New("expected TABLE, INDEX, PROCEDURE, VIEW, TRIGGER, DATABASE, ROLE, or USER after DROP")
	}
}

// parseTruncate parses a TRUNCATE TABLE statement.
// Syntax: TRUNCATE TABLE <table_name>
//
// Example: TRUNCATE TABLE logs
//
// Returns a TruncateTableStmt AST node.
func (p *Parser) parseTruncate() (*TruncateTableStmt, error) {
	// Expect TABLE keyword
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "TABLE" {
		return nil, errors.New("expected TABLE after TRUNCATE")
	}

	// Parse the table name
	if !p.expectPeek(TokenIdent) {
		return nil, errors.New("expected table name after TRUNCATE TABLE")
	}

	return &TruncateTableStmt{TableName: p.cur.Value}, nil
}

// parseAlter parses an ALTER statement (ALTER TABLE or ALTER USER).
// Syntax:
//
//	ALTER TABLE <table_name> ADD COLUMN <column_def>
//	ALTER TABLE <table_name> DROP COLUMN <column_name>
//	ALTER TABLE <table_name> RENAME COLUMN <old_name> TO <new_name>
//	ALTER TABLE <table_name> MODIFY COLUMN <column_name> <new_type>
//	ALTER USER <username> IDENTIFIED BY '<new_password>'
//
// Returns an AlterTableStmt or AlterUserStmt AST node.
func (p *Parser) parseAlter() (Statement, error) {
	// Check what follows ALTER
	if !p.expectPeek(TokenKeyword) {
		return nil, errors.New("expected TABLE or USER after ALTER")
	}

	switch p.cur.Value {
	case "USER":
		return p.parseAlterUser()
	case "TABLE":
		return p.parseAlterTable()
	default:
		return nil, errors.New("expected TABLE or USER after ALTER")
	}
}

// parseAlterUser parses an ALTER USER statement.
// Syntax: ALTER USER <username> IDENTIFIED BY '<new_password>'
//
// Example: ALTER USER alice IDENTIFIED BY 'new_secret123'
//
// Returns an AlterUserStmt AST node.
func (p *Parser) parseAlterUser() (*AlterUserStmt, error) {
	// Parse the username
	if !p.expectPeek(TokenIdent) {
		return nil, errors.New("expected username after ALTER USER")
	}
	username := p.cur.Value

	// Expect IDENTIFIED BY keywords
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "IDENTIFIED" {
		return nil, errors.New("expected IDENTIFIED after username")
	}
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "BY" {
		return nil, errors.New("expected BY after IDENTIFIED")
	}

	// Parse the new password (can be string or identifier)
	p.nextToken()
	if p.cur.Type != TokenString && p.cur.Type != TokenIdent {
		return nil, errors.New("expected new password")
	}
	newPassword := p.cur.Value

	return &AlterUserStmt{Username: username, NewPassword: newPassword}, nil
}

// parseAlterTable parses an ALTER TABLE statement.
// Syntax:
//
//	ALTER TABLE <table_name> ADD COLUMN <column_def>
//	ALTER TABLE <table_name> DROP COLUMN <column_name>
//	ALTER TABLE <table_name> RENAME COLUMN <old_name> TO <new_name>
//	ALTER TABLE <table_name> MODIFY COLUMN <column_name> <new_type>
//
// Returns an AlterTableStmt AST node.
func (p *Parser) parseAlterTable() (*AlterTableStmt, error) {

	// Parse the table name
	if !p.expectPeek(TokenIdent) {
		return nil, errors.New("expected table name after ALTER TABLE")
	}
	stmt := &AlterTableStmt{TableName: p.cur.Value}

	// Parse the action
	p.nextToken()
	if p.cur.Type != TokenKeyword {
		return nil, errors.New("expected ADD, DROP, RENAME, or MODIFY after table name")
	}

	switch p.cur.Value {
	case "ADD":
		// Check if it's ADD COLUMN or ADD CONSTRAINT
		p.nextToken()
		if p.cur.Type != TokenKeyword {
			return nil, errors.New("expected COLUMN or CONSTRAINT after ADD")
		}

		if p.cur.Value == "COLUMN" {
			stmt.Action = AlterActionAddColumn
			// Parse column name
			if !p.expectPeek(TokenIdent) {
				return nil, errors.New("expected column name after ADD COLUMN")
			}
			colName := p.cur.Value

			// Parse column type
			if !p.expectPeek(TokenKeyword) {
				return nil, errors.New("expected column type")
			}
			colType := p.cur.Value

			// Parse optional constraints
			constraints, err := p.parseColumnConstraints()
			if err != nil {
				return nil, err
			}

			stmt.ColumnDef = &ColumnDef{
				Name:        colName,
				Type:        colType,
				Constraints: constraints,
			}
		} else if p.cur.Value == "CONSTRAINT" {
			stmt.Action = AlterActionAddConstraint
			constraint, err := p.parseTableConstraint()
			if err != nil {
				return nil, err
			}
			stmt.Constraint = constraint
		} else {
			return nil, errors.New("expected COLUMN or CONSTRAINT after ADD")
		}

	case "DROP":
		// Check if it's DROP COLUMN or DROP CONSTRAINT
		p.nextToken()
		if p.cur.Type != TokenKeyword {
			return nil, errors.New("expected COLUMN or CONSTRAINT after DROP")
		}

		if p.cur.Value == "COLUMN" {
			stmt.Action = AlterActionDropColumn
			// Parse column name
			if !p.expectPeek(TokenIdent) {
				return nil, errors.New("expected column name after DROP COLUMN")
			}
			stmt.ColumnName = p.cur.Value
		} else if p.cur.Value == "CONSTRAINT" {
			stmt.Action = AlterActionDropConstraint
			// Parse constraint name
			if !p.expectPeek(TokenIdent) {
				return nil, errors.New("expected constraint name after DROP CONSTRAINT")
			}
			stmt.ConstraintName = p.cur.Value
		} else {
			return nil, errors.New("expected COLUMN or CONSTRAINT after DROP")
		}

	case "RENAME":
		// Expect COLUMN keyword
		if !p.expectPeek(TokenKeyword) || p.cur.Value != "COLUMN" {
			return nil, errors.New("expected COLUMN after RENAME")
		}
		stmt.Action = AlterActionRenameColumn

		// Parse old column name
		if !p.expectPeek(TokenIdent) {
			return nil, errors.New("expected column name after RENAME COLUMN")
		}
		stmt.ColumnName = p.cur.Value

		// Expect TO keyword
		if !p.expectPeek(TokenKeyword) || p.cur.Value != "TO" {
			return nil, errors.New("expected TO after column name")
		}

		// Parse new column name
		if !p.expectPeek(TokenIdent) {
			return nil, errors.New("expected new column name after TO")
		}
		stmt.NewColumnName = p.cur.Value

	case "MODIFY":
		// Expect COLUMN keyword
		if !p.expectPeek(TokenKeyword) || p.cur.Value != "COLUMN" {
			return nil, errors.New("expected COLUMN after MODIFY")
		}
		stmt.Action = AlterActionModifyColumn

		// Parse column name
		if !p.expectPeek(TokenIdent) {
			return nil, errors.New("expected column name after MODIFY COLUMN")
		}
		stmt.ColumnName = p.cur.Value

		// Parse new column type
		if !p.expectPeek(TokenKeyword) {
			return nil, errors.New("expected new column type")
		}
		stmt.NewColumnType = p.cur.Value

		// Parse optional constraints for the modified column
		constraints, err := p.parseColumnConstraints()
		if err != nil {
			return nil, err
		}
		if len(constraints) > 0 {
			stmt.ColumnDef = &ColumnDef{
				Name:        stmt.ColumnName,
				Type:        stmt.NewColumnType,
				Constraints: constraints,
			}
		}

	default:
		return nil, fmt.Errorf("unexpected ALTER TABLE action: %s", p.cur.Value)
	}

	return stmt, nil
}

// parseCreateView parses a CREATE VIEW statement.
// Syntax: CREATE [OR REPLACE] VIEW [IF NOT EXISTS] <view_name> AS SELECT ...
//
// Examples:
//   CREATE VIEW active_users AS SELECT * FROM users WHERE status = 'active'
//   CREATE VIEW IF NOT EXISTS active_users AS SELECT * FROM users WHERE status = 'active'
//   CREATE OR REPLACE VIEW active_users AS SELECT * FROM users WHERE status = 'active'
//
// Returns a CreateViewStmt AST node.
func (p *Parser) parseCreateView() (*CreateViewStmt, error) {
	p.nextToken() // Skip CREATE

	// Check for OR REPLACE
	orReplace := false
	if p.cur.Type == TokenKeyword && p.cur.Value == "OR" {
		p.nextToken() // consume OR
		if p.cur.Type != TokenKeyword || p.cur.Value != "REPLACE" {
			return nil, errors.New("expected REPLACE after OR")
		}
		orReplace = true
		p.nextToken() // move past REPLACE
	}

	p.nextToken() // Skip VIEW

	// Check for IF NOT EXISTS
	ifNotExists := false
	if p.cur.Type == TokenKeyword && p.cur.Value == "IF" {
		p.nextToken() // consume IF
		if p.cur.Type != TokenKeyword || p.cur.Value != "NOT" {
			return nil, errors.New("expected NOT after IF")
		}
		p.nextToken() // consume NOT
		if p.cur.Type != TokenKeyword || p.cur.Value != "EXISTS" {
			return nil, errors.New("expected EXISTS after IF NOT")
		}
		ifNotExists = true
		p.nextToken() // move to view name
	}

	// Parse the view name
	if p.cur.Type != TokenIdent {
		return nil, errors.New("expected view name")
	}
	viewName := p.cur.Value

	// Expect AS keyword
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "AS" {
		return nil, errors.New("expected AS after view name")
	}

	// Expect SELECT keyword
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "SELECT" {
		return nil, errors.New("expected SELECT after AS")
	}

	// Parse the SELECT statement
	selectStmt, err := p.parseSelectOrUnion()
	if err != nil {
		return nil, err
	}

	// The result could be a SelectStmt or a UnionStmt
	// For now, we only support simple SELECT statements in views
	stmt, ok := selectStmt.(*SelectStmt)
	if !ok {
		return nil, errors.New("only simple SELECT statements are supported in views")
	}

	return &CreateViewStmt{
		ViewName:    viewName,
		IfNotExists: ifNotExists,
		OrReplace:   orReplace,
		Query:       stmt,
	}, nil
}

// parseCreateTrigger parses a CREATE TRIGGER statement.
// Syntax: CREATE [OR REPLACE] TRIGGER [IF NOT EXISTS] <trigger_name> BEFORE|AFTER INSERT|UPDATE|DELETE ON <table_name> FOR EACH ROW EXECUTE <action_sql>
//
// Examples:
//   CREATE TRIGGER log_insert AFTER INSERT ON users FOR EACH ROW EXECUTE INSERT INTO audit_log VALUES ('insert', 'users')
//   CREATE TRIGGER IF NOT EXISTS log_insert AFTER INSERT ON users FOR EACH ROW EXECUTE INSERT INTO audit_log VALUES ('insert', 'users')
//   CREATE OR REPLACE TRIGGER log_insert AFTER INSERT ON users FOR EACH ROW EXECUTE INSERT INTO audit_log VALUES ('insert', 'users')
//
// Returns a CreateTriggerStmt AST node.
func (p *Parser) parseCreateTrigger() (*CreateTriggerStmt, error) {
	p.nextToken() // Skip CREATE

	// Check for OR REPLACE
	orReplace := false
	if p.cur.Type == TokenKeyword && p.cur.Value == "OR" {
		p.nextToken() // consume OR
		if p.cur.Type != TokenKeyword || p.cur.Value != "REPLACE" {
			return nil, errors.New("expected REPLACE after OR")
		}
		orReplace = true
		p.nextToken() // move past REPLACE
	}

	p.nextToken() // Skip TRIGGER

	// Check for IF NOT EXISTS
	ifNotExists := false
	if p.cur.Type == TokenKeyword && p.cur.Value == "IF" {
		p.nextToken() // consume IF
		if p.cur.Type != TokenKeyword || p.cur.Value != "NOT" {
			return nil, errors.New("expected NOT after IF")
		}
		p.nextToken() // consume NOT
		if p.cur.Type != TokenKeyword || p.cur.Value != "EXISTS" {
			return nil, errors.New("expected EXISTS after IF NOT")
		}
		ifNotExists = true
		p.nextToken() // move to trigger name
	}

	// Parse the trigger name
	if p.cur.Type != TokenIdent {
		return nil, errors.New("expected trigger name")
	}
	triggerName := p.cur.Value

	// Parse timing: BEFORE or AFTER
	if !p.expectPeek(TokenKeyword) {
		return nil, errors.New("expected BEFORE or AFTER")
	}
	var timing TriggerTiming
	switch p.cur.Value {
	case "BEFORE":
		timing = TriggerTimingBefore
	case "AFTER":
		timing = TriggerTimingAfter
	default:
		return nil, errors.New("expected BEFORE or AFTER")
	}

	// Parse event: INSERT, UPDATE, or DELETE
	if !p.expectPeek(TokenKeyword) {
		return nil, errors.New("expected INSERT, UPDATE, or DELETE")
	}
	var event TriggerEvent
	switch p.cur.Value {
	case "INSERT":
		event = TriggerEventInsert
	case "UPDATE":
		event = TriggerEventUpdate
	case "DELETE":
		event = TriggerEventDelete
	default:
		return nil, errors.New("expected INSERT, UPDATE, or DELETE")
	}

	// Expect ON keyword
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "ON" {
		return nil, errors.New("expected ON")
	}

	// Parse table name
	if !p.expectPeek(TokenIdent) {
		return nil, errors.New("expected table name")
	}
	tableName := p.cur.Value

	// Expect FOR EACH ROW
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "FOR" {
		return nil, errors.New("expected FOR EACH ROW")
	}
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "EACH" {
		return nil, errors.New("expected EACH after FOR")
	}
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "ROW" {
		return nil, errors.New("expected ROW after EACH")
	}

	// Expect EXECUTE keyword
	if !p.expectPeek(TokenKeyword) || p.cur.Value != "EXECUTE" {
		return nil, errors.New("expected EXECUTE")
	}

	// Parse the action SQL - everything after EXECUTE is the action
	// We need to capture the remaining tokens as the action SQL
	var actionSQL string
	p.nextToken() // Move to first token of action

	// Collect all remaining tokens as the action SQL
	for p.cur.Type != TokenEOF {
		if actionSQL != "" {
			actionSQL += " "
		}
		actionSQL += p.cur.Value
		p.nextToken()
	}

	if actionSQL == "" {
		return nil, errors.New("expected SQL statement after EXECUTE")
	}

	return &CreateTriggerStmt{
		TriggerName: triggerName,
		IfNotExists: ifNotExists,
		OrReplace:   orReplace,
		Timing:      timing,
		Event:       event,
		TableName:   tableName,
		ActionSQL:   actionSQL,
	}, nil
}


// =============================================================================
// Database Management Parsing
// =============================================================================

// parseCreateDatabase parses a CREATE DATABASE statement.
// Syntax: CREATE DATABASE [IF NOT EXISTS] <database_name>
//
// Examples:
//   - CREATE DATABASE myapp
//   - CREATE DATABASE IF NOT EXISTS test_db
//   - CREATE DATABASE myapp WITH ENCODING 'UTF8' LOCALE 'en_US'
//
// Returns a CreateDatabaseStmt AST node.
func (p *Parser) parseCreateDatabase() (*CreateDatabaseStmt, error) {
	// Skip CREATE and DATABASE keywords (already validated)
	p.nextToken() // Skip CREATE
	p.nextToken() // Skip DATABASE

	// Parse optional IF NOT EXISTS
	ifNotExists := false
	if p.cur.Type == TokenKeyword && p.cur.Value == "IF" {
		p.nextToken()
		if p.cur.Type != TokenKeyword || p.cur.Value != "NOT" {
			return nil, errors.New("expected NOT after IF")
		}
		p.nextToken()
		if p.cur.Type != TokenKeyword || p.cur.Value != "EXISTS" {
			return nil, errors.New("expected EXISTS after NOT")
		}
		ifNotExists = true
		p.nextToken()
	}

	// Parse the database name (can be identifier or keyword like 'default')
	if p.cur.Type != TokenIdent && p.cur.Type != TokenKeyword {
		return nil, errors.New("expected database name")
	}
	databaseName := p.cur.Value

	stmt := &CreateDatabaseStmt{
		DatabaseName: databaseName,
		IfNotExists:  ifNotExists,
	}

	// Check for optional WITH clause
	p.nextToken()
	if p.cur.Type == TokenKeyword && p.cur.Value == "WITH" {
		p.nextToken()

		// Parse options
		for {
			if p.cur.Type == TokenEOF {
				break
			}

			if p.cur.Type != TokenKeyword && p.cur.Type != TokenIdent {
				break
			}

			optionName := strings.ToUpper(p.cur.Value)
			p.nextToken()

			// Expect '=' or value directly
			if p.cur.Type == TokenEqual {
				p.nextToken()
			}

			// Get the value
			var optionValue string
			if p.cur.Type == TokenString {
				optionValue = p.cur.Value
			} else if p.cur.Type == TokenIdent || p.cur.Type == TokenKeyword {
				optionValue = p.cur.Value
			} else {
				return nil, fmt.Errorf("expected value for option %s", optionName)
			}

			switch optionName {
			case "ENCODING":
				stmt.Encoding = optionValue
			case "LOCALE", "LC_COLLATE", "LC_CTYPE":
				stmt.Locale = optionValue
			case "COLLATION", "COLLATE":
				stmt.Collation = optionValue
			case "OWNER":
				stmt.Owner = optionValue
			case "DESCRIPTION", "COMMENT":
				stmt.Description = optionValue
			default:
				return nil, fmt.Errorf("unknown database option: %s", optionName)
			}

			p.nextToken()
		}
	}

	return stmt, nil
}

// parseUse parses a USE statement.
// Syntax: USE <database_name>
//
// Examples:
//   - USE myapp
//   - USE production
//
// Returns a UseDatabaseStmt AST node.
func (p *Parser) parseUse() (*UseDatabaseStmt, error) {
	// Skip USE keyword
	p.nextToken()

	// Parse the database name (can be identifier or keyword like 'default')
	if p.cur.Type != TokenIdent && p.cur.Type != TokenKeyword {
		return nil, errors.New("expected database name after USE")
	}
	databaseName := p.cur.Value

	return &UseDatabaseStmt{
		DatabaseName: databaseName,
	}, nil
}
