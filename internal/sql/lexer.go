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
Package sql contains the Lexer component for SQL tokenization.

Lexer Overview:
===============

The Lexer (also called Scanner or Tokenizer) is the first stage of the
SQL processing pipeline. It transforms a raw SQL string into a stream
of tokens that the Parser can understand.

Lexical Analysis Process:
=========================

	Input: "SELECT name FROM users WHERE id = 1"

	Output Tokens:
	  1. {TokenKeyword, "SELECT"}
	  2. {TokenIdent, "name"}
	  3. {TokenKeyword, "FROM"}
	  4. {TokenIdent, "users"}
	  5. {TokenKeyword, "WHERE"}
	  6. {TokenIdent, "id"}
	  7. {TokenEqual, "="}
	  8. {TokenNumber, "1"}
	  9. {TokenEOF, ""}

Token Types:
============

The Lexer recognizes these token types:

  - TokenEOF: End of input
  - TokenIdent: Identifiers (table names, column names)
  - TokenString: String literals ('hello')
  - TokenNumber: Numeric literals (123)
  - TokenKeyword: SQL keywords (SELECT, FROM, WHERE, etc.)
  - TokenComma: Comma separator (,)
  - TokenLParen: Left parenthesis (()
  - TokenRParen: Right parenthesis ())
  - TokenEqual: Equals sign (=)

Keywords:
=========

The following words are recognized as keywords (case-insensitive):

  DDL: CREATE, TABLE, TEXT, INT
  DML: SELECT, INSERT, INTO, VALUES, UPDATE, SET, DELETE
  Clauses: FROM, WHERE, JOIN, ON, ORDER, LIMIT, ASC, DESC
  Auth: USER, IDENTIFIED, BY, GRANT, TO
  Special: NULL

Identifier Rules:
=================

Identifiers can contain:
  - Letters (a-z, A-Z)
  - Digits (0-9) - but not as the first character
  - Underscores (_)
  - Dots (.) - for qualified names like "users.id"
  - Asterisk (*) - for SELECT *

String Literals:
================

String literals are enclosed in single quotes:
  'hello world'
  'user@example.com'

The Lexer does not currently support escape sequences.

Usage Example:
==============

	lexer := sql.NewLexer("SELECT * FROM users")
	for {
	    token := lexer.NextToken()
	    if token.Type == sql.TokenEOF {
	        break
	    }
	    fmt.Printf("%v: %s\n", token.Type, token.Value)
	}
*/
package sql

import (
	"strings"
	"unicode"
)

// TokenType represents the type of a lexical token.
// Each token type corresponds to a category of SQL syntax elements.
type TokenType int

// Token type constants.
// These are used to identify what kind of token was recognized.
const (
	TokenEOF       TokenType = iota // End of input
	TokenIdent                      // Identifier (table name, column name)
	TokenString                     // String literal ('hello')
	TokenNumber                     // Numeric literal (123)
	TokenKeyword                    // SQL keyword (SELECT, FROM, etc.)
	TokenComma                      // Comma (,)
	TokenLParen                     // Left parenthesis (()
	TokenRParen                     // Right parenthesis ())
	TokenEqual                      // Equals sign (=)
	TokenLessThan                   // Less than (<)
	TokenGreaterThan                // Greater than (>)
	TokenLessEqual                  // Less than or equal (<=)
	TokenGreaterEqual               // Greater than or equal (>=)
)

// Token represents a single lexical unit from the input.
// It contains the token type and the literal value from the input.
type Token struct {
	Type  TokenType // The category of this token
	Value string    // The literal value from the input
}

// Lexer transforms an input string into a stream of tokens.
// It maintains the current position in the input and provides
// the NextToken() method to retrieve tokens one at a time.
//
// The Lexer is stateful - each call to NextToken() advances
// the position in the input string.
type Lexer struct {
	input string // The SQL input string
	pos   int    // Current position in the input
}

// NewLexer creates a new Lexer for the given input string.
// The lexer starts at position 0, ready to tokenize from the beginning.
//
// Parameters:
//   - input: The SQL string to tokenize
//
// Returns a new Lexer ready to produce tokens.
func NewLexer(input string) *Lexer {
	return &Lexer{input: input}
}

// NextToken advances the lexer and returns the next token.
// It skips whitespace, then identifies the next token based on
// the current character.
//
// Token recognition order:
//  1. Check for end of input (return TokenEOF)
//  2. Check for identifier/keyword (starts with letter or *)
//  3. Check for number (starts with digit)
//  4. Check for string literal (starts with ')
//  5. Check for single-character tokens (, ( ) =)
//
// Returns the next Token, or TokenEOF if at end of input.
func (l *Lexer) NextToken() Token {
	// Skip any whitespace before the next token.
	l.skipWhitespace()

	// Check for end of input.
	if l.pos >= len(l.input) {
		return Token{Type: TokenEOF}
	}

	ch := l.input[l.pos]

	// Identifier or keyword: starts with letter or asterisk.
	// Identifiers can contain letters, digits, underscores, dots, and asterisks.
	if unicode.IsLetter(rune(ch)) || ch == '*' {
		start := l.pos

		// Consume all valid identifier characters.
		for l.pos < len(l.input) && (unicode.IsLetter(rune(l.input[l.pos])) ||
			unicode.IsDigit(rune(l.input[l.pos])) ||
			l.input[l.pos] == '_' ||
			l.input[l.pos] == '*' ||
			l.input[l.pos] == '.') {
			l.pos++
		}

		lit := l.input[start:l.pos]
		upper := strings.ToUpper(lit)

		// Check if this identifier is a reserved keyword.
		// Keywords are case-insensitive, so we compare uppercase.
		switch upper {
		case "CREATE", "TABLE", "INSERT", "INTO", "VALUES",
			"SELECT", "FROM", "WHERE", "TEXT", "INT",
			"JOIN", "ON", "USER", "IDENTIFIED", "BY",
			"GRANT", "REVOKE", "TO", "UPDATE", "SET", "DELETE",
			"ORDER", "LIMIT", "OFFSET", "ASC", "DESC", "NULL",
			"BEGIN", "COMMIT", "ROLLBACK", "TRANSACTION", "INDEX", "DROP",
			// Extended column types
			"BOOLEAN", "BOOL", "FLOAT", "DOUBLE", "REAL", "TIMESTAMP", "DATETIME",
			"DATE", "BLOB", "BYTEA", "BINARY", "VARBINARY", "UUID", "JSONB", "JSON",
			"BIGINT", "SMALLINT", "TINYINT", "INTEGER", "DECIMAL", "NUMERIC",
			"TIME", "VARCHAR", "CHAR", "CHARACTER", "SERIAL",
			// Boolean literals
			"TRUE", "FALSE",
			// Prepared statements
			"PREPARE", "EXECUTE", "DEALLOCATE", "AS", "USING",
			// Aggregate functions
			"COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT", "STRING_AGG",
			// GROUP BY and HAVING
			"GROUP", "HAVING",
			// Introspection
			"INTROSPECT",
			// Constraints
			"PRIMARY", "KEY", "FOREIGN", "REFERENCES", "NOT", "UNIQUE",
			"AUTO_INCREMENT", "CONSTRAINT", "DEFAULT", "CHECK", "CASCADE",
			"RESTRICT", "NO", "ACTION",
			// DISTINCT, UNION, and set operations
			"DISTINCT", "UNION", "ALL", "INTERSECT", "EXCEPT",
			// Stored procedures
			"PROCEDURE", "FUNCTION", "CALL", "RETURNS", "RETURN",
			"DECLARE", "IF", "THEN", "ELSE", "END", "WHILE", "DO", "FOR", "LOOP",
			// Subquery keywords
			"IN", "EXISTS", "ANY", "SOME",
			// Comparison operators as keywords
			"AND", "OR", "BETWEEN", "LIKE", "IS",
			// ALTER TABLE keywords
			"ALTER", "ADD", "COLUMN", "RENAME", "MODIFY",
			// Views
			"VIEW",
			// Triggers
			"TRIGGER", "BEFORE", "AFTER", "EACH", "ROW",
			// TRUNCATE
			"TRUNCATE",
			// JOIN types
			"LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS",
			// Transaction control
			"SAVEPOINT", "RELEASE",
			// SQL functions
			"UPPER", "LOWER", "CONCAT", "SUBSTRING", "SUBSTR", "TRIM", "LENGTH", "LEN",
			"REPLACE", "LTRIM", "RTRIM", "REVERSE", "REPEAT",
			"ABS", "ROUND", "CEIL", "CEILING", "FLOOR", "MOD", "POWER", "POW", "SQRT",
			"NOW", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
			"DATE_ADD", "DATE_SUB", "DATEDIFF", "DATEADD", "EXTRACT",
			"YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND",
			"COALESCE", "NULLIF", "IFNULL", "NVL", "ISNULL",
			"CAST", "CONVERT",
			// UPSERT
			"CONFLICT", "NOTHING":
			return Token{Type: TokenKeyword, Value: upper}
		}

		// Not a keyword - return as identifier.
		return Token{Type: TokenIdent, Value: lit}
	}

	// Number: starts with digit.
	// Supports integers and decimal numbers (e.g., 123, 3.14, 0.5).
	if unicode.IsDigit(rune(ch)) {
		start := l.pos

		// Consume all digits before decimal point.
		for l.pos < len(l.input) && unicode.IsDigit(rune(l.input[l.pos])) {
			l.pos++
		}

		// Check for decimal point followed by more digits.
		if l.pos < len(l.input) && l.input[l.pos] == '.' {
			// Look ahead to ensure there's at least one digit after the decimal point.
			if l.pos+1 < len(l.input) && unicode.IsDigit(rune(l.input[l.pos+1])) {
				l.pos++ // Consume the decimal point.
				// Consume all digits after decimal point.
				for l.pos < len(l.input) && unicode.IsDigit(rune(l.input[l.pos])) {
					l.pos++
				}
			}
		}

		return Token{Type: TokenNumber, Value: l.input[start:l.pos]}
	}

	// String literal: enclosed in single quotes.
	// Example: 'hello world'
	if ch == '\'' {
		l.pos++ // Skip opening quote
		start := l.pos

		// Consume until closing quote.
		for l.pos < len(l.input) && l.input[l.pos] != '\'' {
			l.pos++
		}

		lit := l.input[start:l.pos]

		// Skip closing quote if present.
		if l.pos < len(l.input) {
			l.pos++
		}

		return Token{Type: TokenString, Value: lit}
	}

	// Handle $N parameter placeholders for prepared statements.
	// Example: $1, $2, $10
	if ch == '$' {
		start := l.pos
		l.pos++ // Skip the $

		// Consume digits after $
		for l.pos < len(l.input) && unicode.IsDigit(rune(l.input[l.pos])) {
			l.pos++
		}

		// Return the entire placeholder as an identifier
		return Token{Type: TokenIdent, Value: l.input[start:l.pos]}
	}

	// Multi-character operators (check before single-character).
	if ch == '<' {
		l.pos++
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++
			return Token{Type: TokenLessEqual, Value: "<="}
		}
		return Token{Type: TokenLessThan, Value: "<"}
	}
	if ch == '>' {
		l.pos++
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++
			return Token{Type: TokenGreaterEqual, Value: ">="}
		}
		return Token{Type: TokenGreaterThan, Value: ">"}
	}

	// Single-character tokens.
	l.pos++
	switch ch {
	case ',':
		return Token{Type: TokenComma, Value: ","}
	case '(':
		return Token{Type: TokenLParen, Value: "("}
	case ')':
		return Token{Type: TokenRParen, Value: ")"}
	case '=':
		return Token{Type: TokenEqual, Value: "="}
	}

	// Unknown character - return EOF.
	// A production lexer would return an error token here.
	return Token{Type: TokenEOF}
}

// skipWhitespace advances the position past any whitespace characters.
// Whitespace includes spaces, tabs, newlines, and other Unicode space characters.
func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) && unicode.IsSpace(rune(l.input[l.pos])) {
		l.pos++
	}
}
