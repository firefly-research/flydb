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
	TokenEOF              TokenType = iota // End of input
	TokenIdent                             // Identifier (table name, column name)
	TokenString                            // String literal ('hello')
	TokenNumber                            // Numeric literal (123)
	TokenKeyword                           // SQL keyword (SELECT, FROM, etc.)
	TokenComma                             // Comma (,)
	TokenLParen                            // Left parenthesis (()
	TokenRParen                            // Right parenthesis ())
	TokenEqual                             // Equals sign (=)
	TokenLessThan                          // Less than (<)
	TokenGreaterThan                       // Greater than (>)
	TokenLessEqual                         // Less than or equal (<=)
	TokenGreaterEqual                      // Greater than or equal (>=)
	TokenNotEqual                          // Not equal (<> or !=)
	TokenPlus                              // Plus (+)
	TokenMinus                             // Minus (-)
	TokenStar                              // Asterisk/multiply (*)
	TokenSlash                             // Divide (/)
	TokenPercent                           // Modulo (%)
	TokenConcat                            // String concatenation (||)
	TokenSemicolon                         // Semicolon (;)
	TokenDot                               // Dot (.)
	TokenJSONArrow                         // JSON field access (->)
	TokenJSONArrowText                     // JSON field access as text (->>)
	TokenJSONContains                      // JSON contains (@>)
	TokenJSONContainedBy                   // JSON contained by (<@)
	TokenJSONKeyExists                     // JSON key exists (?)
	TokenJSONAllKeysExist                  // JSON all keys exist (?&)
	TokenJSONAnyKeyExists                  // JSON any key exists (?|)
)

// Token represents a single lexical unit from the input.
// It contains the token type, the literal value from the input,
// and the position where it was found.
type Token struct {
	Type   TokenType // The category of this token
	Value  string    // The literal value from the input
	Line   int       // Line number where the token starts (1-based)
	Column int       // Column number where the token starts (1-based)
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
	line  int    // Current line number
	col   int    // Current column number
}

// NewLexer creates a new Lexer for the given input string.
// The lexer starts at position 0, ready to tokenize from the beginning.
//
// Parameters:
//   - input: The SQL string to tokenize
//
// Returns a new Lexer ready to produce tokens.
func NewLexer(input string) *Lexer {
	return &Lexer{
		input: input,
		line:  1,
		col:   1,
	}
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

	// Capture the start position of the token.
	startLine := l.line
	startCol := l.col

	// Check for end of input.
	if l.pos >= len(l.input) {
		return Token{Type: TokenEOF, Line: startLine, Column: startCol}
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
			l.advance()
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
			"MONEY", "CLOB", "NCHAR", "NVARCHAR", "NTEXT",
			// Boolean literals
			"TRUE", "FALSE",
			// Prepared statements
			"PREPARE", "EXECUTE", "DEALLOCATE", "AS", "USING",
			// Aggregate functions
			"COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT", "STRING_AGG",
			// GROUP BY and HAVING
			"GROUP", "HAVING",
			// Inspection
			"INSPECT",
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
			"LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS", "NATURAL",
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
			"CONFLICT", "NOTHING",
			// Database management
			"DATABASE", "USE", "DATABASES",
			// RBAC keywords
			"ROLE", "ROLES", "PRIVILEGES", "DESCRIPTION", "WITH",
			// CASE expressions
			"CASE", "WHEN",
			// Window functions
			"OVER", "PARTITION", "WINDOW", "ROWS", "RANGE", "UNBOUNDED", "PRECEDING", "FOLLOWING", "CURRENT",
			// ORDER BY extensions
			"NULLS", "FIRST", "LAST",
			// Additional aggregate functions (window functions)
			"FIRST_VALUE", "LAST_VALUE", "NTH_VALUE", "LAG", "LEAD", "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE",
			// Additional string functions (LEFT already defined in JOIN types)
			"POSITION", "LOCATE", "INSTR", "LPAD", "RPAD", "SPACE", "ASCII", "CHAR_LENGTH", "CHARACTER_LENGTH",
			// Additional math functions (TRUNCATE already defined above)
			"SIGN", "EXP", "LOG", "LOG10", "LOG2", "LN", "PI", "RAND", "RANDOM",
			// Additional date functions
			"DATE_FORMAT", "TIME_FORMAT", "STRFTIME", "TO_CHAR", "TO_DATE", "TO_TIMESTAMP",
			"DATE_TRUNC", "DATE_PART", "INTERVAL",
			// Type casting
			"TYPEOF", "TYPE",
			// JSON functions
			"JSON_EXTRACT", "JSON_EXTRACT_TEXT", "JSON_ARRAY_LENGTH", "JSON_KEYS",
			"JSON_TYPEOF", "JSON_VALID", "JSON_SET", "JSON_REMOVE", "JSON_MERGE",
			"JSON_ARRAY_APPEND", "JSON_OBJECT", "JSON_ARRAY":
			return Token{Type: TokenKeyword, Value: upper, Line: startLine, Column: startCol}
		}

		// Not a keyword - return as identifier.
		return Token{Type: TokenIdent, Value: lit, Line: startLine, Column: startCol}
	}

	// Number: starts with digit.
	// Supports integers and decimal numbers (e.g., 123, 3.14, 0.5).
	if unicode.IsDigit(rune(ch)) {
		start := l.pos

		// Consume all digits before decimal point.
		for l.pos < len(l.input) && unicode.IsDigit(rune(l.input[l.pos])) {
			l.advance()
		}

		// Check for decimal point followed by more digits.
		if l.pos < len(l.input) && l.input[l.pos] == '.' {
			// Look ahead to ensure there's at least one digit after the decimal point.
			if l.pos+1 < len(l.input) && unicode.IsDigit(rune(l.input[l.pos+1])) {
				l.advance() // Consume the decimal point.
				// Consume all digits after decimal point.
				for l.pos < len(l.input) && unicode.IsDigit(rune(l.input[l.pos])) {
					l.advance()
				}
			}
		}

		return Token{Type: TokenNumber, Value: l.input[start:l.pos], Line: startLine, Column: startCol}
	}

	// String literal: enclosed in single quotes.
	// Example: 'hello world'
	if ch == '\'' {
		l.advance() // Skip opening quote
		start := l.pos

		// Consume until closing quote.
		for l.pos < len(l.input) && l.input[l.pos] != '\'' {
			l.advance()
		}

		lit := l.input[start:l.pos]

		// Skip closing quote if present.
		if l.pos < len(l.input) {
			l.advance()
		}

		return Token{Type: TokenString, Value: lit, Line: startLine, Column: startCol}
	}

	// Handle $N parameter placeholders for prepared statements.
	// Example: $1, $2, $10
	if ch == '$' {
		start := l.pos
		l.advance() // Skip the $

		// Consume digits after $
		for l.pos < len(l.input) && unicode.IsDigit(rune(l.input[l.pos])) {
			l.advance()
		}

		// Return the entire placeholder as an identifier
		return Token{Type: TokenIdent, Value: l.input[start:l.pos], Line: startLine, Column: startCol}
	}

	// Multi-character operators (check before single-character).
	if ch == '<' {
		l.advance()
		if l.pos < len(l.input) {
			switch l.input[l.pos] {
			case '=':
				l.advance()
				return Token{Type: TokenLessEqual, Value: "<=", Line: startLine, Column: startCol}
			case '>':
				l.advance()
				return Token{Type: TokenNotEqual, Value: "<>", Line: startLine, Column: startCol}
			case '@':
				// JSON contained by operator (<@)
				l.advance()
				return Token{Type: TokenJSONContainedBy, Value: "<@", Line: startLine, Column: startCol}
			}
		}
		return Token{Type: TokenLessThan, Value: "<", Line: startLine, Column: startCol}
	}
	if ch == '>' {
		l.advance()
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.advance()
			return Token{Type: TokenGreaterEqual, Value: ">=", Line: startLine, Column: startCol}
		}
		return Token{Type: TokenGreaterThan, Value: ">", Line: startLine, Column: startCol}
	}
	if ch == '@' {
		l.advance()
		if l.pos < len(l.input) && l.input[l.pos] == '>' {
			// JSON contains operator (@>)
			l.advance()
			return Token{Type: TokenJSONContains, Value: "@>", Line: startLine, Column: startCol}
		}
		// Single @ is not a valid SQL operator, return EOF
		return Token{Type: TokenEOF, Line: startLine, Column: startCol}
	}
	if ch == '?' {
		l.advance()
		if l.pos < len(l.input) {
			switch l.input[l.pos] {
			case '&':
				// JSON all keys exist operator (?&)
				l.advance()
				return Token{Type: TokenJSONAllKeysExist, Value: "?&", Line: startLine, Column: startCol}
			case '|':
				// JSON any key exists operator (?|)
				l.advance()
				return Token{Type: TokenJSONAnyKeyExists, Value: "?|", Line: startLine, Column: startCol}
			}
		}
		return Token{Type: TokenJSONKeyExists, Value: "?", Line: startLine, Column: startCol}
	}
	if ch == '!' {
		l.advance()
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.advance()
			return Token{Type: TokenNotEqual, Value: "!=", Line: startLine, Column: startCol}
		}
		// Single ! is not a valid SQL operator, return EOF
		return Token{Type: TokenEOF, Line: startLine, Column: startCol}
	}
	if ch == '|' {
		l.advance()
		if l.pos < len(l.input) && l.input[l.pos] == '|' {
			l.advance()
			return Token{Type: TokenConcat, Value: "||", Line: startLine, Column: startCol}
		}
		// Single | is not commonly used in SQL, return EOF
		return Token{Type: TokenEOF, Line: startLine, Column: startCol}
	}
	// Handle comments and JSON arrow operators
	if ch == '-' {
		if l.pos+1 < len(l.input) {
			next := l.input[l.pos+1]
			if next == '-' {
				// Single-line comment: skip until end of line
				l.advance()
				l.advance()
				for l.pos < len(l.input) && l.input[l.pos] != '\n' {
					l.advance()
				}
				// Skip the newline and continue lexing
				if l.pos < len(l.input) {
					l.advance()
				}
				return l.NextToken()
			}
			if next == '>' {
				// JSON arrow operators (-> or ->>)
				l.advance()
				l.advance()
				if l.pos < len(l.input) && l.input[l.pos] == '>' {
					l.advance()
					return Token{Type: TokenJSONArrowText, Value: "->>", Line: startLine, Column: startCol}
				}
				return Token{Type: TokenJSONArrow, Value: "->", Line: startLine, Column: startCol}
			}
		}
		l.advance()
		return Token{Type: TokenMinus, Value: "-", Line: startLine, Column: startCol}
	}
	if ch == '/' {
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '*' {
			// Multi-line comment: skip until */
			l.advance()
			l.advance()
			for l.pos+1 < len(l.input) {
				if l.input[l.pos] == '*' && l.input[l.pos+1] == '/' {
					l.advance()
					l.advance()
					break
				}
				l.advance()
			}
			return l.NextToken()
		}
		l.advance()
		return Token{Type: TokenSlash, Value: "/", Line: startLine, Column: startCol}
	}

	// Single-character tokens.
	l.advance()
	switch ch {
	case ',':
		return Token{Type: TokenComma, Value: ",", Line: startLine, Column: startCol}
	case '(':
		return Token{Type: TokenLParen, Value: "(", Line: startLine, Column: startCol}
	case ')':
		return Token{Type: TokenRParen, Value: ")", Line: startLine, Column: startCol}
	case '=':
		return Token{Type: TokenEqual, Value: "=", Line: startLine, Column: startCol}
	case '+':
		return Token{Type: TokenPlus, Value: "+", Line: startLine, Column: startCol}
	case '*':
		return Token{Type: TokenStar, Value: "*", Line: startLine, Column: startCol}
	case '%':
		return Token{Type: TokenPercent, Value: "%", Line: startLine, Column: startCol}
	case ';':
		return Token{Type: TokenSemicolon, Value: ";", Line: startLine, Column: startCol}
	case '.':
		return Token{Type: TokenDot, Value: ".", Line: startLine, Column: startCol}
	}

	// Unknown character - return EOF.
	// A production lexer would return an error token here.
	return Token{Type: TokenEOF, Line: startLine, Column: startCol}
}

// advance moves the lexer's position forward by one character
// and updates the line and column counters.
func (l *Lexer) advance() {
	if l.pos < len(l.input) {
		if l.input[l.pos] == '\n' {
			l.line++
			l.col = 1
		} else {
			l.col++
		}
		l.pos++
	}
}

// skipWhitespace advances the position past any whitespace characters.
// Whitespace includes spaces, tabs, newlines, and other Unicode space characters.
func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) && unicode.IsSpace(rune(l.input[l.pos])) {
		l.advance()
	}
}
