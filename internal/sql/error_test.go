package sql

import (
	"strings"
	"testing"
)

func TestImprovedSyntaxErrors(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Missing ( in CREATE TABLE",
			input:    "CREATE TABLE test;",
			expected: "expected (, found \";\" - at line 1, col 18",
		},
		{
			name:     "Missing ( in CREATE TABLE (EOF case)",
			input:    "CREATE TABLE test",
			expected: "expected opening parenthesis '(' after table name, found \"EOF\" - at line 1, col 18",
		},
		{
			name:     "Missing table name in CREATE TABLE",
			input:    "CREATE TABLE (id INT);",
			expected: "expected table name, found \"(\" - at line 1, col 14",
		},
		{
			name:     "Invalid column type",
			input:    "CREATE TABLE test (id 123);",
			expected: "expected column type, found \"123\" - at line 1, col 23",
		},
		{
			name:     "Missing INTO in INSERT",
			input:    "INSERT test VALUES (1);",
			expected: "expected INTO, found \"test\" - at line 1, col 8",
		},
		{
			name:     "Missing FROM in SELECT",
			input:    "SELECT name users;",
			expected: "expected comma or FROM, found \"users\" - at line 1, col 13",
		},
		{
			name:     "Invalid operator in WHERE",
			input:    "SELECT * FROM users WHERE id ?? 1;",
			expected: "expected value in WHERE - at line 1, col 33",
		},
		{
			name:     "Missing ( in function call",
			input:    "SELECT COUNT * FROM users;",
			expected: "expected ( after COUNT, found \"*\" - at line 1, col 14",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			parser := NewParser(lexer)
			_, err := parser.Parse()
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.expected) {
				t.Errorf("expected error containing %q, got %q", tt.expected, err.Error())
			}
		})
	}
}

func TestKeywordsAsIdentifiers(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "CREATE TABLE text",
			input: "CREATE TABLE text(id SERIAL PRIMARY KEY, name TEXT);",
		},
		{
			name:  "CREATE TABLE int",
			input: "CREATE TABLE int(id INT);",
		},
		{
			name:  "DROP TABLE text",
			input: "DROP TABLE text;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			parser := NewParser(lexer)
			_, err := parser.Parse()
			if err != nil {
				t.Fatalf("expected no error for %s, got: %v", tt.name, err)
			}
		})
	}
}
