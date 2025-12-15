package normalizer

import (
	"testing"
)

func TestNormalizer_Normalize(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		options  Options
	}{
		{
			name:     "basic query unchanged",
			input:    "SELECT * FROM users",
			expected: "SELECT * FROM users",
			options:  DefaultOptions(),
		},
		{
			name:     "normalizes whitespace",
			input:    "SELECT  *  FROM  users",
			expected: "SELECT * FROM users",
			options:  DefaultOptions(),
		},
		{
			name:     "normalizes newlines",
			input:    "SELECT *\nFROM users\nWHERE id = ?",
			expected: "SELECT * FROM users WHERE id = ?",
			options:  DefaultOptions(),
		},
		{
			name:     "normalizes tabs",
			input:    "SELECT\t*\tFROM\tusers",
			expected: "SELECT * FROM users",
			options:  DefaultOptions(),
		},
		{
			name:     "uppercases keywords",
			input:    "select * from users where id = ?",
			expected: "SELECT * FROM users WHERE id = ?",
			options:  DefaultOptions(),
		},
		{
			name:     "removes backticks",
			input:    "SELECT * FROM `users` WHERE `id` = ?",
			expected: "SELECT * FROM users WHERE id = ?",
			options:  DefaultOptions(),
		},
		{
			name:     "removes double quotes",
			input:    `SELECT * FROM "users" WHERE "id" = ?`,
			expected: "SELECT * FROM users WHERE id = ?",
			options:  DefaultOptions(),
		},
		{
			name:     "removes brackets",
			input:    "SELECT * FROM [users] WHERE [id] = ?",
			expected: "SELECT * FROM users WHERE id = ?",
			options:  DefaultOptions(),
		},
		{
			name:     "unifies PostgreSQL placeholders",
			input:    "SELECT * FROM users WHERE id = $1 AND name = $2",
			expected: "SELECT * FROM users WHERE id = ? AND name = ?",
			options:  DefaultOptions(),
		},
		{
			name:     "unifies named placeholders",
			input:    "SELECT * FROM users WHERE id = :id AND name = :name",
			expected: "SELECT * FROM users WHERE id = ? AND name = ?",
			options:  DefaultOptions(),
		},
		{
			name:     "unifies SQL Server placeholders",
			input:    "SELECT * FROM users WHERE id = @id AND name = @name",
			expected: "SELECT * FROM users WHERE id = ? AND name = ?",
			options:  DefaultOptions(),
		},
		{
			name:     "removes single-line comments",
			input:    "SELECT * FROM users -- this is a comment",
			expected: "SELECT * FROM users",
			options:  DefaultOptions(),
		},
		{
			name:     "removes multi-line comments",
			input:    "SELECT * /* comment */ FROM users",
			expected: "SELECT * FROM users",
			options:  DefaultOptions(),
		},
		{
			name:     "complex query normalization",
			input:    "select `id`, `name` from `users` where `age` > $1 and `status` = $2",
			expected: "SELECT id, name FROM users WHERE age > ? AND status = ?",
			options:  DefaultOptions(),
		},
		{
			name:  "preserves keywords case when disabled",
			input: "select * from users",
			expected: "select * from users",
			options: Options{
				UnifyPlaceholders: true,
				RemoveComments:    true,
				UppercaseKeywords: false,
				RemoveQuotes:      true,
			},
		},
		{
			name:  "preserves quotes when disabled",
			input: "SELECT * FROM `users`",
			expected: "SELECT * FROM `users`",
			options: Options{
				UnifyPlaceholders: true,
				RemoveComments:    true,
				UppercaseKeywords: true,
				RemoveQuotes:      false,
			},
		},
		{
			name:  "preserves placeholders when disabled",
			input: "SELECT * FROM users WHERE id = $1",
			expected: "SELECT * FROM users WHERE id = $1",
			options: Options{
				UnifyPlaceholders: false,
				RemoveComments:    true,
				UppercaseKeywords: true,
				RemoveQuotes:      true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := New(tt.options)
			result := n.Normalize(tt.input)
			if result != tt.expected {
				t.Errorf("Normalize(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestNormalizer_AllKeywords(t *testing.T) {
	n := NewDefault()

	// Test all SQL keywords are uppercased
	keywords := []string{
		"select", "from", "where", "and", "or", "not", "in", "is", "null",
		"insert", "into", "values", "update", "set", "delete",
		"join", "left", "right", "inner", "outer", "cross", "on",
		"group", "by", "having", "order", "asc", "desc", "limit", "offset",
		"as", "distinct", "all", "union", "intersect", "except",
		"create", "alter", "drop", "table", "index", "view",
		"primary", "key", "foreign", "references", "constraint",
		"like", "between", "exists", "case", "when", "then", "else", "end",
		"count", "sum", "avg", "min", "max", "coalesce", "nullif",
		"true", "false", "returning",
	}

	for _, kw := range keywords {
		input := kw + " test"
		result := n.Normalize(input)
		expected := stringToUpper(kw) + " test"
		if result != expected {
			t.Errorf("Keyword %q not uppercased: got %q, want %q", kw, result, expected)
		}
	}
}

func stringToUpper(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c -= 32
		}
		result[i] = c
	}
	return string(result)
}
