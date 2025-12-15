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
		// NormalizeSelectColumns tests
		{
			name:  "normalizes explicit columns to star",
			input: "SELECT id, name, email FROM users",
			expected: "SELECT * FROM users",
			options: Options{
				UnifyPlaceholders:      true,
				RemoveComments:         true,
				UppercaseKeywords:      true,
				RemoveQuotes:           true,
				NormalizeSelectColumns: true,
			},
		},
		{
			name:  "keeps star unchanged",
			input: "SELECT * FROM users",
			expected: "SELECT * FROM users",
			options: Options{
				UnifyPlaceholders:      true,
				RemoveComments:         true,
				UppercaseKeywords:      true,
				RemoveQuotes:           true,
				NormalizeSelectColumns: true,
			},
		},
		{
			name:  "normalizes qualified columns to star",
			input: "SELECT users.id, users.name FROM users",
			expected: "SELECT * FROM users",
			options: Options{
				UnifyPlaceholders:      true,
				RemoveComments:         true,
				UppercaseKeywords:      true,
				RemoveQuotes:           true,
				NormalizeSelectColumns: true,
			},
		},
		{
			name:  "preserves DISTINCT with normalized columns",
			input: "SELECT DISTINCT id, name FROM users",
			expected: "SELECT DISTINCT * FROM users",
			options: Options{
				UnifyPlaceholders:      true,
				RemoveComments:         true,
				UppercaseKeywords:      true,
				RemoveQuotes:           true,
				NormalizeSelectColumns: true,
			},
		},
		{
			name:  "normalizes columns with WHERE clause",
			input: "SELECT id, name FROM users WHERE age > ?",
			expected: "SELECT * FROM users WHERE age > ?",
			options: Options{
				UnifyPlaceholders:      true,
				RemoveComments:         true,
				UppercaseKeywords:      true,
				RemoveQuotes:           true,
				NormalizeSelectColumns: true,
			},
		},
		{
			name:  "does not normalize when disabled",
			input: "SELECT id, name FROM users",
			expected: "SELECT id, name FROM users",
			options: Options{
				UnifyPlaceholders:      true,
				RemoveComments:         true,
				UppercaseKeywords:      true,
				RemoveQuotes:           true,
				NormalizeSelectColumns: false,
			},
		},
		// NormalizeJoinSyntax tests
		{
			name:     "normalizes INNER JOIN to JOIN",
			input:    "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id",
			expected: "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
			options: Options{
				UnifyPlaceholders:   true,
				RemoveComments:      true,
				UppercaseKeywords:   true,
				RemoveQuotes:        true,
				NormalizeJoinSyntax: true,
			},
		},
		{
			name:     "normalizes LEFT OUTER JOIN to LEFT JOIN",
			input:    "SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id",
			expected: "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id",
			options: Options{
				UnifyPlaceholders:   true,
				RemoveComments:      true,
				UppercaseKeywords:   true,
				RemoveQuotes:        true,
				NormalizeJoinSyntax: true,
			},
		},
		{
			name:     "normalizes RIGHT OUTER JOIN to RIGHT JOIN",
			input:    "SELECT * FROM users RIGHT OUTER JOIN orders ON users.id = orders.user_id",
			expected: "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id",
			options: Options{
				UnifyPlaceholders:   true,
				RemoveComments:      true,
				UppercaseKeywords:   true,
				RemoveQuotes:        true,
				NormalizeJoinSyntax: true,
			},
		},
		{
			name:     "normalizes FULL OUTER JOIN to FULL JOIN",
			input:    "SELECT * FROM users FULL OUTER JOIN orders ON users.id = orders.user_id",
			expected: "SELECT * FROM users FULL JOIN orders ON users.id = orders.user_id",
			options: Options{
				UnifyPlaceholders:   true,
				RemoveComments:      true,
				UppercaseKeywords:   true,
				RemoveQuotes:        true,
				NormalizeJoinSyntax: true,
			},
		},
		{
			name:     "preserves JOIN syntax when disabled",
			input:    "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id",
			expected: "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id",
			options: Options{
				UnifyPlaceholders:   true,
				RemoveComments:      true,
				UppercaseKeywords:   true,
				RemoveQuotes:        true,
				NormalizeJoinSyntax: false,
			},
		},
		// NormalizeOrderByAsc tests
		{
			name:     "removes ASC from ORDER BY",
			input:    "SELECT * FROM users ORDER BY name ASC",
			expected: "SELECT * FROM users ORDER BY name",
			options: Options{
				UnifyPlaceholders:   true,
				RemoveComments:      true,
				UppercaseKeywords:   true,
				RemoveQuotes:        true,
				NormalizeOrderByAsc: true,
			},
		},
		{
			name:     "removes multiple ASC from ORDER BY",
			input:    "SELECT * FROM users ORDER BY name ASC, age ASC",
			expected: "SELECT * FROM users ORDER BY name, age",
			options: Options{
				UnifyPlaceholders:   true,
				RemoveComments:      true,
				UppercaseKeywords:   true,
				RemoveQuotes:        true,
				NormalizeOrderByAsc: true,
			},
		},
		{
			name:     "preserves DESC in ORDER BY",
			input:    "SELECT * FROM users ORDER BY name ASC, age DESC",
			expected: "SELECT * FROM users ORDER BY name, age DESC",
			options: Options{
				UnifyPlaceholders:   true,
				RemoveComments:      true,
				UppercaseKeywords:   true,
				RemoveQuotes:        true,
				NormalizeOrderByAsc: true,
			},
		},
		{
			name:     "handles ORDER BY with LIMIT",
			input:    "SELECT * FROM users ORDER BY name ASC LIMIT 10",
			expected: "SELECT * FROM users ORDER BY name LIMIT 10",
			options: Options{
				UnifyPlaceholders:   true,
				RemoveComments:      true,
				UppercaseKeywords:   true,
				RemoveQuotes:        true,
				NormalizeOrderByAsc: true,
			},
		},
		{
			name:     "preserves ORDER BY ASC when disabled",
			input:    "SELECT * FROM users ORDER BY name ASC",
			expected: "SELECT * FROM users ORDER BY name ASC",
			options: Options{
				UnifyPlaceholders:   true,
				RemoveComments:      true,
				UppercaseKeywords:   true,
				RemoveQuotes:        true,
				NormalizeOrderByAsc: false,
			},
		},
		// SortInsertColumns tests
		{
			name:     "sorts INSERT columns alphabetically",
			input:    "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
			expected: "INSERT INTO users (age, email, name) VALUES (?, ?, ?)",
			options: Options{
				UnifyPlaceholders: true,
				RemoveComments:    true,
				UppercaseKeywords: true,
				RemoveQuotes:      true,
				SortInsertColumns: true,
			},
		},
		{
			name:     "sorts INSERT columns with different order",
			input:    "INSERT INTO users (c, b, a) VALUES (?, ?, ?)",
			expected: "INSERT INTO users (a, b, c) VALUES (?, ?, ?)",
			options: Options{
				UnifyPlaceholders: true,
				RemoveComments:    true,
				UppercaseKeywords: true,
				RemoveQuotes:      true,
				SortInsertColumns: true,
			},
		},
		{
			name:     "preserves INSERT column order when disabled",
			input:    "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
			expected: "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
			options: Options{
				UnifyPlaceholders: true,
				RemoveComments:    true,
				UppercaseKeywords: true,
				RemoveQuotes:      true,
				SortInsertColumns: false,
			},
		},
		// SortUpdateColumns tests
		{
			name:     "sorts UPDATE SET columns alphabetically",
			input:    "UPDATE users SET name = ?, email = ?, age = ? WHERE id = ?",
			expected: "UPDATE users SET age = ?, email = ?, name = ? WHERE id = ?",
			options: Options{
				UnifyPlaceholders: true,
				RemoveComments:    true,
				UppercaseKeywords: true,
				RemoveQuotes:      true,
				SortUpdateColumns: true,
			},
		},
		{
			name:     "sorts UPDATE SET columns with different order",
			input:    "UPDATE users SET c = ?, b = ?, a = ? WHERE id = ?",
			expected: "UPDATE users SET a = ?, b = ?, c = ? WHERE id = ?",
			options: Options{
				UnifyPlaceholders: true,
				RemoveComments:    true,
				UppercaseKeywords: true,
				RemoveQuotes:      true,
				SortUpdateColumns: true,
			},
		},
		{
			name:     "preserves UPDATE SET column order when disabled",
			input:    "UPDATE users SET name = ?, email = ?, age = ? WHERE id = ?",
			expected: "UPDATE users SET name = ?, email = ?, age = ? WHERE id = ?",
			options: Options{
				UnifyPlaceholders: true,
				RemoveComments:    true,
				UppercaseKeywords: true,
				RemoveQuotes:      true,
				SortUpdateColumns: false,
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
