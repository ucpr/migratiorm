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
		// RemoveReturningClause tests
		{
			name:     "removes RETURNING clause from INSERT",
			input:    "INSERT INTO users (name, email) VALUES (?, ?) RETURNING id",
			expected: "INSERT INTO users (name, email) VALUES (?, ?)",
			options: Options{
				UnifyPlaceholders:     true,
				RemoveComments:        true,
				UppercaseKeywords:     true,
				RemoveQuotes:          true,
				RemoveReturningClause: true,
			},
		},
		{
			name:     "removes RETURNING * clause",
			input:    "INSERT INTO users (name) VALUES (?) RETURNING *",
			expected: "INSERT INTO users (name) VALUES (?)",
			options: Options{
				UnifyPlaceholders:     true,
				RemoveComments:        true,
				UppercaseKeywords:     true,
				RemoveQuotes:          true,
				RemoveReturningClause: true,
			},
		},
		{
			name:     "removes RETURNING with multiple columns",
			input:    "INSERT INTO users (name) VALUES (?) RETURNING id, name, created_at",
			expected: "INSERT INTO users (name) VALUES (?)",
			options: Options{
				UnifyPlaceholders:     true,
				RemoveComments:        true,
				UppercaseKeywords:     true,
				RemoveQuotes:          true,
				RemoveReturningClause: true,
			},
		},
		{
			name:     "removes RETURNING clause from UPDATE",
			input:    "UPDATE users SET name = ? WHERE id = ? RETURNING id, name",
			expected: "UPDATE users SET name = ? WHERE id = ?",
			options: Options{
				UnifyPlaceholders:     true,
				RemoveComments:        true,
				UppercaseKeywords:     true,
				RemoveQuotes:          true,
				RemoveReturningClause: true,
			},
		},
		{
			name:     "removes RETURNING clause from DELETE",
			input:    "DELETE FROM users WHERE id = ? RETURNING id",
			expected: "DELETE FROM users WHERE id = ?",
			options: Options{
				UnifyPlaceholders:     true,
				RemoveComments:        true,
				UppercaseKeywords:     true,
				RemoveQuotes:          true,
				RemoveReturningClause: true,
			},
		},
		{
			name:     "preserves RETURNING clause when disabled",
			input:    "INSERT INTO users (name) VALUES (?) RETURNING id",
			expected: "INSERT INTO users (name) VALUES (?) RETURNING id",
			options: Options{
				UnifyPlaceholders:     true,
				RemoveComments:        true,
				UppercaseKeywords:     true,
				RemoveQuotes:          true,
				RemoveReturningClause: false,
			},
		},
		// NormalizeTableQualifiers tests
		{
			name:     "removes table qualifier in simple SELECT",
			input:    "SELECT * FROM users WHERE users.age >= ?",
			expected: "SELECT * FROM users WHERE age >= ?",
			options: Options{
				UnifyPlaceholders:        true,
				RemoveComments:           true,
				UppercaseKeywords:        true,
				RemoveQuotes:             true,
				NormalizeTableQualifiers: true,
			},
		},
		{
			name:     "removes table qualifier in DELETE",
			input:    "DELETE FROM products WHERE products.id = ?",
			expected: "DELETE FROM products WHERE id = ?",
			options: Options{
				UnifyPlaceholders:        true,
				RemoveComments:           true,
				UppercaseKeywords:        true,
				RemoveQuotes:             true,
				NormalizeTableQualifiers: true,
			},
		},
		{
			name:     "removes table qualifier in UPDATE",
			input:    "UPDATE users SET name = ? WHERE users.id = ?",
			expected: "UPDATE users SET name = ? WHERE id = ?",
			options: Options{
				UnifyPlaceholders:        true,
				RemoveComments:           true,
				UppercaseKeywords:        true,
				RemoveQuotes:             true,
				NormalizeTableQualifiers: true,
			},
		},
		{
			name:     "removes multiple table qualifiers",
			input:    "SELECT * FROM users WHERE users.age >= ? AND users.status = ?",
			expected: "SELECT * FROM users WHERE age >= ? AND status = ?",
			options: Options{
				UnifyPlaceholders:        true,
				RemoveComments:           true,
				UppercaseKeywords:        true,
				RemoveQuotes:             true,
				NormalizeTableQualifiers: true,
			},
		},
		{
			name:     "preserves table qualifier with JOIN",
			input:    "SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE users.age >= ?",
			expected: "SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE users.age >= ?",
			options: Options{
				UnifyPlaceholders:        true,
				RemoveComments:           true,
				UppercaseKeywords:        true,
				RemoveQuotes:             true,
				NormalizeTableQualifiers: true,
			},
		},
		{
			name:     "preserves table qualifier with LEFT JOIN",
			input:    "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id",
			expected: "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id",
			options: Options{
				UnifyPlaceholders:        true,
				RemoveComments:           true,
				UppercaseKeywords:        true,
				RemoveQuotes:             true,
				NormalizeTableQualifiers: true,
			},
		},
		{
			name:     "preserves table qualifier with subquery",
			input:    "SELECT * FROM users WHERE users.id IN (SELECT orders.user_id FROM orders)",
			expected: "SELECT * FROM users WHERE users.id IN (SELECT orders.user_id FROM orders)",
			options: Options{
				UnifyPlaceholders:        true,
				RemoveComments:           true,
				UppercaseKeywords:        true,
				RemoveQuotes:             true,
				NormalizeTableQualifiers: true,
			},
		},
		{
			name:     "preserves table qualifier when disabled",
			input:    "SELECT * FROM users WHERE users.age >= ?",
			expected: "SELECT * FROM users WHERE users.age >= ?",
			options: Options{
				UnifyPlaceholders:        true,
				RemoveComments:           true,
				UppercaseKeywords:        true,
				RemoveQuotes:             true,
				NormalizeTableQualifiers: false,
			},
		},
		{
			name:     "handles case-insensitive table name matching",
			input:    "SELECT * FROM Users WHERE USERS.age >= ?",
			expected: "SELECT * FROM Users WHERE age >= ?",
			options: Options{
				UnifyPlaceholders:        true,
				RemoveComments:           true,
				UppercaseKeywords:        true,
				RemoveQuotes:             true,
				NormalizeTableQualifiers: true,
			},
		},
		// Edge case: Table alias (should NOT normalize - alias is different from table name)
		{
			name:     "preserves table alias qualifier",
			input:    "SELECT * FROM users u WHERE u.age >= ?",
			expected: "SELECT * FROM users u WHERE u.age >= ?",
			options: Options{
				UnifyPlaceholders:        true,
				RemoveComments:           true,
				UppercaseKeywords:        true,
				RemoveQuotes:             true,
				NormalizeTableQualifiers: true,
			},
		},
		// Edge case: Comma-join (implicit join - multiple tables)
		{
			name:     "preserves qualifier with comma join",
			input:    "SELECT * FROM users, orders WHERE users.id = orders.user_id",
			expected: "SELECT * FROM users, orders WHERE users.id = orders.user_id",
			options: Options{
				UnifyPlaceholders:        true,
				RemoveComments:           true,
				UppercaseKeywords:        true,
				RemoveQuotes:             true,
				NormalizeTableQualifiers: true,
			},
		},
		// Edge case: EXISTS subquery
		{
			name:     "preserves qualifier with EXISTS subquery",
			input:    "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)",
			expected: "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)",
			options: Options{
				UnifyPlaceholders:        true,
				RemoveComments:           true,
				UppercaseKeywords:        true,
				RemoveQuotes:             true,
				NormalizeTableQualifiers: true,
			},
		},
		// Edge case: INNER JOIN (should also preserve)
		{
			name:     "preserves qualifier with INNER JOIN",
			input:    "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id",
			expected: "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id",
			options: Options{
				UnifyPlaceholders:        true,
				RemoveComments:           true,
				UppercaseKeywords:        true,
				RemoveQuotes:             true,
				NormalizeTableQualifiers: true,
			},
		},
		// Edge case: UPDATE without WHERE
		{
			name:     "sorts UPDATE SET columns without WHERE",
			input:    "UPDATE users SET name = ?, age = ?",
			expected: "UPDATE users SET age = ?, name = ?",
			options: Options{
				UnifyPlaceholders: true,
				RemoveComments:    true,
				UppercaseKeywords: true,
				RemoveQuotes:      true,
				SortUpdateColumns: true,
			},
		},
		// Edge case: UPDATE with expression (unquoted column - will be uppercased)
		{
			name:     "sorts UPDATE SET with expression unquoted",
			input:    "UPDATE users SET count = count + 1, name = ? WHERE id = ?",
			expected: "UPDATE users SET COUNT = COUNT + 1, name = ? WHERE id = ?",
			options: Options{
				UnifyPlaceholders: true,
				RemoveComments:    true,
				UppercaseKeywords: true,
				RemoveQuotes:      true,
				SortUpdateColumns: true,
			},
		},
		// Edge case: UPDATE with quoted keyword column (should preserve case)
		{
			name:     "preserves case of quoted keyword column",
			input:    `UPDATE users SET "count" = "count" + 1, name = ? WHERE id = ?`,
			expected: "UPDATE users SET count = count + 1, name = ? WHERE id = ?",
			options: Options{
				UnifyPlaceholders: true,
				RemoveComments:    true,
				UppercaseKeywords: true,
				RemoveQuotes:      true,
				SortUpdateColumns: true,
			},
		},
		// Edge case: ORDER BY with OFFSET
		{
			name:     "removes ASC with OFFSET",
			input:    "SELECT * FROM users ORDER BY name ASC LIMIT 10 OFFSET 5",
			expected: "SELECT * FROM users ORDER BY name LIMIT 10 OFFSET 5",
			options: Options{
				UnifyPlaceholders:   true,
				RemoveComments:      true,
				UppercaseKeywords:   true,
				RemoveQuotes:        true,
				NormalizeOrderByAsc: true,
			},
		},
		// Edge case: SELECT with function (should normalize to *)
		{
			name:     "normalizes SELECT with COUNT to star",
			input:    "SELECT COUNT(*) FROM users",
			expected: "SELECT * FROM users",
			options: Options{
				UnifyPlaceholders:      true,
				RemoveComments:         true,
				UppercaseKeywords:      true,
				RemoveQuotes:           true,
				NormalizeSelectColumns: true,
			},
		},
		// Edge case: SELECT with alias
		{
			name:     "normalizes SELECT with alias to star",
			input:    "SELECT id AS user_id, name AS user_name FROM users",
			expected: "SELECT * FROM users",
			options: Options{
				UnifyPlaceholders:      true,
				RemoveComments:         true,
				UppercaseKeywords:      true,
				RemoveQuotes:           true,
				NormalizeSelectColumns: true,
			},
		},
		// Edge case: Multiple JOINs
		{
			name:     "normalizes multiple JOINs",
			input:    "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id LEFT OUTER JOIN items ON orders.id = items.order_id",
			expected: "SELECT * FROM users JOIN orders ON users.id = orders.user_id LEFT JOIN items ON orders.id = items.order_id",
			options: Options{
				UnifyPlaceholders:   true,
				RemoveComments:      true,
				UppercaseKeywords:   true,
				RemoveQuotes:        true,
				NormalizeJoinSyntax: true,
			},
		},
		// Edge case: CROSS JOIN unchanged
		{
			name:     "preserves CROSS JOIN",
			input:    "SELECT * FROM users CROSS JOIN products",
			expected: "SELECT * FROM users CROSS JOIN products",
			options: Options{
				UnifyPlaceholders:   true,
				RemoveComments:      true,
				UppercaseKeywords:   true,
				RemoveQuotes:        true,
				NormalizeJoinSyntax: true,
			},
		},
		// Edge case: lowercase RETURNING
		{
			name:     "removes lowercase returning clause",
			input:    "INSERT INTO users (name) VALUES (?) returning id",
			expected: "INSERT INTO users (name) VALUES (?)",
			options: Options{
				UnifyPlaceholders:     true,
				RemoveComments:        true,
				UppercaseKeywords:     true,
				RemoveQuotes:          true,
				RemoveReturningClause: true,
			},
		},
		// Edge case: schema.table.column (should preserve schema)
		{
			name:     "preserves schema prefix in table qualifier",
			input:    "SELECT * FROM public.users WHERE public.users.age >= ?",
			expected: "SELECT * FROM public.users WHERE public.users.age >= ?",
			options: Options{
				UnifyPlaceholders:        true,
				RemoveComments:           true,
				UppercaseKeywords:        true,
				RemoveQuotes:             true,
				NormalizeTableQualifiers: true,
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
