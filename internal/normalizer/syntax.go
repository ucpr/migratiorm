package normalizer

import (
	"regexp"
	"strings"
)

// normalizeJoinSyntax normalizes JOIN syntax to canonical form.
// - INNER JOIN → JOIN
// - LEFT OUTER JOIN → LEFT JOIN
// - RIGHT OUTER JOIN → RIGHT JOIN
// - FULL OUTER JOIN → FULL JOIN
func normalizeJoinSyntax(query string) string {
	result := query

	// INNER JOIN → JOIN (INNER is redundant)
	re := regexp.MustCompile(`(?i)\bINNER\s+JOIN\b`)
	result = re.ReplaceAllString(result, "JOIN")

	// LEFT OUTER JOIN → LEFT JOIN (OUTER is redundant)
	re = regexp.MustCompile(`(?i)\bLEFT\s+OUTER\s+JOIN\b`)
	result = re.ReplaceAllString(result, "LEFT JOIN")

	// RIGHT OUTER JOIN → RIGHT JOIN (OUTER is redundant)
	re = regexp.MustCompile(`(?i)\bRIGHT\s+OUTER\s+JOIN\b`)
	result = re.ReplaceAllString(result, "RIGHT JOIN")

	// FULL OUTER JOIN → FULL JOIN (OUTER is redundant)
	re = regexp.MustCompile(`(?i)\bFULL\s+OUTER\s+JOIN\b`)
	result = re.ReplaceAllString(result, "FULL JOIN")

	return result
}

// normalizeOrderByAsc removes redundant ASC in ORDER BY clauses.
// ASC is the default sort order, so "ORDER BY x ASC" is equivalent to "ORDER BY x".
func normalizeOrderByAsc(query string) string {
	// Match ORDER BY column ASC patterns
	// This handles:
	// - ORDER BY x ASC → ORDER BY x
	// - ORDER BY x ASC, y DESC → ORDER BY x, y DESC
	// - ORDER BY x ASC, y ASC → ORDER BY x, y

	// Remove ASC followed by comma (with optional whitespace)
	re := regexp.MustCompile(`(?i)\s+ASC\s*,`)
	result := re.ReplaceAllString(query, ",")

	// Remove ASC at end of query or before LIMIT/OFFSET/etc
	re = regexp.MustCompile(`(?i)\s+ASC(\s*$|\s+LIMIT\b|\s+OFFSET\b|\s+HAVING\b|\s+UNION\b|\s*\))`)
	result = re.ReplaceAllString(result, "$1")

	// Clean up any double spaces that might result
	re = regexp.MustCompile(`\s+`)
	result = re.ReplaceAllString(result, " ")

	return result
}

// removeReturningClause removes the RETURNING clause from INSERT/UPDATE/DELETE statements.
// This handles PostgreSQL's RETURNING clause which is used to return values from modified rows.
// Examples:
//   - INSERT INTO users (name) VALUES (?) RETURNING id → INSERT INTO users (name) VALUES (?)
//   - UPDATE users SET name = ? WHERE id = ? RETURNING * → UPDATE users SET name = ? WHERE id = ?
//   - DELETE FROM users WHERE id = ? RETURNING id, name → DELETE FROM users WHERE id = ?
func removeReturningClause(query string) string {
	// Match RETURNING clause and everything after it (column list)
	// The RETURNING clause can contain:
	// - Single column: RETURNING id
	// - Multiple columns: RETURNING id, name, email
	// - Star: RETURNING *
	// - Expressions: RETURNING id, created_at
	re := regexp.MustCompile(`(?i)\s+RETURNING\s+.+$`)
	return re.ReplaceAllString(query, "")
}

// normalizeTableQualifiers removes redundant table qualifiers from simple queries.
// This only applies to queries without JOINs or subqueries where table qualifiers are unnecessary.
// Examples:
//   - SELECT * FROM users WHERE users.age >= ? → SELECT * FROM users WHERE age >= ?
//   - DELETE FROM products WHERE products.id = ? → DELETE FROM products WHERE id = ?
//
// Queries with JOINs or subqueries are left unchanged to avoid ambiguity:
//   - SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE users.age >= ? → unchanged
//   - SELECT * FROM users WHERE users.id IN (SELECT user_id FROM orders) → unchanged
func normalizeTableQualifiers(query string) string {
	upperQuery := strings.ToUpper(query)

	// Check for JOINs - if present, don't normalize (need qualifiers for disambiguation)
	if hasJoin(upperQuery) {
		return query
	}

	// Check for subqueries - if present, don't normalize
	if hasSubquery(upperQuery) {
		return query
	}

	// Check for multiple tables (comma join) - if present, don't normalize
	if hasMultipleTables(upperQuery) {
		return query
	}

	// Check for schema-qualified table name - if present, don't normalize
	if hasSchemaPrefix(query) {
		return query
	}

	// Extract table name from the query
	tableName := extractTableName(query)
	if tableName == "" {
		return query
	}

	// Remove table qualifier (tablename.) from column references
	// Match: tablename.columnname (case-insensitive for table name)
	// Be careful not to match inside string literals
	pattern := `(?i)\b` + regexp.QuoteMeta(tableName) + `\.(\w+)`
	re := regexp.MustCompile(pattern)
	return re.ReplaceAllString(query, "$1")
}

// hasJoin checks if the query contains any JOIN clause.
func hasJoin(upperQuery string) bool {
	// Check for various JOIN types
	joinPatterns := []string{
		" JOIN ",
		" LEFT JOIN ",
		" RIGHT JOIN ",
		" INNER JOIN ",
		" OUTER JOIN ",
		" CROSS JOIN ",
		" FULL JOIN ",
		" NATURAL JOIN ",
	}
	for _, pattern := range joinPatterns {
		if strings.Contains(upperQuery, pattern) {
			return true
		}
	}
	return false
}

// hasSubquery checks if the query contains a subquery.
func hasSubquery(upperQuery string) bool {
	// Look for SELECT within parentheses (common subquery patterns)
	// Patterns: (SELECT, IN (SELECT, EXISTS (SELECT, etc.
	subqueryPatterns := []string{
		"(SELECT ",
		"( SELECT ",
	}
	for _, pattern := range subqueryPatterns {
		if strings.Contains(upperQuery, pattern) {
			return true
		}
	}
	return false
}

// hasMultipleTables checks if the query has multiple tables (comma join).
func hasMultipleTables(upperQuery string) bool {
	// Look for comma-separated tables in FROM clause
	// Pattern: FROM table1, table2 or FROM table1 , table2
	re := regexp.MustCompile(`\bFROM\s+\w+\s*,`)
	return re.MatchString(upperQuery)
}

// hasSchemaPrefix checks if the query has schema-qualified table names.
func hasSchemaPrefix(query string) bool {
	// Look for schema.table pattern in FROM/UPDATE/INSERT INTO clause
	// Pattern: FROM schema.table or UPDATE schema.table
	re := regexp.MustCompile(`(?i)\b(FROM|UPDATE|INSERT\s+INTO)\s+\w+\.\w+`)
	return re.MatchString(query)
}

// extractTableName extracts the main table name from a SQL query.
func extractTableName(query string) string {
	// Try to match FROM clause for SELECT/DELETE
	// Pattern: FROM tablename (with optional schema)
	fromRe := regexp.MustCompile(`(?i)\bFROM\s+(\w+)`)
	if matches := fromRe.FindStringSubmatch(query); len(matches) >= 2 {
		return matches[1]
	}

	// Try to match UPDATE tablename
	updateRe := regexp.MustCompile(`(?i)\bUPDATE\s+(\w+)`)
	if matches := updateRe.FindStringSubmatch(query); len(matches) >= 2 {
		return matches[1]
	}

	// Try to match INSERT INTO tablename
	insertRe := regexp.MustCompile(`(?i)\bINSERT\s+INTO\s+(\w+)`)
	if matches := insertRe.FindStringSubmatch(query); len(matches) >= 2 {
		return matches[1]
	}

	return ""
}
