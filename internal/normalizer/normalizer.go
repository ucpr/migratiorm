package normalizer

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Options contains configuration for the normalizer.
type Options struct {
	UnifyPlaceholders        bool // Unify placeholders to ? (default: true)
	RemoveComments           bool // Remove SQL comments (default: true)
	UppercaseKeywords        bool // Convert keywords to uppercase (default: true)
	RemoveQuotes             bool // Remove identifier quotes (default: true)
	NormalizeSelectColumns   bool // Normalize SELECT columns to * (default: false)
	NormalizeJoinSyntax      bool // Normalize JOIN syntax: INNER JOIN -> JOIN (default: false)
	NormalizeOrderByAsc      bool // Remove redundant ASC in ORDER BY (default: false)
	SortInsertColumns        bool // Sort INSERT column order for comparison (default: false)
	SortUpdateColumns        bool // Sort UPDATE SET column order for comparison (default: false)
	RemoveReturningClause    bool // Remove RETURNING clause from INSERT/UPDATE/DELETE (default: false)
	NormalizeTableQualifiers bool // Remove redundant table qualifiers in simple queries (default: false)
}

// DefaultOptions returns the default normalizer options.
func DefaultOptions() Options {
	return Options{
		UnifyPlaceholders:        true,
		RemoveComments:           true,
		UppercaseKeywords:        true,
		RemoveQuotes:             true,
		NormalizeSelectColumns:   false,
		NormalizeJoinSyntax:      false,
		NormalizeOrderByAsc:      false,
		SortInsertColumns:        false,
		SortUpdateColumns:        false,
		RemoveReturningClause:    false,
		NormalizeTableQualifiers: false,
	}
}

// Normalizer normalizes SQL queries for comparison.
type Normalizer struct {
	options Options
}

// New creates a new Normalizer with the given options.
func New(opts Options) *Normalizer {
	return &Normalizer{
		options: opts,
	}
}

// NewDefault creates a new Normalizer with default options.
func NewDefault() *Normalizer {
	return New(DefaultOptions())
}

// Normalize normalizes a SQL query string.
func (n *Normalizer) Normalize(query string) string {
	result := query

	if n.options.RemoveComments {
		result = removeComments(result)
	}

	// Handle quotes and keywords together to preserve quoted identifier case
	if n.options.RemoveQuotes && n.options.UppercaseKeywords {
		result = removeQuotesPreservingCase(result)
	} else if n.options.RemoveQuotes {
		result = removeQuotes(result)
	}

	if n.options.UnifyPlaceholders {
		result = unifyPlaceholders(result)
	}

	result = normalizeWhitespace(result)

	// Only uppercase if we haven't already done it with quote removal
	if n.options.UppercaseKeywords && !n.options.RemoveQuotes {
		result = uppercaseKeywords(result)
	}

	if n.options.NormalizeSelectColumns {
		result = normalizeSelectColumns(result)
	}

	if n.options.NormalizeJoinSyntax {
		result = normalizeJoinSyntax(result)
	}

	if n.options.NormalizeOrderByAsc {
		result = normalizeOrderByAsc(result)
	}

	if n.options.SortInsertColumns {
		result = sortInsertColumns(result)
	}

	if n.options.SortUpdateColumns {
		result = sortUpdateColumns(result)
	}

	if n.options.RemoveReturningClause {
		result = removeReturningClause(result)
	}

	if n.options.NormalizeTableQualifiers {
		result = normalizeTableQualifiers(result)
	}

	return strings.TrimSpace(result)
}

// normalizeWhitespace normalizes whitespace in the query.
func normalizeWhitespace(query string) string {
	// Replace all whitespace sequences with a single space
	re := regexp.MustCompile(`\s+`)
	return re.ReplaceAllString(query, " ")
}

// removeComments removes SQL comments from the query.
func removeComments(query string) string {
	// Remove single-line comments (-- ...)
	re := regexp.MustCompile(`--[^\n]*`)
	result := re.ReplaceAllString(query, "")

	// Remove multi-line comments (/* ... */)
	re = regexp.MustCompile(`/\*[\s\S]*?\*/`)
	result = re.ReplaceAllString(result, "")

	return result
}

// removeQuotes removes identifier quotes (backticks, double quotes, brackets).
func removeQuotes(query string) string {
	result := query

	// Remove backticks (MySQL)
	re := regexp.MustCompile("`([^`]+)`")
	result = re.ReplaceAllString(result, "$1")

	// Remove double quotes (PostgreSQL, standard SQL)
	re = regexp.MustCompile(`"([^"]+)"`)
	result = re.ReplaceAllString(result, "$1")

	// Remove brackets (SQL Server)
	re = regexp.MustCompile(`\[([^\]]+)\]`)
	result = re.ReplaceAllString(result, "$1")

	return result
}

// removeQuotesPreservingCase removes quotes and uppercases keywords,
// but preserves the original case of quoted identifiers.
// This prevents column names like "count" from being uppercased to COUNT.
func removeQuotesPreservingCase(query string) string {
	// Step 1: Extract all quoted identifiers and replace with placeholders
	placeholders := make(map[string]string)
	counter := 0
	result := query

	// Process backticks (MySQL)
	reBacktick := regexp.MustCompile("`([^`]+)`")
	result = reBacktick.ReplaceAllStringFunc(result, func(match string) string {
		inner := reBacktick.FindStringSubmatch(match)[1]
		placeholder := fmt.Sprintf("__QUOTED_%d__", counter)
		placeholders[placeholder] = inner
		counter++
		return placeholder
	})

	// Process double quotes (PostgreSQL, standard SQL)
	reDoubleQuote := regexp.MustCompile(`"([^"]+)"`)
	result = reDoubleQuote.ReplaceAllStringFunc(result, func(match string) string {
		inner := reDoubleQuote.FindStringSubmatch(match)[1]
		placeholder := fmt.Sprintf("__QUOTED_%d__", counter)
		placeholders[placeholder] = inner
		counter++
		return placeholder
	})

	// Process brackets (SQL Server)
	reBracket := regexp.MustCompile(`\[([^\]]+)\]`)
	result = reBracket.ReplaceAllStringFunc(result, func(match string) string {
		inner := reBracket.FindStringSubmatch(match)[1]
		placeholder := fmt.Sprintf("__QUOTED_%d__", counter)
		placeholders[placeholder] = inner
		counter++
		return placeholder
	})

	// Step 2: Uppercase keywords (only affects non-quoted parts)
	result = uppercaseKeywords(result)

	// Step 3: Replace placeholders with original identifiers (without quotes)
	for placeholder, original := range placeholders {
		result = strings.Replace(result, placeholder, original, 1)
	}

	return result
}

// unifyPlaceholders converts various placeholder formats to ?.
func unifyPlaceholders(query string) string {
	result := query

	// Convert $1, $2, ... (PostgreSQL) to ?
	re := regexp.MustCompile(`\$\d+`)
	result = re.ReplaceAllString(result, "?")

	// Convert :name (named parameters) to ?
	re = regexp.MustCompile(`:(\w+)`)
	result = re.ReplaceAllString(result, "?")

	// Convert @name (SQL Server parameters) to ?
	re = regexp.MustCompile(`@(\w+)`)
	result = re.ReplaceAllString(result, "?")

	return result
}

// SQL keywords to uppercase
var sqlKeywords = []string{
	"SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "NULL",
	"INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
	"JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "ON",
	"GROUP", "BY", "HAVING", "ORDER", "ASC", "DESC", "LIMIT", "OFFSET",
	"AS", "DISTINCT", "ALL", "UNION", "INTERSECT", "EXCEPT",
	"CREATE", "ALTER", "DROP", "TABLE", "INDEX", "VIEW",
	"PRIMARY", "KEY", "FOREIGN", "REFERENCES", "CONSTRAINT",
	"LIKE", "BETWEEN", "EXISTS", "CASE", "WHEN", "THEN", "ELSE", "END",
	"COUNT", "SUM", "AVG", "MIN", "MAX", "COALESCE", "NULLIF",
	"TRUE", "FALSE", "RETURNING",
}

// uppercaseKeywords converts SQL keywords to uppercase.
func uppercaseKeywords(query string) string {
	result := query
	for _, keyword := range sqlKeywords {
		// Use word boundary to avoid replacing partial words
		re := regexp.MustCompile(`(?i)\b` + keyword + `\b`)
		result = re.ReplaceAllString(result, keyword)
	}
	return result
}

// normalizeSelectColumns normalizes SELECT column lists to *.
// This enables semantic comparison where "SELECT *" and "SELECT id, name" are considered equivalent.
func normalizeSelectColumns(query string) string {
	// Match SELECT ... FROM pattern and replace the column list with *
	// This handles:
	// - SELECT * FROM ...
	// - SELECT id, name, email FROM ...
	// - SELECT users.id, users.name FROM ...
	// - SELECT DISTINCT id, name FROM ...
	//
	// Note: This is a simplified implementation that may not handle all edge cases
	// such as subqueries in SELECT clause, CASE expressions, etc.

	// Pattern explanation:
	// SELECT\s+ - SELECT keyword followed by whitespace
	// (DISTINCT\s+)? - optional DISTINCT keyword
	// .+? - column list (non-greedy)
	// \s+FROM\b - whitespace followed by FROM keyword
	re := regexp.MustCompile(`(?i)(SELECT\s+)(DISTINCT\s+)?(.+?)(\s+FROM\b)`)

	return re.ReplaceAllStringFunc(query, func(match string) string {
		submatches := re.FindStringSubmatch(match)
		if len(submatches) < 5 {
			return match
		}

		selectPart := submatches[1]   // "SELECT "
		distinctPart := submatches[2] // "DISTINCT " or ""
		// submatches[3] is the column list - we replace this with *
		fromPart := submatches[4] // " FROM"

		return selectPart + distinctPart + "*" + fromPart
	})
}

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

// sortInsertColumns sorts the column order in INSERT statements for comparison.
// INSERT INTO t (c, b, a) VALUES (?, ?, ?) → INSERT INTO t (a, b, c) VALUES (?, ?, ?)
func sortInsertColumns(query string) string {
	// Match INSERT INTO table (columns) VALUES (values) pattern
	re := regexp.MustCompile(`(?i)(INSERT\s+INTO\s+\w+\s*)\(([^)]+)\)(\s*VALUES\s*)\(([^)]+)\)`)

	return re.ReplaceAllStringFunc(query, func(match string) string {
		submatches := re.FindStringSubmatch(match)
		if len(submatches) < 5 {
			return match
		}

		insertPart := submatches[1]  // "INSERT INTO table "
		columnsPart := submatches[2] // "c, b, a"
		valuesPart := submatches[3]  // " VALUES "
		valuesData := submatches[4]  // "?, ?, ?"

		// Parse columns
		columns := strings.Split(columnsPart, ",")
		for i := range columns {
			columns[i] = strings.TrimSpace(columns[i])
		}

		// Parse values
		values := strings.Split(valuesData, ",")
		for i := range values {
			values[i] = strings.TrimSpace(values[i])
		}

		// If column count doesn't match value count, return original
		if len(columns) != len(values) {
			return match
		}

		// Create column-value pairs
		type pair struct {
			column string
			value  string
		}
		pairs := make([]pair, len(columns))
		for i := range columns {
			pairs[i] = pair{column: columns[i], value: values[i]}
		}

		// Sort by column name
		sort.Slice(pairs, func(i, j int) bool {
			return pairs[i].column < pairs[j].column
		})

		// Rebuild
		sortedColumns := make([]string, len(pairs))
		sortedValues := make([]string, len(pairs))
		for i, p := range pairs {
			sortedColumns[i] = p.column
			sortedValues[i] = p.value
		}

		return insertPart + "(" + strings.Join(sortedColumns, ", ") + ")" +
			valuesPart + "(" + strings.Join(sortedValues, ", ") + ")"
	})
}

// sortUpdateColumns sorts the SET column order in UPDATE statements for comparison.
// UPDATE t SET c = ?, b = ?, a = ? WHERE ... → UPDATE t SET a = ?, b = ?, c = ? WHERE ...
func sortUpdateColumns(query string) string {
	// First try to match UPDATE with WHERE clause
	reWithWhere := regexp.MustCompile(`(?i)(UPDATE\s+\w+\s+SET\s+)(.+?)(\s+WHERE\b.*)`)
	if matches := reWithWhere.FindStringSubmatch(query); len(matches) >= 4 {
		return sortUpdateSetClause(matches[1], matches[2], matches[3])
	}

	// Try to match UPDATE without WHERE clause
	reNoWhere := regexp.MustCompile(`(?i)(UPDATE\s+\w+\s+SET\s+)(.+)$`)
	if matches := reNoWhere.FindStringSubmatch(query); len(matches) >= 3 {
		return sortUpdateSetClause(matches[1], matches[2], "")
	}

	return query
}

// sortUpdateSetClause sorts the SET clause assignments.
func sortUpdateSetClause(prefix, setClause, suffix string) string {
	// Parse SET assignments (column = value pairs)
	// This is a simplified parser that handles basic cases
	assignments := parseSetAssignments(setClause)
	if len(assignments) == 0 {
		return prefix + setClause + suffix
	}

	// Sort by column name
	sort.Slice(assignments, func(i, j int) bool {
		return assignments[i].column < assignments[j].column
	})

	// Rebuild
	parts := make([]string, len(assignments))
	for i, a := range assignments {
		parts[i] = a.column + " = " + a.value
	}

	return prefix + strings.Join(parts, ", ") + suffix
}

// assignment represents a column = value pair in SET clause.
type assignment struct {
	column string
	value  string
}

// parseSetAssignments parses "col1 = val1, col2 = val2" into assignments.
func parseSetAssignments(setClause string) []assignment {
	var result []assignment

	// Split by comma, but we need to be careful about commas inside function calls
	// For simplicity, we'll use a basic split that works for common cases
	parts := splitSetClause(setClause)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		eqIdx := strings.Index(part, "=")
		if eqIdx == -1 {
			return nil // Invalid format
		}
		col := strings.TrimSpace(part[:eqIdx])
		val := strings.TrimSpace(part[eqIdx+1:])
		result = append(result, assignment{column: col, value: val})
	}

	return result
}

// splitSetClause splits SET clause by comma, respecting parentheses.
func splitSetClause(s string) []string {
	var result []string
	var current strings.Builder
	depth := 0

	for _, ch := range s {
		switch ch {
		case '(':
			depth++
			current.WriteRune(ch)
		case ')':
			depth--
			current.WriteRune(ch)
		case ',':
			if depth == 0 {
				result = append(result, current.String())
				current.Reset()
			} else {
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		result = append(result, current.String())
	}

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
