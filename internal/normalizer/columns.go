package normalizer

import (
	"regexp"
	"sort"
	"strings"
)

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
