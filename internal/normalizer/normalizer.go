package normalizer

import (
	"regexp"
	"strings"
)

// Options contains configuration for the normalizer.
type Options struct {
	UnifyPlaceholders     bool // Unify placeholders to ? (default: true)
	RemoveComments        bool // Remove SQL comments (default: true)
	UppercaseKeywords     bool // Convert keywords to uppercase (default: true)
	RemoveQuotes          bool // Remove identifier quotes (default: true)
	NormalizeSelectColumns bool // Normalize SELECT columns to * (default: false)
}

// DefaultOptions returns the default normalizer options.
func DefaultOptions() Options {
	return Options{
		UnifyPlaceholders:     true,
		RemoveComments:        true,
		UppercaseKeywords:     true,
		RemoveQuotes:          true,
		NormalizeSelectColumns: false,
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

	if n.options.RemoveQuotes {
		result = removeQuotes(result)
	}

	if n.options.UnifyPlaceholders {
		result = unifyPlaceholders(result)
	}

	result = normalizeWhitespace(result)

	if n.options.UppercaseKeywords {
		result = uppercaseKeywords(result)
	}

	if n.options.NormalizeSelectColumns {
		result = normalizeSelectColumns(result)
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

		selectPart := submatches[1]    // "SELECT "
		distinctPart := submatches[2]  // "DISTINCT " or ""
		// submatches[3] is the column list - we replace this with *
		fromPart := submatches[4]      // " FROM"

		return selectPart + distinctPart + "*" + fromPart
	})
}
