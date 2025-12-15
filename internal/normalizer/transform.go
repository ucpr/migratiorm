package normalizer

import (
	"fmt"
	"regexp"
	"strings"
)

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
