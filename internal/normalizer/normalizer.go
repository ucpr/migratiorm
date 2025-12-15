package normalizer

import (
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
