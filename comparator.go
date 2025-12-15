package migratiorm

import (
	"fmt"
	"strings"
)

// CompareMode defines how queries should be compared.
type CompareMode int

const (
	// CompareStrict requires queries to match in exact order.
	CompareStrict CompareMode = iota
	// CompareUnordered compares queries as sets, ignoring order.
	CompareUnordered
)

// DiffType represents the type of difference found.
type DiffType int

const (
	// DiffMatch indicates the queries match.
	DiffMatch DiffType = iota
	// DiffMissing indicates expected query is missing from actual.
	DiffMissing
	// DiffExtra indicates actual query is not in expected.
	DiffExtra
	// DiffModified indicates queries at same position differ.
	DiffModified
)

func (d DiffType) String() string {
	switch d {
	case DiffMatch:
		return "OK"
	case DiffMissing:
		return "MISSING"
	case DiffExtra:
		return "EXTRA"
	case DiffModified:
		return "MODIFIED"
	default:
		return "UNKNOWN"
	}
}

// Difference represents a single difference between expected and actual queries.
type Difference struct {
	Type     DiffType
	Index    int
	Expected string
	Actual   string
}

// CompareResult holds the result of comparing two query sets.
type CompareResult struct {
	Equal       bool
	Differences []Difference
}

// comparator compares query sets and reports differences.
type comparator struct {
	mode CompareMode
}

// newComparator creates a new comparator with the given mode.
func newComparator(mode CompareMode) *comparator {
	return &comparator{mode: mode}
}

// Compare compares expected and actual queries.
func (c *comparator) Compare(expected, actual []Query) CompareResult {
	switch c.mode {
	case CompareUnordered:
		return c.compareUnordered(expected, actual)
	default:
		return c.compareStrict(expected, actual)
	}
}

// compareStrict compares queries in order.
func (c *comparator) compareStrict(expected, actual []Query) CompareResult {
	result := CompareResult{
		Equal:       true,
		Differences: make([]Difference, 0),
	}

	maxLen := max(len(actual), len(expected))

	for i := 0; i < maxLen; i++ {
		var diff Difference
		diff.Index = i

		if i >= len(expected) {
			// Extra query in actual
			diff.Type = DiffExtra
			diff.Actual = actual[i].Normalized
			result.Equal = false
		} else if i >= len(actual) {
			// Missing query in actual
			diff.Type = DiffMissing
			diff.Expected = expected[i].Normalized
			result.Equal = false
		} else if expected[i].Normalized != actual[i].Normalized {
			// Modified query
			diff.Type = DiffModified
			diff.Expected = expected[i].Normalized
			diff.Actual = actual[i].Normalized
			result.Equal = false
		} else {
			// Match
			diff.Type = DiffMatch
			diff.Expected = expected[i].Normalized
			diff.Actual = actual[i].Normalized
		}

		result.Differences = append(result.Differences, diff)
	}

	return result
}

// compareUnordered compares queries as sets.
func (c *comparator) compareUnordered(expected, actual []Query) CompareResult {
	result := CompareResult{
		Equal:       true,
		Differences: make([]Difference, 0),
	}

	// Build a map of expected queries
	expectedMap := make(map[string]int)
	for _, q := range expected {
		expectedMap[q.Normalized]++
	}

	// Build a map of actual queries
	actualMap := make(map[string]int)
	for _, q := range actual {
		actualMap[q.Normalized]++
	}

	// Find missing queries (in expected but not in actual)
	idx := 0
	for _, q := range expected {
		if actualMap[q.Normalized] <= 0 {
			result.Differences = append(result.Differences, Difference{
				Type:     DiffMissing,
				Index:    idx,
				Expected: q.Normalized,
			})
			result.Equal = false
		} else {
			result.Differences = append(result.Differences, Difference{
				Type:     DiffMatch,
				Index:    idx,
				Expected: q.Normalized,
				Actual:   q.Normalized,
			})
			actualMap[q.Normalized]--
		}
		idx++
	}

	// Find extra queries (in actual but not in expected)
	for _, q := range actual {
		if expectedMap[q.Normalized] <= 0 {
			result.Differences = append(result.Differences, Difference{
				Type:   DiffExtra,
				Index:  idx,
				Actual: q.Normalized,
			})
			result.Equal = false
			idx++
		} else {
			expectedMap[q.Normalized]--
		}
	}

	return result
}

// FormatDifferences formats the differences as a human-readable string.
func FormatDifferences(result CompareResult, expected, actual []Query) string {
	var sb strings.Builder

	sb.WriteString("migratiorm: queries do not match\n\n")
	sb.WriteString(fmt.Sprintf("Expected %d queries, got %d queries\n\n", len(expected), len(actual)))
	sb.WriteString("Differences:\n")

	for _, diff := range result.Differences {
		switch diff.Type {
		case DiffMatch:
			sb.WriteString(fmt.Sprintf("  [%d] OK: %s\n", diff.Index, diff.Expected))
		case DiffMissing:
			sb.WriteString(fmt.Sprintf("  [%d] MISSING:\n", diff.Index))
			sb.WriteString(fmt.Sprintf("      expected: %s\n", diff.Expected))
		case DiffExtra:
			sb.WriteString(fmt.Sprintf("  [%d] EXTRA:\n", diff.Index))
			sb.WriteString(fmt.Sprintf("      actual:   %s\n", diff.Actual))
		case DiffModified:
			sb.WriteString(fmt.Sprintf("  [%d] MODIFIED:\n", diff.Index))
			sb.WriteString(fmt.Sprintf("      expected: %s\n", diff.Expected))
			sb.WriteString(fmt.Sprintf("      actual:   %s\n", diff.Actual))
		}
	}

	return sb.String()
}
