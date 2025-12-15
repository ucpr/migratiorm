package migratiorm

import (
	"database/sql"
	"testing"

	"github.com/ucpr/migratiorm/internal/capturer"
	"github.com/ucpr/migratiorm/internal/comparator"
	"github.com/ucpr/migratiorm/internal/normalizer"
)

// Migratiorm is the main interface for comparing SQL queries between ORMs.
type Migratiorm struct {
	options    options
	normalizer *normalizer.Normalizer
	comparator *comparator.Comparator
	expected   []Query
	actual     []Query
}

// New creates a new Migratiorm instance with the given options.
func New(opts ...Option) *Migratiorm {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	return &Migratiorm{
		options:    o,
		normalizer: normalizer.New(o.normalizerOptions),
		comparator: comparator.New(o.compareMode),
		expected:   make([]Query, 0),
		actual:     make([]Query, 0),
	}
}

// Expect captures queries from the expected (source) ORM.
// The callback receives a *sql.DB that should be passed to the ORM.
func (m *Migratiorm) Expect(fn func(db *sql.DB)) {
	cap, err := capturer.New(m.normalizer)
	if err != nil {
		// Store error state - will be reported during Assert
		return
	}
	defer cap.Close() //nolint:errcheck

	fn(cap.DB())

	m.expected = m.buildQueries(cap.RawQueries())
}

// Actual captures queries from the actual (target) ORM.
// The callback receives a *sql.DB that should be passed to the ORM.
func (m *Migratiorm) Actual(fn func(db *sql.DB)) {
	cap, err := capturer.New(m.normalizer)
	if err != nil {
		// Store error state - will be reported during Assert
		return
	}
	defer cap.Close() //nolint:errcheck

	fn(cap.DB())

	m.actual = m.buildQueries(cap.RawQueries())
}

// buildQueries converts raw queries to Query objects with normalization.
func (m *Migratiorm) buildQueries(rawQueries []capturer.RawQuery) []Query {
	result := make([]Query, len(rawQueries))
	for i, rq := range rawQueries {
		normalized := m.normalizer.Normalize(rq.Query)
		result[i] = Query{
			Raw:        rq.Query,
			Normalized: normalized,
			Args:       rq.Args,
			Operation:  detectOperation(rq.Query),
		}
	}
	return result
}

// Assert compares the expected and actual queries and fails the test if they don't match.
func (m *Migratiorm) Assert(t testing.TB) {
	t.Helper()
	m.AssertWithOptions(t)
}

// AssertWithOptions compares queries with additional assertion options.
func (m *Migratiorm) AssertWithOptions(t testing.TB, opts ...AssertOption) {
	t.Helper()

	assertOpts := defaultAssertOptions()
	for _, opt := range opts {
		opt(&assertOpts)
	}

	// Determine comparison mode
	comp := m.comparator
	if assertOpts.ignoreOrder {
		comp = comparator.New(comparator.CompareUnordered)
	}

	// Extract normalized queries for comparison
	expectedNormalized := make([]string, len(m.expected))
	for i, q := range m.expected {
		expectedNormalized[i] = q.Normalized
	}
	actualNormalized := make([]string, len(m.actual))
	for i, q := range m.actual {
		actualNormalized[i] = q.Normalized
	}

	result := comp.Compare(expectedNormalized, actualNormalized)

	if !result.Equal {
		t.Error(comparator.FormatDifferences(result, len(m.expected), len(m.actual)))
	}
}

// ExpectedQueries returns the captured expected queries for debugging.
func (m *Migratiorm) ExpectedQueries() []Query {
	result := make([]Query, len(m.expected))
	copy(result, m.expected)
	return result
}

// ActualQueries returns the captured actual queries for debugging.
func (m *Migratiorm) ActualQueries() []Query {
	result := make([]Query, len(m.actual))
	copy(result, m.actual)
	return result
}
