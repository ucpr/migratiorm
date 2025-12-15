package migratiorm

import "github.com/ucpr/migratiorm/internal/normalizer"

// Option configures a Migratiorm instance.
type Option func(*options)

// options holds the configuration for Migratiorm.
type options struct {
	compareMode       CompareMode
	normalizerOptions normalizer.Options
}

// defaultOptions returns the default options.
func defaultOptions() options {
	return options{
		compareMode:       CompareStrict,
		normalizerOptions: normalizer.DefaultOptions(),
	}
}

// WithCompareMode sets the comparison mode.
func WithCompareMode(mode CompareMode) Option {
	return func(o *options) {
		o.compareMode = mode
	}
}

// WithUnifyPlaceholders enables or disables placeholder unification.
func WithUnifyPlaceholders(enabled bool) Option {
	return func(o *options) {
		o.normalizerOptions.UnifyPlaceholders = enabled
	}
}

// WithRemoveComments enables or disables comment removal.
func WithRemoveComments(enabled bool) Option {
	return func(o *options) {
		o.normalizerOptions.RemoveComments = enabled
	}
}

// WithUppercaseKeywords enables or disables keyword uppercasing.
func WithUppercaseKeywords(enabled bool) Option {
	return func(o *options) {
		o.normalizerOptions.UppercaseKeywords = enabled
	}
}

// WithRemoveQuotes enables or disables quote removal from identifiers.
func WithRemoveQuotes(enabled bool) Option {
	return func(o *options) {
		o.normalizerOptions.RemoveQuotes = enabled
	}
}

// WithSemanticComparison enables semantic comparison mode.
// When enabled, the following normalizations are applied:
//   - SELECT column lists are normalized to * (SELECT id, name → SELECT *)
//   - JOIN syntax is normalized (INNER JOIN → JOIN, LEFT OUTER JOIN → LEFT JOIN)
//   - Redundant ASC in ORDER BY is removed (ORDER BY x ASC → ORDER BY x)
//   - INSERT column order is sorted alphabetically
//   - UPDATE SET column order is sorted alphabetically
func WithSemanticComparison(enabled bool) Option {
	return func(o *options) {
		o.normalizerOptions.NormalizeSelectColumns = enabled
		o.normalizerOptions.NormalizeJoinSyntax = enabled
		o.normalizerOptions.NormalizeOrderByAsc = enabled
		o.normalizerOptions.SortInsertColumns = enabled
		o.normalizerOptions.SortUpdateColumns = enabled
	}
}

// AssertOption configures assertion behavior.
type AssertOption func(*assertOptions)

// assertOptions holds assertion configuration.
type assertOptions struct {
	ignoreOrder bool
}

// defaultAssertOptions returns the default assertion options.
func defaultAssertOptions() assertOptions {
	return assertOptions{
		ignoreOrder: false,
	}
}

// IgnoreOrder makes the assertion ignore query order.
func IgnoreOrder() AssertOption {
	return func(o *assertOptions) {
		o.ignoreOrder = true
	}
}
