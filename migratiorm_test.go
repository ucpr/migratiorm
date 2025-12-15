package migratiorm_test

import (
	"database/sql"
	"testing"

	"github.com/ucpr/migratiorm"
)

func TestMigratiorm_BasicUsage(t *testing.T) {
	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		db.Query("SELECT * FROM users WHERE age > ?", 18)
	})

	m.Actual(func(db *sql.DB) {
		db.Query("SELECT * FROM users WHERE age > ?", 18)
	})

	m.Assert(t)
}

func TestMigratiorm_NormalizesWhitespace(t *testing.T) {
	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		db.Query("SELECT * FROM users WHERE age > ?", 18)
	})

	m.Actual(func(db *sql.DB) {
		db.Query("SELECT  *  FROM  users  WHERE  age  >  ?", 18)
	})

	m.Assert(t)
}

func TestMigratiorm_NormalizesKeywords(t *testing.T) {
	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		db.Query("SELECT * FROM users WHERE age > ?", 18)
	})

	m.Actual(func(db *sql.DB) {
		db.Query("select * from users where age > ?", 18)
	})

	m.Assert(t)
}

func TestMigratiorm_NormalizesPlaceholders(t *testing.T) {
	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		db.Query("SELECT * FROM users WHERE age > ?", 18)
	})

	m.Actual(func(db *sql.DB) {
		db.Query("SELECT * FROM users WHERE age > $1", 18)
	})

	m.Assert(t)
}

func TestMigratiorm_NormalizesQuotes(t *testing.T) {
	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		db.Query("SELECT * FROM users WHERE age > ?", 18)
	})

	m.Actual(func(db *sql.DB) {
		db.Query("SELECT * FROM `users` WHERE `age` > ?", 18)
	})

	m.Assert(t)
}

func TestMigratiorm_DetectsDifference(t *testing.T) {
	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		db.Query("SELECT * FROM users WHERE age > ?", 18)
	})

	m.Actual(func(db *sql.DB) {
		db.Query("SELECT * FROM users WHERE name = ?", "Alice")
	})

	expected := m.ExpectedQueries()
	actual := m.ActualQueries()

	if len(expected) != 1 || len(actual) != 1 {
		t.Fatalf("Expected 1 query each, got expected=%d, actual=%d", len(expected), len(actual))
	}

	if expected[0].Normalized == actual[0].Normalized {
		t.Error("Expected queries to differ, but they are the same")
	}
}

func TestMigratiorm_MultipleQueries(t *testing.T) {
	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		db.Query("SELECT * FROM users WHERE id = ?", 1)
		db.Exec("UPDATE users SET name = ? WHERE id = ?", "Alice", 1)
		db.Query("SELECT * FROM users WHERE id = ?", 1)
	})

	m.Actual(func(db *sql.DB) {
		db.Query("select * from users where id = $1", 1)
		db.Exec("update users set name = $1 where id = $2", "Alice", 1)
		db.Query("select * from users where id = $1", 1)
	})

	m.Assert(t)
}

func TestMigratiorm_IgnoreOrder(t *testing.T) {
	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		db.Query("SELECT * FROM users")
		db.Query("SELECT * FROM orders")
	})

	m.Actual(func(db *sql.DB) {
		db.Query("SELECT * FROM orders")
		db.Query("SELECT * FROM users")
	})

	// Should pass with IgnoreOrder
	m.AssertWithOptions(t, migratiorm.IgnoreOrder())
}

func TestMigratiorm_StrictOrderFails(t *testing.T) {
	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		db.Query("SELECT * FROM users")
		db.Query("SELECT * FROM orders")
	})

	m.Actual(func(db *sql.DB) {
		db.Query("SELECT * FROM orders")
		db.Query("SELECT * FROM users")
	})

	expected := m.ExpectedQueries()
	actual := m.ActualQueries()

	// Verify that queries are in different order
	if expected[0].Normalized == actual[0].Normalized {
		t.Error("Expected first queries to differ in strict order")
	}
}

func TestMigratiorm_ExpectedQueries(t *testing.T) {
	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		db.Query("SELECT * FROM users")
	})

	queries := m.ExpectedQueries()
	if len(queries) != 1 {
		t.Errorf("Expected 1 query, got %d", len(queries))
	}
}

func TestMigratiorm_ActualQueries(t *testing.T) {
	m := migratiorm.New()

	m.Actual(func(db *sql.DB) {
		db.Query("SELECT * FROM users")
		db.Query("SELECT * FROM orders")
	})

	queries := m.ActualQueries()
	if len(queries) != 2 {
		t.Errorf("Expected 2 queries, got %d", len(queries))
	}
}

func TestMigratiorm_WithUnorderedMode(t *testing.T) {
	m := migratiorm.New(
		migratiorm.WithCompareMode(migratiorm.CompareUnordered),
	)

	m.Expect(func(db *sql.DB) {
		db.Query("SELECT * FROM users")
		db.Query("SELECT * FROM orders")
	})

	m.Actual(func(db *sql.DB) {
		db.Query("SELECT * FROM orders")
		db.Query("SELECT * FROM users")
	})

	// Should pass because we're using unordered mode
	m.Assert(t)
}

func TestMigratiorm_QueryOperation(t *testing.T) {
	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		db.Query("SELECT * FROM users")
		db.Exec("INSERT INTO users (name) VALUES (?)", "Alice")
		db.Exec("UPDATE users SET name = ? WHERE id = ?", "Bob", 1)
		db.Exec("DELETE FROM users WHERE id = ?", 1)
	})

	queries := m.ExpectedQueries()

	if queries[0].Operation != migratiorm.OperationSelect {
		t.Errorf("Expected SELECT operation, got %v", queries[0].Operation)
	}
	if queries[1].Operation != migratiorm.OperationInsert {
		t.Errorf("Expected INSERT operation, got %v", queries[1].Operation)
	}
	if queries[2].Operation != migratiorm.OperationUpdate {
		t.Errorf("Expected UPDATE operation, got %v", queries[2].Operation)
	}
	if queries[3].Operation != migratiorm.OperationDelete {
		t.Errorf("Expected DELETE operation, got %v", queries[3].Operation)
	}
}

func TestMigratiorm_RawAndNormalizedQueries(t *testing.T) {
	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		db.Query("select * from `users` where age > $1", 18)
	})

	queries := m.ExpectedQueries()

	if queries[0].Raw != "select * from `users` where age > $1" {
		t.Errorf("Unexpected raw query: %s", queries[0].Raw)
	}

	// Normalized should have uppercase keywords, no quotes, unified placeholder
	expected := "SELECT * FROM users WHERE age > ?"
	if queries[0].Normalized != expected {
		t.Errorf("Expected normalized query %q, got %q", expected, queries[0].Normalized)
	}
}

func TestMigratiorm_SemanticComparison(t *testing.T) {
	m := migratiorm.New(
		migratiorm.WithSemanticComparison(true),
	)

	// SELECT * vs explicit columns should be equivalent with semantic comparison
	m.Expect(func(db *sql.DB) {
		db.Query("SELECT id, name, email FROM users WHERE age > ?", 18)
	})

	m.Actual(func(db *sql.DB) {
		db.Query("SELECT * FROM users WHERE age > ?", 18)
	})

	m.Assert(t)
}

func TestMigratiorm_SemanticComparisonWithQuotes(t *testing.T) {
	m := migratiorm.New(
		migratiorm.WithSemanticComparison(true),
	)

	// PostgreSQL style vs MySQL style with different column selection
	m.Expect(func(db *sql.DB) {
		db.Query(`SELECT "users"."id", "users"."name" FROM "users" WHERE "age" > $1`, 18)
	})

	m.Actual(func(db *sql.DB) {
		db.Query("SELECT * FROM `users` WHERE `age` > ?", 18)
	})

	m.Assert(t)
}

func TestMigratiorm_SemanticComparisonDisabled(t *testing.T) {
	m := migratiorm.New(
		migratiorm.WithSemanticComparison(false), // explicitly disabled
	)

	m.Expect(func(db *sql.DB) {
		db.Query("SELECT id, name FROM users", 18)
	})

	m.Actual(func(db *sql.DB) {
		db.Query("SELECT * FROM users", 18)
	})

	// These should be different when semantic comparison is disabled
	expected := m.ExpectedQueries()
	actual := m.ActualQueries()

	if expected[0].Normalized == actual[0].Normalized {
		t.Error("Expected queries to differ when semantic comparison is disabled")
	}
}
