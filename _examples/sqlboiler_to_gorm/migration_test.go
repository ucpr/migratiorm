package sqlboiler_to_gorm

import (
	"context"
	"database/sql"
	"testing"

	"github.com/ucpr/migratiorm"
)

// TestMigration_FindAll verifies that FindAll generates equivalent queries.
// SQLBoiler: SELECT "users"."id", "users"."name", ... FROM "users"
// GORM:      SELECT * FROM `users`
//
// With SemanticComparison enabled, SELECT column differences are normalized.
func TestMigration_FindAll(t *testing.T) {
	m := migratiorm.New(
		migratiorm.WithSemanticComparison(true),
	)

	m.Expect(func(db *sql.DB) {
		repo := NewSQLBoilerUserRepository(db)
		repo.FindAll(context.Background())
	})

	m.Actual(func(db *sql.DB) {
		repo := NewGORMUserRepository(db)
		repo.FindAll(context.Background())
	})

	m.Assert(t)
}

// TestMigration_FindByID shows a case where GORM's First() generates different query.
// SQLBoiler: SELECT ... WHERE "users"."id" = $1
// GORM:      SELECT * FROM `users` WHERE `users`.`id` = ? ORDER BY `users`.`id` LIMIT 1
//
// GORM's First() adds ORDER BY and LIMIT, which is semantically different.
func TestMigration_FindByID(t *testing.T) {
	m := migratiorm.New(
		migratiorm.WithSemanticComparison(true),
	)

	m.Expect(func(db *sql.DB) {
		repo := NewSQLBoilerUserRepository(db)
		repo.FindByID(context.Background(), 1)
	})

	m.Actual(func(db *sql.DB) {
		repo := NewGORMUserRepository(db)
		repo.FindByID(context.Background(), 1)
	})

	// These queries are intentionally different - GORM adds ORDER BY and LIMIT
	expected := m.ExpectedQueries()
	actual := m.ActualQueries()

	t.Logf("Expected: %s", expected[0].Normalized)
	t.Logf("Actual:   %s", actual[0].Normalized)
}

// TestMigration_FindByAge verifies FindByAge with semantic comparison.
func TestMigration_FindByAge(t *testing.T) {
	m := migratiorm.New(
		migratiorm.WithSemanticComparison(true),
	)

	m.Expect(func(db *sql.DB) {
		repo := NewSQLBoilerUserRepository(db)
		repo.FindByAge(context.Background(), 18)
	})

	m.Actual(func(db *sql.DB) {
		repo := NewGORMUserRepository(db)
		repo.FindByAge(context.Background(), 18)
	})

	m.Assert(t)
}

// TestMigration_Delete verifies that Delete generates equivalent queries.
func TestMigration_Delete(t *testing.T) {
	m := migratiorm.New(
		migratiorm.WithSemanticComparison(true),
	)

	m.Expect(func(db *sql.DB) {
		repo := NewSQLBoilerUserRepository(db)
		repo.Delete(context.Background(), 1)
	})

	m.Actual(func(db *sql.DB) {
		repo := NewGORMUserRepository(db)
		repo.Delete(context.Background(), 1)
	})

	m.Assert(t)
}

// TestMigration_DebugQueries demonstrates how to debug captured queries.
func TestMigration_DebugQueries(t *testing.T) {
	m := migratiorm.New(
		migratiorm.WithSemanticComparison(true),
	)

	m.Expect(func(db *sql.DB) {
		repo := NewSQLBoilerUserRepository(db)
		repo.Delete(context.Background(), 42)
	})

	m.Actual(func(db *sql.DB) {
		repo := NewGORMUserRepository(db)
		repo.Delete(context.Background(), 42)
	})

	// Debug: print captured queries
	t.Log("Expected queries:")
	for i, q := range m.ExpectedQueries() {
		t.Logf("  [%d] Raw:        %s", i, q.Raw)
		t.Logf("  [%d] Normalized: %s", i, q.Normalized)
		t.Logf("  [%d] Args:       %v", i, q.Args)
	}

	t.Log("Actual queries:")
	for i, q := range m.ActualQueries() {
		t.Logf("  [%d] Raw:        %s", i, q.Raw)
		t.Logf("  [%d] Normalized: %s", i, q.Normalized)
		t.Logf("  [%d] Args:       %v", i, q.Args)
	}

	m.Assert(t)
}
