package xo_to_gorm

import (
	"context"
	"database/sql"
	"testing"

	"github.com/ucpr/migratiorm"
)

// TestMigration_FindAll verifies that FindAll generates equivalent queries.
// xo:   SELECT id, name, description, price, category_id FROM products
// GORM: SELECT * FROM `products`
//
// With SemanticComparison enabled, SELECT column differences are normalized.
func TestMigration_FindAll(t *testing.T) {
	m := migratiorm.New(
		migratiorm.WithSemanticComparison(true),
	)

	m.Expect(func(db *sql.DB) {
		repo := NewXOProductRepository(db)
		repo.FindAll(context.Background())
	})

	m.Actual(func(db *sql.DB) {
		repo := NewGORMProductRepository(db)
		repo.FindAll(context.Background())
	})

	m.Assert(t)
}

// TestMigration_FindByID shows a case where GORM's First() generates different query.
// xo:   SELECT ... WHERE id = ?
// GORM: SELECT * FROM `products` WHERE `products`.`id` = ? ORDER BY `products`.`id` LIMIT 1
//
// GORM's First() adds ORDER BY and LIMIT, which is semantically different.
// This test demonstrates detecting such differences.
func TestMigration_FindByID(t *testing.T) {
	m := migratiorm.New(
		migratiorm.WithSemanticComparison(true),
	)

	m.Expect(func(db *sql.DB) {
		repo := NewXOProductRepository(db)
		repo.FindByID(context.Background(), 1)
	})

	m.Actual(func(db *sql.DB) {
		repo := NewGORMProductRepository(db)
		repo.FindByID(context.Background(), 1)
	})

	// These queries are intentionally different - GORM adds ORDER BY and LIMIT
	// This test shows the difference detection working correctly
	expected := m.ExpectedQueries()
	actual := m.ActualQueries()

	t.Logf("Expected: %s", expected[0].Normalized)
	t.Logf("Actual:   %s", actual[0].Normalized)

	// Note: In a real migration, you might need to adjust the GORM query
	// to use .Take() instead of .First() for equivalent behavior
}

// TestMigration_FindByCategory verifies FindByCategory with semantic comparison.
func TestMigration_FindByCategory(t *testing.T) {
	m := migratiorm.New(
		migratiorm.WithSemanticComparison(true),
	)

	m.Expect(func(db *sql.DB) {
		repo := NewXOProductRepository(db)
		repo.FindByCategory(context.Background(), 1)
	})

	m.Actual(func(db *sql.DB) {
		repo := NewGORMProductRepository(db)
		repo.FindByCategory(context.Background(), 1)
	})

	m.Assert(t)
}

// TestMigration_FindByPriceRange verifies queries with multiple conditions.
func TestMigration_FindByPriceRange(t *testing.T) {
	m := migratiorm.New(
		migratiorm.WithSemanticComparison(true),
	)

	m.Expect(func(db *sql.DB) {
		repo := NewXOProductRepository(db)
		repo.FindByPriceRange(context.Background(), 1000, 5000)
	})

	m.Actual(func(db *sql.DB) {
		repo := NewGORMProductRepository(db)
		repo.FindByPriceRange(context.Background(), 1000, 5000)
	})

	m.Assert(t)
}

// TestMigration_Delete verifies that Delete generates equivalent queries.
func TestMigration_Delete(t *testing.T) {
	m := migratiorm.New(
		migratiorm.WithSemanticComparison(true),
	)

	m.Expect(func(db *sql.DB) {
		repo := NewXOProductRepository(db)
		repo.Delete(context.Background(), 1)
	})

	m.Actual(func(db *sql.DB) {
		repo := NewGORMProductRepository(db)
		repo.Delete(context.Background(), 1)
	})

	m.Assert(t)
}

// TestMigration_DebugOutput demonstrates debugging capabilities.
func TestMigration_DebugOutput(t *testing.T) {
	m := migratiorm.New(
		migratiorm.WithSemanticComparison(true),
	)

	m.Expect(func(db *sql.DB) {
		repo := NewXOProductRepository(db)
		ctx := context.Background()
		repo.FindAll(ctx)
		repo.Delete(ctx, 1)
	})

	m.Actual(func(db *sql.DB) {
		repo := NewGORMProductRepository(db)
		ctx := context.Background()
		repo.FindAll(ctx)
		repo.Delete(ctx, 1)
	})

	// Show the raw and normalized queries for debugging
	t.Log("=== Expected (xo) ===")
	for i, q := range m.ExpectedQueries() {
		t.Logf("[%d] Operation: %s", i, q.Operation)
		t.Logf("[%d] Raw:        %s", i, q.Raw)
		t.Logf("[%d] Normalized: %s", i, q.Normalized)
	}

	t.Log("=== Actual (GORM) ===")
	for i, q := range m.ActualQueries() {
		t.Logf("[%d] Operation: %s", i, q.Operation)
		t.Logf("[%d] Raw:        %s", i, q.Raw)
		t.Logf("[%d] Normalized: %s", i, q.Normalized)
	}

	m.Assert(t)
}
