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

// TestMigration_FindByID verifies that FindByID generates equivalent queries.
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

	m.Assert(t)
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

// TestMigration_Create verifies that Create generates equivalent queries.
// After normalization, the INSERT statements should match.
func TestMigration_Create(t *testing.T) {
	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		repo := NewXOProductRepository(db)
		repo.Create(context.Background(), &Product{
			Name:        "Widget",
			Description: "A useful widget",
			Price:       2999,
			CategoryID:  1,
		})
	})

	m.Actual(func(db *sql.DB) {
		repo := NewGORMProductRepository(db)
		repo.Create(context.Background(), &Product{
			Name:        "Widget",
			Description: "A useful widget",
			Price:       2999,
			CategoryID:  1,
		})
	})

	// After normalization:
	// xo:   INSERT INTO products (name, description, price, category_id) VALUES (?, ?, ?, ?)
	// GORM: INSERT INTO products (name, description, price, category_id) VALUES (?, ?, ?, ?)
	m.Assert(t)
}

// TestMigration_Update verifies that Update generates equivalent queries.
func TestMigration_Update(t *testing.T) {
	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		repo := NewXOProductRepository(db)
		repo.Update(context.Background(), &Product{
			ID:          1,
			Name:        "Updated Widget",
			Description: "An updated widget",
			Price:       3999,
			CategoryID:  2,
		})
	})

	m.Actual(func(db *sql.DB) {
		repo := NewGORMProductRepository(db)
		repo.Update(context.Background(), &Product{
			ID:          1,
			Name:        "Updated Widget",
			Description: "An updated widget",
			Price:       3999,
			CategoryID:  2,
		})
	})

	// After normalization:
	// xo:   UPDATE products SET name = ?, description = ?, price = ?, category_id = ? WHERE id = ?
	// GORM: UPDATE products SET name = ?, description = ?, price = ?, category_id = ? WHERE id = ?
	m.Assert(t)
}

// TestMigration_Delete verifies that Delete generates equivalent queries.
func TestMigration_Delete(t *testing.T) {
	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		repo := NewXOProductRepository(db)
		repo.Delete(context.Background(), 1)
	})

	m.Actual(func(db *sql.DB) {
		repo := NewGORMProductRepository(db)
		repo.Delete(context.Background(), 1)
	})

	// After normalization:
	// xo:   DELETE FROM products WHERE id = ?
	// GORM: DELETE FROM products WHERE id = ?
	m.Assert(t)
}

// TestMigration_ComplexWorkflow verifies a complex sequence of operations.
func TestMigration_ComplexWorkflow(t *testing.T) {
	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		repo := NewXOProductRepository(db)
		ctx := context.Background()

		// Create a product
		repo.Create(ctx, &Product{Name: "Product A", Description: "Desc A", Price: 1000, CategoryID: 1})
		// Update it
		repo.Update(ctx, &Product{ID: 1, Name: "Product A Updated", Description: "Desc A Updated", Price: 1500, CategoryID: 1})
		// Create another
		repo.Create(ctx, &Product{Name: "Product B", Description: "Desc B", Price: 2000, CategoryID: 2})
		// Delete the first
		repo.Delete(ctx, 1)
	})

	m.Actual(func(db *sql.DB) {
		repo := NewGORMProductRepository(db)
		ctx := context.Background()

		repo.Create(ctx, &Product{Name: "Product A", Description: "Desc A", Price: 1000, CategoryID: 1})
		repo.Update(ctx, &Product{ID: 1, Name: "Product A Updated", Description: "Desc A Updated", Price: 1500, CategoryID: 1})
		repo.Create(ctx, &Product{Name: "Product B", Description: "Desc B", Price: 2000, CategoryID: 2})
		repo.Delete(ctx, 1)
	})

	m.Assert(t)
}

// TestMigration_UnorderedComparison demonstrates unordered comparison mode.
func TestMigration_UnorderedComparison(t *testing.T) {
	m := migratiorm.New(migratiorm.WithCompareMode(migratiorm.CompareUnordered))

	m.Expect(func(db *sql.DB) {
		repo := NewXOProductRepository(db)
		ctx := context.Background()

		repo.Delete(ctx, 1)
		repo.Delete(ctx, 2)
		repo.Delete(ctx, 3)
	})

	m.Actual(func(db *sql.DB) {
		repo := NewGORMProductRepository(db)
		ctx := context.Background()

		// Different order, but same queries
		repo.Delete(ctx, 3)
		repo.Delete(ctx, 1)
		repo.Delete(ctx, 2)
	})

	// Passes because we're comparing as sets (unordered)
	m.Assert(t)
}

// TestMigration_DebugOutput demonstrates debugging capabilities.
func TestMigration_DebugOutput(t *testing.T) {
	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		repo := NewXOProductRepository(db)
		ctx := context.Background()
		repo.Create(ctx, &Product{Name: "Test", Description: "Test Desc", Price: 100, CategoryID: 1})
		repo.Update(ctx, &Product{ID: 1, Name: "Test Updated", Description: "Updated Desc", Price: 200, CategoryID: 1})
	})

	m.Actual(func(db *sql.DB) {
		repo := NewGORMProductRepository(db)
		ctx := context.Background()
		repo.Create(ctx, &Product{Name: "Test", Description: "Test Desc", Price: 100, CategoryID: 1})
		repo.Update(ctx, &Product{ID: 1, Name: "Test Updated", Description: "Updated Desc", Price: 200, CategoryID: 1})
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
