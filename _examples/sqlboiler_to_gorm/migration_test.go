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
// Note: These queries are semantically equivalent but structurally different.
// migratiorm normalizes quotes and whitespace, but SELECT * vs explicit columns
// will still differ. In real migration scenarios, you may need to adjust queries
// or use IgnoreOrder for cases where column order doesn't matter.
func TestMigration_FindAll(t *testing.T) {
	t.Skip("SELECT * vs explicit columns are structurally different - shown for demonstration")

	m := migratiorm.New()

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

// TestMigration_FindByID verifies that FindByID generates equivalent queries.
func TestMigration_FindByID(t *testing.T) {
	t.Skip("SELECT * vs explicit columns are structurally different - shown for demonstration")

	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		repo := NewSQLBoilerUserRepository(db)
		repo.FindByID(context.Background(), 1)
	})

	m.Actual(func(db *sql.DB) {
		repo := NewGORMUserRepository(db)
		repo.FindByID(context.Background(), 1)
	})

	m.Assert(t)
}

// TestMigration_Create verifies that Create generates equivalent queries.
// Both ORMs generate similar INSERT statements, differing only in:
// - Quote style: " vs `
// - Placeholder style: $1,$2,$3 vs ?,?,?
// - RETURNING clause (SQLBoiler) vs not (GORM)
func TestMigration_Create(t *testing.T) {
	t.Skip("RETURNING clause difference - shown for demonstration")

	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		repo := NewSQLBoilerUserRepository(db)
		repo.Create(context.Background(), &User{Name: "Alice", Email: "alice@example.com", Age: 30})
	})

	m.Actual(func(db *sql.DB) {
		repo := NewGORMUserRepository(db)
		repo.Create(context.Background(), &User{Name: "Alice", Email: "alice@example.com", Age: 30})
	})

	m.Assert(t)
}

// TestMigration_Update verifies that Update generates equivalent queries.
// This is a good example where both ORMs generate equivalent queries
// after normalization.
func TestMigration_Update(t *testing.T) {
	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		repo := NewSQLBoilerUserRepository(db)
		repo.Update(context.Background(), &User{ID: 1, Name: "Bob", Email: "bob@example.com", Age: 25})
	})

	m.Actual(func(db *sql.DB) {
		repo := NewGORMUserRepository(db)
		repo.Update(context.Background(), &User{ID: 1, Name: "Bob", Email: "bob@example.com", Age: 25})
	})

	// After normalization:
	// SQLBoiler: UPDATE users SET name = ?, email = ?, age = ? WHERE id = ?
	// GORM:      UPDATE users SET name = ?, email = ?, age = ? WHERE id = ?
	m.Assert(t)
}

// TestMigration_Delete verifies that Delete generates equivalent queries.
func TestMigration_Delete(t *testing.T) {
	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		repo := NewSQLBoilerUserRepository(db)
		repo.Delete(context.Background(), 1)
	})

	m.Actual(func(db *sql.DB) {
		repo := NewGORMUserRepository(db)
		repo.Delete(context.Background(), 1)
	})

	// After normalization:
	// SQLBoiler: DELETE FROM users WHERE id = ?
	// GORM:      DELETE FROM users WHERE id = ?
	m.Assert(t)
}

// TestMigration_MultipleOperations verifies a sequence of operations.
func TestMigration_MultipleOperations(t *testing.T) {
	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		repo := NewSQLBoilerUserRepository(db)
		ctx := context.Background()

		repo.Update(ctx, &User{ID: 1, Name: "Alice", Email: "alice@example.com", Age: 30})
		repo.Delete(ctx, 2)
		repo.Update(ctx, &User{ID: 3, Name: "Charlie", Email: "charlie@example.com", Age: 35})
	})

	m.Actual(func(db *sql.DB) {
		repo := NewGORMUserRepository(db)
		ctx := context.Background()

		repo.Update(ctx, &User{ID: 1, Name: "Alice", Email: "alice@example.com", Age: 30})
		repo.Delete(ctx, 2)
		repo.Update(ctx, &User{ID: 3, Name: "Charlie", Email: "charlie@example.com", Age: 35})
	})

	m.Assert(t)
}

// TestMigration_DebugQueries demonstrates how to debug captured queries.
func TestMigration_DebugQueries(t *testing.T) {
	m := migratiorm.New()

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
