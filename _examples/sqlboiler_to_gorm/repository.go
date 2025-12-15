package sqlboiler_to_gorm

import (
	"context"
	"database/sql"
)

// User represents a user entity.
type User struct {
	ID    int64
	Name  string
	Email string
	Age   int
}

// UserRepository defines the interface for user data access.
type UserRepository interface {
	FindAll(ctx context.Context) ([]User, error)
	FindByID(ctx context.Context, id int64) (*User, error)
	FindByAge(ctx context.Context, minAge int) ([]User, error)
	Create(ctx context.Context, user *User) error
	Update(ctx context.Context, user *User) error
	Delete(ctx context.Context, id int64) error
}

// SQLBoilerUserRepository simulates SQLBoiler-style query generation.
// SQLBoiler generates queries with explicit column lists and PostgreSQL-style placeholders.
type SQLBoilerUserRepository struct {
	db *sql.DB
}

func NewSQLBoilerUserRepository(db *sql.DB) *SQLBoilerUserRepository {
	return &SQLBoilerUserRepository{db: db}
}

func (r *SQLBoilerUserRepository) FindAll(ctx context.Context) ([]User, error) {
	// SQLBoiler typically generates queries with explicit column selection
	query := `SELECT "users"."id", "users"."name", "users"."email", "users"."age" FROM "users"`
	_, err := r.db.QueryContext(ctx, query)
	return nil, err
}

func (r *SQLBoilerUserRepository) FindByID(ctx context.Context, id int64) (*User, error) {
	// SQLBoiler uses PostgreSQL-style $1, $2 placeholders
	query := `SELECT "users"."id", "users"."name", "users"."email", "users"."age" FROM "users" WHERE "users"."id" = $1`
	_, err := r.db.QueryContext(ctx, query, id)
	return nil, err
}

func (r *SQLBoilerUserRepository) FindByAge(ctx context.Context, minAge int) ([]User, error) {
	query := `SELECT "users"."id", "users"."name", "users"."email", "users"."age" FROM "users" WHERE "users"."age" >= $1`
	_, err := r.db.QueryContext(ctx, query, minAge)
	return nil, err
}

func (r *SQLBoilerUserRepository) Create(ctx context.Context, user *User) error {
	query := `INSERT INTO "users" ("name", "email", "age") VALUES ($1, $2, $3) RETURNING "id"`
	_, err := r.db.ExecContext(ctx, query, user.Name, user.Email, user.Age)
	return err
}

func (r *SQLBoilerUserRepository) Update(ctx context.Context, user *User) error {
	query := `UPDATE "users" SET "name" = $1, "email" = $2, "age" = $3 WHERE "id" = $4`
	_, err := r.db.ExecContext(ctx, query, user.Name, user.Email, user.Age, user.ID)
	return err
}

func (r *SQLBoilerUserRepository) Delete(ctx context.Context, id int64) error {
	query := `DELETE FROM "users" WHERE "id" = $1`
	_, err := r.db.ExecContext(ctx, query, id)
	return err
}

// GORMUserRepository simulates GORM-style query generation.
// GORM generates queries with backticks (MySQL) or can be configured for PostgreSQL.
type GORMUserRepository struct {
	db *sql.DB
}

func NewGORMUserRepository(db *sql.DB) *GORMUserRepository {
	return &GORMUserRepository{db: db}
}

func (r *GORMUserRepository) FindAll(ctx context.Context) ([]User, error) {
	// GORM typically uses SELECT * and table aliases
	query := "SELECT * FROM `users`"
	_, err := r.db.QueryContext(ctx, query)
	return nil, err
}

func (r *GORMUserRepository) FindByID(ctx context.Context, id int64) (*User, error) {
	// GORM uses ? placeholders
	query := "SELECT * FROM `users` WHERE `users`.`id` = ?"
	_, err := r.db.QueryContext(ctx, query, id)
	return nil, err
}

func (r *GORMUserRepository) FindByAge(ctx context.Context, minAge int) ([]User, error) {
	query := "SELECT * FROM `users` WHERE `users`.`age` >= ?"
	_, err := r.db.QueryContext(ctx, query, minAge)
	return nil, err
}

func (r *GORMUserRepository) Create(ctx context.Context, user *User) error {
	query := "INSERT INTO `users` (`name`, `email`, `age`) VALUES (?, ?, ?)"
	_, err := r.db.ExecContext(ctx, query, user.Name, user.Email, user.Age)
	return err
}

func (r *GORMUserRepository) Update(ctx context.Context, user *User) error {
	query := "UPDATE `users` SET `name` = ?, `email` = ?, `age` = ? WHERE `id` = ?"
	_, err := r.db.ExecContext(ctx, query, user.Name, user.Email, user.Age, user.ID)
	return err
}

func (r *GORMUserRepository) Delete(ctx context.Context, id int64) error {
	query := "DELETE FROM `users` WHERE `id` = ?"
	_, err := r.db.ExecContext(ctx, query, id)
	return err
}
