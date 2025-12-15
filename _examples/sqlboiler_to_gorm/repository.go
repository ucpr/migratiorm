package sqlboiler_to_gorm

import (
	"context"
	"database/sql"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// User represents a user entity.
type User struct {
	ID    int64  `gorm:"primaryKey"`
	Name  string `gorm:"column:name"`
	Email string `gorm:"column:email"`
	Age   int    `gorm:"column:age"`
}

// TableName specifies the table name for GORM.
func (User) TableName() string {
	return "users"
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

// SQLBoilerUserRepository represents SQLBoiler-generated repository.
// SQLBoiler generates Go code that uses database/sql with PostgreSQL-style queries.
type SQLBoilerUserRepository struct {
	db *sql.DB
}

func NewSQLBoilerUserRepository(db *sql.DB) *SQLBoilerUserRepository {
	return &SQLBoilerUserRepository{db: db}
}

func (r *SQLBoilerUserRepository) FindAll(ctx context.Context) ([]User, error) {
	// SQLBoiler generates queries with explicit column selection and quoted identifiers
	query := `SELECT "users"."id", "users"."name", "users"."email", "users"."age" FROM "users"`
	_, err := r.db.QueryContext(ctx, query)
	return nil, err
}

func (r *SQLBoilerUserRepository) FindByID(ctx context.Context, id int64) (*User, error) {
	// SQLBoiler uses PostgreSQL-style $1, $2 placeholders with table-qualified columns
	query := `SELECT "users"."id", "users"."name", "users"."email", "users"."age" FROM "users" WHERE "users"."id" = $1`
	_, err := r.db.QueryContext(ctx, query, id)
	return nil, err
}

func (r *SQLBoilerUserRepository) FindByAge(ctx context.Context, minAge int) ([]User, error) {
	// SQLBoiler uses table-qualified columns with double quotes
	query := `SELECT "users"."id", "users"."name", "users"."email", "users"."age" FROM "users" WHERE "users"."age" >= $1`
	_, err := r.db.QueryContext(ctx, query, minAge)
	return nil, err
}

func (r *SQLBoilerUserRepository) Create(ctx context.Context, user *User) error {
	// SQLBoiler uses RETURNING clause for PostgreSQL
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
	query := `DELETE FROM "users" WHERE "users"."id" = $1`
	_, err := r.db.ExecContext(ctx, query, id)
	return err
}

// GORMUserRepository uses the actual GORM package.
type GORMUserRepository struct {
	db *gorm.DB
}

// NewGORMUserRepository creates a GORM repository from a *sql.DB.
func NewGORMUserRepository(sqlDB *sql.DB) *GORMUserRepository {
	// Initialize GORM with the existing *sql.DB connection
	gormDB, err := gorm.Open(mysql.New(mysql.Config{
		Conn:                      sqlDB,
		SkipInitializeWithVersion: true,
	}), &gorm.Config{
		SkipDefaultTransaction: true,
	})
	if err != nil {
		panic(err)
	}
	return &GORMUserRepository{db: gormDB}
}

func (r *GORMUserRepository) FindAll(ctx context.Context) ([]User, error) {
	var users []User
	err := r.db.WithContext(ctx).Find(&users).Error
	return users, err
}

func (r *GORMUserRepository) FindByID(ctx context.Context, id int64) (*User, error) {
	var user User
	err := r.db.WithContext(ctx).First(&user, id).Error
	return &user, err
}

func (r *GORMUserRepository) FindByAge(ctx context.Context, minAge int) ([]User, error) {
	var users []User
	err := r.db.WithContext(ctx).Where("age >= ?", minAge).Find(&users).Error
	return users, err
}

func (r *GORMUserRepository) Create(ctx context.Context, user *User) error {
	return r.db.WithContext(ctx).Create(user).Error
}

func (r *GORMUserRepository) Update(ctx context.Context, user *User) error {
	return r.db.WithContext(ctx).Save(user).Error
}

func (r *GORMUserRepository) Delete(ctx context.Context, id int64) error {
	return r.db.WithContext(ctx).Delete(&User{}, id).Error
}
