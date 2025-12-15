package xo_to_gorm

import (
	"context"
	"database/sql"
)

// Product represents a product entity.
type Product struct {
	ID          int64
	Name        string
	Description string
	Price       int64
	CategoryID  int64
}

// ProductRepository defines the interface for product data access.
type ProductRepository interface {
	FindAll(ctx context.Context) ([]Product, error)
	FindByID(ctx context.Context, id int64) (*Product, error)
	FindByCategory(ctx context.Context, categoryID int64) ([]Product, error)
	FindByPriceRange(ctx context.Context, minPrice, maxPrice int64) ([]Product, error)
	Create(ctx context.Context, product *Product) error
	Update(ctx context.Context, product *Product) error
	Delete(ctx context.Context, id int64) error
}

// XOProductRepository simulates xo-style query generation.
// xo generates explicit queries based on database schema with PostgreSQL style.
type XOProductRepository struct {
	db *sql.DB
}

func NewXOProductRepository(db *sql.DB) *XOProductRepository {
	return &XOProductRepository{db: db}
}

func (r *XOProductRepository) FindAll(ctx context.Context) ([]Product, error) {
	// xo generates queries with explicit column names
	query := `SELECT id, name, description, price, category_id FROM products`
	_, err := r.db.QueryContext(ctx, query)
	return nil, err
}

func (r *XOProductRepository) FindByID(ctx context.Context, id int64) (*Product, error) {
	// xo uses $1 style placeholders for PostgreSQL
	query := `SELECT id, name, description, price, category_id FROM products WHERE id = $1`
	_, err := r.db.QueryContext(ctx, query, id)
	return nil, err
}

func (r *XOProductRepository) FindByCategory(ctx context.Context, categoryID int64) ([]Product, error) {
	query := `SELECT id, name, description, price, category_id FROM products WHERE category_id = $1`
	_, err := r.db.QueryContext(ctx, query, categoryID)
	return nil, err
}

func (r *XOProductRepository) FindByPriceRange(ctx context.Context, minPrice, maxPrice int64) ([]Product, error) {
	query := `SELECT id, name, description, price, category_id FROM products WHERE price >= $1 AND price <= $2`
	_, err := r.db.QueryContext(ctx, query, minPrice, maxPrice)
	return nil, err
}

func (r *XOProductRepository) Create(ctx context.Context, product *Product) error {
	query := `INSERT INTO products (name, description, price, category_id) VALUES ($1, $2, $3, $4)`
	_, err := r.db.ExecContext(ctx, query, product.Name, product.Description, product.Price, product.CategoryID)
	return err
}

func (r *XOProductRepository) Update(ctx context.Context, product *Product) error {
	query := `UPDATE products SET name = $1, description = $2, price = $3, category_id = $4 WHERE id = $5`
	_, err := r.db.ExecContext(ctx, query, product.Name, product.Description, product.Price, product.CategoryID, product.ID)
	return err
}

func (r *XOProductRepository) Delete(ctx context.Context, id int64) error {
	query := `DELETE FROM products WHERE id = $1`
	_, err := r.db.ExecContext(ctx, query, id)
	return err
}

// GORMProductRepository simulates GORM-style query generation.
type GORMProductRepository struct {
	db *sql.DB
}

func NewGORMProductRepository(db *sql.DB) *GORMProductRepository {
	return &GORMProductRepository{db: db}
}

func (r *GORMProductRepository) FindAll(ctx context.Context) ([]Product, error) {
	// GORM uses SELECT * with backticks for MySQL
	query := "SELECT * FROM `products`"
	_, err := r.db.QueryContext(ctx, query)
	return nil, err
}

func (r *GORMProductRepository) FindByID(ctx context.Context, id int64) (*Product, error) {
	query := "SELECT * FROM `products` WHERE `id` = ?"
	_, err := r.db.QueryContext(ctx, query, id)
	return nil, err
}

func (r *GORMProductRepository) FindByCategory(ctx context.Context, categoryID int64) ([]Product, error) {
	query := "SELECT * FROM `products` WHERE `category_id` = ?"
	_, err := r.db.QueryContext(ctx, query, categoryID)
	return nil, err
}

func (r *GORMProductRepository) FindByPriceRange(ctx context.Context, minPrice, maxPrice int64) ([]Product, error) {
	query := "SELECT * FROM `products` WHERE `price` >= ? AND `price` <= ?"
	_, err := r.db.QueryContext(ctx, query, minPrice, maxPrice)
	return nil, err
}

func (r *GORMProductRepository) Create(ctx context.Context, product *Product) error {
	query := "INSERT INTO `products` (`name`, `description`, `price`, `category_id`) VALUES (?, ?, ?, ?)"
	_, err := r.db.ExecContext(ctx, query, product.Name, product.Description, product.Price, product.CategoryID)
	return err
}

func (r *GORMProductRepository) Update(ctx context.Context, product *Product) error {
	query := "UPDATE `products` SET `name` = ?, `description` = ?, `price` = ?, `category_id` = ? WHERE `id` = ?"
	_, err := r.db.ExecContext(ctx, query, product.Name, product.Description, product.Price, product.CategoryID, product.ID)
	return err
}

func (r *GORMProductRepository) Delete(ctx context.Context, id int64) error {
	query := "DELETE FROM `products` WHERE `id` = ?"
	_, err := r.db.ExecContext(ctx, query, id)
	return err
}
