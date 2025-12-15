package xo_to_gorm

import (
	"context"
	"database/sql"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// Product represents a product entity.
type Product struct {
	ID          int64  `gorm:"primaryKey"`
	Name        string `gorm:"column:name"`
	Description string `gorm:"column:description"`
	Price       int64  `gorm:"column:price"`
	CategoryID  int64  `gorm:"column:category_id"`
}

// TableName specifies the table name for GORM.
func (Product) TableName() string {
	return "products"
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

// XOProductRepository represents xo-generated repository.
// xo generates Go code that uses database/sql directly with explicit queries.
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
	query := `SELECT id, name, description, price, category_id FROM products WHERE products.id = $1`
	_, err := r.db.QueryContext(ctx, query, id)
	return nil, err
}

func (r *XOProductRepository) FindByCategory(ctx context.Context, categoryID int64) ([]Product, error) {
	// xo typically uses table-qualified columns
	query := `SELECT id, name, description, price, category_id FROM products WHERE products.category_id = $1`
	_, err := r.db.QueryContext(ctx, query, categoryID)
	return nil, err
}

func (r *XOProductRepository) FindByPriceRange(ctx context.Context, minPrice, maxPrice int64) ([]Product, error) {
	// xo typically uses table-qualified columns
	query := `SELECT id, name, description, price, category_id FROM products WHERE products.price >= $1 AND products.price <= $2`
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
	query := `DELETE FROM products WHERE products.id = $1`
	_, err := r.db.ExecContext(ctx, query, id)
	return err
}

// GORMProductRepository uses the actual GORM package.
type GORMProductRepository struct {
	db *gorm.DB
}

// NewGORMProductRepository creates a GORM repository from a *sql.DB.
func NewGORMProductRepository(sqlDB *sql.DB) *GORMProductRepository {
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
	return &GORMProductRepository{db: gormDB}
}

func (r *GORMProductRepository) FindAll(ctx context.Context) ([]Product, error) {
	var products []Product
	err := r.db.WithContext(ctx).Find(&products).Error
	return products, err
}

func (r *GORMProductRepository) FindByID(ctx context.Context, id int64) (*Product, error) {
	var product Product
	err := r.db.WithContext(ctx).First(&product, id).Error
	return &product, err
}

func (r *GORMProductRepository) FindByCategory(ctx context.Context, categoryID int64) ([]Product, error) {
	var products []Product
	err := r.db.WithContext(ctx).Where("category_id = ?", categoryID).Find(&products).Error
	return products, err
}

func (r *GORMProductRepository) FindByPriceRange(ctx context.Context, minPrice, maxPrice int64) ([]Product, error) {
	var products []Product
	err := r.db.WithContext(ctx).Where("price >= ? AND price <= ?", minPrice, maxPrice).Find(&products).Error
	return products, err
}

func (r *GORMProductRepository) Create(ctx context.Context, product *Product) error {
	return r.db.WithContext(ctx).Create(product).Error
}

func (r *GORMProductRepository) Update(ctx context.Context, product *Product) error {
	return r.db.WithContext(ctx).Save(product).Error
}

func (r *GORMProductRepository) Delete(ctx context.Context, id int64) error {
	return r.db.WithContext(ctx).Delete(&Product{}, id).Error
}
