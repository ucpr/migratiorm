package migratiorm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/ucpr/migratiorm/internal/normalizer"
)

// capturer captures SQL queries executed against a database.
type capturer struct {
	db         *sql.DB
	driver     *capturingDriver
	normalizer *normalizer.Normalizer
}

// newCapturer creates a new capturer instance.
func newCapturer(n *normalizer.Normalizer) (*capturer, error) {
	drv := &capturingDriver{
		queries:    make([]rawQuery, 0),
		normalizer: n,
	}

	// Register the driver with a unique name
	driverName := drv.register()

	db, err := sql.Open(driverName, "")
	if err != nil {
		return nil, err
	}

	return &capturer{
		db:         db,
		driver:     drv,
		normalizer: n,
	}, nil
}

// DB returns the database connection for use by ORMs.
func (c *capturer) DB() *sql.DB {
	return c.db
}

// Queries returns all captured queries.
func (c *capturer) Queries() []Query {
	return c.driver.Queries()
}

// Close closes the database connection.
func (c *capturer) Close() error {
	return c.db.Close()
}

// rawQuery holds the raw captured query data.
type rawQuery struct {
	query string
	args  []any
}

// capturingDriver is a database driver that captures all executed queries.
type capturingDriver struct {
	queries    []rawQuery
	normalizer *normalizer.Normalizer
	mu         sync.Mutex
}

var driverCounter int64

// register registers the driver and returns its unique name.
func (d *capturingDriver) register() string {
	name := fmt.Sprintf("migratiorm_%d", atomic.AddInt64(&driverCounter, 1))
	sql.Register(name, d)
	return name
}

// Open returns a new connection to the database.
func (d *capturingDriver) Open(name string) (driver.Conn, error) {
	return &capturingConn{driver: d}, nil
}

// recordQuery records a query with its arguments.
func (d *capturingDriver) recordQuery(query string, args []any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.queries = append(d.queries, rawQuery{query: query, args: args})
}

// Queries returns all captured queries as Query structs.
func (d *capturingDriver) Queries() []Query {
	d.mu.Lock()
	defer d.mu.Unlock()

	result := make([]Query, len(d.queries))
	for i, rq := range d.queries {
		normalized := d.normalizer.Normalize(rq.query)
		result[i] = Query{
			Raw:        rq.query,
			Normalized: normalized,
			Args:       rq.args,
			Operation:  detectOperation(rq.query),
		}
	}
	return result
}

// capturingConn is a database connection that captures queries.
type capturingConn struct {
	driver *capturingDriver
}

func (c *capturingConn) Prepare(query string) (driver.Stmt, error) {
	return &capturingStmt{conn: c, query: query}, nil
}

func (c *capturingConn) Close() error {
	return nil
}

func (c *capturingConn) Begin() (driver.Tx, error) {
	return &capturingTx{conn: c}, nil
}

// Implement driver.QueryerContext for direct Query calls.
func (c *capturingConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.driver.recordQuery(query, namedValuesToAny(args))
	return &emptyRows{}, nil
}

// Implement driver.ExecerContext for direct Exec calls.
func (c *capturingConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.driver.recordQuery(query, namedValuesToAny(args))
	return &emptyResult{}, nil
}

// capturingStmt is a prepared statement that captures queries.
type capturingStmt struct {
	conn  *capturingConn
	query string
}

func (s *capturingStmt) Close() error {
	return nil
}

func (s *capturingStmt) NumInput() int {
	return -1 // Unknown number of inputs
}

func (s *capturingStmt) Exec(args []driver.Value) (driver.Result, error) {
	s.conn.driver.recordQuery(s.query, valuesToAny(args))
	return &emptyResult{}, nil
}

func (s *capturingStmt) Query(args []driver.Value) (driver.Rows, error) {
	s.conn.driver.recordQuery(s.query, valuesToAny(args))
	return &emptyRows{}, nil
}

// capturingTx is a transaction that does nothing but satisfies the interface.
type capturingTx struct {
	conn *capturingConn
}

func (t *capturingTx) Commit() error {
	return nil
}

func (t *capturingTx) Rollback() error {
	return nil
}

// emptyResult is a result that returns zero values.
type emptyResult struct{}

func (r *emptyResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r *emptyResult) RowsAffected() (int64, error) {
	return 0, nil
}

// emptyRows is a rows iterator that returns no rows.
type emptyRows struct {
	closed bool
}

func (r *emptyRows) Columns() []string {
	return []string{}
}

func (r *emptyRows) Close() error {
	r.closed = true
	return nil
}

func (r *emptyRows) Next(dest []driver.Value) error {
	return io.EOF
}

// Helper functions to convert driver values.
func valuesToAny(values []driver.Value) []any {
	result := make([]any, len(values))
	for i, v := range values {
		result[i] = v
	}
	return result
}

func namedValuesToAny(values []driver.NamedValue) []any {
	result := make([]any, len(values))
	for i, v := range values {
		result[i] = v.Value
	}
	return result
}
