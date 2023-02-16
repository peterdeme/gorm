package tests_test

import (
	"context"
	"database/sql"

	"gorm.io/gorm"
)

type wrapperTx struct {
	*sql.Tx
	conn *wrapperConnPool
}

func (c *wrapperTx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	c.conn.got = append(c.conn.got, query)
	return c.Tx.PrepareContext(ctx, query)
}

func (c *wrapperTx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	c.conn.got = append(c.conn.got, query)
	return c.Tx.ExecContext(ctx, query, args...)
}

func (c *wrapperTx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	c.conn.got = append(c.conn.got, query)
	return c.Tx.QueryContext(ctx, query, args...)
}

func (c *wrapperTx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	c.conn.got = append(c.conn.got, query)
	return c.Tx.QueryRowContext(ctx, query, args...)
}

type wrapperConnPool struct {
	db            *sql.DB
	got           []string
	expect        []string
	returnErrorOn map[string]error
}

func (c *wrapperConnPool) Ping() error {
	return c.db.Ping()
}

// If you use BeginTx returned *sql.Tx as shown below then you can't record queries in a transaction.
//
//	func (c *wrapperConnPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
//		 return c.db.BeginTx(ctx, opts)
//	}
//
// You should use BeginTx returned gorm.Tx which could wrap *sql.Tx then you can record all queries.
func (c *wrapperConnPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (gorm.ConnPool, error) {
	tx, err := c.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &wrapperTx{Tx: tx, conn: c}, nil
}

func (c *wrapperConnPool) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	c.got = append(c.got, query)
	return c.db.PrepareContext(ctx, query)
}

func (c *wrapperConnPool) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	for q, err := range c.returnErrorOn {
		if q == query {
			return nil, err
		}
	}

	c.got = append(c.got, query)
	return c.db.ExecContext(ctx, query, args...)
}

func (c *wrapperConnPool) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	for q, err := range c.returnErrorOn {
		if q == query {
			return nil, err
		}
	}

	c.got = append(c.got, query)
	return c.db.QueryContext(ctx, query, args...)
}

func (c *wrapperConnPool) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	c.got = append(c.got, query)
	return c.db.QueryRowContext(ctx, query, args...)
}
