package testdb

import (
	"database/sql"
	"database/sql/driver"
	"errors"
)

type conn struct {
	queries      map[string]query
	queryFunc    func(query string) (result driver.Rows, err error)
	execFunc     func(query string, args ...interface{}) (sql.Result, error)
	commitFunc   func() (err error)
	rollbackFunc func() (err error)
	beginFunc    func() (driver.Tx, error)
	closeFunc    func() error
}

func newConn() *conn {
	return &conn{
		queries: make(map[string]query),
	}
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	if c.queryFunc != nil {
		rows, err := c.queryFunc(query)

		return &stmt{
			rows: rows,
			err:  err,
		}, nil
	}

	if q, ok := d.conn.queries[getQueryHash(query)]; ok {
		return &stmt{
			rows:   q.rows,
			err:    q.err,
			result: q.result,
		}, nil
	}

	return new(stmt), errors.New("Query not stubbed: " + query)
}

func (c *conn) Close() error {
	if c.closeFunc != nil {
		return c.closeFunc()
	}
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	if c.beginFunc != nil {
		return c.beginFunc()
	}
	return &tx{commitFunc: c.commitFunc, rollbackFunc: c.rollbackFunc}, nil
}

func toInterface(args []driver.Value) []interface{} {
	result := make([]interface{}, len(args))
	for i, value := range args {
		result[i] = value
	}
	return result
}

func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if c.execFunc != nil {
		return c.execFunc(query, toInterface(args)...)
	}

	if q, ok := d.conn.queries[getQueryHash(query)]; ok {
		if q.result != nil {
			return q.result, nil
		} else if q.err != nil {
			return nil, q.err
		}
	}

	return nil, errors.New("Exec call not stubbed: " + query)
}
