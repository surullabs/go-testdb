package testdb

import (
	"crypto/sha1"
	"database/sql"
	"database/sql/driver"
	"encoding/csv"
	"io"
	"regexp"
	"strings"
)

var d *testDriver

func init() {
	d = newDriver()
	sql.Register("testdb", d)
}

type testDriver struct {
	openFunc func(dsn string) (driver.Conn, error)
	conn     *conn
}

type query struct {
	rows   driver.Rows
	result *Result
	err    error
}

func newDriver() *testDriver {
	return &testDriver{
		conn: newConn(),
	}
}

func (d *testDriver) Open(dsn string) (driver.Conn, error) {
	if d.openFunc != nil {
		conn, err := d.openFunc(dsn)
		return conn, err
	}

	if d.conn == nil {
		d.conn = newConn()
	}

	return d.conn, nil
}

var whitespaceRegexp = regexp.MustCompile("\\s")

func getQueryHash(query string) string {
	// Remove whitespace and lowercase to make stubbing less brittle
	query = strings.ToLower(whitespaceRegexp.ReplaceAllString(query, ""))

	h := sha1.New()
	io.WriteString(h, query)

	return string(h.Sum(nil))
}

// Set your own function to be executed when db.Query() is called. As with StubQuery() you can use the RowsFromCSVString() method to easily generate the driver.Rows, or you can return your own.
func SetQueryFunc(f func(query string) (result driver.Rows, err error)) {
	d.conn.queryFunc = f
}

// Stubs the global driver.Conn to return the supplied driver.Rows when db.Query() is called, query stubbing is case insensitive, and whitespace is also ignored.
func StubQuery(q string, rows driver.Rows) {
	d.conn.queries[getQueryHash(q)] = query{
		rows: rows,
	}
}

// Stubs the global driver.Conn to return the supplied error when db.Query() is called, query stubbing is case insensitive, and whitespace is also ignored.
func StubQueryError(q string, err error) {
	d.conn.queries[getQueryHash(q)] = query{
		err: err,
	}
}

// Set your own function to be executed when db.Open() is called. You can either hand back a valid connection, or an error. Conn() can be used to grab the global Conn object containing stubbed queries.
func SetOpenFunc(f func(dsn string) (driver.Conn, error)) {
	d.openFunc = f
}

// Set your own function to be executed when db.Exec is called. You can return an error or a Result object with the LastInsertId and RowsAffected
func SetExecFunc(f func(query string, args ...interface{}) (sql.Result, error)) {
	d.conn.execFunc = f
}

// Stubs the global driver.Conn to return the supplied Result when db.Exec is called, query stubbing is case insensitive, and whitespace is also ignored.
func StubExec(q string, r *Result) {
	d.conn.queries[getQueryHash(q)] = query{
		result: r,
	}
}

// Stubs the global driver.Conn to return the supplied error when db.Exec() is called, query stubbing is case insensitive, and whitespace is also ignored.
func StubExecError(q string, err error) {
	StubQueryError(q, err)
}

// Set your own function to be executed when tx.Commit() is called. You can return an error or nil. This must be called prior to a call to db.Begin. A call to this function after a call to db.Begin() does not change the function set for the earlier transaction.
func SetCommitFunc(f func() error) {
	d.conn.commitFunc = f
}

// Set your own function to be executed when tx.Rollback() is called. Has the same behaviour as SetCommitFunc.
func SetRollbackFunc(f func() error) {
	d.conn.rollbackFunc = f
}

//Set your own function to be executed when sql.DB.Begin() is called to start a transaction.
func SetBeginFunc(f func() (driver.Tx, error)) {
	d.conn.beginFunc = f
}

// Set your own function to be executed when sql.DB.Close() is called to close a connection
func SetCloseFunc(f func() error) {
	d.conn.closeFunc = f
}

// Clears all stubbed queries, and replaced functions.
func Reset() {
	d.conn = newConn()
	d.openFunc = nil
}

// Returns a pointer to the global conn object associated with this driver.
func Conn() driver.Conn {
	return d.conn
}

func RowsFromCSVString(columns []string, s string) driver.Rows {
	rs := &rows{
		columns: columns,
		closed:  false,
	}

	r := strings.NewReader(strings.TrimSpace(s))
	csvReader := csv.NewReader(r)

	for {
		r, err := csvReader.Read()

		if err != nil || r == nil {
			break
		}

		row := make([]driver.Value, len(columns))

		for i, v := range r {
			v := strings.TrimSpace(v)
			row[i] = v
		}

		rs.rows = append(rs.rows, row)
	}

	return rs
}
