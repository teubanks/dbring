package msring

import (
	"database/sql"
	"database/sql/driver"
)

type rows struct {
	rows *sql.Rows
}

func (r *rows) Columns() []string {
	c, _ := r.rows.Columns()
	return c
}

func (r *rows) Close() error {
	return r.rows.Close()
}

func (r *rows) Next(dest []driver.Value) error {
	return r.rows.Scan(dest)
}
