package msring

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"sync/atomic"
)

var _ driver.Driver = (*Driver)(nil)

type Driver struct {
	driverName string
	masterConn *sql.DB
	slaveConns []*sql.DB
	count      uint64 // Monotonically incrementing counter on each query
}

func init() {
	sql.Register("msring", &Driver{})
}

func (d *Driver) Open(rawDSNs string) (driver.Conn, error) {
	dsns := strings.Split(rawDSNs, ";")

	masterDSN := dsns[0]

	var err error
	d.masterConn, err = sql.Open(d.driverName, masterDSN)
	if err != nil {
		return nil, err
	}
	return d, nil
}

// Prepare returns a prepared statement, bound to this connection.
func (d *Driver) Prepare(query string) (driver.Stmt, error) {
	stmts := make([]*sql.Stmt, len(d.slaveConns)+1)

	ps, err := d.masterConn.Prepare(query)
	if err != nil {
		return nil, err
	}

	stmts[0] = ps

	err = onEach(len(d.slaveConns), func(i int) (err error) {
		stmts[i+1], err = d.slaveConns[i].Prepare(query)
		return err
	})

	if err != nil {
		return nil, err
	}

	return &prepStmt{dvr: d, stmts: stmts}, nil
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (d *Driver) Close() error {
	return nil
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (d *Driver) Begin() (driver.Tx, error) {
	return d.masterConn.Begin()
}

func (d *Driver) slave() *sql.DB {
	return d.slaveConns[d.nextSlaveNum()]
}

// slaveNum round-robins through the slaves and picks the next one
func (d *Driver) nextSlaveNum() int {
	return int((atomic.AddUint64(&d.count, 1) % uint64(len(d.slaveConns)-1)))
}

func (d *Driver) Query(query string, args []driver.Value) (driver.Rows, error) {
	res, err := d.slave().Query(query, args)
	if err != nil {
		return nil, err
	}
	return &rows{res}, nil
}

func (d *Driver) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	res, err := d.slave().QueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return &rows{res}, nil
}
