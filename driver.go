package dbring

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync/atomic"
)

var _ driver.Driver = (*Driver)(nil)

type Driver struct {
	internalDriver driver.Driver
	masterConn     driver.Conn
	slaveConns     []driver.Conn
	count          uint64 // Monotonically incrementing counter on each query
}

func (d *Driver) SetDriver(drv driver.Driver) {
	d.internalDriver = drv
}

func Register(name string, internalDriver driver.Driver) {
	sql.Register(name, &Driver{
		internalDriver: internalDriver,
	})
}

func (d *Driver) Open(rawDSNs string) (driver.Conn, error) {
	dsns := strings.Split(rawDSNs, ";")

	masterDSN := dsns[0]

	var err error
	d.masterConn, err = d.internalDriver.Open(masterDSN)
	if err != nil {
		return nil, err
	}
	return d, nil
}

// Prepare returns a prepared statement, bound to this connection.
func (d *Driver) Prepare(query string) (driver.Stmt, error) {
	stmts := make([]driver.Stmt, len(d.slaveConns)+1)

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

func (d *Driver) slave() driver.Conn {
	if len(d.slaveConns) == 0 {
		return d.masterConn
	}
	return d.slaveConns[d.nextSlaveNum()]
}

// slaveNum round-robins through the slaves and picks the next one
func (d *Driver) nextSlaveNum() int {
	return int((atomic.AddUint64(&d.count, 1) % uint64(len(d.slaveConns)-1)))
}

func (d *Driver) Query(query string, args []driver.Value) (driver.Rows, error) {
	dbExecer := d.slave()
	if queryer, ok := dbExecer.(driver.Queryer); ok {
		return queryer.Query(query, args)
	}
	return nil, driver.ErrSkip
}

func (d *Driver) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	dbExecer := d.slave()
	if queryerContext, ok := dbExecer.(driver.QueryerContext); ok {
		rows, err := queryerContext.QueryContext(ctx, query, args)
		return rows, err
	}

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return d.Query(query, dargs)
}

// copied from stdlib database/sql package: src/database/sql/ctxutil.go
func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	dargs := make([]driver.Value, len(named))
	for n, param := range named {
		if len(param.Name) > 0 {
			return nil, errors.New("sql: driver does not support the use of Named Parameters")
		}
		dargs[n] = param.Value
	}
	return dargs, nil
}
