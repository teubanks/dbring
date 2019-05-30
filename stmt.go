package dbring

import (
	"context"
	"database/sql/driver"
)

// prepStmt is a prepared statement that conforms to the driver.Stmt interface.
//
type prepStmt struct {
	// dvr gives us access to the db connections
	dvr   *Driver
	stmts []driver.Stmt
}

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
func (s *prepStmt) Close() error {
	s.dvr.masterConn.Close()
	err := onEach(len(s.dvr.slaveConns), func(i int) error {
		err := s.dvr.slaveConns[i].Close()
		return err
	})

	return err
}

func (s *prepStmt) NumInput() int {
	return s.stmts[0].NumInput()
}

func (s *prepStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.stmts[0].Exec(args)
}

func (s *prepStmt) Query(args []driver.Value) (driver.Rows, error) {
	smt := s.stmts[s.dvr.nextSlaveNum()+1]
	return smt.Query(args)
}

func (s *prepStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	smt := s.stmts[0]
	if execer, ok := smt.(driver.StmtExecContext); ok {
		return execer.ExecContext(ctx, args)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	return smt.Exec(dargs)
}

func (s *prepStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	smt := s.stmts[s.dvr.nextSlaveNum()+1]

	if queryer, ok := smt.(driver.StmtQueryContext); ok {
		return queryer.QueryContext(ctx, args)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	return smt.Query(dargs)
}
