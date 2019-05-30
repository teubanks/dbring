package msring

import (
	"database/sql"
	"database/sql/driver"
)

// prepStmt is a prepared statement that conforms to the driver.Stmt interface.
//
type prepStmt struct {
	dvr   *Driver
	stmts []*sql.Stmt
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

func (s *prepStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.stmts[0].Exec(args)
}

func (s *prepStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.stmts[s.dvr.nextSlaveNum()+1].Query(args)
}
