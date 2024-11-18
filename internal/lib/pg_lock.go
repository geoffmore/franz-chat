package lib

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultSessionLockTable = "metrics"
const defaultSessionLockID = 12345

type SessionLock struct {
	Table   string
	ID      int
	hasLock bool // TODO
}

type Stmt string

const getLockStmt Stmt = "SELECT pg_try_advisory_lock($1) FROM $2;"

// See https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS

func getLock(ctx context.Context, c *pgxpool.Conn) bool {
	var hasLock bool
	//row := c.QueryRow(ctx, string(getLock), lock.ID, lock.Table)
	//row := c.QueryRow(ctx, "SELECT pg_try_advisory_lock($1) FROM $2;", 12345, "metrics")
	//row := c.QueryRow(ctx, "SELECT pg_try_advisory_lock(12345) FROM metrics;")
	/* TODO - deterministically set a lock bigint that can be accessed across multiple instances
	maybe INSERT into metrics random(bigint) if a SELECT returns no rows?
	*/
	// TODO - determine why "SELECT pg_try_advisory_lock(12345) FROM metrics" does not work
	row := c.QueryRow(ctx, "SELECT pg_try_advisory_lock(12345);")
	err := row.Scan(&hasLock)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		//
	}
	return hasLock
}

// A releaseLock function would be useful, but is not necessary since session locks are released on session end
