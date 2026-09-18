package store

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

// IsTransientError reports whether err is a Postgres/connection failure
// worth retrying forever (EC-8), as opposed to a permanent error (a
// constraint violation, invalid input syntax, etc.) that will fail
// identically on every retry and should be treated like a poison message
// (EC-7) instead: log it and stop, rather than spin forever with no
// progress and no signal that anything is wrong.
//
// A *pgconn.PgError means Postgres itself returned a structured response,
// so its SQLSTATE class decides: connection exception (08), insufficient
// resources (53), operator intervention (57 — admin/crash shutdown), and
// transaction rollback (40 — serialization failure, deadlock) are worth
// retrying. Anything else with a SQLSTATE (22 data exception, 23 integrity
// constraint violation, 42 syntax/access rule violation, ...) is permanent:
// the exact same statement fails the exact same way every time.
//
// An error that ISN'T a *pgconn.PgError at all never got a structured
// response from the server in the first place — a dropped connection, a
// timeout, "connection refused" while Postgres is down — which is exactly
// EC-8's scenario, so it's treated as transient too.
func IsTransientError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return true
	}
	switch pgErr.Code[:2] {
	case "08", "53", "57", "40":
		return true
	default:
		return false
	}
}
