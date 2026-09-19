package idempotency

import "errors"

var (
	// ErrFenceUnavailable means the fence could not be consulted (Redis or a
	// Reconcile call kept failing). The tool did not run; the worker exits 4
	// without a terminal event so the run stays resumable (FR-8).
	ErrFenceUnavailable = errors.New("idempotency fence unavailable")
	// ErrUnresolvable means a stale claim exists for a tool that cannot
	// check whether its effect happened. The run fails rather than guess.
	ErrUnresolvable = errors.New("claimed side effect cannot be resolved")
	// ErrRecordMismatch means the stored record does not describe this
	// invocation (or is corrupt). The tool is never executed.
	ErrRecordMismatch = errors.New("idempotency record does not match invocation")
	// ErrOutcomeUnknown means Execute may or may not have taken effect. The
	// key is left claimed and the worker exits 4.
	ErrOutcomeUnknown = errors.New("side effect outcome unknown; key left claimed")
	// ErrClaimLost means a compare-and-set found the claim no longer held.
	ErrClaimLost = errors.New("claim no longer held")
	// ErrCorruptRecord is returned by a Store when the value under a key is
	// not valid IdemRecord JSON. The Guard maps it to ErrRecordMismatch.
	ErrCorruptRecord = errors.New("idempotency record is not valid json")
)
