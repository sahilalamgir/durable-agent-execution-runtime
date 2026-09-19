package idempotency

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Fail-closed retry policy (FR-8): 5 attempts in total, 500ms per attempt,
// backoff 200ms doubling to a 2s cap. Worst case before giving up is ~6s.
const (
	retryAttempts       = 5
	retryAttemptTimeout = 500 * time.Millisecond
	retryInitialBackoff = 200 * time.Millisecond
	retryMaxBackoff     = 2 * time.Second
)

// retry runs op up to retryAttempts times, each under its own timeout. Every
// failed attempt prints a redis_retry line. Errors that retrying cannot fix
// (a lost claim, a corrupt record) are returned immediately and unchanged.
// When every attempt fails it returns an error wrapping ErrFenceUnavailable.
func retry[T any](ctx context.Context, r *run, op func(ctx context.Context) (T, error)) (T, error) {
	var zero T
	backoff := retryInitialBackoff
	var last error
	for attempt := 1; attempt <= retryAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, retryAttemptTimeout)
		val, err := op(attemptCtx)
		cancel()
		if err == nil {
			return val, nil
		}
		if errors.Is(err, ErrClaimLost) || errors.Is(err, ErrCorruptRecord) {
			return zero, err
		}
		last = err
		r.log("redis_retry", fmt.Sprintf("attempt=%d/%d err=%q", attempt, retryAttempts, err.Error()))
		if ctx.Err() != nil {
			break
		}
		if attempt == retryAttempts {
			break
		}
		if err := r.g.cfg.Sleep(ctx, backoff); err != nil {
			last = err
			break
		}
		backoff *= 2
		if backoff > retryMaxBackoff {
			backoff = retryMaxBackoff
		}
	}
	return zero, fmt.Errorf("%w: %v", ErrFenceUnavailable, last)
}

// retryErr is retry for operations that return only an error.
func retryErr(ctx context.Context, r *run, op func(ctx context.Context) error) error {
	_, err := retry(ctx, r, func(c context.Context) (struct{}, error) { return struct{}{}, op(c) })
	return err
}

// sleepCtx waits d or until ctx is done.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
