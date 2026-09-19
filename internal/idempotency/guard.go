package idempotency

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/replay"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/tools"
)

// Resolution says how a side-effecting invocation's result was obtained. The
// string values match events.ToolResultedPayload.Resolution.
type Resolution string

const (
	ResolutionExecuted   Resolution = "executed"
	ResolutionCached     Resolution = "cached"
	ResolutionReconciled Resolution = "reconciled"
	ResolutionReexecuted Resolution = "reexecuted"
)

// Outcome is what the caller journals as ToolResulted.
type Outcome struct {
	Result     string
	Status     string // "success" | "error"
	Resolution Resolution
}

// DefaultStaleGrace is how long past tool_timeout a claim must age before it
// is presumed to belong to a dead process (D-5).
const DefaultStaleGrace = 30 * time.Second

const (
	// pollInterval is how often a waiting Guard re-reads a claim that is not
	// stale yet.
	pollInterval = time.Second
	// waitLogInterval bounds how often "waiting" is printed.
	waitLogInterval = 5 * time.Second
	// maxClaimRaces is how many times a lost compare-and-set may restart the
	// decision before giving up.
	maxClaimRaces = 3
	// ExpiryMargin is subtracted from the TTL when deciding whether an absent
	// key still proves "never claimed" (FR-13). The TTL must exceed it (plus
	// tool_timeout and the stale grace) or every claim would look expired.
	ExpiryMargin = time.Minute
)

// errRestart is internal: the record changed under us, so re-run the
// decision from Claim.
var errRestart = errors.New("restart decision at claim")

// GuardConfig configures a Guard. Zero values get defaults, except
// ToolTimeout and TTL which the caller must set.
type GuardConfig struct {
	// ToolTimeout bounds Execute via a context deadline.
	ToolTimeout time.Duration
	// StaleGrace defaults to DefaultStaleGrace.
	StaleGrace time.Duration
	// TTL is the key expiry; it must match the Store's TTL.
	TTL time.Duration
	// Out receives the "fence ..." decision lines. Defaults to io.Discard.
	Out io.Writer
	// Crash is the crash-injection hook. Defaults to never crashing.
	Crash CrashHook
	// Now and Sleep are injectable so tests need no real waiting.
	Now   func() time.Time
	Sleep func(ctx context.Context, d time.Duration) error
}

// Guard is the one code path that runs a side-effecting tool: the live
// EXECUTE_TOOL path and the recovery RESOLVE_IN_FLIGHT_TOOL path both call
// Run (D-3). There is no separate recovery logic.
type Guard struct {
	store     Store
	cfg       GuardConfig
	claimedBy string
}

// NewGuard builds a Guard over store.
func NewGuard(store Store, cfg GuardConfig) *Guard {
	if cfg.StaleGrace == 0 {
		cfg.StaleGrace = DefaultStaleGrace
	}
	if cfg.Out == nil {
		cfg.Out = io.Discard
	}
	if cfg.Crash == nil {
		cfg.Crash = noCrash{}
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Sleep == nil {
		cfg.Sleep = sleepCtx
	}
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return &Guard{store: store, cfg: cfg, claimedBy: fmt.Sprintf("%s-%d", host, os.Getpid())}
}

// run holds the immutable inputs of one Guard.Run call, so the per-row
// helpers below don't each need a long parameter list.
type run struct {
	g        *Guard
	inv      tools.Invocation
	tc       replay.ToolCall
	tool     tools.Tool
	key      string
	argsHash string
}

// Run executes the decision table in phase3-idempotency.md's API Contracts,
// starting with an atomic Claim. It is called only after ToolInvoked is
// acked, for tools whose journaled has_side_effect is true, and returns an
// Outcome to journal as ToolResulted or one of the Err* values.
func (g *Guard) Run(ctx context.Context, inv tools.Invocation, tc replay.ToolCall, tool tools.Tool) (Outcome, error) {
	if inv.IdempotencyKey == nil {
		return Outcome{}, errors.New("guard run: invocation has no idempotency key")
	}
	hash, err := ArgsHash(tc.ToolName, tc.ToolArgs)
	if err != nil {
		return Outcome{}, fmt.Errorf("guard run: hashing tool args: %w", err)
	}
	r := &run{g: g, inv: inv, tc: tc, tool: tool, key: *inv.IdempotencyKey, argsHash: hash}

	for race := 0; race < maxClaimRaces; race++ {
		out, err := r.attempt(ctx)
		if !errors.Is(err, errRestart) {
			return out, err
		}
	}
	r.log("fence_unavailable", "reason=lost_claim_races")
	return Outcome{}, fmt.Errorf("%w: lost the claim race %d times", ErrFenceUnavailable, maxClaimRaces)
}

// attempt is one pass through the decision table, starting at Claim.
func (r *run) attempt(ctx context.Context) (Outcome, error) {
	mine := r.newRecord()
	res, err := retry(ctx, r, func(c context.Context) (ClaimResult, error) {
		return r.g.store.Claim(c, r.key, mine)
	})
	if err != nil {
		return Outcome{}, r.storeErr(err)
	}
	if res.Acquired {
		return r.acquired(ctx, mine)
	}
	if res.Existing == nil {
		// A Store must return the existing record when the claim is not
		// acquired. Without it we cannot know who holds the key: fail closed.
		r.log("fence_unavailable", "reason=claim_neither_acquired_nor_existing")
		return Outcome{}, fmt.Errorf("%w: store returned neither an acquired claim nor an existing record", ErrFenceUnavailable)
	}
	return r.existing(ctx, mine, *res.Existing)
}

// acquired handles rows 1 and 2: the key was absent and we now hold it.
func (r *run) acquired(ctx context.Context, mine IdemRecord) (Outcome, error) {
	if r.keyMayHaveExpired() {
		// Row 2: an absent key is only proof of "never claimed" while the
		// key could not have expired (FR-13). Reconcile before executing.
		return r.reconcile(ctx, mine, false)
	}
	r.log("acquired", "")
	return r.execute(ctx, mine, ResolutionExecuted)
}

// existing handles rows 3-6: the key already held a record.
func (r *run) existing(ctx context.Context, mine, ex IdemRecord) (Outcome, error) {
	if ex.State == StateClaimed && ex.ClaimToken == mine.ClaimToken {
		// Row 3: an earlier SET of ours took effect but its reply was lost;
		// the retry sees our own record (EC-3). That is row 1's situation (the
		// key was absent), so it must pass the same FR-13 expiry check.
		return r.acquired(ctx, mine)
	}
	if err := r.verify(ex); err != nil {
		return Outcome{}, err
	}
	if ex.State == StateResolved {
		return r.cacheHit(ex)
	}

	settled, err := r.waitUntilSettled(ctx, ex)
	if err != nil {
		return Outcome{}, err
	}
	if settled.State == StateResolved {
		return r.cacheHit(settled)
	}
	return r.reconcile(ctx, settled, true)
}

// cacheHit is row 5: the effect already happened and was recorded.
func (r *run) cacheHit(rec IdemRecord) (Outcome, error) {
	if rec.Result == nil || rec.Status == nil {
		r.log("record_mismatch", "reason=resolved_without_result")
		return Outcome{}, r.mismatchErr("resolved record has no result")
	}
	r.log("cache_hit", "")
	return Outcome{Result: *rec.Result, Status: *rec.Status, Resolution: ResolutionCached}, nil
}

// waitUntilSettled is row 6: respect another claimant's claim until it is
// resolved or stale. It returns the record when it is resolved, or claimed
// and stale. A key that disappears restarts the decision at Claim.
func (r *run) waitUntilSettled(ctx context.Context, cur IdemRecord) (IdemRecord, error) {
	var lastLog time.Time
	for {
		if r.isStale(cur) {
			return cur, nil
		}
		now := r.g.cfg.Now()
		if lastLog.IsZero() || now.Sub(lastLog) >= waitLogInterval {
			age := now.Sub(cur.ClaimedAt)
			r.log("waiting", fmt.Sprintf("claim_age=%s stale_in=%s", age.Round(time.Second), (r.staleAfter()-age).Round(time.Second)))
			lastLog = now
		}
		if err := r.g.cfg.Sleep(ctx, pollInterval); err != nil {
			r.log("fence_unavailable", "reason=wait_interrupted")
			return IdemRecord{}, fmt.Errorf("%w: waiting for claim: %v", ErrFenceUnavailable, err)
		}
		next, err := retry(ctx, r, func(c context.Context) (*IdemRecord, error) {
			return r.g.store.Get(c, r.key)
		})
		if err != nil {
			return IdemRecord{}, r.storeErr(err)
		}
		if next == nil {
			return IdemRecord{}, errRestart
		}
		if err := r.verify(*next); err != nil {
			return IdemRecord{}, err
		}
		if next.State == StateResolved {
			return *next, nil
		}
		cur = *next
	}
}

// reconcile handles rows 2 and 7-9. held is the claim the outcome will be
// written under: our own claim (row 2), or a stale one to take over first.
func (r *run) reconcile(ctx context.Context, held IdemRecord, takeOver bool) (Outcome, error) {
	rc, ok := r.tool.(tools.Reconciler)
	if !ok {
		r.log("unresolvable", "")
		return Outcome{}, fmt.Errorf("%w: key=%s tool=%s tool_use_id=%s: stale claim and the tool cannot check whether it took effect",
			ErrUnresolvable, r.key, r.tc.ToolName, r.tc.ToolUseID)
	}
	rr, err := retry(ctx, r, func(c context.Context) (tools.ReconcileResult, error) {
		return rc.Reconcile(c, r.inv, r.tc.ToolArgs)
	})
	if err != nil {
		return Outcome{}, r.storeErr(err)
	}

	if takeOver {
		if held, err = r.takeOver(ctx, held); err != nil {
			return Outcome{}, err
		}
	}
	if rr.Applied {
		r.log("reconciled_found", "")
		return r.resolve(ctx, held, rr.Result, "success", ResolutionReconciled)
	}
	r.log("reconciled_not_found", "")
	return r.execute(ctx, held, ResolutionReexecuted)
}

// takeOver swaps a stale claim for one carrying a fresh token. A lost
// compare-and-set (row 11) restarts the decision, unless the key already
// carries our own token: an earlier attempt of ours applied and only its
// reply was lost (the takeover twin of EC-3).
func (r *run) takeOver(ctx context.Context, stale IdemRecord) (IdemRecord, error) {
	mine := r.newRecord()
	err := retryErr(ctx, r, func(c context.Context) error {
		return r.g.store.TakeOver(c, r.key, stale.ClaimToken, mine)
	})
	if errors.Is(err, ErrClaimLost) {
		return r.recheckOwnTakeOver(ctx, mine)
	}
	if err != nil {
		return IdemRecord{}, r.storeErr(err)
	}
	return mine, nil
}

func (r *run) recheckOwnTakeOver(ctx context.Context, mine IdemRecord) (IdemRecord, error) {
	cur, err := retry(ctx, r, func(c context.Context) (*IdemRecord, error) {
		return r.g.store.Get(c, r.key)
	})
	if err != nil {
		return IdemRecord{}, r.storeErr(err)
	}
	if cur != nil && cur.State == StateClaimed && cur.ClaimToken == mine.ClaimToken {
		return mine, nil
	}
	return IdemRecord{}, errRestart
}

// execute calls the tool under a tool_timeout deadline, then resolves the
// key. It must only be reached while we hold the claim (FR-4).
func (r *run) execute(ctx context.Context, held IdemRecord, res Resolution) (Outcome, error) {
	r.g.cfg.Crash.At(r.tc.ToolName, CrashAfterClaim)

	execCtx, cancel := context.WithTimeout(ctx, r.g.cfg.ToolTimeout)
	defer cancel()
	result, execErr := r.tool.Execute(execCtx, r.inv, r.tc.ToolArgs)
	if errors.Is(execErr, tools.ErrOutcomeUnknown) || (execErr != nil && execCtx.Err() != nil) {
		// The tool said so, or it failed while cancelled or timed out: we
		// cannot tell whether the effect landed. Leave the key claimed so the
		// next resume reconciles (FR-12). A clean (result, nil) return is a
		// definite outcome even if the deadline fired at the same instant, so
		// it is resolved rather than discarded.
		r.log("outcome_unknown", "")
		return Outcome{}, fmt.Errorf("%w: key=%s tool=%s", ErrOutcomeUnknown, r.key, r.tc.ToolName)
	}
	r.g.cfg.Crash.At(r.tc.ToolName, CrashAfterExecute)

	status := "success"
	if execErr != nil {
		// Any other error asserts the effect did not happen.
		status = "error"
		result = fmt.Sprintf("error: %v", execErr)
	}
	return r.resolve(ctx, held, result, status, res)
}

// resolve records the outcome under the claim we hold. A failure here is
// logged but does not stop the caller journaling ToolResulted: the journal
// is authoritative and recovery never consults Redis once ToolResulted is
// in Kafka (D-6).
func (r *run) resolve(ctx context.Context, held IdemRecord, result, status string, res Resolution) (Outcome, error) {
	resolved := held
	now := r.g.cfg.Now().UTC().Truncate(time.Microsecond)
	resStr := string(res)
	resolved.State = StateResolved
	resolved.ResolvedAt = &now
	resolved.Result = &result
	resolved.Status = &status
	resolved.Resolution = &resStr

	err := retryErr(ctx, r, func(c context.Context) error {
		return r.g.store.Resolve(c, r.key, held.ClaimToken, resolved)
	})
	if err != nil {
		r.log("resolve_failed", fmt.Sprintf("err=%q", err.Error()))
	}
	r.g.cfg.Crash.At(r.tc.ToolName, CrashAfterResolve)
	return Outcome{Result: result, Status: status, Resolution: res}, nil
}

// verify checks that rec describes this invocation (row 4).
func (r *run) verify(rec IdemRecord) error {
	switch {
	case rec.State != StateClaimed && rec.State != StateResolved:
	case rec.RunID != r.inv.RunID:
	case rec.ToolUseID != r.inv.ToolUseID:
	case rec.ToolName != r.tc.ToolName:
	case rec.ArgsHash != r.argsHash:
	default:
		return nil
	}
	r.log("record_mismatch", "")
	return r.mismatchErr("record identity or args_hash differs")
}

func (r *run) mismatchErr(why string) error {
	return fmt.Errorf("%w: key=%s tool=%s tool_use_id=%s: %s", ErrRecordMismatch, r.key, r.tc.ToolName, r.tc.ToolUseID, why)
}

// storeErr maps a Store/Reconcile error to what Run returns, logging the
// terminal decision.
func (r *run) storeErr(err error) error {
	switch {
	case errors.Is(err, errRestart):
		return err
	case errors.Is(err, ErrCorruptRecord):
		r.log("record_mismatch", "reason=corrupt_json")
		return fmt.Errorf("%w: key=%s: %v", ErrRecordMismatch, r.key, err)
	case errors.Is(err, ErrFenceUnavailable):
		r.log("fence_unavailable", "")
		return err
	default:
		r.log("fence_unavailable", fmt.Sprintf("err=%q", err.Error()))
		return fmt.Errorf("%w: %v", ErrFenceUnavailable, err)
	}
}

// newRecord builds a fresh claimed record with a new claim token.
func (r *run) newRecord() IdemRecord {
	return IdemRecord{
		State:      StateClaimed,
		RunID:      r.inv.RunID,
		Step:       r.inv.Step,
		ToolUseID:  r.inv.ToolUseID,
		ToolName:   r.tc.ToolName,
		ArgsHash:   r.argsHash,
		ClaimToken: uuid.NewString(),
		ClaimedBy:  r.g.claimedBy,
		ClaimedAt:  r.g.cfg.Now().UTC().Truncate(time.Microsecond),
	}
}

func (r *run) staleAfter() time.Duration {
	return r.g.cfg.ToolTimeout + r.g.cfg.StaleGrace
}

// isStale: now - claimed_at >= tool_timeout + claim_stale_grace (FR-9).
func (r *run) isStale(rec IdemRecord) bool {
	return r.g.cfg.Now().Sub(rec.ClaimedAt) >= r.staleAfter()
}

// keyMayHaveExpired: an absent key proves "never claimed" only if the
// in-flight ToolInvoked is younger than the key's TTL (FR-13).
func (r *run) keyMayHaveExpired() bool {
	return r.g.cfg.Now().Sub(r.tc.InvokedAt) >= r.g.cfg.TTL-ExpiryMargin
}

// log prints one FR-28 decision line.
func (r *run) log(decision, extra string) {
	if extra != "" {
		extra = " " + extra
	}
	fmt.Fprintf(r.g.cfg.Out, "fence key=%s tool=%s decision=%s%s\n", r.key, r.tc.ToolName, decision, extra)
}
