package idempotency

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/tools"
)

// staleAge is a claim age past tool_timeout + claim_stale_grace.
const staleAge = testToolTimeout + DefaultStaleGrace + time.Second

// TestGuardDecisionTable has one case per row of the decision table in
// phase3-idempotency.md (rows 10 and 11 are covered below and in
// TestGuardFailsClosed).
func TestGuardDecisionTable(t *testing.T) {
	type env struct {
		h *harness
		p *fakeTool
		r *fakeReconTool
	}
	tests := []struct {
		name       string
		reconciler bool
		reconRes   tools.ReconcileResult
		setup      func(e env)
		wantErr    error
		wantDec    string // substring of the fence output
		wantOut    Outcome
		wantExec   int
		wantRecon  int
		wantState  RecordState // final stored state, "" = key absent
		wantRes    string      // final stored resolution
	}{
		{
			name: "row 1: absent key is claimed and executed", reconciler: true,
			setup:   func(e env) {},
			wantDec: "decision=acquired", wantOut: Outcome{"opened PR #1: T", "success", ResolutionExecuted},
			wantExec: 1, wantState: StateResolved, wantRes: "executed",
		},
		{
			name: "row 2: absent but key may have expired, reconciler finds it", reconciler: true,
			reconRes: tools.ReconcileResult{Applied: true, Result: "opened PR #1: T"},
			setup:    func(e env) { e.h.tc.InvokedAt = e.h.clock.now.Add(-25 * time.Hour) },
			wantDec:  "decision=reconciled_found", wantOut: Outcome{"opened PR #1: T", "success", ResolutionReconciled},
			wantExec: 0, wantRecon: 1, wantState: StateResolved, wantRes: "reconciled",
		},
		{
			name: "row 2: absent but key may have expired, reconciler finds nothing", reconciler: true,
			setup:   func(e env) { e.h.tc.InvokedAt = e.h.clock.now.Add(-25 * time.Hour) },
			wantDec: "decision=reconciled_not_found", wantOut: Outcome{"opened PR #1: T", "success", ResolutionReexecuted},
			wantExec: 1, wantRecon: 1, wantState: StateResolved, wantRes: "reexecuted",
		},
		{
			name:    "row 2: absent but key may have expired, no reconciler",
			setup:   func(e env) { e.h.tc.InvokedAt = e.h.clock.now.Add(-25 * time.Hour) },
			wantErr: ErrUnresolvable, wantDec: "decision=unresolvable", wantState: StateClaimed,
		},
		{
			name: "row 4: args_hash differs", reconciler: true,
			setup: func(e env) {
				rec := e.h.rec(StateClaimed, "other", time.Second)
				rec.ArgsHash = "deadbeef"
				e.h.store.data[e.h.key] = rec
			},
			wantErr: ErrRecordMismatch, wantDec: "decision=record_mismatch", wantState: StateClaimed,
		},
		{
			name: "row 4: tool_use_id differs", reconciler: true,
			setup: func(e env) {
				rec := e.h.resolvedRec("other", "x")
				rec.ToolUseID = "toolu_OTHER"
				e.h.store.data[e.h.key] = rec
			},
			wantErr: ErrRecordMismatch, wantDec: "decision=record_mismatch", wantState: StateResolved,
		},
		{
			name: "row 4: stored value is corrupt json", reconciler: true,
			setup:   func(e env) { e.h.store.claimAlways = ErrCorruptRecord },
			wantErr: ErrRecordMismatch, wantDec: "decision=record_mismatch",
		},
		{
			name: "row 5: resolved record is a cache hit", reconciler: true,
			setup:   func(e env) { e.h.store.data[e.h.key] = e.h.resolvedRec("other", "opened PR #1: T") },
			wantDec: "decision=cache_hit", wantOut: Outcome{"opened PR #1: T", "success", ResolutionCached},
			wantState: StateResolved, wantRes: "executed",
		},
		{
			name: "row 6 then 5: waits, then the claimant resolves", reconciler: true,
			setup: func(e env) {
				e.h.store.data[e.h.key] = e.h.rec(StateClaimed, "other", 2*time.Second)
				e.h.clock.onSleep = func() {
					e.h.store.data[e.h.key] = e.h.resolvedRec("other", "opened PR #1: T")
				}
			},
			wantDec: "decision=waiting", wantOut: Outcome{"opened PR #1: T", "success", ResolutionCached},
			wantState: StateResolved,
		},
		{
			name: "row 6 then 7: waits until stale, reconciler finds it", reconciler: true,
			reconRes: tools.ReconcileResult{Applied: true, Result: "opened PR #1: T"},
			setup: func(e env) {
				e.h.store.data[e.h.key] = e.h.rec(StateClaimed, "other", staleAge-3*time.Second)
			},
			wantDec: "decision=reconciled_found", wantOut: Outcome{"opened PR #1: T", "success", ResolutionReconciled},
			wantExec: 0, wantRecon: 1, wantState: StateResolved, wantRes: "reconciled",
		},
		{
			name: "row 8: stale claim, reconciler finds nothing, re-executes", reconciler: true,
			setup:   func(e env) { e.h.store.data[e.h.key] = e.h.rec(StateClaimed, "other", staleAge) },
			wantDec: "decision=reconciled_not_found", wantOut: Outcome{"opened PR #1: T", "success", ResolutionReexecuted},
			wantExec: 1, wantRecon: 1, wantState: StateResolved, wantRes: "reexecuted",
		},
		{
			name:    "row 9: stale claim, tool has no reconciler",
			setup:   func(e env) { e.h.store.data[e.h.key] = e.h.rec(StateClaimed, "other", staleAge) },
			wantErr: ErrUnresolvable, wantDec: "decision=unresolvable", wantState: StateClaimed,
		},
		{
			name: "row 11: takeover keeps losing the race", reconciler: true,
			setup: func(e env) {
				e.h.store.data[e.h.key] = e.h.rec(StateClaimed, "other", staleAge)
				e.h.store.takeOverErr = ErrClaimLost
			},
			wantErr: ErrFenceUnavailable, wantDec: "decision=fence_unavailable",
			wantRecon: 3, wantState: StateClaimed,
		},
		{
			name: "key disappears while waiting: restart and acquire", reconciler: true,
			setup: func(e env) {
				e.h.store.data[e.h.key] = e.h.rec(StateClaimed, "other", 2*time.Second)
				e.h.clock.onSleep = func() { delete(e.h.store.data, e.h.key) }
			},
			wantDec: "decision=acquired", wantOut: Outcome{"opened PR #1: T", "success", ResolutionExecuted},
			wantExec: 1, wantState: StateResolved, wantRes: "executed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			plain := newTool()
			recon := &fakeReconTool{fakeTool: plain, reconRes: tt.reconRes}
			tt.setup(env{h, plain, recon})

			var tool tools.Tool = plain
			if tt.reconciler {
				tool = recon
			}
			got, err := h.guard.Run(context.Background(), h.inv, h.tc, tool)

			if !errors.Is(err, tt.wantErr) || (tt.wantErr == nil && err != nil) {
				t.Fatalf("Run() error = %v, want %v", err, tt.wantErr)
			}
			if got != tt.wantOut {
				t.Errorf("Outcome = %+v, want %+v", got, tt.wantOut)
			}
			if !strings.Contains(h.out.String(), tt.wantDec) {
				t.Errorf("output missing %q:\n%s", tt.wantDec, h.out.String())
			}
			if plain.execCount != tt.wantExec {
				t.Errorf("Execute count = %d, want %d", plain.execCount, tt.wantExec)
			}
			if recon.reconCount != tt.wantRecon {
				t.Errorf("Reconcile count = %d, want %d", recon.reconCount, tt.wantRecon)
			}
			rec, present := h.store.record(h.key)
			if (tt.wantState == "") == present || (present && rec.State != tt.wantState) {
				t.Errorf("stored state = %q (present=%v), want %q", rec.State, present, tt.wantState)
			}
			if tt.wantRes != "" && (rec.Resolution == nil || *rec.Resolution != tt.wantRes) {
				t.Errorf("stored resolution = %v, want %q", rec.Resolution, tt.wantRes)
			}
		})
	}
}

// TestGuardFailsClosed covers row 10 and the ambiguous-write case (AC-3).
func TestGuardFailsClosed(t *testing.T) {
	tests := []struct {
		name         string
		setup        func(s *fakeStore)
		wantErr      error
		wantAttempts int
		wantExec     int
		wantRetries  int
	}{
		{
			name:    "claim always fails: 5 attempts, never executes",
			setup:   func(s *fakeStore) { s.claimAlways = errors.New("redis: i/o timeout") },
			wantErr: ErrFenceUnavailable, wantAttempts: 5, wantExec: 0, wantRetries: 5,
		},
		{
			name:         "claim fails twice then succeeds: executes once",
			setup:        func(s *fakeStore) { s.claimErrsLeft = 2 },
			wantAttempts: 3, wantExec: 1, wantRetries: 2,
		},
		{
			name:         "ambiguous write: retry sees our own token and executes once",
			setup:        func(s *fakeStore) { s.claimApplyThenErr = true },
			wantAttempts: 2, wantExec: 1, wantRetries: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			tt.setup(h.store)
			tool := newTool()

			_, err := h.guard.Run(context.Background(), h.inv, h.tc, tool)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Run() error = %v, want %v", err, tt.wantErr)
			}
			if h.store.claimAttempts != tt.wantAttempts {
				t.Errorf("Claim attempts = %d, want %d", h.store.claimAttempts, tt.wantAttempts)
			}
			if tool.execCount != tt.wantExec {
				t.Errorf("Execute count = %d, want %d", tool.execCount, tt.wantExec)
			}
			if got := strings.Count(h.out.String(), "decision=redis_retry"); got != tt.wantRetries {
				t.Errorf("redis_retry lines = %d, want %d\n%s", got, tt.wantRetries, h.out.String())
			}
			if tt.wantErr != nil && !strings.Contains(h.out.String(), "decision=fence_unavailable") {
				t.Errorf("missing fence_unavailable line:\n%s", h.out.String())
			}
		})
	}
}

func TestGuardReconcileErrorFailsClosed(t *testing.T) {
	h := newHarness(t)
	h.store.data[h.key] = h.rec(StateClaimed, "other", staleAge)
	tool := &fakeReconTool{fakeTool: newTool(), reconErr: errors.New("ledger unreadable")}

	_, err := h.guard.Run(context.Background(), h.inv, h.tc, tool)

	if !errors.Is(err, ErrFenceUnavailable) {
		t.Fatalf("Run() error = %v, want ErrFenceUnavailable", err)
	}
	if tool.reconCount != 5 || tool.execCount != 0 {
		t.Errorf("Reconcile = %d (want 5), Execute = %d (want 0)", tool.reconCount, tool.execCount)
	}
}

func TestGuardAfterExecute(t *testing.T) {
	t.Run("outcome unknown leaves the key claimed and does not resolve", func(t *testing.T) {
		h := newHarness(t)
		tool := newTool()
		tool.execErr = tools.ErrOutcomeUnknown

		_, err := h.guard.Run(context.Background(), h.inv, h.tc, tool)

		if !errors.Is(err, ErrOutcomeUnknown) {
			t.Fatalf("Run() error = %v, want ErrOutcomeUnknown", err)
		}
		if rec, _ := h.store.record(h.key); rec.State != StateClaimed || h.store.resolveCalls != 0 {
			t.Errorf("state = %q, Resolve calls = %d; want claimed and 0", rec.State, h.store.resolveCalls)
		}
		if !strings.Contains(h.out.String(), "decision=outcome_unknown") {
			t.Errorf("missing outcome_unknown line:\n%s", h.out.String())
		}
	})

	t.Run("tool_timeout leaves the key claimed and does not resolve", func(t *testing.T) {
		h := newHarness(t)
		h.guard = NewGuard(h.store, GuardConfig{
			ToolTimeout: 20 * time.Millisecond, TTL: testTTL, Out: h.out,
			Now: h.clock.Now, Sleep: h.clock.Sleep,
		})
		tool := newTool()
		tool.block = true

		_, err := h.guard.Run(context.Background(), h.inv, h.tc, tool)

		if !errors.Is(err, ErrOutcomeUnknown) {
			t.Fatalf("Run() error = %v, want ErrOutcomeUnknown", err)
		}
		if rec, _ := h.store.record(h.key); rec.State != StateClaimed || h.store.resolveCalls != 0 {
			t.Errorf("state = %q, Resolve calls = %d; want claimed and 0", rec.State, h.store.resolveCalls)
		}
	})

	t.Run("ordinary tool error is resolved as status error", func(t *testing.T) {
		h := newHarness(t)
		tool := newTool()
		tool.execErr = errors.New("bad args")

		got, err := h.guard.Run(context.Background(), h.inv, h.tc, tool)

		want := Outcome{"error: bad args", "error", ResolutionExecuted}
		if err != nil || got != want {
			t.Fatalf("Run() = %+v, %v; want %+v", got, err, want)
		}
		if rec, _ := h.store.record(h.key); rec.State != StateResolved || *rec.Status != "error" {
			t.Errorf("stored record = %+v, want resolved with status error", rec)
		}
	})

	t.Run("resolve failure is logged but the outcome is still returned", func(t *testing.T) {
		h := newHarness(t)
		h.store.resolveErr = errors.New("redis down")
		tool := newTool()

		got, err := h.guard.Run(context.Background(), h.inv, h.tc, tool)

		if err != nil || got.Resolution != ResolutionExecuted || tool.execCount != 1 {
			t.Fatalf("Run() = %+v, %v (exec %d)", got, err, tool.execCount)
		}
		if !strings.Contains(h.out.String(), "decision=resolve_failed") {
			t.Errorf("missing resolve_failed line:\n%s", h.out.String())
		}
	})
}

func TestGuardCrashHooksFireInOrder(t *testing.T) {
	h := newHarness(t)
	var got []CrashPoint
	h.guard = NewGuard(h.store, GuardConfig{
		ToolTimeout: testToolTimeout, TTL: testTTL, Out: h.out,
		Now: h.clock.Now, Sleep: h.clock.Sleep,
		Crash: hookFunc(func(_ string, p CrashPoint) { got = append(got, p) }),
	})

	if _, err := h.guard.Run(context.Background(), h.inv, h.tc, newTool()); err != nil {
		t.Fatal(err)
	}
	want := []CrashPoint{CrashAfterClaim, CrashAfterExecute, CrashAfterResolve}
	if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("crash points = %v, want %v", got, want)
	}
}

type hookFunc func(tool string, p CrashPoint)

func (f hookFunc) At(tool string, p CrashPoint) { f(tool, p) }

func TestParseCrashSpec(t *testing.T) {
	tests := []struct {
		spec     string
		wantErr  bool
		tool     string
		point    CrashPoint
		wantExit bool
	}{
		{spec: "", tool: "open_pr", point: CrashAfterClaim},
		{spec: "open_pr:after_claim", tool: "open_pr", point: CrashAfterClaim, wantExit: true},
		{spec: "open_pr:after_execute", tool: "open_pr", point: CrashAfterClaim},
		{spec: "apply_fix:after_resolve", tool: "open_pr", point: CrashAfterResolve},
		{spec: "open_pr", wantErr: true},
		{spec: "open_pr:sometime", wantErr: true},
		{spec: ":after_claim", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.spec, func(t *testing.T) {
			var codes []int
			hook, err := ParseCrashSpec(tt.spec, func(c int) { codes = append(codes, c) })
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseCrashSpec() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			hook.At(tt.tool, tt.point)
			hook.At(tt.tool, tt.point) // must fire at most once
			if tt.wantExit && (len(codes) != 1 || codes[0] != 137) {
				t.Errorf("exit codes = %v, want exactly [137]", codes)
			}
			if !tt.wantExit && len(codes) != 0 {
				t.Errorf("exit codes = %v, want none", codes)
			}
		})
	}
}

// Reviewer F1: an ambiguous claim (the SET applied, the reply was lost) on a
// key that may have expired must reconcile before executing, exactly like an
// unambiguous claim (row 2). It used to fall straight through to Execute.
func TestGuardAmbiguousClaimOnPossiblyExpiredKey(t *testing.T) {
	tests := []struct {
		name      string
		reconcile bool
		reconRes  tools.ReconcileResult
		wantErr   error
		wantDec   string
		wantExec  int
	}{
		{
			name: "reconciler finds the effect: adopt it, never execute", reconcile: true,
			reconRes: tools.ReconcileResult{Applied: true, Result: "opened PR #1: T"},
			wantDec:  "decision=reconciled_found", wantExec: 0,
		},
		{
			name: "no reconciler: unresolvable, never execute", reconcile: false,
			wantErr: ErrUnresolvable, wantDec: "decision=unresolvable", wantExec: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.tc.InvokedAt = h.clock.now.Add(-25 * time.Hour)
			h.store.claimApplyThenErr = true
			plain := newTool()
			var tool tools.Tool = plain
			if tt.reconcile {
				tool = &fakeReconTool{fakeTool: plain, reconRes: tt.reconRes}
			}

			_, err := h.guard.Run(context.Background(), h.inv, h.tc, tool)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Run() error = %v, want %v", err, tt.wantErr)
			}
			if plain.execCount != tt.wantExec {
				t.Errorf("Execute count = %d, want %d", plain.execCount, tt.wantExec)
			}
			if !strings.Contains(h.out.String(), tt.wantDec) {
				t.Errorf("output missing %q:\n%s", tt.wantDec, h.out.String())
			}
		})
	}
}

// Reviewer F4: a TakeOver that applied but lost its reply must not make the
// worker wait on its own claim.
func TestGuardAmbiguousTakeOver(t *testing.T) {
	h := newHarness(t)
	h.store.data[h.key] = h.rec(StateClaimed, "dead-worker", staleAge)
	h.store.takeOverApplyThenErr = true
	plain := newTool()
	tool := &fakeReconTool{fakeTool: plain}

	got, err := h.guard.Run(context.Background(), h.inv, h.tc, tool)

	if err != nil || got.Resolution != ResolutionReexecuted || plain.execCount != 1 {
		t.Fatalf("Run() = %+v, %v (Execute %d); want reexecuted once", got, err, plain.execCount)
	}
	if strings.Contains(h.out.String(), "decision=waiting") {
		t.Errorf("waited on its own claim:\n%s", h.out.String())
	}
	if rec, _ := h.store.record(h.key); rec.State != StateResolved {
		t.Errorf("stored state = %q, want resolved", rec.State)
	}
}

// Reviewer F8: a clean (result, nil) return is a definite outcome even if the
// tool_timeout deadline fired around the same time.
func TestGuardKeepsCleanResultPastDeadline(t *testing.T) {
	h := newHarness(t)
	h.guard = NewGuard(h.store, GuardConfig{
		ToolTimeout: 20 * time.Millisecond, TTL: testTTL, Out: h.out,
		Now: h.clock.Now, Sleep: h.clock.Sleep,
	})
	tool := newTool()
	tool.succeedAfterDeadline = true

	got, err := h.guard.Run(context.Background(), h.inv, h.tc, tool)

	if err != nil || got != (Outcome{"opened PR #1: T", "success", ResolutionExecuted}) {
		t.Fatalf("Run() = %+v, %v; want the executed result", got, err)
	}
	if rec, _ := h.store.record(h.key); rec.State != StateResolved {
		t.Errorf("stored state = %q, want resolved", rec.State)
	}
}

// Reviewer F10: a Store that returns neither Acquired nor Existing must fail
// closed instead of panicking.
func TestGuardStoreReturningNothingFailsClosed(t *testing.T) {
	h := newHarness(t)
	h.store.claimNilExisting = true
	tool := newTool()

	_, err := h.guard.Run(context.Background(), h.inv, h.tc, tool)

	if !errors.Is(err, ErrFenceUnavailable) || tool.execCount != 0 {
		t.Fatalf("Run() error = %v, Execute = %d; want ErrFenceUnavailable and 0", err, tool.execCount)
	}
}

// Reviewer F11: the crash hook fires exactly once even when called
// concurrently (run under -race).
func TestCrashHookFiresOnceConcurrently(t *testing.T) {
	var calls atomic.Int32
	hook, err := ParseCrashSpec("open_pr:after_claim", func(int) { calls.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hook.At("open_pr", CrashAfterClaim)
		}()
	}
	wg.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("exit called %d times, want 1", got)
	}
}
