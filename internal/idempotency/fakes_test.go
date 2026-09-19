package idempotency

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/replay"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/tools"
)

// fakeStore is an in-memory Store with injectable failures.
type fakeStore struct {
	mu   sync.Mutex
	data map[string]IdemRecord

	claimAttempts int
	resolveCalls  int
	takeOverCalls int

	claimAlways       error // every Claim fails without applying
	claimErrsLeft     int   // this many Claims fail without applying, then succeed
	claimApplyThenErr bool  // the next Claim applies, then reports an error
	getErr            error
	resolveErr        error
	takeOverErr       error
	// takeOverApplyThenErr makes the next TakeOver apply, then report an error
	// (a lost reply).
	takeOverApplyThenErr bool
	// claimNilExisting makes Claim return neither Acquired nor Existing.
	claimNilExisting bool
}

func newFakeStore() *fakeStore { return &fakeStore{data: map[string]IdemRecord{}} }

func (s *fakeStore) Claim(_ context.Context, key string, rec IdemRecord) (ClaimResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimAttempts++
	if s.claimAlways != nil {
		return ClaimResult{}, s.claimAlways
	}
	if s.claimNilExisting {
		return ClaimResult{}, nil
	}
	if s.claimErrsLeft > 0 {
		s.claimErrsLeft--
		return ClaimResult{}, errors.New("redis: connection refused")
	}
	if existing, ok := s.data[key]; ok {
		return ClaimResult{Existing: &existing}, nil
	}
	s.data[key] = rec
	if s.claimApplyThenErr {
		s.claimApplyThenErr = false
		return ClaimResult{}, errors.New("redis: reply lost")
	}
	return ClaimResult{Acquired: true}, nil
}

func (s *fakeStore) Get(_ context.Context, key string) (*IdemRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getErr != nil {
		return nil, s.getErr
	}
	rec, ok := s.data[key]
	if !ok {
		return nil, nil
	}
	return &rec, nil
}

func (s *fakeStore) cas(key, token string, rec IdemRecord) error {
	cur, ok := s.data[key]
	if !ok || cur.State != StateClaimed || cur.ClaimToken != token {
		return ErrClaimLost
	}
	s.data[key] = rec
	return nil
}

func (s *fakeStore) Resolve(_ context.Context, key, token string, rec IdemRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resolveCalls++
	if s.resolveErr != nil {
		return s.resolveErr
	}
	return s.cas(key, token, rec)
}

func (s *fakeStore) TakeOver(_ context.Context, key, token string, rec IdemRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.takeOverCalls++
	if s.takeOverErr != nil {
		return s.takeOverErr
	}
	if err := s.cas(key, token, rec); err != nil {
		return err
	}
	if s.takeOverApplyThenErr {
		s.takeOverApplyThenErr = false
		return errors.New("redis: reply lost")
	}
	return nil
}

func (s *fakeStore) record(key string) (IdemRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.data[key]
	return rec, ok
}

// fakeClock is a manual clock: Sleep advances it instead of waiting.
type fakeClock struct {
	now     time.Time
	onSleep func()
}

func (c *fakeClock) Now() time.Time { return c.now }

func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	c.now = c.now.Add(d)
	if c.onSleep != nil {
		c.onSleep()
	}
	return ctx.Err()
}

// fakeTool counts Execute calls.
type fakeTool struct {
	name       string
	execCount  int
	execResult string
	execErr    error
	block      bool // Execute waits for its context to end
	// succeedAfterDeadline makes Execute wait for its context to end and then
	// still return execResult with no error.
	succeedAfterDeadline bool
}

func (f *fakeTool) Name() string        { return f.name }
func (f *fakeTool) Description() string { return "fake" }
func (f *fakeTool) HasSideEffect() bool { return true }
func (f *fakeTool) InputSchema() anthropic.ToolInputSchemaParam {
	return anthropic.ToolInputSchemaParam{}
}

func (f *fakeTool) Execute(ctx context.Context, _ tools.Invocation, _ json.RawMessage) (string, error) {
	f.execCount++
	if f.block {
		<-ctx.Done()
		return "", ctx.Err()
	}
	if f.succeedAfterDeadline {
		<-ctx.Done()
		return f.execResult, nil
	}
	return f.execResult, f.execErr
}

// fakeReconTool is a fakeTool that also implements tools.Reconciler.
type fakeReconTool struct {
	*fakeTool
	reconCount int
	reconRes   tools.ReconcileResult
	reconErr   error
}

func (f *fakeReconTool) Reconcile(context.Context, tools.Invocation, json.RawMessage) (tools.ReconcileResult, error) {
	f.reconCount++
	return f.reconRes, f.reconErr
}

// harness wires a Guard to fakes for one test.
type harness struct {
	t     *testing.T
	store *fakeStore
	clock *fakeClock
	out   *bytes.Buffer
	guard *Guard
	inv   tools.Invocation
	tc    replay.ToolCall
	key   string
}

const (
	testToolTimeout = 20 * time.Second
	testTTL         = 24 * time.Hour
	testArgs        = `{"title":"T","body":"B"}`
)

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{
		t:     t,
		store: newFakeStore(),
		clock: &fakeClock{now: time.Date(2026, 9, 20, 14, 0, 0, 0, time.UTC)},
		out:   &bytes.Buffer{},
	}
	h.key = Key("run-1", 5, "toolu_05E")
	h.inv = tools.Invocation{RunID: "run-1", Step: 5, ToolUseID: "toolu_05E", IdempotencyKey: &h.key}
	h.tc = replay.ToolCall{
		ToolUseID: "toolu_05E", ToolName: "open_pr", ToolArgs: json.RawMessage(testArgs),
		HasSideEffect: true, IdempotencyKey: &h.key, InvokedAt: h.clock.now.Add(-time.Second),
	}
	h.guard = NewGuard(h.store, GuardConfig{
		ToolTimeout: testToolTimeout, TTL: testTTL, Out: h.out,
		Now: h.clock.Now, Sleep: h.clock.Sleep,
	})
	return h
}

// rec builds a record for this harness's invocation.
func (h *harness) rec(state RecordState, token string, claimedAgo time.Duration) IdemRecord {
	hash, err := ArgsHash("open_pr", json.RawMessage(testArgs))
	if err != nil {
		h.t.Fatal(err)
	}
	return IdemRecord{
		State: state, RunID: "run-1", Step: 5, ToolUseID: "toolu_05E", ToolName: "open_pr",
		ArgsHash: hash, ClaimToken: token, ClaimedBy: "other-1", ClaimedAt: h.clock.now.Add(-claimedAgo),
	}
}

func (h *harness) resolvedRec(token, result string) IdemRecord {
	r := h.rec(StateResolved, token, time.Second)
	status, res := "success", "executed"
	r.Result, r.Status, r.Resolution = &result, &status, &res
	return r
}

func newTool() *fakeTool { return &fakeTool{name: "open_pr", execResult: "opened PR #1: T"} }
