package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/idempotency"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/tools"
)

// recorder is a shared, ordered call log: the fake publisher, the fake store
// and the counting tools all append to one recorder, so a test can assert the
// exact interleaving (AC-4).
type recorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *recorder) add(call string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

// memStore is an in-memory idempotency.Store that logs into a recorder.
type memStore struct {
	mu   sync.Mutex
	data map[string]idempotency.IdemRecord
	rec  *recorder
	// ops counts every store call, so a test can assert Redis was never
	// consulted.
	ops int
}

func newMemStore(rec *recorder) *memStore {
	return &memStore{data: map[string]idempotency.IdemRecord{}, rec: rec}
}

func (s *memStore) log(op string) {
	s.ops++
	s.rec.add(op)
}

func (s *memStore) Claim(_ context.Context, key string, r idempotency.IdemRecord) (idempotency.ClaimResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log("Claim")
	if ex, ok := s.data[key]; ok {
		return idempotency.ClaimResult{Existing: &ex}, nil
	}
	s.data[key] = r
	return idempotency.ClaimResult{Acquired: true}, nil
}

func (s *memStore) Get(_ context.Context, key string) (*idempotency.IdemRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log("Get")
	if r, ok := s.data[key]; ok {
		return &r, nil
	}
	return nil, nil
}

func (s *memStore) cas(key, token string, r idempotency.IdemRecord) error {
	cur, ok := s.data[key]
	if !ok || cur.State != idempotency.StateClaimed || cur.ClaimToken != token {
		return idempotency.ErrClaimLost
	}
	s.data[key] = r
	return nil
}

func (s *memStore) Resolve(_ context.Context, key, token string, r idempotency.IdemRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log("Resolve")
	return s.cas(key, token, r)
}

func (s *memStore) TakeOver(_ context.Context, key, token string, r idempotency.IdemRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log("TakeOver")
	return s.cas(key, token, r)
}

// fenceFixture bundles a real Guard over an in-memory store, a temp-dir
// ledger, and the tools built on that ledger.
type fenceFixture struct {
	rec    *recorder
	store  *memStore
	ledger *tools.Ledger
	guard  *idempotency.Guard
	fence  bytes.Buffer
	// exec counts Execute calls per tool name.
	execs map[string]int
}

func newFenceFixture(t *testing.T) *fenceFixture {
	t.Helper()
	f := &fenceFixture{
		rec:    &recorder{},
		ledger: tools.NewLedger(filepath.Join(t.TempDir(), "ledger.jsonl")),
		execs:  map[string]int{},
	}
	f.store = newMemStore(f.rec)
	f.guard = idempotency.NewGuard(f.store, idempotency.GuardConfig{
		ToolTimeout: 5 * time.Second,
		TTL:         24 * time.Hour,
		Out:         &f.fence,
	})
	return f
}

// registry builds the four mock tools over the fixture's ledger, each wrapped
// so its Execute calls are counted and logged.
func (f *fenceFixture) registry() *tools.Registry {
	return tools.NewRegistry(
		f.wrap(tools.NewCloneRepoTool()),
		f.wrap(tools.NewRunTestsTool(f.ledger)),
		f.wrap(tools.NewApplyFixTool(f.ledger)),
		f.wrapRecon(tools.NewOpenPRTool(f.ledger)),
	)
}

func (f *fenceFixture) wrap(t tools.Tool) tools.Tool { return &countingTool{Tool: t, fx: f} }

// wrapRecon wraps a tool that also implements tools.Reconciler, so the
// wrapper still satisfies it (embedding an interface does not promote it).
func (f *fenceFixture) wrapRecon(t interface {
	tools.Tool
	tools.Reconciler
}) tools.Tool {
	return &countingReconTool{countingTool: countingTool{Tool: t, fx: f}, rc: t}
}

// countingTool wraps a tools.Tool to count and log Execute calls, without
// adding test-only instrumentation to the production mocks themselves.
type countingTool struct {
	tools.Tool
	fx *fenceFixture
}

func (t *countingTool) Execute(ctx context.Context, inv tools.Invocation, rawArgs json.RawMessage) (string, error) {
	t.fx.execs[t.Name()]++
	t.fx.rec.add("Execute(" + t.Name() + ")")
	return t.Tool.Execute(ctx, inv, rawArgs)
}

type countingReconTool struct {
	countingTool
	rc tools.Reconciler
}

func (t *countingReconTool) Reconcile(ctx context.Context, inv tools.Invocation, rawArgs json.RawMessage) (tools.ReconcileResult, error) {
	t.fx.rec.add("Reconcile(" + t.Name() + ")")
	return t.rc.Reconcile(ctx, inv, rawArgs)
}

// newLoop builds a Loop with the standard test settings.
func (f *fenceFixture) newLoop(llm LLMClient, journal JournalConfig) *Loop {
	return NewLoop(llm, f.registry(), testModel, 1024, 10, io.Discard, journal, f.guard)
}

// mkEvent builds a well-formed envelope for the journal prefix of a resume test.
func mkEvent(t *testing.T, runID string, seq int64, p events.Payload, at time.Time) events.Envelope {
	t.Helper()
	env, err := events.NewEnvelope(runID, "local-dev", seq, p, at)
	if err != nil {
		t.Fatalf("NewEnvelope(seq=%d): %v", seq, err)
	}
	if inv, ok := p.(events.ToolInvokedPayload); ok {
		env.IdempotencyKey = inv.IdempotencyKey
	}
	return env
}
