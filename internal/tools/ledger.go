package tools

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// LedgerEntry is one real execution of a side-effecting mock tool.
type LedgerEntry struct {
	RunID          string    `json:"run_id"`
	ToolUseID      string    `json:"tool_use_id"`
	ToolName       string    `json:"tool_name"`
	IdempotencyKey string    `json:"idempotency_key"`
	Result         string    `json:"result"`
	RecordedAt     time.Time `json:"recorded_at"`
	WriterPID      int       `json:"writer_pid"`
}

// Ledger is an append-only JSONL file standing in for "the outside world"
// (GitHub) for the mock tools. It is independent of Redis on purpose: the
// evidence that an effect did not happen twice must not come from the
// mechanism that is supposed to prevent it. A local file does not work
// across Kubernetes pods; Phase 6/8 will need a shared equivalent.
type Ledger struct {
	path string
}

// NewLedger returns a Ledger backed by the file at path. The file and its
// parent directory are created on first Record.
func NewLedger(path string) *Ledger {
	return &Ledger{path: path}
}

// Record appends e as one line, using a single write() on an O_APPEND file
// followed by fsync, so a kill -9 leaves either the whole line or none of it.
func (l *Ledger) Record(ctx context.Context, e LedgerEntry) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("recording ledger entry: %w", err)
	}
	line, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("encoding ledger entry: %w", err)
	}
	line = append(line, '\n')

	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil {
		return fmt.Errorf("creating ledger directory: %w", err)
	}
	f, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("opening ledger %s: %w", l.path, err)
	}
	// Once Write succeeds the entry is visible to every reader, i.e. the
	// effect HAS happened. A later failure must therefore report an unknown
	// outcome, never "did not happen" (Tool.Execute's contract): otherwise the
	// run would journal an error and the LLM would retry it under a new key.
	n, err := f.Write(line)
	if err != nil {
		_ = f.Close()
		if n > 0 {
			return fmt.Errorf("%w: partial ledger write: %v", ErrOutcomeUnknown, err)
		}
		return fmt.Errorf("writing ledger entry: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("%w: syncing ledger: %v", ErrOutcomeUnknown, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("%w: closing ledger: %v", ErrOutcomeUnknown, err)
	}
	return nil
}

// FindByKey returns the first entry recorded under idempotencyKey.
func (l *Ledger) FindByKey(ctx context.Context, idempotencyKey string) (LedgerEntry, bool, error) {
	entries, err := l.Entries(ctx)
	if err != nil {
		return LedgerEntry{}, false, fmt.Errorf("looking up ledger key: %w", err)
	}
	for _, e := range entries {
		if e.IdempotencyKey == idempotencyKey {
			return e, true, nil
		}
	}
	return LedgerEntry{}, false, nil
}

// Entries returns every entry in append order. A missing file is an empty
// ledger; use Exists to tell the difference.
func (l *Ledger) Entries(ctx context.Context) ([]LedgerEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("reading ledger: %w", err)
	}
	raw, err := os.ReadFile(l.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading ledger %s: %w", l.path, err)
	}
	return parseLedger(raw)
}

// Exists reports whether the ledger file has been created.
func (l *Ledger) Exists() bool {
	_, err := os.Stat(l.path)
	return err == nil
}

func parseLedger(raw []byte) ([]LedgerEntry, error) {
	var out []LedgerEntry
	sc := bufio.NewScanner(bytes.NewReader(raw))
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for n := 1; sc.Scan(); n++ {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var e LedgerEntry
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, fmt.Errorf("decoding ledger line %d: %w", n, err)
		}
		out = append(out, e)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scanning ledger: %w", err)
	}
	return out, nil
}

// entryKey returns the idempotency key carried by inv, or "" if none.
func entryKey(inv Invocation) string {
	if inv.IdempotencyKey == nil {
		return ""
	}
	return *inv.IdempotencyKey
}
