// Command effects reads the mock side-effect ledger and reports duplicated
// side effects. It is the pass/fail evidence for Phase 3 and, later, the
// chaos test (Phase 8).
//
// Definition used from Phase 3 onward: a duplicated side effect is two or
// more ledger entries with the same idempotency_key.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/tools"
)

const defaultLedgerPath = ".dae/mock-ledger.jsonl"

// Exit codes: 0 no duplicates, 1 duplicates found, 2 the ledger file is
// missing, 3 the ledger could not be read. An unreadable ledger is not
// evidence of a duplicate, so it must not share the duplicate exit code.
const (
	exitOK         = 0
	exitDuplicates = 1
	exitNoLedger   = 2
	exitReadError  = 3
)

func main() {
	ledgerPath := flag.String("ledger", envOr("DAE_MOCK_LEDGER_PATH", defaultLedgerPath), "path to the mock ledger (default $DAE_MOCK_LEDGER_PATH or .dae/mock-ledger.jsonl)")
	runID := flag.String("run-id", "", "only count entries for this run_id")
	flag.Parse()
	os.Exit(run(context.Background(), os.Stdout, os.Stderr, *ledgerPath, *runID))
}

func run(ctx context.Context, stdout, stderr io.Writer, ledgerPath, runID string) int {
	ledger := tools.NewLedger(ledgerPath)
	if !ledger.Exists() {
		fmt.Fprintf(stderr, "ledger %s does not exist\n", ledgerPath)
		return exitNoLedger
	}
	entries, err := ledger.Entries(ctx)
	if err != nil {
		fmt.Fprintf(stderr, "reading ledger: %v\n", err)
		return exitReadError
	}

	rows, summary := summarize(entries, runID)
	for _, r := range rows {
		fmt.Fprintf(stdout, "%s  %s  %d\n", r.key, r.tool, r.count)
	}
	fmt.Fprintf(stdout, "entries=%d distinct_keys=%d duplicates=%d\n", summary.entries, summary.distinctKeys, summary.duplicates)
	if summary.duplicates > 0 {
		return exitDuplicates
	}
	return exitOK
}

type row struct {
	key   string
	tool  string
	count int
}

type totals struct {
	entries      int
	distinctKeys int
	// duplicates is the number of extra entries beyond the first for each
	// key: a key with 3 entries contributes 2.
	duplicates int
}

// summarize groups entries by idempotency_key (restricted to runID if set),
// in first-seen order.
func summarize(entries []tools.LedgerEntry, runID string) ([]row, totals) {
	index := map[string]int{}
	var rows []row
	var t totals
	for _, e := range entries {
		if runID != "" && e.RunID != runID {
			continue
		}
		t.entries++
		i, seen := index[e.IdempotencyKey]
		if !seen {
			i = len(rows)
			index[e.IdempotencyKey] = i
			rows = append(rows, row{key: e.IdempotencyKey, tool: e.ToolName})
		}
		rows[i].count++
	}
	sort.SliceStable(rows, func(a, b int) bool { return rows[a].key < rows[b].key })
	t.distinctKeys = len(rows)
	for _, r := range rows {
		t.duplicates += r.count - 1
	}
	return rows, t
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
