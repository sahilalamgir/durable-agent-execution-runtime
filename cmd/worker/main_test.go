package main

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/agent"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/idempotency"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/replay"
)

func env(kv map[string]string) func(string) string {
	return func(k string) string { return kv[k] }
}

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr bool
		check   func(t *testing.T, c config)
	}{
		{
			name: "defaults",
			env:  map[string]string{},
			check: func(t *testing.T, c config) {
				if c.redisAddr != "localhost:6379" || c.idemTTL != 24*time.Hour || c.toolTimeout != 2*time.Minute ||
					c.ledgerPath != ".dae/mock-ledger.jsonl" || c.resumeRunID != "" || c.crashAfterSeq != nil {
					t.Errorf("unexpected defaults: %+v", c)
				}
			},
		},
		{
			name: "manual-test settings",
			env: map[string]string{
				"DAE_TOOL_TIMEOUT": "20s", "DAE_REDIS_ADDR": "r:1", "DAE_MOCK_LEDGER_PATH": "/x/l.jsonl",
				"DAE_RESUME_RUN_ID": "7f1c4b8e-2d3a-4f6b-9c1e-0a2b3c4d5e6f",
				"DAE_CRASH_AT":      "open_pr:after_execute", "DAE_CRASH_AFTER_SEQ": "0",
			},
			check: func(t *testing.T, c config) {
				if c.toolTimeout != 20*time.Second || c.crashAfterSeq == nil || *c.crashAfterSeq != 0 || c.resumeRunID == "" {
					t.Errorf("unexpected config: %+v", c)
				}
			},
		},
		{name: "ttl must exceed tool timeout + 30s", env: map[string]string{"DAE_IDEMPOTENCY_TTL": "2m", "DAE_TOOL_TIMEOUT": "2m"}, wantErr: true},
		{name: "ttl exactly tool timeout + 30s + margin is rejected", env: map[string]string{"DAE_IDEMPOTENCY_TTL": "110s", "DAE_TOOL_TIMEOUT": "20s"}, wantErr: true},
		{name: "ttl must also leave room for the 1m expiry margin", env: map[string]string{"DAE_IDEMPOTENCY_TTL": "90s", "DAE_TOOL_TIMEOUT": "20s"}, wantErr: true},
		{name: "ttl just above tool timeout + grace + margin", env: map[string]string{"DAE_IDEMPOTENCY_TTL": "111s", "DAE_TOOL_TIMEOUT": "20s"}},
		{name: "bad duration", env: map[string]string{"DAE_TOOL_TIMEOUT": "soon"}, wantErr: true},
		{name: "non-positive duration", env: map[string]string{"DAE_TOOL_TIMEOUT": "-5s"}, wantErr: true},
		{name: "bad resume uuid", env: map[string]string{"DAE_RESUME_RUN_ID": "not-a-uuid"}, wantErr: true},
		{name: "bad crash spec", env: map[string]string{"DAE_CRASH_AT": "open_pr:whenever"}, wantErr: true},
		{name: "bad crash seq", env: map[string]string{"DAE_CRASH_AFTER_SEQ": "x"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := loadConfig(env(tt.env))
			if (err != nil) != tt.wantErr {
				t.Fatalf("loadConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.check != nil {
				tt.check(t, c)
			}
		})
	}
}

func TestExitCodeFor(t *testing.T) {
	tests := []struct {
		name    string
		outcome agent.Outcome
		err     error
		want    int
	}{
		{"completed", agent.Outcome{Terminal: true}, nil, 0},
		{"run failed", agent.Outcome{Failed: true}, nil, 1},
		{"other error", nil2(), errors.New("boom"), 1},
		{"publish failed", nil2(), fmt.Errorf("x: %w", agent.ErrPublishFailed), 3},
		{"fence unavailable", nil2(), fmt.Errorf("x: %w", idempotency.ErrFenceUnavailable), 4},
		{"outcome unknown", nil2(), fmt.Errorf("x: %w", idempotency.ErrOutcomeUnknown), 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := exitCodeFor(tt.outcome, tt.err); got != tt.want {
				t.Fatalf("exitCodeFor() = %d, want %d", got, tt.want)
			}
		})
	}
}

func nil2() agent.Outcome { return agent.Outcome{} }

func TestTerminalExitCode(t *testing.T) {
	tests := []struct {
		status       replay.RunStatus
		wantTerminal bool
		wantCode     int
	}{
		{replay.StatusCompleted, true, 0},
		{replay.StatusFailed, true, 1},
		{replay.StatusCancelled, true, 1},
		{replay.StatusRunning, false, 0},
		{replay.StatusAwaitingApproval, false, 0},
	}
	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			terminal, code := terminalExitCode(tt.status)
			if terminal != tt.wantTerminal || code != tt.wantCode {
				t.Fatalf("terminalExitCode(%s) = %v, %d; want %v, %d", tt.status, terminal, code, tt.wantTerminal, tt.wantCode)
			}
		})
	}
}
