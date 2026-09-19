package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/replay"
)

// TestCountUpToSeq and TestBuildEventSourceUnknownSource are this command's
// only unit coverage without infrastructure — everything else (a real
// --source kafka/postgres run) is exercised manually per AC-11 onward.

func TestCountUpToSeq(t *testing.T) {
	evs := []events.Envelope{
		{SequenceNumber: 0},
		{SequenceNumber: 1},
		{SequenceNumber: 2},
		{SequenceNumber: 3},
	}
	tests := []struct {
		upToSeq int64
		want    int
	}{
		{upToSeq: -1, want: 0},
		{upToSeq: 0, want: 1},
		{upToSeq: 1, want: 2},
		{upToSeq: 3, want: 4},
		{upToSeq: 100, want: 4},
	}
	for _, tt := range tests {
		if got := countUpToSeq(evs, tt.upToSeq); got != tt.want {
			t.Errorf("countUpToSeq(evs, %d) = %d, want %d", tt.upToSeq, got, tt.want)
		}
	}
}

func TestBuildEventSourceUnknownSource(t *testing.T) {
	_, _, err := buildEventSource(context.Background(), "not-a-real-source")
	if err == nil {
		t.Fatal("buildEventSource() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "not-a-real-source") {
		t.Errorf("buildEventSource() error = %v, want it to name the bad --source value", err)
	}
}

func TestNextActionLineInFlightShowsFenceFields(t *testing.T) {
	key := "idem:run:5:toolu_05E"
	at := time.Date(2026, 9, 20, 14, 2, 11, 104233000, time.UTC)
	tests := []struct {
		name string
		tool replay.ToolCall
		want string
	}{
		{
			name: "side-effecting tool with a key",
			tool: replay.ToolCall{ToolUseID: "toolu_05E", ToolName: "open_pr", HasSideEffect: true, IdempotencyKey: &key, InvokedAt: at},
			want: "next_action=RESOLVE_IN_FLIGHT_TOOL in_flight=open_pr(toolu_05E) side_effect=true idem_key=idem:run:5:toolu_05E invoked_at=2026-09-20T14:02:11.104233Z",
		},
		{
			name: "read-only tool has a null key",
			tool: replay.ToolCall{ToolUseID: "toolu_02B", ToolName: "run_tests", InvokedAt: at},
			want: "next_action=RESOLVE_IN_FLIGHT_TOOL in_flight=run_tests(toolu_02B) side_effect=false idem_key=null invoked_at=2026-09-20T14:02:11.104233Z",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := replay.RunState{NextAction: replay.ActionResolveInFlightTool, InFlightTool: &tt.tool}
			if got := nextActionLine(s); got != tt.want {
				t.Fatalf("nextActionLine() =\n%s\nwant\n%s", got, tt.want)
			}
		})
	}
}
