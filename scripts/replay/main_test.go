package main

import (
	"context"
	"strings"
	"testing"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
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
