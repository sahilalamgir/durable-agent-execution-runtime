package tools

import (
	"context"
	"os"
	"time"
)

// mockToolDelayEnvVar is the env var that controls how long every mock
// tool's Execute call sleeps before doing its (fake) work. It is read fresh
// on every call rather than cached at init, per CLAUDE.md's no-package-level
// mutable-state rule, and because a manual chaos test may want to change it
// between separate `docker compose up` sessions without recompiling.
const mockToolDelayEnvVar = "DAE_MOCK_TOOL_DELAY"

// mockToolDelay sleeps for the duration in DAE_MOCK_TOOL_DELAY (a
// time.ParseDuration string, default 0), respecting ctx so a cancelled
// context returns promptly instead of sleeping out the full delay. Every
// mock tool's Execute calls this first, so a manual `kill -9` can reliably
// land between ToolInvoked and ToolResulted (FR-29).
func mockToolDelay(ctx context.Context) error {
	raw := os.Getenv(mockToolDelayEnvVar)
	if raw == "" {
		return nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
