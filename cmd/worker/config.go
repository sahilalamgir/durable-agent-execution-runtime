package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/idempotency"
)

const (
	defaultKafkaBrokers = "localhost:9092"
	defaultRedisAddr    = "localhost:6379"
	defaultIdemTTL      = 24 * time.Hour
	defaultToolTimeout  = 2 * time.Minute
	defaultLedgerPath   = ".dae/mock-ledger.jsonl"
)

// config is everything the worker reads from its environment.
type config struct {
	brokers     []string
	redisAddr   string
	idemTTL     time.Duration
	toolTimeout time.Duration
	ledgerPath  string
	// resumeRunID is empty for a fresh run.
	resumeRunID string
	crashSpec   string
	// crashAfterSeq is nil unless DAE_CRASH_AFTER_SEQ is set (Phase 2).
	crashAfterSeq *int64
}

// loadConfig reads and validates the worker's environment. Any error is a
// startup configuration error: main fails loudly and immediately.
func loadConfig(getenv func(string) string) (config, error) {
	cfg := config{
		brokers:     splitBrokers(orDefault(getenv("DAE_KAFKA_BROKERS"), defaultKafkaBrokers)),
		redisAddr:   orDefault(getenv("DAE_REDIS_ADDR"), defaultRedisAddr),
		ledgerPath:  orDefault(getenv("DAE_MOCK_LEDGER_PATH"), defaultLedgerPath),
		resumeRunID: getenv("DAE_RESUME_RUN_ID"),
		crashSpec:   getenv("DAE_CRASH_AT"),
	}

	var err error
	if cfg.idemTTL, err = durationEnv(getenv, "DAE_IDEMPOTENCY_TTL", defaultIdemTTL); err != nil {
		return config{}, err
	}
	if cfg.toolTimeout, err = durationEnv(getenv, "DAE_TOOL_TIMEOUT", defaultToolTimeout); err != nil {
		return config{}, err
	}
	// The fence treats an absent key as proof a tool never ran, so a claim
	// must not be able to expire while its tool could still be running. It
	// also needs room for the expiry margin: with a smaller TTL every fresh
	// claim would look "possibly expired" and be diverted to reconciliation.
	if min := cfg.toolTimeout + idempotency.DefaultStaleGrace + idempotency.ExpiryMargin; cfg.idemTTL <= min {
		return config{}, fmt.Errorf("DAE_IDEMPOTENCY_TTL=%s must be greater than DAE_TOOL_TIMEOUT + %s + %s = %s",
			cfg.idemTTL, idempotency.DefaultStaleGrace, idempotency.ExpiryMargin, min)
	}

	if cfg.resumeRunID != "" {
		if _, err := uuid.Parse(cfg.resumeRunID); err != nil {
			return config{}, fmt.Errorf("DAE_RESUME_RUN_ID=%q is not a valid uuid: %w", cfg.resumeRunID, err)
		}
	}
	if _, err := idempotency.ParseCrashSpec(cfg.crashSpec, nil); err != nil {
		return config{}, fmt.Errorf("DAE_CRASH_AT: %w", err)
	}
	if raw := getenv("DAE_CRASH_AFTER_SEQ"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return config{}, fmt.Errorf("DAE_CRASH_AFTER_SEQ=%q is not a valid integer: %w", raw, err)
		}
		cfg.crashAfterSeq = &n
	}
	return cfg, nil
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func splitBrokers(raw string) []string { return strings.Split(raw, ",") }

func durationEnv(getenv func(string) string, name string, def time.Duration) (time.Duration, error) {
	raw := getenv(name)
	if raw == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s=%q is not a positive duration (e.g. \"20s\")", name, raw)
	}
	return d, nil
}
