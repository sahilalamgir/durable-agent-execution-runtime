package main

import "testing"

// TestEnvDefaults is the minimal unit coverage cmd/projector gets: the
// rest (FetchMessage/ApplyEvent/CommitMessages against real infra) is
// exercised manually per AC-15/AC-16, not automated here.
func TestEnvDefaults(t *testing.T) {
	t.Run("kafkaBrokers defaults to localhost:9092", func(t *testing.T) {
		t.Setenv("DAE_KAFKA_BROKERS", "")
		got := kafkaBrokers()
		want := []string{defaultKafkaBrokers}
		if len(got) != 1 || got[0] != want[0] {
			t.Errorf("kafkaBrokers() = %v, want %v", got, want)
		}
	})

	t.Run("kafkaBrokers splits a comma-separated list", func(t *testing.T) {
		t.Setenv("DAE_KAFKA_BROKERS", "broker-a:9092,broker-b:9092")
		got := kafkaBrokers()
		if len(got) != 2 || got[0] != "broker-a:9092" || got[1] != "broker-b:9092" {
			t.Errorf("kafkaBrokers() = %v, want [broker-a:9092 broker-b:9092]", got)
		}
	})

	t.Run("postgresDSN defaults to the local dev DSN", func(t *testing.T) {
		t.Setenv("DAE_POSTGRES_DSN", "")
		if got := postgresDSN(); got != defaultPostgresDSN {
			t.Errorf("postgresDSN() = %q, want %q", got, defaultPostgresDSN)
		}
	})

	t.Run("crashBeforeOffsetCommitFromEnv defaults to 0 (disabled)", func(t *testing.T) {
		t.Setenv("DAE_PROJECTOR_CRASH_BEFORE_OFFSET_COMMIT", "")
		if got := crashBeforeOffsetCommitFromEnv(); got != 0 {
			t.Errorf("crashBeforeOffsetCommitFromEnv() = %d, want 0", got)
		}
	})

	t.Run("crashBeforeOffsetCommitFromEnv parses a set value", func(t *testing.T) {
		t.Setenv("DAE_PROJECTOR_CRASH_BEFORE_OFFSET_COMMIT", "5")
		if got := crashBeforeOffsetCommitFromEnv(); got != 5 {
			t.Errorf("crashBeforeOffsetCommitFromEnv() = %d, want 5", got)
		}
	})
}
