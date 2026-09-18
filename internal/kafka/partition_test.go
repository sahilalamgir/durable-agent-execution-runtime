package kafka

import "testing"

func TestPartitionForDeterministic(t *testing.T) {
	runID := "7f1c4b8e-2d3a-4f6b-9c1e-0a2b3c4d5e6f"
	first := PartitionFor(runID, RunEventsPartitions)
	for i := 0; i < 100; i++ {
		if got := PartitionFor(runID, RunEventsPartitions); got != first {
			t.Fatalf("PartitionFor(%q, %d) = %d on call %d, want %d (same every call)", runID, RunEventsPartitions, got, i, first)
		}
	}
}

func TestPartitionForInRange(t *testing.T) {
	runIDs := []string{
		"7f1c4b8e-2d3a-4f6b-9c1e-0a2b3c4d5e6f",
		"00000000-0000-0000-0000-000000000000",
		"ffffffff-ffff-ffff-ffff-ffffffffffff",
		"a-short-string",
		"",
	}
	for _, runID := range runIDs {
		got := PartitionFor(runID, RunEventsPartitions)
		if got < 0 || got >= RunEventsPartitions {
			t.Fatalf("PartitionFor(%q, %d) = %d, want in [0, %d)", runID, RunEventsPartitions, got, RunEventsPartitions)
		}
	}
}

func TestPartitionForDistributes(t *testing.T) {
	seen := make(map[int]bool)
	for i := 0; i < 1000; i++ {
		runID := randomishRunID(i)
		seen[PartitionFor(runID, RunEventsPartitions)] = true
	}
	if len(seen) < 2 {
		t.Fatalf("PartitionFor distributed 1000 distinct run_ids across only %d partition(s), want more than 1 (every run landing in the same partition suggests the hash isn't being used)", len(seen))
	}
}

// randomishRunID builds a deterministic but varied string, avoiding a
// dependency on math/rand for a simple distribution smoke test.
func randomishRunID(i int) string {
	return "run-" + string(rune('a'+i%26)) + string(rune('A'+(i/26)%26)) + string(rune('0'+i%10))
}
