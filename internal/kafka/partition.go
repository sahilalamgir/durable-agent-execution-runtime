package kafka

import "github.com/segmentio/kafka-go"

// PartitionFor is the only partitioning function used anywhere a run_id
// needs to be mapped to a run-events partition — both the production
// writer's Balancer and NewKafkaEventSource's scan call this, so they can
// never disagree about which partition a run lives on. It wraps
// kafka.Hash's FNV-1a algorithm (the same one Sarama's hash partitioner
// uses), not the JVM client's murmur2, so a future non-Go producer would
// have to match this hash specifically, not "whatever the Java client
// does".
func PartitionFor(runID string, numPartitions int) int {
	h := &kafka.Hash{}
	return h.Balance(kafka.Message{Key: []byte(runID)}, partitionIndices(numPartitions)...)
}

// partitionIndices returns [0, 1, ..., n-1]. kafka.Hash.Balance only cares
// about how many partitions there are, but its signature takes the actual
// partition list, so this builds the contiguous 0-based list Kafka assigns
// by default.
func partitionIndices(n int) []int {
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}
	return indices
}
