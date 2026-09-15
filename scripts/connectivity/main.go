// Command connectivity is a Phase 0 sanity check: it proves a Go process on
// the host can produce and consume a Kafka message, and read/write a Redis
// key, against the services started by docker-compose.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

const (
	kafkaBroker          = "localhost:9092"
	connectivityTopic    = "phase0-connectivity-test"
	connectivityGroupID  = "phase0-connectivity-group"
	redisAddr            = "localhost:6379"
	connectivityRedisKey = "phase0:connectivity:check"
	connectivityRedisTTL = time.Minute
	connectivityWait     = 10 * time.Second
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatalf("connectivity check failed: %v", err)
	}
}

func run(ctx context.Context) error {
	// A per-run value, not a constant, so a stale leftover record or Redis
	// key from a previous run can never be mistaken for proof that this
	// run's own write actually happened.
	value := fmt.Sprintf("phase0 connectivity check %d", time.Now().UnixNano())

	if err := ensureTopic(ctx, connectivityTopic); err != nil {
		return fmt.Errorf("ensuring kafka topic: %w", err)
	}
	if err := produceMessage(ctx, value); err != nil {
		return fmt.Errorf("producing kafka message: %w", err)
	}
	if err := consumeMessage(ctx, value); err != nil {
		return fmt.Errorf("consuming kafka message: %w", err)
	}
	if err := checkRedis(ctx, value); err != nil {
		return fmt.Errorf("checking redis: %w", err)
	}

	fmt.Println("phase 0 connectivity check: OK")
	return nil
}

// ensureTopic creates the topic explicitly rather than relying on the
// producer's AllowAutoTopicCreation: that path races, because creation
// happens asynchronously on the broker and a produce that triggers it can be
// rejected before creation finishes. Conn.CreateTopics already treats
// "already exists" as success, so this is safe to call on every run.
func ensureTopic(ctx context.Context, topic string) error {
	dialCtx, cancel := context.WithTimeout(ctx, connectivityWait)
	defer cancel()

	conn, err := kafka.DialContext(dialCtx, "tcp", kafkaBroker)
	if err != nil {
		return fmt.Errorf("dialing kafka: %w", err)
	}
	defer closeQuietly("kafka dial conn", conn)

	if err := conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); err != nil {
		return fmt.Errorf("creating topic: %w", err)
	}
	return nil
}

// produceMessage writes through a single Conn rather than a buffering
// kafka.Writer. A Writer batches asynchronously, and its Close flushes any
// pending batch regardless of context — so a produce that times out and is
// reported as failed can still land on the topic moments later during
// cleanup, and a retry loop wrapped around it can silently double-write. A
// raw Conn write is synchronous: it either lands before the deadline or it
// doesn't, and nothing is left running afterward to retry or flush.
func produceMessage(ctx context.Context, value string) error {
	dialCtx, cancel := context.WithTimeout(ctx, connectivityWait)
	defer cancel()

	conn, err := kafka.DialLeader(dialCtx, "tcp", kafkaBroker, connectivityTopic, 0)
	if err != nil {
		return fmt.Errorf("dialing partition leader: %w", err)
	}
	defer closeQuietly("kafka leader conn", conn)

	if err := conn.SetWriteDeadline(time.Now().Add(connectivityWait)); err != nil {
		return fmt.Errorf("setting write deadline: %w", err)
	}
	if _, err := conn.WriteMessages(kafka.Message{Value: []byte(value)}); err != nil {
		return fmt.Errorf("writing message: %w", err)
	}
	return nil
}

func consumeMessage(ctx context.Context, want string) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   connectivityTopic,
		GroupID: connectivityGroupID,
	})
	defer closeQuietly("kafka reader", reader)

	readCtx, cancel := context.WithTimeout(ctx, connectivityWait)
	defer cancel()

	msg, err := reader.ReadMessage(readCtx)
	if err != nil {
		return fmt.Errorf("reading message: %w", err)
	}
	if got := string(msg.Value); got != want {
		return fmt.Errorf("read message %q, want %q (stale record from a previous run?)", got, want)
	}

	fmt.Printf("kafka message received: %s\n", msg.Value)
	return nil
}

func checkRedis(ctx context.Context, value string) error {
	client := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer closeQuietly("redis client", client)

	checkCtx, cancel := context.WithTimeout(ctx, connectivityWait)
	defer cancel()

	if err := client.Set(checkCtx, connectivityRedisKey, value, connectivityRedisTTL).Err(); err != nil {
		return fmt.Errorf("setting key: %w", err)
	}

	got, err := client.Get(checkCtx, connectivityRedisKey).Result()
	if err != nil {
		return fmt.Errorf("getting key: %w", err)
	}
	if got != value {
		return fmt.Errorf("read value %q, want %q (stale key from a previous run?)", got, value)
	}

	fmt.Printf("redis value retrieved: %s\n", got)
	return nil
}

func closeQuietly(what string, c io.Closer) {
	if err := c.Close(); err != nil {
		log.Printf("closing %s: %v", what, err)
	}
}
