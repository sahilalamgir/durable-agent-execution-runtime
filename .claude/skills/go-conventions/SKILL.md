---
name: go-conventions
description: Worked code examples for this project's Go conventions — error wrapping, context propagation, event-type enums, table-driven tests, dependency injection. Load whenever writing or reviewing Go code in this repo.
---

This is the detailed reference for the rules summarized in CLAUDE.md's Code Style section. If you're about to write Go code in this repo, follow these patterns exactly. This file grows over time — every time a mistake gets corrected twice, the fix belongs here, not just in the chat.

## Error wrapping
Always wrap with context on the way up, never return a bare error:
```go
if err != nil {
    return fmt.Errorf("publishing event to kafka: %w", err)
}
```

## Context propagation
Every function doing I/O takes `context.Context` as its first argument, and it propagates all the way down — this is load-bearing for `CancelRun` and for the fail-closed Redis timeout:
```go
func (w *Worker) checkFencingKey(ctx context.Context, key string) (bool, error) {
    ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
    defer cancel()
    return w.redis.SetNX(ctx, key, "CLAIMED", 24*time.Hour).Result()
}
```

## Event type enums
Never compare raw strings. Always a typed constant:
```go
type EventType string

const (
    EventRunStarted     EventType = "RunStarted"
    EventToolInvoked    EventType = "ToolInvoked"
    EventToolResulted   EventType = "ToolResulted"
    EventRunCompleted   EventType = "RunCompleted"
)
```

## Dependency injection, no globals
Dependencies are passed in via constructor, never package-level variables — this is what makes idempotency logic unit-testable:
```go
type Worker struct {
    redis *redis.Client
    kafka *kafka.Writer
    db    *pgxpool.Pool
}

func NewWorker(redis *redis.Client, kafka *kafka.Writer, db *pgxpool.Pool) *Worker {
    return &Worker{redis: redis, kafka: kafka, db: db}
}
```

## Table-driven tests
Default format for anything with multiple cases — especially idempotency-state transitions:
```go
func TestFencingKey(t *testing.T) {
    tests := []struct {
        name      string
        existing  string
        wantClaim bool
    }{
        {"no existing key", "", true},
        {"already claimed", "CLAIMED", false},
        {"already resolved", `{"status":"success"}`, false},
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // ...
        })
    }
}
```

## JSON struct tags
Go fields stay PascalCase; JSON on the wire matches the spec's snake_case exactly:
```go
type ToolInvokedPayload struct {
    Step           int    `json:"step"`
    ToolName       string `json:"tool_name"`
    IdempotencyKey string `json:"idempotency_key"`
}
```
