//go:build integration

package idempotency

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func integrationStore(t *testing.T) (*RedisStore, *redis.Client, string) {
	t.Helper()
	addr := os.Getenv("DAE_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	client := NewRedisClient(addr)
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis at %s unreachable (run docker compose up): %v", addr, err)
	}
	key := "idem:test:" + uuid.NewString() + ":1:toolu_x"
	t.Cleanup(func() { client.Del(context.Background(), key) })
	return NewRedisStore(client, time.Hour), client, key
}

func claimedRec(token string) IdemRecord {
	return IdemRecord{
		State: StateClaimed, RunID: "r", Step: 1, ToolUseID: "toolu_x", ToolName: "open_pr",
		ArgsHash: "h", ClaimToken: token, ClaimedBy: "test-1", ClaimedAt: time.Now().UTC(),
	}
}

// AC-10: SET NX GET is atomic. 50 goroutines released together produce
// exactly one winner, and every loser sees the winner's record.
func TestRedisClaimIsAtomic(t *testing.T) {
	store, client, key := integrationStore(t)
	ctx := context.Background()
	const n = 50

	var (
		start    = make(chan struct{})
		wg       sync.WaitGroup
		mu       sync.Mutex
		acquired []string
		losers   []string
		errs     []error
	)
	for i := 0; i < n; i++ {
		token := uuid.NewString()
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := store.Claim(ctx, key, claimedRec(token))
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil:
				errs = append(errs, err)
			case res.Acquired:
				acquired = append(acquired, token)
			default:
				losers = append(losers, res.Existing.ClaimToken)
			}
		}()
	}
	close(start)
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("Claim errors: %v", errs)
	}
	if len(acquired) != 1 || len(losers) != n-1 {
		t.Fatalf("acquired = %d (want 1), losers = %d (want %d)", len(acquired), len(losers), n-1)
	}
	for _, tok := range losers {
		if tok != acquired[0] {
			t.Fatalf("a loser saw token %q, want the winner's %q", tok, acquired[0])
		}
	}
	if ttl := client.TTL(ctx, key).Val(); ttl <= 0 {
		t.Fatalf("TTL = %v, want > 0", ttl)
	}
}

// AC-11: the compare-and-set script.
func TestRedisCompareAndSet(t *testing.T) {
	store, client, key := integrationStore(t)
	ctx := context.Background()

	if res, err := store.Claim(ctx, key, claimedRec("tok-a")); err != nil || !res.Acquired {
		t.Fatalf("Claim() = %+v, %v; want acquired", res, err)
	}

	resolved := claimedRec("tok-a")
	resolved.State = StateResolved
	result, status, how := "opened PR #1", "success", "executed"
	resolved.Result, resolved.Status, resolved.Resolution = &result, &status, &how

	t.Run("wrong token loses and leaves the value unchanged", func(t *testing.T) {
		if err := store.Resolve(ctx, key, "tok-WRONG", resolved); !errors.Is(err, ErrClaimLost) {
			t.Fatalf("Resolve() error = %v, want ErrClaimLost", err)
		}
		got, err := store.Get(ctx, key)
		if err != nil || got.State != StateClaimed || got.ClaimToken != "tok-a" {
			t.Fatalf("Get() = %+v, %v; want unchanged claimed/tok-a", got, err)
		}
	})

	t.Run("takeover swaps the token", func(t *testing.T) {
		if err := store.TakeOver(ctx, key, "tok-a", claimedRec("tok-b")); err != nil {
			t.Fatalf("TakeOver() error = %v", err)
		}
		if got, _ := store.Get(ctx, key); got.ClaimToken != "tok-b" {
			t.Fatalf("token = %q, want tok-b", got.ClaimToken)
		}
		if err := store.Resolve(ctx, key, "tok-a", resolved); !errors.Is(err, ErrClaimLost) {
			t.Fatalf("Resolve() with the old token: error = %v, want ErrClaimLost", err)
		}
	})

	t.Run("right token resolves and refreshes the TTL", func(t *testing.T) {
		client.Expire(ctx, key, 10*time.Second)
		resolved.ClaimToken = "tok-b"
		if err := store.Resolve(ctx, key, "tok-b", resolved); err != nil {
			t.Fatalf("Resolve() error = %v", err)
		}
		got, _ := store.Get(ctx, key)
		if got.State != StateResolved || got.Result == nil || *got.Result != result {
			t.Fatalf("Get() = %+v, want resolved with the result", got)
		}
		if ttl := client.TTL(ctx, key).Val(); ttl < 30*time.Minute {
			t.Fatalf("TTL after resolve = %v, want it refreshed to ~1h", ttl)
		}
	})

	t.Run("resolving an already-resolved key is a lost claim", func(t *testing.T) {
		if err := store.Resolve(ctx, key, "tok-b", resolved); !errors.Is(err, ErrClaimLost) {
			t.Fatalf("Resolve() error = %v, want ErrClaimLost", err)
		}
		if err := store.TakeOver(ctx, key, "tok-b", claimedRec("tok-c")); !errors.Is(err, ErrClaimLost) {
			t.Fatalf("TakeOver() on a resolved key: error = %v, want ErrClaimLost", err)
		}
	})
}

func TestRedisCorruptAndAbsent(t *testing.T) {
	store, client, key := integrationStore(t)
	ctx := context.Background()

	if got, err := store.Get(ctx, key); err != nil || got != nil {
		t.Fatalf("Get() on absent key = %v, %v; want nil, nil", got, err)
	}
	if err := store.Resolve(ctx, key, "t", claimedRec("t")); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("Resolve() on absent key: error = %v, want ErrClaimLost", err)
	}

	client.Set(ctx, key, "not json", time.Minute)
	if _, err := store.Claim(ctx, key, claimedRec("t")); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("Claim() over a corrupt value: error = %v, want ErrCorruptRecord", err)
	}
	if _, err := store.Get(ctx, key); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("Get() of a corrupt value: error = %v, want ErrCorruptRecord", err)
	}
	if err := store.Resolve(ctx, key, "t", claimedRec("t")); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("Resolve() over a corrupt value: error = %v, want ErrClaimLost (not a script error)", err)
	}
}
