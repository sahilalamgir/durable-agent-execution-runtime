package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ClaimResult is the outcome of Store.Claim.
type ClaimResult struct {
	Acquired bool
	// Existing is the record already stored, non-nil iff !Acquired.
	Existing *IdemRecord
}

// Store is the Redis-shaped storage the Guard needs. It is an interface so
// the Guard's decision table can be tested with a fake store and no Redis.
type Store interface {
	// Claim does SET key rec NX GET EX ttl. Acquired is true on a nil reply;
	// otherwise the existing record is returned and the key is unchanged.
	Claim(ctx context.Context, key string, rec IdemRecord) (ClaimResult, error)
	// Get returns nil, nil if the key is absent.
	Get(ctx context.Context, key string) (*IdemRecord, error)
	// Resolve and TakeOver are compare-and-set on (state == claimed &&
	// claim_token == expectToken); otherwise they return ErrClaimLost.
	Resolve(ctx context.Context, key, expectToken string, resolved IdemRecord) error
	TakeOver(ctx context.Context, key, expectToken string, claimed IdemRecord) error
}

// casScript is shared by Resolve and TakeOver. SET ... IFEQ would avoid Lua
// but needs Redis 8.4; the compose file runs redis:7.
//
//	KEYS[1]=key  ARGV[1]=expected claim_token  ARGV[2]=new value  ARGV[3]=ttl seconds
var casScript = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if not cur then return 0 end
local ok, rec = pcall(cjson.decode, cur)
if not ok or type(rec) ~= 'table' then return 0 end
if rec.state ~= 'claimed' or rec.claim_token ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
return 1
`)

// RedisStore is the Store backed by a real Redis (>= 7.0 for SET NX GET).
type RedisStore struct {
	client *redis.Client
	ttl    time.Duration
}

// NewRedisStore returns a RedisStore that expires keys after ttl.
func NewRedisStore(client *redis.Client, ttl time.Duration) *RedisStore {
	return &RedisStore{client: client, ttl: ttl}
}

// NewRedisClient builds a client whose retries are disabled: the Guard owns
// retrying so every retry shows up as a redis_retry line and is testable.
func NewRedisClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  time.Second,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
		MaxRetries:   -1, // go-redis v9: -1 means no retries.
	})
}

// Claim atomically claims key if absent.
func (s *RedisStore) Claim(ctx context.Context, key string, rec IdemRecord) (ClaimResult, error) {
	val, err := json.Marshal(rec)
	if err != nil {
		return ClaimResult{}, fmt.Errorf("encoding claim record: %w", err)
	}
	old, err := s.client.SetArgs(ctx, key, val, redis.SetArgs{
		Mode: "NX", Get: true, TTL: s.ttl,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return ClaimResult{Acquired: true}, nil
	}
	if err != nil {
		return ClaimResult{}, fmt.Errorf("claiming %s: %w", key, err)
	}
	existing, err := decodeRecord([]byte(old))
	if err != nil {
		return ClaimResult{}, fmt.Errorf("reading existing record for %s: %w", key, err)
	}
	return ClaimResult{Existing: &existing}, nil
}

// Get returns the record under key, or nil, nil if absent.
func (s *RedisStore) Get(ctx context.Context, key string) (*IdemRecord, error) {
	raw, err := s.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting %s: %w", key, err)
	}
	rec, err := decodeRecord(raw)
	if err != nil {
		return nil, fmt.Errorf("reading record for %s: %w", key, err)
	}
	return &rec, nil
}

// Resolve stores resolved iff the claim identified by expectToken is still held.
func (s *RedisStore) Resolve(ctx context.Context, key, expectToken string, resolved IdemRecord) error {
	return s.compareAndSet(ctx, key, expectToken, resolved)
}

// TakeOver replaces a stale claim held under expectToken with claimed.
func (s *RedisStore) TakeOver(ctx context.Context, key, expectToken string, claimed IdemRecord) error {
	return s.compareAndSet(ctx, key, expectToken, claimed)
}

func (s *RedisStore) compareAndSet(ctx context.Context, key, expectToken string, rec IdemRecord) error {
	val, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("encoding record: %w", err)
	}
	ttlSeconds := int64(s.ttl / time.Second)
	n, err := casScript.Run(ctx, s.client, []string{key}, expectToken, val, ttlSeconds).Int()
	if err != nil {
		return fmt.Errorf("compare-and-set on %s: %w", key, err)
	}
	if n != 1 {
		return fmt.Errorf("compare-and-set on %s: %w", key, ErrClaimLost)
	}
	return nil
}

func decodeRecord(raw []byte) (IdemRecord, error) {
	var rec IdemRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return IdemRecord{}, fmt.Errorf("%w: %v", ErrCorruptRecord, err)
	}
	return rec, nil
}
