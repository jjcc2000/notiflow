package redis

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type Client struct {
	rdb *redis.Client
}

func New(addr, password string, db int) *Client {

	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     20,
	})
	return &Client{rdb: rdb}
}

func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

func (c *Client) Close() error {
	return c.rdb.Close()
}

// --- Rate limiting ---
// RateLimitResult describes the current state of a tenant's rate limit window.
type RateLimitResult struct {
	Allowed   bool
	Remaining int64
	ResetIn   time.Duration
}

func (c *Client) CheckRateLimit(ctx context.Context, tenantID string, limit int64) (RateLimitResult, error) {

	key := fmt.Sprintf("rl:%s:%d", tenantID, time.Now().Unix()/60)

	pipe := c.rdb.Pipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, 70*time.Second)

	if _, err := pipe.Exec(ctx); err != nil {
		return RateLimitResult{}, fmt.Errorf("rate limit pipeline: %w", err)
	}

	count := incr.Val()
	remaining := limit - count
	if remaining < 0 {
		remaining = 0
	}

	resetIn := time.Duration(60-(time.Now().Unix()%60)) * time.Second

	return RateLimitResult{
		Allowed:   count <= limit,
		Remaining: remaining,
		ResetIn:   resetIn,
	}, nil
}

// --- Auth cache ---

// CacheTenantAuth stores a tenant ID for a hashed API key.
// TTL of 5 minutes — short enough for key rotation to propagate quickly.
func (c *Client) GetCachedTenantAuth(ctx context.Context, apiKey string) (string, error) {

	key := authCached(apiKey)
	val, err := c.rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil
	}

	if err != nil {
		return "", err
	}

	return val, nil

}

func (c *Client) InvalitedTenantAuth(ctx context.Context, apiKey string) error {
	return c.rdb.Del(ctx, authCached(apiKey)).Err()
}

func authCached(apiKey string) string {
	h := sha256.Sum256([]byte(apiKey))
	return fmt.Sprintf("auth:%x", h[:8])
}

// --- Idempotency ---

// SetIdempotencyKey marks an idempotency key as seen.
// Returns true if this is the FIRST time we've seen this key (safe to process).
// Returns false if it's a duplicate (skip processing).
// TTL of 24 hours — matches typical idempotency window.

func (c *Client) SetIdempotencyKey(ctx context.Context, key string) (bool, error) {
	redisKey := fmt.Sprintf("idem:%s", key)
	// SetNX = SET if Not eXists — atomic check-and-set
	set, err := c.rdb.SetNX(ctx, redisKey, "1", 24*time.Hour).Result()

	if err != nil {
		return false, err
	}
	return set, nil
}

// --- Delivery status cache ---

// CacheNotificationStatus caches the status string for a notification ID.
// Avoids hitting Postgres on every GET /notifications/:id poll.
func (c *Client) CacheNotificationStatus(ctx context.Context, notificationId string, status string) error {
	key := fmt.Sprintf("notif:status:%s", notificationId)
	return c.rdb.Set(ctx, key, status, 10*time.Minute).Err()
}

func (c *Client) GetNotificationStatus(ctx context.Context, notificationId string) (string, error) {

	key := fmt.Sprintf("notif:status:%s", notificationId)
	val, err := c.rdb.Get(ctx, key).Result()
	if err != nil {
		return "", err
	}
	return val, nil
}
