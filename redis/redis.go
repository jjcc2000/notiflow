package redis

import (
	"context"
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
	return c.rdb.Close()
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

func (c *Client) GetCachedTenantAuth(ctx context.Context, apiKey string,tenantID) error {

	key := authCached(apiKey)
	return c.rdb.Set(ctx, key, tenantID, 5*time.Minute).Err()
}

func (c *Client)GetCachedTenantAuth(ctx context.Context, apiKey string) (string ,error){

	
	

}