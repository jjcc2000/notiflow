package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gin-gonic/gin"
	"github.com/jjcc2000/notiflow/pkg/redis"
	"go.uber.org/zap"

	redisclient "github.com/jjcc2000/notiflow/pkg/redis"
)

// AuthMiddleware authenticates API keys using Redis cache → Postgres fallback.
// Cache hit: ~1ms. Cache miss (Postgres): ~5ms. Zero penalty after first request.
func AuthMiddleware(redis *redis.Client, db *pgxpool.Pool, log *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		apiKey := c.GetHeader("X-API-KEY")
		if apiKey == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing API key"})
			return
		}

		//1 Check for the cache first
		tenantID, err := redis.GetCachedTenantAuth(c.Request.Context(), apiKey)

		if err != nil {
			log.Warn("Redis auth cache error - falling back to postgres", zap.Error(err))

		}

		//2 Cache miss - hit Postgres and populate cache
		if tenantID == "" {
			tenantID, err = lookUpTenantFromDB(c.Request.Context(), db, apiKey)

			if err != nil {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid API key"})
				return
			}

			_ = redis.CacheTenantAuth(c.Request.Context(), apiKey, tenantID)

		}

		c.Set("tenant_id", tenantID)
		c.Next()
	}
}

func RateLimitMiddleware(redis *redisclient.Client, limitPerMinute int64) gin.HandlerFunc {

	return func(c *gin.Context) {
		tenantID, _ := c.Get("tenant_id")

		result, err := redis.CheckRateLimit(c.Request.Context(), tenantID.(string), limitPerMinute)

		if err != nil {
			c.Next()
			return
		}

		c.Header("X-RateLimit-Limit", fmt.Sprintf("%d", limitPerMinute))
		c.Header("X-RateLimit-Remaining", fmt.Sprintf("%d", result.Remaining))
		c.Header("X-RateLimit-Reset", fmt.Sprintf("%d", int(result.ResetIn)))

		if !result.Allowed {
			c.Header("Retry-After", fmt.Sprintf("%d", int(result.ResetIn.Seconds())))
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded",
				"retry_after": result.ResetIn.Seconds(),
			})

			return
		}

		c.Next()
	}
}

func lookUpTenantFromDB(ctx context.Context, db *pgxpool.Pool, apiKey string) (string, error) {
	hash := sha256.Sum256([]byte(apiKey))
	keyHash := fmt.Sprintf("%x", hash[:])

	var tenantID string
	err := db.QueryRow(ctx, "SELECT id FROM tenants WHERE api_key_hash = $1 AND active = true", keyHash).Scan(&tenantID)

	return tenantID, err
}

// proxyTo is a simplifpkg/ied reverse proxy handler.
// In production use httputil.ReverseProxy with connection pooling.
func proxyTo(target string) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		// Forward tenant_id as internal header so downstream services trust it
		ctx.Request.Header.Set("X-Tenant-ID", ctx.GetString("tenant_id"))
		// Real Implementacio
		ctx.Status(http.StatusOK)
	}
}

func main() {
	log, _ := zap.NewProduction()
	defer log.Sync()

	db, _ := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))
	redis := redisclient.New(

		os.Getenv("REDIS_ADDR"),
		os.Getenv("REDIS_PASSWORD"), 0,
	)

	r := gin.New()
	r.Use(gin.Recovery())

	//! ROUTES

	api := r.Group("/v1")
	api.Use(AuthMiddleware(redis, db, log))
	api.Use(RateLimitMiddleware(redis, 1000))

	api.POST("/notifications", proxyTo("http://notification-service:8081"))
	api.POST("/notifications/:id", proxyTo("http://notification-service:8081"))
	api.POST("/templates", proxyTo("http://template-service:8082"))
	api.POST("/subscription", proxyTo("http://template-service:8083"))

	r.GET("/healthz", func(ctx *gin.Context) { ctx.Status(http.StatusOK) })

	log.Info("gateway listening", zap.String("addr", ":8080"))

	r.Run(":8080")
}
