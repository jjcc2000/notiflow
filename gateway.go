package main

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gin-gonic/gin"
	"github.com/jjcc2000/notiflow/redis"
	"go.uber.org/zap"
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

		// if tenantID ==""{
		// 	tenantID, err = lookupTenant


		// }
	}
}





// proxyTo is a simplified reverse proxy handler.
// In production use httputil.ReverseProxy with connection pooling.
func proxyTo(target string) gin.HandlerFunc{

	return func(ctx *gin.Context) {
		c
	}
}