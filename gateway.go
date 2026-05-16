package main

import (
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gin-gonic/gin"
	"github.com/jjcc2000/notiflow/redis"
	"go.uber.org/zap"
)

// AuthMiddleware authenticates API keys using Redis cache → Postgres fallback.
// Cache hit: ~1ms. Cache miss (Postgres): ~5ms. Zero penalty after first request.
func AuthMiddleware(redis *redis.Client, db *pgxpool.Pool, log *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		

	}
}
