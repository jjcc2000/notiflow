package main

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type SubscriptionService struct {
	db  *pgxpool.Pool
	log *zap.Logger
}

type UpsertSubscriptionRequest struct {
	UserID           string `json:"user_id" 	binding:"required"`
	Channel          string `json:"channel" 	binding:"required,oneof=email webhook sms"`
	NotificationType string `json:"notification_type"`
	OptedOut         bool   `json:"opted_out"`
}

// POST /subscription — opt a user in or out
func (s *SubscriptionService) Upsert(ctx *gin.Context) {
	tenantID, err := uuid.Parse(ctx.GetHeader("X-Tenant-ID"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid X-Tenant-ID "})
		return
	}

	var req UpsertSubscriptionRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.NotificationType == "" {
		req.NotificationType = "all"
	}

	_, err = s.db.Exec(ctx.Request.Context(), `
        INSERT INTO subscriptions (tenant_id, user_id, channel, notification_type, opted_out, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (tenant_id, user_id, channel, notification_type)
        DO UPDATE SET opted_out = EXCLUDED.opted_out, updated_at = EXCLUDED.updated_at
    `, tenantID, req.UserID, req.Channel, req.NotificationType, req.OptedOut, time.Now())
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save subscription"})
		return
	}

	action := "subscribed"
	if req.OptedOut {
		action = "unsubscribed"
	}
	ctx.JSON(http.StatusOK, gin.H{"status": action, "user_id": req.UserID, "channel": req.Channel})

}

//Get /subscription/:userid?channel=email -> check if user opted out

func (s *SubscriptionService) Check(ctx *gin.Context) {
	tenantID, err := uuid.Parse(ctx.GetHeader("X-Tenant-ID"))

	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid X-Tenant-ID"})
		return
	}

	userID := ctx.Param("user_id")
	channel := ctx.Query("channel")

	if channel == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "channel query param required"})
		return
	}

	var optedOut bool
	err = s.db.QueryRow(ctx.Request.Context(), `
        SELECT opted_out FROM subscriptions
        WHERE tenant_id = $1 AND user_id = $2 AND channel = $3
        AND notification_type = 'all'
    `, tenantID, userID, channel).Scan(&optedOut)
	if err != nil {
		// No row = never opted out, allow the send
		ctx.JSON(http.StatusOK, gin.H{"opted_out": false})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"opted_out": optedOut})

}

func main() {
	log, _ := zap.NewProduction()
	defer log.Sync()

	db, _ := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))
	svc := &SubscriptionService{db: db, log: log}

	r := gin.New()
	r.Use(gin.Recovery())
	r.POST("/subscription", svc.Upsert)
	r.GET("/subscription/:user_id", svc.Check)
	r.GET("/healthz", func(ctx *gin.Context) { ctx.Status(http.StatusOK) })

	log.Info("subscription service listening", zap.String("addr", ":8083"))
	r.Run(":8083")
}
