package notification

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkaclient "github.com/jjcc2000/notiflow/pkg/kafka"
	"github.com/jjcc2000/notiflow/pkg/models"
	redisclient "github.com/jjcc2000/notiflow/pkg/redis"
	"go.uber.org/zap"
)

type NotificationService struct {
	db    *pgxpool.Pool
	kafka *kafkaclient.Producer
	redis *redisclient.Client
	log   *zap.Logger
}

type CreateNotificaionRequest struct {
	UserID         string            `json:"user_id" binding:"required"`
	TemplateID     string            `json:"template_id" binding:"required"`
	Channel        models.Channel    `json:"channel" binding:"required,oneof=email webhook sms"`
	Payload        map[string]string `json:"payload"`
	IdempotencyKey string            `json:"idempondency_key"`
	ScheduledAt    *time.Time        `json:"scheduled_at,omitempty"`
}

// Create POST /v1/notifications
// Flow: validate -> idempondency check -> write to Postgres -> publish to Kafka
func (s *NotificationService) Create(c *gin.Context) {
	tenantID := c.GetHeader("X-Tenant-ID")

	// Verify is the the tenantID is valid
	tenantUUID, err := uuid.Parse(tenantID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid x-Tenant-Id header",
		})
		return
	}

	var req CreateNotificaionRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	templateUUID, err := uuid.Parse(req.TemplateID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid template_id"})
		return
	}

	//Idempontency check - prevent duplicate sends on retry
	if req.IdempotencyKey != "" {
		isNew, err := s.redis.SetIdempotencyKey(c.Request.Context(), req.IdempotencyKey)
		if err != nil {
			s.log.Warn("idepotency check failed", zap.Error(err))
			//Fail open - proceed without idempontency guarantes
		} else if !isNew {
			c.JSON(http.StatusOK, gin.H{"status": "duplicate", "message": "notification already queued"})
			return
		}
	}

	//Render template by calling template service\
	subject, body, err := s.renderTemplate(c.Request.Context(), req.TemplateID, req.Payload)

	if err != nil {
		s.log.Error("template render failed", zap.String("template_id", req.TemplateID), zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "template render failed"})
		return
	}

	// Write notification to Postgres with status=queued
	notif := &models.Notification{
		ID:             uuid.New(),
		TenantID:       tenantUUID,
		UserID:         req.UserID,
		TemplateID:     templateUUID,
		Channel:        req.Channel,
		Status:         models.StatusQueued,
		Payload:        req.Payload,
		IdempotencyKey: req.IdempotencyKey,
		ScheduledAt:    req.ScheduledAt,
		CreatedAt:      time.Now(),
	}

	if err := s.insertNotification(c.Request.Context(), notif); err != nil {

		s.log.Error("db insert failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save notifications"})

		return
	}

	// Publish to the correct Kafka topic based on channel
	// Partition key = tenant_id ensures ordering per tenant

	topic := channelTopic(req.Channel)
	event := models.NotificationEvent{
		NotificationID: notif.ID,
		TenantID:       notif.TenantID,
		Channel:        req.Channel,
		Recipient:      req.UserID,
		Subject:        subject,
		Body:           body,
		Attempt:        1,
	}

	if err := s.kafka.Publish(c.Request.Context(), topic, tenantID, event); err != nil {
		// Kafka publish failed — mark notification as failed in DB
		// A background reconciler can retry pending notifications

		s.log.Error("kafka publish failed", zap.String("notification_id", notif.ID.String()), zap.Error(err))

		_ = s.updateStatus(c.Request.Context(), notif.ID, models.StatusFailed)

		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed top queue notification"})

		return
	}

	s.log.Info("notification queued",

		zap.String("id", notif.ID.String()),
		zap.String("channel", string(req.Channel)),
		zap.String("topic", topic),
	)

	c.JSON(http.StatusCreated, gin.H{
		"id":     notif.ID,
		"status": notif.Status,
	})

}

func channelTopic(ch models.Channel) string {
	switch ch {

	case models.ChannelEmail:
		return kafkaclient.TopicEmail
	case models.ChannelWebhook:
		return kafkaclient.TopicWebHook
	case models.ChannelSMS:
		return kafkaclient.TopicSMS
	default:
		return kafkaclient.TopicEmail
	}

}
func (s *NotificationService) insertNotification(ctx context.Context, n *models.Notification) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO notifications (id, tenant_id, user_id, template_id, channel, status, idempotency_key, scheduled_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, n.ID, n.TenantID, n.UserID, n.TemplateID, n.Channel, n.Status, n.IdempotencyKey, n.ScheduledAt, n.CreatedAt)
	return err
}

func (s *NotificationService) updateStatus(ctx context.Context, id uuid.UUID, status models.NotificationStatus) error {
	_, err := s.db.Exec(ctx, "UPDATE notifications SET status =$1 WHERE id =$2", status, id)
	return err
}

func (s *NotificationService) renderTemplate(ctx context.Context, templateID string, paylod map[string]string) (string, string, error) {
	return "Your notification", "<p>Hello from Notiflow</p>", nil

}

func main() {

}
