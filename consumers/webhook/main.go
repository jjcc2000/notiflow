package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/jjcc2000/notiflow/pkg/kafka"
	"github.com/jjcc2000/notiflow/pkg/models"
)

var (
	deliveriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notiflow_webhook_deliveries_total",
		Help: "Total webhook delivery attempts",
	}, []string{"status"})

	deliveryDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "notiflow_webhook_delivery_duration_seconds",
		Help:    "Time from consumer fetch to HTTP call completion",
		Buckets: prometheus.DefBuckets,
	})
)

type WebhookConsumer struct {
	consumer   *kafka.Consumer
	httpClient *http.Client
	db         *pgxpool.Pool
	log        *zap.Logger
}

func NewWebhookConsumer(brokers []string, db *pgxpool.Pool, log *zap.Logger) *WebhookConsumer {
	cfg := kafka.DefaultConsumerConfig(brokers, kafka.TopicWebHook, "notiflow-webhook-consumer")
	return &WebhookConsumer{
		consumer:   kafka.NewConsumer(cfg, log),
		httpClient: &http.Client{Timeout: 10 * time.Second},
		db:         db,
		log:        log,
	}
}

func (c *WebhookConsumer) Run(ctx context.Context) error {
	c.log.Info("webhook consumer started", zap.String("topic", kafka.TopicWebHook))

	for {
		msg, err := c.consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.log.Info("consumer shutting down gracefully")
				return nil
			}
			c.log.Error("fetch error", zap.Error(err))
			time.Sleep(time.Second)
			continue
		}

		if err := c.handle(ctx, msg); err != nil {
			c.log.Error("handle failed — not committing, will redeliver",
				zap.Int64("offset", msg.Offset),
				zap.Error(err),
			)
			deliveriesTotal.WithLabelValues("failed").Inc()
			continue
		}

		if err := c.consumer.Commit(ctx, msg); err != nil {
			c.log.Error("commit failed", zap.Error(err))
		}
	}
}

func (c *WebhookConsumer) handle(ctx context.Context, msg *kafka.Message) error {
	start := time.Now()

	var event models.NotificationEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		c.log.Error("unmarshal failed — skipping poison pill", zap.Error(err))
		return nil // commit and skip bad message
	}

	c.log.Info("processing webhook",
		zap.String("notification_id", event.NotificationID.String()),
		zap.String("url", event.Recipient),
	)

	// Build the payload to POST to the tenant's webhook URL
	payload, _ := json.Marshal(map[string]string{
		"notification_id": event.NotificationID.String(),
		"subject":         event.Subject,
		"body":            event.Body,
	})

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, event.Recipient, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)

	duration := time.Since(start)
	deliveryDuration.Observe(duration.Seconds())

	status := "delivered"
	providerResponse := "ok"
	if err != nil {
		status = "failed"
		providerResponse = err.Error()
	} else if resp.StatusCode >= 400 {
		status = "failed"
		providerResponse = resp.Status
		resp.Body.Close()
	} else {
		resp.Body.Close()
	}

	c.writeDeliveryLog(ctx, event, status, providerResponse)
	c.updateNotificationStatus(ctx, event.NotificationID, models.NotificationStatus(status))

	if status == "failed" {
		return fmt.Errorf("webhook delivery failed: %s", providerResponse)
	}

	deliveriesTotal.WithLabelValues("success").Inc()
	return nil
}

func (c *WebhookConsumer) writeDeliveryLog(ctx context.Context, event models.NotificationEvent, status, providerResponse string) {
	c.db.Exec(ctx, `
        INSERT INTO delivery_logs (id, notification_id, attempt, status, provider_response, delivered_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
    `, uuid.New(), event.NotificationID, event.Attempt, status, providerResponse)
}

func (c *WebhookConsumer) updateNotificationStatus(ctx context.Context, notifID uuid.UUID, status models.NotificationStatus) {
	c.db.Exec(ctx, "UPDATE notifications SET status = $1 WHERE id = $2", status, notifID)
}

func main() {
	log, _ := zap.NewProduction()
	defer log.Sync()

	db, _ := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))

	consumer := NewWebhookConsumer(
		strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
		db, log,
	)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.Handle("/healthz", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		log.Info("metrics server listening", zap.String("addr", ":9091"))
		http.ListenAndServe(":9091", nil)
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	consumer.Run(ctx)
	log.Info("webhook consumer stopped cleanly")
}
