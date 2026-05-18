package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/jjcc2000/notiflow/pkg/kafka"
	"github.com/jjcc2000/notiflow/pkg/models"
)

// --- Prometheus metrics ---
// These expose consumer lag, delivery counts, and latency to Grafana.

var (
	deliveriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notiflow_email_deliveries_total",
		Help: "Total email delivery attempts",
	}, []string{"status"}) // status: success | failed | duplicate

	deliveryDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "notiflow_email_delivery_duration_seconds",
		Help:    "Time from consumer fetch to SES call completion",
		Buckets: prometheus.DefBuckets,
	})

	consumerLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "notiflow_email_consumer_lag",
		Help: "Current Kafka consumer lag for email topic",
	})
)

// EmailConsumer reads from notifications.email and delivers via AWS SES.
type EmailConsumer struct {
	consumer *kafka.Consumer
	ses      *ses.Client
	db       *pgxpool.Pool
	log      *zap.Logger
}

func NewEmailConsumer(brokers []string, db *pgxpool.Pool, log *zap.Logger) (*EmailConsumer, error) {
	cfg := kafka.DefaultConsumerConfig(brokers, kafka.TopicEmail, "notiflow-email-consumer")
	consumer := kafka.NewConsumer(cfg, log)

	awsCfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	return &EmailConsumer{
		consumer: consumer,
		ses:      ses.NewFromConfig(awsCfg),
		db:       db,
		log:      log,
	}, nil
}

// Run is the main consumer loop. It runs until ctx is cancelled (e.g. on SIGTERM).
// Kubernetes sends SIGTERM before killing the pod — we catch it for graceful shutdown.
func (c *EmailConsumer) Run(ctx context.Context) error {
	c.log.Info("email consumer started", zap.String("topic", kafka.TopicEmail))

	for {
		// Update lag metric on every iteration for Grafana dashboards
		consumerLag.Set(float64(c.consumer.Lag()))

		msg, err := c.consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.log.Info("consumer shutting down gracefully")
				return nil
			}
			c.log.Error("fetch error", zap.Error(err))
			time.Sleep(time.Second) // backoff before retry
			continue
		}

		if err := c.handle(ctx, msg); err != nil {
			// Do NOT commit on failure — Kafka will redeliver from last committed offset.
			// This gives us at-least-once delivery semantics.
			c.log.Error("handle failed — not committing, will redeliver",
				zap.Int64("offset", msg.Offset),
				zap.Error(err),
			)
			deliveriesTotal.WithLabelValues("failed").Inc()
			continue
		}

		// Commit ONLY after successful processing.
		if err := c.consumer.Commit(ctx, msg); err != nil {
			c.log.Error("commit failed", zap.Error(err))
		}
	}
}

// handle decodes the message, delivers via SES, and writes the delivery log.
// This is the critical section — every step is logged for the audit trail.
func (c *EmailConsumer) handle(ctx context.Context, msg *kafka.Message) error {
	start := time.Now()

	var event models.NotificationEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		// Bad message format — log and commit to skip it (dead-letter pattern).
		c.log.Error("unmarshal failed — skipping poison pill",
			zap.String("raw", string(msg.Value)),
			zap.Error(err),
		)
		deliveriesTotal.WithLabelValues("failed").Inc()
		return nil // return nil so we DO commit and skip this bad message
	}

	c.log.Info("processing email",
		zap.String("notification_id", event.NotificationID.String()),
		zap.String("tenant_id", event.TenantID.String()),
		zap.String("recipient", event.Recipient),
		zap.Int("attempt", event.Attempt),
	)

	// Call AWS SES
	sesInput := &ses.SendEmailInput{
		Source: strPtr(os.Getenv("SES_FROM_ADDRESS")),
		Destination: &types.Destination{
			ToAddresses: []string{event.Recipient},
		},
		Message: &types.Message{
			Subject: &types.Content{Data: strPtr(event.Subject)},
			Body: &types.Body{
				Html: &types.Content{Data: strPtr(event.Body)},
			},
		},
	}

	_, sesErr := c.ses.SendEmail(ctx, sesInput)

	duration := time.Since(start)
	deliveryDuration.Observe(duration.Seconds())

	status := "delivered"
	providerResponse := "ok"
	if sesErr != nil {
		status = "failed"
		providerResponse = sesErr.Error()
		c.log.Error("SES send failed",
			zap.String("notification_id", event.NotificationID.String()),
			zap.Error(sesErr),
		)
	}

	// Write delivery log to Postgres regardless of success/failure.
	// This is what makes the audit trail complete.
	if dbErr := c.writeDeliveryLog(ctx, event, status, providerResponse, event.Attempt); dbErr != nil {
		c.log.Error("delivery log write failed", zap.Error(dbErr))
		// Don't return — still report the SES result
	}

	// Update notification status in Postgres
	if dbErr := c.updateNotificationStatus(ctx, event.NotificationID, models.NotificationStatus(status)); dbErr != nil {
		c.log.Error("status update failed", zap.Error(dbErr))
	}

	if sesErr != nil {
		deliveriesTotal.WithLabelValues("failed").Inc()
		return sesErr // return error so the offset is NOT committed — Kafka will redeliver
	}

	deliveriesTotal.WithLabelValues("success").Inc()
	c.log.Info("email delivered",
		zap.String("notification_id", event.NotificationID.String()),
		zap.Duration("duration", duration),
	)
	return nil
}

func (c *EmailConsumer) writeDeliveryLog(ctx context.Context, event models.NotificationEvent, status, providerResponse string, attempt int) error {
	_, err := c.db.Exec(ctx, `
		INSERT INTO delivery_logs (id, notification_id, attempt, status, provider_response, delivered_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
	`, uuid.New(), event.NotificationID, attempt, status, providerResponse)
	return err
}

func (c *EmailConsumer) updateNotificationStatus(ctx context.Context, notifID uuid.UUID, status models.NotificationStatus) error {
	_, err := c.db.Exec(ctx, `
		UPDATE notifications SET status = $1 WHERE id = $2
	`, status, notifID)
	return err
}

func (c *EmailConsumer) Close() {
	c.consumer.Close()
	c.db.Close()
}

func main() {
	log, _ := zap.NewProduction()
	defer log.Sync()

	brokers := []string{os.Getenv("KAFKA_BROKERS")} // e.g. "kafka-1:9092,kafka-2:9092"
	dbURL := os.Getenv("DATABASE_URL")

	db, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatal("db connect failed", zap.Error(err))
	}

	consumer, err := NewEmailConsumer(brokers, db, log)
	if err != nil {
		log.Fatal("consumer init failed", zap.Error(err))
	}
	defer consumer.Close()

	// Expose /metrics for Prometheus scraping (Kubernetes annotation: prometheus.io/scrape)
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.Handle("/healthz", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		log.Info("metrics server listening", zap.String("addr", ":9090"))
		if err := http.ListenAndServe(":9090", nil); err != nil {
			log.Error("metrics server failed", zap.Error(err))
		}
	}()

	// Graceful shutdown: catch SIGTERM (sent by Kubernetes before pod termination)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if err := consumer.Run(ctx); err != nil {
		log.Fatal("consumer exited with error", zap.Error(err))
	}

	log.Info("email consumer stopped cleanly")
}

func strPtr(s string) *string { return &s }
