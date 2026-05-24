package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jjcc2000/notiflow/pkg/kafka"
	"github.com/jjcc2000/notiflow/pkg/models"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var (
	deliveriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notiflow_sms_deliveries_total",
		Help: "Total SMS delivery attempts",
	}, []string{"status"})

	deliveryDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "notiflow_sms_delivery_duration_seconds",
		Help:    "Time from consumer fetch to SNS call completion",
		Buckets: prometheus.DefBuckets,
	})
)

type SMSConsumer struct {
	consumer *kafka.Consumer
	sns      *sns.Client
	db       *pgxpool.Pool
	log      *zap.Logger
}

func NewSMSConsumer(brokers []string, db *pgxpool.Pool, log *zap.Logger) (*SMSConsumer, error) {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	consumerCfg := kafka.DefaultConsumerConfig(brokers, kafka.TopicSMS, "notiflow-sms-consumer")
	return &SMSConsumer{
		consumer: kafka.NewConsumer(consumerCfg, log),
		sns:      sns.NewFromConfig(cfg),
		db:       db,
		log:      log,
	}, nil
}

func (c *SMSConsumer) Run(ctx context.Context) error {
	c.log.Info("sms consumer started", zap.String("topic", kafka.TopicSMS))

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

func (c *SMSConsumer) handle(ctx context.Context, msg *kafka.Message) error {
	start := time.Now()

	var event models.NotificationEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		c.log.Error("unmarshal failed — skipping poison pill", zap.Error(err))
		return nil
	}

	c.log.Info("processing sms",
		zap.String("notification_id", event.NotificationID.String()),
		zap.String("recipient", event.Recipient),
	)

	_, snsErr := c.sns.Publish(ctx, &sns.PublishInput{
		PhoneNumber: aws.String(event.Recipient),
		Message:     aws.String(event.Body),
	})

	duration := time.Since(start)
	deliveryDuration.Observe(duration.Seconds())

	status := "delivered"
	providerResponse := "ok"
	if snsErr != nil {
		status = "failed"
		providerResponse = snsErr.Error()
		c.log.Error("SNS send failed",
			zap.String("notification_id", event.NotificationID.String()),
			zap.Error(snsErr),
		)
	}

	c.writeDeliveryLog(ctx, event, status, providerResponse)
	c.updateNotificationStatus(ctx, event.NotificationID, models.NotificationStatus(status))

	if snsErr != nil {
		deliveriesTotal.WithLabelValues("failed").Inc()
		return snsErr
	}

	deliveriesTotal.WithLabelValues("success").Inc()
	c.log.Info("sms delivered",
		zap.String("notification_id", event.NotificationID.String()),
		zap.Duration("duration", duration),
	)
	return nil
}

func (c *SMSConsumer) writeDeliveryLog(ctx context.Context, event models.NotificationEvent, status, providerResponse string) {
	c.db.Exec(ctx, `
        INSERT INTO delivery_logs (id, notification_id, attempt, status, provider_response, delivered_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
    `, uuid.New(), event.NotificationID, event.Attempt, status, providerResponse)
}

func (c *SMSConsumer) updateNotificationStatus(ctx context.Context, notifID uuid.UUID, status models.NotificationStatus) {
	c.db.Exec(ctx, "UPDATE notifications SET status = $1 WHERE id = $2", status, notifID)
}

func main() {
	log, _ := zap.NewProduction()
	defer log.Sync()

	db, _ := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))

	consumer, err := NewSMSConsumer(
		strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
		db, log,
	)
	if err != nil {
		log.Fatal("consumer init failed", zap.Error(err))
	}

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.Handle("/healthz", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		log.Info("metrics server listening", zap.String("addr", ":9092"))
		http.ListenAndServe(":9092", nil)
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	consumer.Run(ctx)
	log.Info("sms consumer stopped cleanly")
}
