package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Topic names — one per delivery channel plus an audit stream.
const (
	TopicEmail   = "notifications.email"
	TopicWebHook = "notifications.webhook"
	TopicSMS     = "notifications.sms"
	TopicAudit   = "notifications.audit"
)

// Producer wraps kafka-go writer with structured logging and JSON encoding.
type Producer struct {
	writer *kafka.Writer
	log    *zap.Logger
}

func NewProducer(brokers []string, log *zap.Logger) *Producer {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Balancer:               &kafka.LeastBytes{},
		WriteTimeout:           10 * time.Second,
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
	}
	return &Producer{writer: w, log: log}
}

// Publish serialises v to JSON and writes it to topic, keyed by partitionKey.
// Using a partition key (e.g. tenant_id) ensures ordering per tenant.
func (p *Producer) Publish(ctx context.Context, topic, partitionKey string, v any) error {

	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshall message: %w", err)
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(partitionKey),
		Value: payload,
		Time:  time.Now(),
	})

	if err != nil {
		p.log.Error("Kafka publish failed", zap.String("topic", topic), zap.String("key", partitionKey))
		return fmt.Errorf("Publish to %s: %w", topic, err)
	}
	p.log.Debug("published event", zap.String("topic", topic), zap.String("key", partitionKey))
	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

// ConsumerConfig holds everything needed to start a consumer group reader.
type ConsumerConfig struct {
	Brokers     []string
	Topic       string
	GroupId     string
	MineBytes   int
	MaxWait     time.Duration
	MaxBytes    int
	StartOffSet int64
}

func DefaultConsumerConfig(brokers []string, topic, groupId string) ConsumerConfig {

	return ConsumerConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupId:     groupId,
		MineBytes:   1e3,
		MaxBytes:    10e6,
		MaxWait:     time.Second,
		StartOffSet: kafka.LastOffset,
	}
}

type Consumer struct {
	reader *kafka.Reader
	log    *zap.Logger
}

func NewConsumer(cfg ConsumerConfig, log *zap.Logger) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		Topic:       cfg.Topic,
		GroupID:     cfg.GroupId,
		MinBytes:    cfg.MineBytes,
		MaxBytes:    cfg.MaxBytes,
		StartOffset: cfg.StartOffSet,
		// Commit offsets explicitly — never auto-commit.
		// This guarantees at-least-once delivery.
		CommitInterval: 0,
	})

	return &Consumer{reader: r, log: log}
}

// Message is a decoded Kafka message ready to process.
type Message struct {
	Key    string
	Value  []byte
	Offset int64
	raw    kafka.Message
}

// Fetch blocks until a message is available or ctx is cancelled.
func (c *Consumer) Fetch(ctx context.Context) (*Message, error) {
	m, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return nil, err
	}
	return &Message{
		Key:    string(m.Key),
		Value:  m.Value,
		Offset: m.Offset,
		raw:    m,
	}, nil
}

// Message is a decoded Kafka message ready to process.
func (c *Consumer) Commit(ctx context.Context, m *Message) error {
	if err := c.reader.CommitMessages(ctx, m.raw); err != nil {
		c.log.Error("commit failed", zap.Int64("offset", m.Offset), zap.Error(err))
		return err
	}
	c.log.Debug("commited offset", zap.Int64("offset", m.Offset))
	return nil
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

func (c *Consumer) Lag() int64 {
	return c.reader.Lag()
}
