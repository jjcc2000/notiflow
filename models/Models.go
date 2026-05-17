package models

import (
	"github.com/google/uuid"
	"time"
)

type Channel string

const (
	ChannelEmail   Channel = "email"
	ChannelWebhook Channel = "webhook"
	ChannelSMS     Channel = "sms"
)

type NotificationStatus string

const (
	StatusPending   NotificationStatus = "pending"
	StatusQueued    NotificationStatus = "queued"
	StatusDelivered NotificationStatus = "delivered"
	StatusFailed    NotificationStatus = "failed"
)

// Tenant is a company that uses NotiFlow via API key.
type Tenant struct {
	ID         uuid.UUID `db:"id" json:"id"`
	Name       string    `db:"name" json:"name"`
	APIKeyHash string    `db:"api_key_hash" json:"-"`
	Plan       string    `db:"plan" json:"plan"`
	CreatedAt  time.Time `db:"created_at" json:"created_at"`
	Active     bool      `db:"active" json:"active"`
}

// Notification is the core domain object — one send request.
type Notification struct {
	ID             uuid.UUID          `db:"id" json:"id"`
	TenantID       uuid.UUID          `db:"tenant_id" json:"tenant_id"`
	UserID         string             `db:"user_id" json:"user_id"`
	TemplateID     uuid.UUID          `db:"template_id" json:"template_id"`
	Channel        Channel            `db:"channel" json:"channel"`
	Status         NotificationStatus `db:"status" json:"status"`
	Payload        map[string]string  `db:"payload" json:"payload"`
	IdempotencyKey string             `db:"idempotency_key" json:"idempotency_key"`
	ScheduledAt    *time.Time         `db:"scheduled_at" json:"scheduled_at,omitempty"`
	CreatedAt      time.Time          `db:"created_at" json:"created_at"`
}

// DeliveryLog records every delivery attempt — success or failure.
type DeliveryLog struct {
	ID               uuid.UUID `db:"id" json:"id"`
	NotificationID   uuid.UUID `db:"notification_id" json:"notification_id"`
	Attempt          int       `db:"attempt" json:"attempt"`
	Status           string    `db:"status" json:"status"`
	ProviderResponse string    `db:"provider_response" json:"provider_response"`
	DeliveredAt      time.Time `db:"delivered_at" json:"delivered_at"`
}

// NotificationEvent is the Kafka message published to each channel topic.
type NotificationEvent struct {
	NotificationID uuid.UUID         `json:"notification_id"`
	TenantID       uuid.UUID         `json:"tenant_id"`
	Channel        Channel           `json:"channel"`
	Recipient      string            `json:"recipient"`
	Subject        string            `json:"subject"`
	Body           string            `json:"body"`
	Attempt        int               `json:"attempt"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}
