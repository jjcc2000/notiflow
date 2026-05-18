-- Migration: 002_create_core_tables.up.sql
 
CREATE TABLE templates (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id   UUID NOT NULL REFERENCES tenants(id),
    name        TEXT NOT NULL,
    version     INT NOT NULL DEFAULT 1,
    subject     TEXT NOT NULL,
    body_html   TEXT NOT NULL,
    body_text   TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(tenant_id, name, version)
);
 
CREATE TABLE notifications (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id        UUID NOT NULL REFERENCES tenants(id),
    user_id          TEXT NOT NULL,
    template_id      UUID NOT NULL REFERENCES templates(id),
    channel          TEXT NOT NULL CHECK (channel IN ('email', 'webhook', 'sms')),
    status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'queued', 'delivered', 'failed')),
    idempotency_key  TEXT,
    scheduled_at     TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
 
-- Tenant-scoped lookups (most queries are per-tenant)
CREATE INDEX idx_notifications_tenant_status ON notifications(tenant_id, status);
-- Idempotency enforcement
CREATE UNIQUE INDEX idx_notifications_idempotency ON notifications(tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
-- Scheduled notifications poller
CREATE INDEX idx_notifications_scheduled ON notifications(scheduled_at)
    WHERE status = 'pending' AND scheduled_at IS NOT NULL;
 
CREATE TABLE delivery_logs (
    id                UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    notification_id   UUID NOT NULL REFERENCES notifications(id),
    attempt           INT NOT NULL DEFAULT 1,
    status            TEXT NOT NULL,
    provider_response TEXT,
    delivered_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
 
CREATE INDEX idx_delivery_logs_notification ON delivery_logs(notification_id);
 
CREATE TABLE subscriptions (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id           UUID NOT NULL REFERENCES tenants(id),
    user_id             TEXT NOT NULL,
    channel             TEXT NOT NULL,
    notification_type   TEXT NOT NULL DEFAULT 'all',
    opted_out           BOOLEAN NOT NULL DEFAULT false,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(tenant_id, user_id, channel, notification_type)
);
 
CREATE INDEX idx_subscriptions_lookup ON subscriptions(tenant_id, user_id, channel);
 