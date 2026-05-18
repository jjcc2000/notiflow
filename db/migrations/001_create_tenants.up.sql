CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
 
CREATE TABLE tenants (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name        TEXT NOT NULL,
    api_key_hash TEXT NOT NULL UNIQUE,
    plan        TEXT NOT NULL DEFAULT 'free',
    active      BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
 
CREATE INDEX idx_tenants_api_key_hash ON tenants(api_key_hash);
 