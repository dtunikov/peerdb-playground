CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE peers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    type SMALLINT NOT NULL,
    config BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE cdc_flows (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    source UUID NOT NULL REFERENCES peers(id),
    destination UUID NOT NULL REFERENCES peers(id),
    config BYTEA NOT NULL,
    internal_version INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
