CREATE TABLE cdc_flow_source_checkpoints (
    flow_id UUID PRIMARY KEY REFERENCES cdc_flows(id) ON DELETE CASCADE,
    checkpoint TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
