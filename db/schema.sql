-- db/schema.sql

-- Core append-only event log
CREATE TABLE IF NOT EXISTS events (
    id            BIGSERIAL PRIMARY KEY,
    stream_id     TEXT        NOT NULL,
    version       BIGINT      NOT NULL,
    event_type    TEXT        NOT NULL,
    event_data    JSONB       NOT NULL DEFAULT '{}',
    metadata      JSONB       NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT events_stream_version_unique UNIQUE (stream_id, version)
);

CREATE INDEX IF NOT EXISTS idx_events_stream_id ON events (stream_id);
CREATE INDEX IF NOT EXISTS idx_events_stream_version ON events (stream_id, version);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events (created_at);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_id ON events (id);

-- Tracks the current (latest) version of each stream — used for optimistic concurrency
CREATE TABLE IF NOT EXISTS event_streams (
    stream_id      TEXT        PRIMARY KEY,
    current_version BIGINT     NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archived_at    TIMESTAMPTZ
);

-- Tracks how far each named projection has read through the events table
CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_name TEXT        PRIMARY KEY,
    last_event_id   BIGINT      NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Outbox for guaranteed at-least-once delivery to downstream consumers
CREATE TABLE IF NOT EXISTS outbox (
    id            BIGSERIAL   PRIMARY KEY,
    event_id      BIGINT      NOT NULL REFERENCES events(id),
    destination   TEXT        NOT NULL,
    payload       JSONB       NOT NULL DEFAULT '{}',
    status        TEXT        NOT NULL DEFAULT 'pending'
                              CHECK (status IN ('pending', 'delivered', 'failed')),
    attempts      INT         NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at  TIMESTAMPTZ,
    delivered_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox (status);
-- Projection: one row per loan application, current state
CREATE TABLE IF NOT EXISTS application_summary (
    application_id        TEXT PRIMARY KEY,
    applicant_id          TEXT NOT NULL,
    status                TEXT NOT NULL DEFAULT 'Submitted',
    requested_amount_usd  NUMERIC(15,2),
    approved_amount_usd   NUMERIC(15,2),
    interest_rate         NUMERIC(6,4),
    assigned_agent_id     TEXT,
    recommendation        TEXT,
    confidence_score      NUMERIC(4,3),
    risk_tier             TEXT,
    reviewer_id           TEXT,
    final_decision        TEXT,
    submission_channel    TEXT,
    submitted_at          TIMESTAMPTZ,
    decided_at            TIMESTAMPTZ,
    last_updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Projection: metrics per agent per model version
CREATE TABLE IF NOT EXISTS agent_performance_ledger (
    agent_id              TEXT NOT NULL,
    model_version         TEXT NOT NULL,
    total_analyses        INT NOT NULL DEFAULT 0,
    total_fraud_screens   INT NOT NULL DEFAULT 0,
    avg_confidence_score  NUMERIC(4,3),
    avg_duration_ms       NUMERIC(10,2),
    last_active_at        TIMESTAMPTZ,
    PRIMARY KEY (agent_id, model_version)
);

-- Projection: full regulatory read model with temporal query support
CREATE TABLE IF NOT EXISTS compliance_audit_view (
    id                    BIGSERIAL PRIMARY KEY,
    application_id        TEXT NOT NULL,
    rule_id               TEXT NOT NULL,
    rule_version          TEXT NOT NULL,
    status                TEXT NOT NULL,   -- 'passed' | 'failed' | 'pending'
    failure_reason        TEXT,
    remediation_required  BOOLEAN NOT NULL DEFAULT FALSE,
    regulation_set_version TEXT,
    evidence_hash         TEXT,
    evaluated_at          TIMESTAMPTZ,
    recorded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_app_summary_status ON application_summary (status);
CREATE INDEX IF NOT EXISTS idx_compliance_audit_app ON compliance_audit_view (application_id);
CREATE INDEX IF NOT EXISTS idx_compliance_audit_time ON compliance_audit_view (evaluated_at);
