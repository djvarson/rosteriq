-- RosterIQ — Webhook delivery durability
-- Created: 2026-06-14
-- Creates the webhook_deliveries and webhook_dead_letters tables backing the
-- persistent WebhookRetryQueue. These were referenced by PostgresStore but
-- never created by a migration, and save_webhook_delivery dropped the
-- url/payload/headers columns — so durable retry/redelivery was impossible on
-- Postgres. Both tables now carry the full delivery so a queued webhook
-- survives a restart and can be redelivered.

-- ============================================================================
-- WEBHOOK DELIVERIES — the durable retry queue
-- ============================================================================

CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id               TEXT PRIMARY KEY,
    subscription_id  TEXT,
    venue_id         TEXT,
    event_type       TEXT,
    url              TEXT,
    payload          JSONB,
    headers          JSONB,
    status           TEXT,                       -- pending | success | failed | dead_letter
    attempt          INTEGER DEFAULT 0,          -- current attempt counter
    attempts         JSONB DEFAULT '[]'::jsonb,  -- per-attempt log
    response_code    INTEGER,
    error            TEXT,
    last_attempt_at  TIMESTAMPTZ,
    next_retry_at    TIMESTAMPTZ,
    created_at       TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- The queue processor polls for pending deliveries due for retry.
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_pending
    ON webhook_deliveries (status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_subscription
    ON webhook_deliveries (subscription_id, created_at DESC);

-- ============================================================================
-- WEBHOOK DEAD LETTERS — deliveries that exhausted all retries
-- ============================================================================

CREATE TABLE IF NOT EXISTS webhook_dead_letters (
    id               TEXT PRIMARY KEY,
    subscription_id  TEXT,
    venue_id         TEXT,
    event_type       TEXT,
    url              TEXT,
    payload          JSONB,
    headers          JSONB,
    status           TEXT,
    attempt          INTEGER DEFAULT 0,
    attempts         JSONB DEFAULT '[]'::jsonb,
    response_code    INTEGER,
    error            TEXT,
    last_attempt_at  TIMESTAMPTZ,
    dead_lettered_at TIMESTAMPTZ,
    created_at       TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_webhook_dead_letters_venue
    ON webhook_dead_letters (venue_id, dead_lettered_at DESC);
