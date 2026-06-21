-- RosterIQ PostgreSQL Schema — REFERENCE SNAPSHOT ONLY, DO NOT USE TO PROVISION.
--
-- This file is an early snapshot of the core tables and has DRIFTED: it does
-- not include the v3 feature tables (shift bids, approval workflows, web push
-- subscriptions, backups, conflicts, etc) added in migrations 002/003.
--
-- The authoritative schema is migrations/*.sql, applied by:
--     python -m rosteriq.migrations.run_migrations --run
-- Provision every environment (Docker, Railway, Render, local) via the runner,
-- never by piping this file into psql.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Venues
CREATE TABLE venues (
    id              TEXT PRIMARY KEY DEFAULT uuid_generate_v4()::text,
    name            TEXT NOT NULL,
    tanda_org_id    TEXT NOT NULL,
    state           TEXT NOT NULL CHECK (state IN ('nsw','vic','qld','sa','wa','tas','nt','act')),
    timezone        TEXT NOT NULL DEFAULT 'Australia/Melbourne',
    min_staff       JSONB NOT NULL DEFAULT '{}',
    max_labour_pct  NUMERIC(5,2) NOT NULL DEFAULT 32.0,
    pos_system      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Tanda credentials (encrypted at rest in production)
CREATE TABLE tanda_credentials (
    venue_id        TEXT PRIMARY KEY REFERENCES venues(id),
    client_id       TEXT NOT NULL,
    client_secret   TEXT NOT NULL,
    access_token    TEXT,
    refresh_token   TEXT,
    token_expires   TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Xero credentials (encrypted at rest in production)
CREATE TABLE xero_credentials (
    venue_id        TEXT PRIMARY KEY REFERENCES venues(id),
    client_id       TEXT NOT NULL,
    client_secret   TEXT NOT NULL,
    tenant_id       TEXT NOT NULL,
    access_token    TEXT NOT NULL,
    refresh_token   TEXT NOT NULL,
    token_expires   TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Employees (synced from Tanda)
CREATE TABLE employees (
    id                  TEXT PRIMARY KEY,
    venue_id            TEXT NOT NULL REFERENCES venues(id),
    tanda_id            TEXT,
    name                TEXT NOT NULL,
    employment_type     TEXT NOT NULL CHECK (employment_type IN ('full_time','part_time','casual')),
    award_level         TEXT NOT NULL,
    hourly_base_rate    NUMERIC(8,2) NOT NULL,
    skills              TEXT[] DEFAULT '{}',
    availability        JSONB DEFAULT '{}',
    max_hours_per_week  NUMERIC(4,1) DEFAULT 38.0,
    consecutive_days    INTEGER DEFAULT 6,
    phone               TEXT,
    email               TEXT,
    active              BOOLEAN DEFAULT true,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_employees_venue ON employees(venue_id);

-- Demand forecasts
CREATE TABLE forecasts (
    id                  TEXT PRIMARY KEY DEFAULT uuid_generate_v4()::text,
    venue_id            TEXT NOT NULL REFERENCES venues(id),
    forecast_date       DATE NOT NULL,
    hour                INTEGER NOT NULL CHECK (hour BETWEEN 0 AND 23),
    predicted_covers    NUMERIC(8,1) NOT NULL,
    confidence          NUMERIC(3,2) NOT NULL,
    signals_used        TEXT[] DEFAULT '{}',
    model_version       TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_forecasts_venue_date ON forecasts(venue_id, forecast_date);
CREATE UNIQUE INDEX idx_forecasts_unique ON forecasts(venue_id, forecast_date, hour, model_version);

-- Rosters
CREATE TABLE rosters (
    id              TEXT PRIMARY KEY DEFAULT uuid_generate_v4()::text,
    venue_id        TEXT NOT NULL REFERENCES venues(id),
    week_start      DATE NOT NULL,
    week_end        DATE NOT NULL,
    total_cost      NUMERIC(10,2),
    status          TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','published','archived')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_rosters_venue_week ON rosters(venue_id, week_start);

-- Shifts
CREATE TABLE shifts (
    id                  TEXT PRIMARY KEY DEFAULT uuid_generate_v4()::text,
    roster_id           TEXT NOT NULL REFERENCES rosters(id) ON DELETE CASCADE,
    employee_id         TEXT NOT NULL REFERENCES employees(id),
    shift_date          DATE NOT NULL,
    start_time          TIME NOT NULL,
    end_time            TIME NOT NULL,
    break_minutes       INTEGER DEFAULT 0,
    status              TEXT NOT NULL DEFAULT 'scheduled',
    role                TEXT NOT NULL DEFAULT 'general',
    cost                NUMERIC(8,2),
    penalty_multiplier  NUMERIC(4,2) DEFAULT 1.0,
    tanda_shift_id      TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_shifts_roster ON shifts(roster_id);
CREATE INDEX idx_shifts_employee_date ON shifts(employee_id, shift_date);

-- Historical POS data (imported from CSV)
CREATE TABLE pos_data (
    id              SERIAL PRIMARY KEY,
    venue_id        TEXT NOT NULL REFERENCES venues(id),
    record_date     DATE NOT NULL,
    hour            INTEGER NOT NULL,
    revenue         NUMERIC(10,2),
    covers          INTEGER,
    transactions    INTEGER,
    pos_system      TEXT,
    imported_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_pos_venue_date ON pos_data(venue_id, record_date);

-- Audit log
CREATE TABLE audit_log (
    id              SERIAL PRIMARY KEY,
    venue_id        TEXT REFERENCES venues(id),
    action          TEXT NOT NULL,
    details         JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_audit_venue ON audit_log(venue_id, created_at DESC);

-- Venue locations (geographic data for data feeds)
CREATE TABLE venue_locations (
    venue_id        TEXT PRIMARY KEY REFERENCES venues(id),
    latitude        NUMERIC(10,7) NOT NULL,
    longitude       NUMERIC(10,7) NOT NULL,
    address         TEXT DEFAULT '',
    suburb          TEXT DEFAULT '',
    postcode        TEXT DEFAULT '',
    google_place_id TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Data feed API credentials per venue
CREATE TABLE data_feed_configs (
    venue_id                TEXT PRIMARY KEY REFERENCES venues(id),
    weather_api_key         TEXT,
    google_places_api_key   TEXT,
    ticketmaster_api_key    TEXT,
    eventbrite_token        TEXT,
    predicthq_token         TEXT,
    resdiary_api_key        TEXT,
    nowbookit_api_key       TEXT,
    opentable_token         TEXT,
    sevenrooms_api_key      TEXT,
    ubereats_client_id      TEXT,
    ubereats_client_secret  TEXT,
    doordash_developer_id   TEXT,
    doordash_key_id         TEXT,
    menulog_api_key         TEXT,
    sportradar_api_key      TEXT,
    besttime_api_key        TEXT,
    str_api_key             TEXT,
    airdna_api_key          TEXT,
    ptv_dev_id              TEXT,
    ptv_api_key             TEXT,
    tfnsw_api_key           TEXT,
    booking_no_show_rate    NUMERIC(3,2) DEFAULT 0.15,
    foot_traffic_radius_km  NUMERIC(4,1) DEFAULT 1.0,
    event_radius_km         NUMERIC(4,1) DEFAULT 5.0,
    competitor_radius_km    NUMERIC(4,1) DEFAULT 1.0,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Cached external signals (persisted for historical analysis and ML training)
CREATE TABLE external_signals (
    id              SERIAL PRIMARY KEY,
    venue_id        TEXT NOT NULL REFERENCES venues(id),
    category        TEXT NOT NULL,
    source          TEXT NOT NULL,
    signal_date     DATE NOT NULL,
    signal_hour     INTEGER,
    strength        TEXT NOT NULL,
    value           NUMERIC(5,3) NOT NULL,
    confidence      NUMERIC(3,2) NOT NULL,
    description     TEXT DEFAULT '',
    raw_data        JSONB DEFAULT '{}',
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ
);

CREATE INDEX idx_signals_venue_date ON external_signals(venue_id, signal_date);
CREATE INDEX idx_signals_category ON external_signals(category, signal_date);
CREATE INDEX idx_signals_expiry ON external_signals(expires_at) WHERE expires_at IS NOT NULL;

-- Reservation snapshots (persisted booking data for training)
CREATE TABLE reservation_snapshots (
    id                      SERIAL PRIMARY KEY,
    venue_id                TEXT NOT NULL REFERENCES venues(id),
    snapshot_date           DATE NOT NULL,
    snapshot_hour           INTEGER,
    total_bookings          INTEGER NOT NULL DEFAULT 0,
    total_covers            INTEGER NOT NULL DEFAULT 0,
    avg_party_size          NUMERIC(4,1) DEFAULT 0,
    large_parties           INTEGER DEFAULT 0,
    no_show_adjusted_covers NUMERIC(8,1) DEFAULT 0,
    source                  TEXT DEFAULT 'manual',
    captured_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_reservations_venue_date ON reservation_snapshots(venue_id, snapshot_date);

-- Delivery order snapshots (persisted for training)
CREATE TABLE delivery_snapshots (
    id              SERIAL PRIMARY KEY,
    venue_id        TEXT NOT NULL REFERENCES venues(id),
    snapshot_date   DATE NOT NULL,
    snapshot_hour   INTEGER NOT NULL,
    order_count     INTEGER NOT NULL DEFAULT 0,
    avg_order_value NUMERIC(8,2) DEFAULT 0,
    platform        TEXT NOT NULL,
    prep_time_mins  INTEGER DEFAULT 0,
    captured_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_delivery_venue_date ON delivery_snapshots(venue_id, snapshot_date);

-- Xero revenue snapshots (inbound from Xero bank transactions)
CREATE TABLE xero_revenue_snapshots (
    id              SERIAL PRIMARY KEY,
    venue_id        TEXT NOT NULL REFERENCES venues(id),
    snapshot_date   DATE NOT NULL,
    total_revenue   NUMERIC(10,2) NOT NULL,
    food_revenue    NUMERIC(10,2) NOT NULL DEFAULT 0,
    beverage_revenue NUMERIC(10,2) NOT NULL DEFAULT 0,
    gaming_revenue  NUMERIC(10,2) NOT NULL DEFAULT 0,
    function_revenue NUMERIC(10,2) NOT NULL DEFAULT 0,
    gst_collected   NUMERIC(10,2) NOT NULL DEFAULT 0,
    source          TEXT DEFAULT 'xero',
    captured_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_xero_revenue_venue_date ON xero_revenue_snapshots(venue_id, snapshot_date);
CREATE UNIQUE INDEX idx_xero_revenue_unique ON xero_revenue_snapshots(venue_id, snapshot_date);

-- Xero labour cost journals (outbound to Xero)
CREATE TABLE xero_labour_journals (
    id              SERIAL PRIMARY KEY,
    venue_id        TEXT NOT NULL REFERENCES venues(id),
    journal_date    DATE NOT NULL,
    xero_journal_id TEXT,
    total_wages_expense NUMERIC(10,2) NOT NULL,
    total_super_payable NUMERIC(10,2) NOT NULL,
    breakdown       JSONB NOT NULL DEFAULT '{}',
    award_levels    JSONB NOT NULL DEFAULT '{}',
    status          TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','posted','cancelled')),
    description     TEXT DEFAULT '',
    pushed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    posted_at       TIMESTAMPTZ
);

CREATE INDEX idx_xero_journals_venue_date ON xero_labour_journals(venue_id, journal_date);
