-- RosterIQ Initial Schema
-- Created: 2026-04-25
-- Contains all core tables for venues, employees, rosters, forecasts, and authentication

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- VENUES
-- ============================================================================

CREATE TABLE venues (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    tanda_org_id TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'Australia/Melbourne',
    min_staff JSONB DEFAULT '{}',  -- role -> min count mapping
    max_labour_pct NUMERIC(5, 2) NOT NULL,
    pos_system TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_venues_name ON venues(name);
CREATE INDEX idx_venues_state ON venues(state);
CREATE INDEX idx_venues_tanda_org_id ON venues(tanda_org_id);

-- ============================================================================
-- EMPLOYEES
-- ============================================================================

CREATE TABLE employees (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL REFERENCES venues(id) ON DELETE CASCADE,
    tanda_id TEXT,
    name TEXT NOT NULL,
    employment_type TEXT NOT NULL,  -- full_time, part_time, casual
    award_level TEXT NOT NULL,  -- level_1 through level_6
    hourly_base_rate NUMERIC(10, 2) NOT NULL,
    skills JSONB DEFAULT '[]',  -- array of skill strings
    availability JSONB DEFAULT '{}',  -- day -> list of {start, end} ranges
    max_hours_per_week NUMERIC(5, 2) DEFAULT 38.0,
    consecutive_days INTEGER DEFAULT 6,
    phone TEXT,
    email TEXT,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_employees_venue_id ON employees(venue_id);
CREATE INDEX idx_employees_tanda_id ON employees(tanda_id);
CREATE INDEX idx_employees_name ON employees(name);
CREATE INDEX idx_employees_active ON employees(active) WHERE active = true;
CREATE INDEX idx_employees_skills ON employees USING GIN (skills);

-- ============================================================================
-- ROSTERS
-- ============================================================================

CREATE TABLE rosters (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL REFERENCES venues(id) ON DELETE CASCADE,
    week_start DATE NOT NULL,
    week_end DATE NOT NULL,
    total_cost NUMERIC(12, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_rosters_venue_id ON rosters(venue_id);
CREATE INDEX idx_rosters_week_start ON rosters(week_start);
CREATE INDEX idx_rosters_venue_week ON rosters(venue_id, week_start);

-- ============================================================================
-- SHIFTS
-- ============================================================================

CREATE TABLE shifts (
    id TEXT PRIMARY KEY,
    roster_id TEXT NOT NULL REFERENCES rosters(id) ON DELETE CASCADE,
    employee_id TEXT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
    shift_date DATE NOT NULL,
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
    break_minutes INTEGER DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'scheduled',  -- scheduled, confirmed, in_progress, completed, cancelled, no_show
    role TEXT DEFAULT 'general',
    cost NUMERIC(10, 2),
    penalty_multiplier NUMERIC(5, 2) DEFAULT 1.0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_shifts_roster_id ON shifts(roster_id);
CREATE INDEX idx_shifts_employee_id ON shifts(employee_id);
CREATE INDEX idx_shifts_date ON shifts(shift_date);
CREATE INDEX idx_shifts_status ON shifts(status);

-- ============================================================================
-- DEMAND FORECASTS
-- ============================================================================

CREATE TABLE forecasts (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL REFERENCES venues(id) ON DELETE CASCADE,
    forecast_date DATE NOT NULL,
    hour INTEGER NOT NULL CHECK (hour >= 0 AND hour <= 23),
    predicted_covers NUMERIC(10, 2) NOT NULL,
    confidence NUMERIC(3, 2) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    signals_used TEXT[] DEFAULT '{}',  -- array of signal type strings
    model_version TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(venue_id, forecast_date, hour, model_version)
);

CREATE INDEX idx_forecasts_venue_id ON forecasts(venue_id);
CREATE INDEX idx_forecasts_date ON forecasts(forecast_date);
CREATE INDEX idx_forecasts_venue_date ON forecasts(venue_id, forecast_date);

-- ============================================================================
-- USERS & AUTHENTICATION
-- ============================================================================

CREATE TABLE users (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    name TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'staff',  -- owner, manager, staff
    api_key_hash TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_is_active ON users(is_active) WHERE is_active = true;

-- ============================================================================
-- REFRESH TOKENS
-- ============================================================================

CREATE TABLE refresh_tokens (
    token_hash TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    is_revoked BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_refresh_tokens_user_id ON refresh_tokens(user_id);
CREATE INDEX idx_refresh_tokens_expires_at ON refresh_tokens(expires_at);
CREATE INDEX idx_refresh_tokens_revoked ON refresh_tokens(is_revoked) WHERE is_revoked = false;

-- ============================================================================
-- LOGIN ATTEMPTS
-- ============================================================================

CREATE TABLE login_attempts (
    id BIGSERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    ip_address TEXT NOT NULL,
    success BOOLEAN NOT NULL,
    attempted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_login_attempts_email ON login_attempts(email);
CREATE INDEX idx_login_attempts_ip ON login_attempts(ip_address);
CREATE INDEX idx_login_attempts_timestamp ON login_attempts(attempted_at);
CREATE INDEX idx_login_attempts_failed ON login_attempts(ip_address, attempted_at) WHERE success = false;

-- ============================================================================
-- WEBHOOK EVENTS (Idempotency)
-- ============================================================================

CREATE TABLE webhook_events (
    webhook_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_webhook_events_type ON webhook_events(event_type);
CREATE INDEX idx_webhook_events_timestamp ON webhook_events(processed_at);

-- ============================================================================
-- SUBSCRIPTIONS
-- ============================================================================

CREATE TABLE subscriptions (
    venue_id TEXT PRIMARY KEY REFERENCES venues(id) ON DELETE CASCADE,
    stripe_customer_id TEXT,
    stripe_subscription_id TEXT UNIQUE,
    tier TEXT NOT NULL DEFAULT 'starter',  -- starter, professional, enterprise
    status TEXT NOT NULL DEFAULT 'inactive',  -- inactive, active, suspended, cancelled
    current_period_start TIMESTAMP WITH TIME ZONE,
    current_period_end TIMESTAMP WITH TIME ZONE,
    payment_method TEXT,
    last_payment_date TIMESTAMP WITH TIME ZONE,
    next_billing_date TIMESTAMP WITH TIME ZONE,
    cancel_at_period_end BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_subscriptions_stripe_customer_id ON subscriptions(stripe_customer_id);
CREATE INDEX idx_subscriptions_status ON subscriptions(status);
CREATE INDEX idx_subscriptions_tier ON subscriptions(tier);

-- ============================================================================
-- BILLING EVENTS
-- ============================================================================

CREATE TABLE billing_events (
    event_id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL REFERENCES venues(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,  -- subscription_created, payment_succeeded, payment_failed, etc.
    stripe_event_id TEXT UNIQUE,
    payload JSONB NOT NULL,
    processed BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_billing_events_venue_id ON billing_events(venue_id);
CREATE INDEX idx_billing_events_event_type ON billing_events(event_type);
CREATE INDEX idx_billing_events_stripe_event_id ON billing_events(stripe_event_id);
CREATE INDEX idx_billing_events_processed ON billing_events(processed) WHERE processed = false;

-- ============================================================================
-- ONBOARDING STATES
-- ============================================================================

CREATE TABLE onboarding_states (
    venue_id TEXT PRIMARY KEY REFERENCES venues(id) ON DELETE CASCADE,
    state_data JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- PLUGIN INSTALLS
-- ============================================================================

CREATE TABLE plugin_installs (
    organisation_id TEXT PRIMARY KEY,
    venue_id TEXT REFERENCES venues(id) ON DELETE SET NULL,
    status TEXT NOT NULL DEFAULT 'active',  -- active, inactive, error
    tokens JSONB DEFAULT '{}',  -- {access_token, refresh_token, etc.}
    installed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_plugin_installs_venue_id ON plugin_installs(venue_id);
CREATE INDEX idx_plugin_installs_status ON plugin_installs(status);

-- ============================================================================
-- XERO CREDENTIALS
-- ============================================================================

CREATE TABLE xero_credentials (
    venue_id TEXT PRIMARY KEY REFERENCES venues(id) ON DELETE CASCADE,
    client_id TEXT,
    client_secret TEXT,
    tenant_id TEXT,  -- Xero Organization ID (UUID)
    access_token TEXT,
    refresh_token TEXT,
    token_expires TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- FEED CONFIGURATIONS
-- ============================================================================

CREATE TABLE feed_configs (
    venue_id TEXT NOT NULL REFERENCES venues(id) ON DELETE CASCADE,
    feed_name TEXT NOT NULL,
    enabled BOOLEAN DEFAULT true,
    api_key TEXT,
    poll_interval_minutes INTEGER DEFAULT 30,
    last_updated_at TIMESTAMP WITH TIME ZONE,
    last_tested_at TIMESTAMP WITH TIME ZONE,
    last_test_status TEXT,  -- success, failed, error message
    custom_params JSONB DEFAULT '{}',  -- flexible config storage
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (venue_id, feed_name)
);

CREATE INDEX idx_feed_configs_venue_id ON feed_configs(venue_id);
CREATE INDEX idx_feed_configs_enabled ON feed_configs(enabled) WHERE enabled = true;
CREATE INDEX idx_feed_configs_updated_at ON feed_configs(updated_at);

-- ============================================================================
-- MIGRATION HISTORY
-- ============================================================================

CREATE TABLE migrations_history (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    status TEXT NOT NULL DEFAULT 'success'  -- success, failed
);

CREATE INDEX idx_migrations_history_applied_at ON migrations_history(applied_at);
