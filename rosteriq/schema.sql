-- RosterIQ Database Schema
-- SQLite for development, PostgreSQL for production
-- All tables use TEXT PRIMARY KEY for UUIDs, created_at timestamps in ISO 8601 format

-- Venues table: Physical venues (bars, restaurants, pubs)
CREATE TABLE IF NOT EXISTS venues (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    address TEXT,
    state TEXT CHECK(state IN ('NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT')),
    timezone TEXT DEFAULT 'Australia/Sydney',
    tanda_org_id TEXT UNIQUE,
    swiftpos_site_id TEXT,
    nowbookit_venue_id TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_venues_tanda_org_id ON venues(tanda_org_id);
CREATE INDEX IF NOT EXISTS idx_venues_state ON venues(state);

-- Employees table: Staff members across venues
CREATE TABLE IF NOT EXISTS employees (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    tanda_id TEXT,
    name TEXT NOT NULL,
    email TEXT,
    phone TEXT,
    role TEXT,
    employment_type TEXT NOT NULL CHECK(employment_type IN ('casual', 'part_time', 'full_time')),
    hourly_rate REAL,
    skills TEXT,  -- JSON array of skill strings
    max_hours_week REAL,
    is_active BOOLEAN DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(venue_id) REFERENCES venues(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_employees_venue_id ON employees(venue_id);
CREATE INDEX IF NOT EXISTS idx_employees_tanda_id ON employees(tanda_id);
CREATE INDEX IF NOT EXISTS idx_employees_is_active ON employees(is_active);

-- Availability table: Weekly availability patterns for employees
CREATE TABLE IF NOT EXISTS availability (
    id TEXT PRIMARY KEY,
    employee_id TEXT NOT NULL,
    day_of_week INTEGER NOT NULL CHECK(day_of_week BETWEEN 0 AND 6),  -- 0=Monday, 6=Sunday
    start_time TEXT NOT NULL,  -- HH:MM format
    end_time TEXT NOT NULL,    -- HH:MM format
    preference TEXT NOT NULL CHECK(preference IN ('available', 'preferred', 'unavailable')),
    FOREIGN KEY(employee_id) REFERENCES employees(id) ON DELETE CASCADE,
    UNIQUE(employee_id, day_of_week)
);

CREATE INDEX IF NOT EXISTS idx_availability_employee_id ON availability(employee_id);
CREATE INDEX IF NOT EXISTS idx_availability_day_of_week ON availability(day_of_week);

-- Leave records table: Annual leave, sick leave, unpaid time off
CREATE TABLE IF NOT EXISTS leave_records (
    id TEXT PRIMARY KEY,
    employee_id TEXT NOT NULL,
    start_date TEXT NOT NULL,  -- ISO 8601 date
    end_date TEXT NOT NULL,    -- ISO 8601 date
    leave_type TEXT NOT NULL,
    status TEXT NOT NULL,
    tanda_leave_id TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(employee_id) REFERENCES employees(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_leave_records_employee_id ON leave_records(employee_id);
CREATE INDEX IF NOT EXISTS idx_leave_records_start_date ON leave_records(start_date);

-- Rosters table: Weekly roster documents
CREATE TABLE IF NOT EXISTS rosters (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    week_start TEXT NOT NULL,  -- ISO 8601 date
    status TEXT NOT NULL CHECK(status IN ('draft', 'published', 'archived')),
    total_cost REAL,
    coverage_score REAL,  -- 0.0-1.0 how well demand is covered
    fairness_score REAL,  -- 0.0-1.0 fairness across employees
    created_by TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(venue_id) REFERENCES venues(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_rosters_venue_id ON rosters(venue_id);
CREATE INDEX IF NOT EXISTS idx_rosters_week_start ON rosters(week_start);
CREATE INDEX IF NOT EXISTS idx_rosters_status ON rosters(status);

-- Shifts table: Individual shifts in a roster
CREATE TABLE IF NOT EXISTS shifts (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    employee_id TEXT,
    roster_id TEXT,
    date TEXT NOT NULL,  -- ISO 8601 date
    start_time TEXT NOT NULL,  -- HH:MM format
    end_time TEXT NOT NULL,    -- HH:MM format
    role TEXT,
    break_minutes INTEGER DEFAULT 0,
    status TEXT NOT NULL CHECK(status IN ('draft', 'published', 'confirmed', 'completed')),
    source TEXT NOT NULL CHECK(source IN ('ai_generated', 'manual', 'tanda_sync')),
    cost_estimate REAL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(venue_id) REFERENCES venues(id) ON DELETE CASCADE,
    FOREIGN KEY(employee_id) REFERENCES employees(id) ON DELETE SET NULL,
    FOREIGN KEY(roster_id) REFERENCES rosters(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_shifts_venue_id ON shifts(venue_id);
CREATE INDEX IF NOT EXISTS idx_shifts_employee_id ON shifts(employee_id);
CREATE INDEX IF NOT EXISTS idx_shifts_roster_id ON shifts(roster_id);
CREATE INDEX IF NOT EXISTS idx_shifts_date ON shifts(date);
CREATE INDEX IF NOT EXISTS idx_shifts_status ON shifts(status);

-- Demand forecasts table: Predicted demand by hour
CREATE TABLE IF NOT EXISTS demand_forecasts (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    date TEXT NOT NULL,  -- ISO 8601 date
    hour INTEGER NOT NULL CHECK(hour BETWEEN 0 AND 23),
    predicted_demand REAL NOT NULL,  -- number of staff needed
    confidence REAL,  -- 0.0-1.0 confidence in prediction
    model_version TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(venue_id) REFERENCES venues(id) ON DELETE CASCADE,
    UNIQUE(venue_id, date, hour)
);

CREATE INDEX IF NOT EXISTS idx_demand_forecasts_venue_id ON demand_forecasts(venue_id);
CREATE INDEX IF NOT EXISTS idx_demand_forecasts_date ON demand_forecasts(date);

-- Signals table: External data sources (weather, events, bookings, POS, foot traffic)
CREATE TABLE IF NOT EXISTS signals (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    date TEXT NOT NULL,  -- ISO 8601 date
    signal_type TEXT NOT NULL CHECK(signal_type IN ('weather', 'event', 'booking', 'pos_sales', 'foot_traffic', 'delivery')),
    source TEXT NOT NULL,
    data TEXT NOT NULL,  -- JSON object with signal details
    fetched_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(venue_id) REFERENCES venues(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_signals_venue_id ON signals(venue_id);
CREATE INDEX IF NOT EXISTS idx_signals_date ON signals(date);
CREATE INDEX IF NOT EXISTS idx_signals_signal_type ON signals(signal_type);

-- Shift events table: Accountability tracking for roster changes
CREATE TABLE IF NOT EXISTS shift_events (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    shift_date TEXT NOT NULL,  -- ISO 8601 date
    event_type TEXT NOT NULL CHECK(event_type IN ('staff_cut', 'staff_called_in', 'demand_spike', 'demand_drop')),
    details TEXT,  -- JSON object with context
    decided_by TEXT,  -- user or system
    ai_recommendation TEXT,  -- what AI recommended
    action_taken TEXT,  -- what actually happened
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(venue_id) REFERENCES venues(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_shift_events_venue_id ON shift_events(venue_id);
CREATE INDEX IF NOT EXISTS idx_shift_events_shift_date ON shift_events(shift_date);
CREATE INDEX IF NOT EXISTS idx_shift_events_event_type ON shift_events(event_type);

-- Shift summaries table: End-of-day summary data
CREATE TABLE IF NOT EXISTS shift_summaries (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    date TEXT NOT NULL,  -- ISO 8601 date
    actual_revenue REAL,
    expected_revenue REAL,
    staff_count INTEGER,
    notes TEXT,
    created_by TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(venue_id) REFERENCES venues(id) ON DELETE CASCADE,
    UNIQUE(venue_id, date)
);

CREATE INDEX IF NOT EXISTS idx_shift_summaries_venue_id ON shift_summaries(venue_id);
CREATE INDEX IF NOT EXISTS idx_shift_summaries_date ON shift_summaries(date);

-- Venue settings table: Key-value store for venue configuration
CREATE TABLE IF NOT EXISTS venue_settings (
    venue_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT,
    PRIMARY KEY(venue_id, key),
    FOREIGN KEY(venue_id) REFERENCES venues(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_venue_settings_venue_id ON venue_settings(venue_id);
