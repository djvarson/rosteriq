-- RosterIQ v3 New Features Schema
-- Created: 2026-04-27
-- Adds tables for shift bidding, approval workflows, backups, notifications,
-- conflicts, and web push subscriptions

-- ============================================================================
-- SHIFT BIDS - Employee bids on open shifts
-- ============================================================================

CREATE TABLE IF NOT EXISTS shift_bids (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    shift_id TEXT NOT NULL,
    employee_id TEXT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
    venue_id TEXT NOT NULL REFERENCES venues(id) ON DELETE CASCADE,
    bid_amount DECIMAL(10, 2) NOT NULL,
    priority INTEGER DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',  -- pending, accepted, rejected, withdrawn
    reason TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_shift_bids_shift_status
    ON shift_bids(shift_id, status);
CREATE INDEX IF NOT EXISTS idx_shift_bids_employee_status
    ON shift_bids(employee_id, status);
CREATE INDEX IF NOT EXISTS idx_shift_bids_venue_created
    ON shift_bids(venue_id, created_at DESC);

-- ============================================================================
-- APPROVAL WORKFLOWS - Roster approval workflow tracking
-- ============================================================================

CREATE TABLE IF NOT EXISTS approval_workflows (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    roster_id TEXT NOT NULL REFERENCES rosters(id) ON DELETE CASCADE,
    venue_id TEXT NOT NULL REFERENCES venues(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',  -- pending, approved, rejected, cancelled
    initiated_by TEXT NOT NULL,  -- user_id
    current_step INTEGER DEFAULT 1,
    total_steps INTEGER DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_approval_workflows_venue_status
    ON approval_workflows(venue_id, status);
CREATE INDEX IF NOT EXISTS idx_approval_workflows_roster_id
    ON approval_workflows(roster_id);

-- ============================================================================
-- APPROVAL STEPS - Individual approval steps within a workflow
-- ============================================================================

CREATE TABLE IF NOT EXISTS approval_steps (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID NOT NULL REFERENCES approval_workflows(id) ON DELETE CASCADE,
    step_number INTEGER NOT NULL,
    approver_id TEXT,  -- user_id of the approver
    status VARCHAR(20) NOT NULL DEFAULT 'pending',  -- pending, approved, rejected, skipped
    comment TEXT,
    decided_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_approval_steps_workflow_number
    ON approval_steps(workflow_id, step_number);
CREATE INDEX IF NOT EXISTS idx_approval_steps_approver
    ON approval_steps(approver_id);

-- ============================================================================
-- BACKUP METADATA - Backup records and tracking
-- ============================================================================

CREATE TABLE IF NOT EXISTS backup_metadata (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    backup_type VARCHAR(20) NOT NULL,  -- full, incremental
    file_path TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    checksum VARCHAR(64),
    status VARCHAR(20) NOT NULL DEFAULT 'created',  -- created, uploaded, deleted
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_backup_metadata_created
    ON backup_metadata(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_backup_metadata_status
    ON backup_metadata(status);

-- ============================================================================
-- NOTIFICATION LOG - Audit trail for all dispatched notifications
-- ============================================================================

CREATE TABLE IF NOT EXISTS notification_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(50) NOT NULL,
    venue_id TEXT REFERENCES venues(id) ON DELETE SET NULL,
    employee_id TEXT REFERENCES employees(id) ON DELETE SET NULL,
    channel VARCHAR(20) NOT NULL,  -- email, sms, push, websocket
    status VARCHAR(20) NOT NULL DEFAULT 'sent',  -- sent, failed, queued, skipped
    message_preview TEXT,
    error TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_notification_log_venue_created
    ON notification_log(venue_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notification_log_employee_created
    ON notification_log(employee_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notification_log_event_status
    ON notification_log(event_type, status);

-- ============================================================================
-- NOTIFICATION PREFERENCES - Per-employee notification settings
-- ============================================================================

-- Blob model: keyed by user_id with the whole preference payload as JSON.
-- This matches what the application code and MemoryStore use (an earlier
-- normalised schema here was never referenced by any code path).
CREATE TABLE IF NOT EXISTS notification_preferences (
    user_id TEXT PRIMARY KEY,
    preferences JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- ROSTER CONFLICTS - Detected conflicts cache
-- ============================================================================

CREATE TABLE IF NOT EXISTS roster_conflicts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    roster_id TEXT NOT NULL REFERENCES rosters(id) ON DELETE CASCADE,
    conflict_type VARCHAR(30) NOT NULL,
    severity VARCHAR(10) NOT NULL,  -- critical, warning, info
    message TEXT NOT NULL,
    employee_ids TEXT[],
    shift_ids TEXT[],
    suggestion TEXT,
    resolved BOOLEAN DEFAULT FALSE,
    resolved_by TEXT,  -- user_id
    resolved_at TIMESTAMP WITH TIME ZONE,
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_roster_conflicts_roster_resolved
    ON roster_conflicts(roster_id, resolved);
CREATE INDEX IF NOT EXISTS idx_roster_conflicts_type_severity
    ON roster_conflicts(conflict_type, severity);
CREATE INDEX IF NOT EXISTS idx_roster_conflicts_detected
    ON roster_conflicts(detected_at DESC);

-- ============================================================================
-- PUSH SUBSCRIPTIONS - Web push subscription data
-- ============================================================================

-- Blob model: keyed by user_id, the full web-push subscription stored as JSON.
-- Matches the application code / MemoryStore (the earlier normalised schema
-- with endpoint/p256dh/auth columns was never referenced by any code path).
CREATE TABLE IF NOT EXISTS push_subscriptions (
    user_id TEXT PRIMARY KEY,
    venue_id TEXT,
    subscription_data JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_push_subscriptions_venue
    ON push_subscriptions(venue_id);
