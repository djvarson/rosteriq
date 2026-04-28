# RosterIQ Database Migrations

Complete schema definition and migration management for RosterIQ PostgreSQL database.

## Files

- **001_initial_schema.sql** — Complete initial schema with all tables, indexes, and constraints
- **002_seed_data.sql** — Optional seed data for development/demo (commented out by default)
- **run_migrations.py** — Python migration runner with idempotent application
- **README.md** — This file

## Quick Start

### 1. Set DATABASE_URL

```bash
export DATABASE_URL="postgresql://user:password@localhost/rosteriq"
```

### 2. Run Migrations

```bash
python migrations/run_migrations.py
```

The runner will:
- Create the `migrations_history` table if needed
- Track which migrations have been applied
- Run only pending migrations (idempotent)
- Log all actions with timestamps
- Rollback on error

### 3. Verify Status

```bash
python migrations/run_migrations.py --status
```

## Schema Overview

### Core Tables

**venues** — Venue configuration and metadata
- Primary key: `id` (TEXT)
- Unique constraint: `tanda_org_id`
- Indexes: `name`, `state`, `tanda_org_id`

**employees** — Staff records with skills and availability
- Primary key: `id` (TEXT)
- Foreign key: `venue_id` → venues
- Indexes: `venue_id`, `tanda_id`, `name`, `active`, `skills` (GIN)

**rosters** — Weekly rosters
- Primary key: `id` (TEXT)
- Foreign key: `venue_id` → venues
- Indexes: `venue_id`, `week_start`, combined on (`venue_id`, `week_start`)

**shifts** — Individual shift records
- Primary key: `id` (TEXT)
- Foreign keys: `roster_id`, `employee_id`
- Indexes: `roster_id`, `employee_id`, `shift_date`, `status`

**forecasts** — Demand predictions
- Primary key: `id` (TEXT)
- Foreign key: `venue_id` → venues
- Unique constraint: (`venue_id`, `forecast_date`, `hour`, `model_version`)
- Indexes: `venue_id`, `forecast_date`, combined

### Authentication Tables

**users** — User accounts
- Primary key: `id` (TEXT)
- Unique constraint: `email`
- Indexes: `email`, `is_active`

**refresh_tokens** — OAuth refresh tokens
- Primary key: `token_hash` (TEXT)
- Foreign key: `user_id` → users
- Indexes: `user_id`, `expires_at`, `is_revoked`

**login_attempts** — Login audit trail
- Primary key: `id` (BIGSERIAL)
- Indexes: `email`, `ip_address`, `attempted_at`
- Filtered index: (`ip_address`, `attempted_at`) WHERE `success = false`

### Integration Tables

**webhook_events** — Webhook idempotency
- Primary key: `webhook_id` (TEXT)
- Indexes: `event_type`, `processed_at`

**subscriptions** — Stripe billing
- Primary key: `venue_id` (TEXT, FK)
- Unique constraint: `stripe_subscription_id`
- Indexes: `stripe_customer_id`, `status`, `tier`

**billing_events** — Billing audit trail
- Primary key: `event_id` (TEXT)
- Foreign key: `venue_id` → venues
- Unique constraint: `stripe_event_id`
- Indexes: `venue_id`, `event_type`, `stripe_event_id`, `processed`

**onboarding_states** — Onboarding progress tracking
- Primary key: `venue_id` (TEXT, FK)

**plugin_installs** — Tanda plugin installation tracking
- Primary key: `organisation_id` (TEXT)
- Foreign key: `venue_id` → venues (nullable)
- Indexes: `venue_id`, `status`

**xero_credentials** — Xero OAuth credentials
- Primary key: `venue_id` (TEXT, FK)

### System Tables

**migrations_history** — Migration tracking
- Primary key: `id` (SERIAL)
- Unique constraint: `name`
- Indexes: `applied_at`

## Data Types

- **TEXT** — Variable-length strings (venue IDs, emails, names)
- **JSONB** — PostgreSQL native JSON for flexible data (skills, availability, tokens)
- **NUMERIC(p,s)** — Decimal for money (hourly_rate, costs)
- **TIMESTAMP WITH TIME ZONE** — All timestamps are UTC with timezone info
- **BOOLEAN** — Flags (is_active, is_revoked, processed)
- **DATE** — Calendar dates (shift dates, billing periods)
- **TIME** — Clock times (shift start/end times)
- **TEXT[]** — String arrays (signals_used)

## Indexes

All indexes are created automatically by the migration script:

- **B-tree indexes** — Equality and range queries (names, emails, timestamps)
- **GIN indexes** — JSONB queries (skills, availability, tokens)
- **Filtered indexes** — Optimized for active/pending records (active = true, is_revoked = false, success = false)
- **Composite indexes** — Multi-column filters (venue_id + week_start, ip_address + timestamp)

## Constraints

- **Foreign keys** with ON DELETE CASCADE ensure referential integrity
- **CHECK constraints** validate numeric ranges (hour 0-23, confidence 0-1)
- **UNIQUE constraints** prevent duplicates (email, stripe IDs, webhook IDs)
- **NOT NULL** enforces required fields

## Using with Python

### With the migration runner:

```bash
python migrations/run_migrations.py
```

### Programmatically in Python:

```python
from rosteriq.database import PostgresStore, get_db

# Migrations run automatically on first connection
# Get the store instance (will use PostgreSQL if DATABASE_URL is set)
db = get_db()

# Use the database normally
venues = db.list_venues()
```

## Using with psql Directly

If you prefer to run migrations manually:

```bash
psql $DATABASE_URL -f migrations/001_initial_schema.sql
psql $DATABASE_URL -f migrations/002_seed_data.sql
```

## Enabling Seed Data

The seed data file contains commented-out demo data. To enable it:

1. Edit `migrations/002_seed_data.sql`
2. Uncomment the INSERT statements
3. Run the migration: `python migrations/run_migrations.py`

Seed data includes:
- One demo venue ("The Ace Hotel")
- Three demo employees with different roles
- One demo user account

## Resetting the Database

⚠️ **This is destructive — use only in development!**

```bash
python migrations/run_migrations.py --reset
# Then re-run migrations:
python migrations/run_migrations.py
```

This drops all tables and the public schema, then recreates it.

## Troubleshooting

### "psycopg2 not installed"

Install the driver:
```bash
pip install psycopg2-binary
```

### "DATABASE_URL not set"

Set it before running:
```bash
export DATABASE_URL="postgresql://user:password@host/dbname"
python migrations/run_migrations.py
```

### "Connection refused"

Ensure PostgreSQL is running and the credentials in DATABASE_URL are correct:
```bash
psql $DATABASE_URL -c "SELECT 1"
```

### "Relation already exists"

Migrations are idempotent — re-running should skip already-applied migrations. If you see "Relation already exists" errors, check that the `migrations_history` table was created correctly:

```bash
psql $DATABASE_URL -c "SELECT * FROM migrations_history"
```

### Manual Cleanup

If migrations_history becomes corrupted:

```bash
psql $DATABASE_URL -c "DELETE FROM migrations_history WHERE name = '001_initial_schema';"
python migrations/run_migrations.py
```

## Migration Philosophy

These migrations follow these principles:

1. **Idempotent** — Can be run multiple times safely
2. **Atomic** — Each migration either fully applies or fully rolls back
3. **Tracked** — Every migration recorded in migrations_history
4. **Immutable** — Never modify existing migrations; create new ones
5. **Indexed** — All performance-critical fields have indexes
6. **Documented** — Comments in schema explain intent

## Future Migrations

To add a new migration:

1. Create `migrations/003_your_change.sql` with descriptive name
2. Run `python migrations/run_migrations.py`

The runner will automatically detect and apply it.

Example:

```sql
-- migrations/003_add_notes_to_employees.sql
ALTER TABLE employees ADD COLUMN notes TEXT;
```

Then run:
```bash
python migrations/run_migrations.py
```

## Performance Notes

- Composite indexes on (`venue_id`, `week_start`) speed up roster lookups
- Filtered indexes on status/active fields reduce index bloat
- JSONB GIN indexes enable efficient skill/availability queries
- Login attempts filtered index optimizes rate limiting queries
- All foreign keys use ON DELETE CASCADE for clean data cleanup
