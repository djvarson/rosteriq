# RosterIQ Onboarding Service Implementation

## Overview

A complete onboarding service has been implemented to manage first-run setup when a new venue connects via Tanda. The service handles a multi-step workflow with error handling, progress tracking, and asynchronous execution.

## Files Created

### 1. `services/onboarding.py` (412 lines)

**Core service for managing the onboarding workflow.**

#### Key Classes

- **OnboardingStep** (Enum)
  - CONNECT_TANDA: Validate token, fetch venue info from Tanda
  - IMPORT_EMPLOYEES: Pull all employees, map to Employee model, save to DB
  - IMPORT_ROSTERS: Pull current + next week rosters, save shifts
  - IMPORT_DEPARTMENTS: Pull department list, store in venue config
  - CONFIGURE_VENUE: Set default award rules, timezone, create VenueConfig
  - COMPLETE: Mark venue as onboarded, log completion

- **OnboardingState** (Dataclass)
  - Tracks venue_id, current_step, started_at, completed_steps, errors
  - Tracks imported counts: employees, rosters, shifts, departments
  - Serializable to/from dict for database storage
  - Includes last_error for debugging

- **OnboardingService**
  - Main service class with step orchestration
  - **Methods:**
    - `start_onboarding(venue_id, tanda_token)` - Begin workflow
    - `get_status(venue_id)` - Get current progress with % completion
    - `run_step(venue_id, step, tanda_credentials)` - Execute specific step
    - `run_all(venue_id, tanda_credentials)` - Execute all steps sequentially
    - `retry_step(venue_id, tanda_credentials)` - Retry failed step
    - `get_summary(venue_id)` - Get full import statistics

#### Step Implementations

Each step is a private async method that handles one phase:
- Error handling with try/except, errors stored in state
- Logging of progress at each stage
- State persisted to database after each step
- One failed step doesn't block others (can retry)

### 2. `routes/onboarding.py` (273 lines)

**FastAPI routes for the onboarding workflow.**

#### Endpoints

- **POST /api/onboarding/start**
  - Start onboarding for a venue
  - Body: venue_id, tanda_client_id, tanda_client_secret, tanda_access_token, tanda_org_id
  - Response: Status message, runs all steps in background
  - Uses BackgroundTasks for non-blocking execution

- **GET /api/onboarding/status/{venue_id}**
  - Get current onboarding progress
  - Response: OnboardingStatusResponse
    - current_step, progress_pct, completed_steps
    - errors list, last_error, imported_counts
    - venue_name, started_at

- **POST /api/onboarding/step/{venue_id}/{step}**
  - Run a specific step manually
  - Path: venue_id, step name (connect_tanda, import_employees, etc.)
  - Body: Optional Tanda credentials for authentication
  - Response: {success: bool, message: str, last_error: optional}

- **POST /api/onboarding/retry/{venue_id}**
  - Retry the current failed step
  - Body: Optional Tanda credentials
  - Response: {success: bool, message: str}

- **GET /api/onboarding/summary/{venue_id}**
  - Get full import summary with statistics
  - Response: OnboardingSummaryResponse
    - venue_name, status (completed/in_progress)
    - imported_counts (employees, rosters, shifts, departments)
    - total_errors, started_at, completed_steps

#### Pydantic Models

- OnboardingStartRequest
- OnboardingStepRequest
- OnboardingStatusResponse
- OnboardingSummaryResponse

All models use Optional fields for flexibility and proper validation.

## Files Modified

### 1. `database.py`

Added onboarding state persistence to BaseStore interface and implementations:

#### BaseStore (Abstract Interface)
```python
def save_onboarding_state(self, state: dict) -> None
def get_onboarding_state(self, venue_id: str) -> Optional[dict]
```

#### MemoryStore
- Added `_onboarding_states` dict in `__init__`
- Implemented state save/get with in-memory dict storage

#### PostgresStore
- Implemented state save/get with PostgreSQL
- Uses JSON serialization for state data
- SQL: INSERT...ON CONFLICT for upsert pattern

### 2. `routes/__init__.py`

Added onboarding router import:
```python
try:
    from .onboarding import router as onboarding_router
except ImportError:
    onboarding_router = None

__all__ = ["auth_router", "webhook_router", "onboarding_router"]
```

### 3. `api.py`

Registered onboarding routes with try/except pattern:
```python
try:
    from rosteriq.routes.onboarding import router as onboarding_router
    app.include_router(onboarding_router)
except ImportError:
    pass
```

## Architecture & Design Patterns

### Async/Await
- All Tanda API calls wrapped in async context manager
- Steps execute sequentially with `await`
- Background tasks for non-blocking execution

### Error Handling
- Each step wrapped in try/except
- Errors stored in OnboardingState.errors list with timestamp
- Failed step doesn't block others - can retry individually
- last_error field tracks most recent failure

### State Management
- OnboardingState dataclass with to_dict/from_dict serialization
- State persisted to DB after each step
- Resilient to interruptions - can resume from current step

### Logging
- Every step logs start, completion, or failure
- Progress tracked with elapsed time
- Import counts logged at completion

### API Design
- RESTful endpoints with clear HTTP semantics
- Comprehensive error responses with HTTPException
- Optional background execution for long-running workflows
- Pydantic validation on all inputs

## Integration with Existing Systems

### TandaAdapter
Uses existing TandaAdapter class for all Tanda API calls:
- `health_check()` - Verify API connectivity
- `get_employees()` - Fetch employee roster
- `get_roster(week_start)` - Fetch shifts for a week
- Async context manager pattern for connection management

### Database Layer
Uses custom BaseStore pattern (NOT SQLAlchemy):
- Works with both MemoryStore (dev) and PostgresStore (prod)
- Follows existing save/get patterns
- Singleton via get_db() function

### Models
Uses existing Pydantic models:
- Employee, Roster, Shift
- VenueConfig, TandaCredentials
- State, EmploymentType, AwardLevel, ShiftStatus

## Default Configuration

When configuring a venue, the service sets:
- **State:** Victoria (vic) - configurable per venue
- **Timezone:** Australia/Melbourne
- **Default Award Rule:** MA000009 (set during CONFIGURE_VENUE step)
- **Max Labour %:** 30.0% (tunable per venue)
- **Min Staff:** Empty dict (configured per role later)

## Usage Examples

### Start Onboarding (Background)
```python
POST /api/onboarding/start
{
  "venue_id": "venue-123",
  "tanda_client_id": "client_id",
  "tanda_client_secret": "secret",
  "tanda_access_token": "access_token",
  "tanda_org_id": "org-456"
}
```

### Check Progress
```python
GET /api/onboarding/status/venue-123
```

Response shows progress_pct, current_step, imported_counts, errors

### Retry Failed Step
```python
POST /api/onboarding/retry/venue-123
{
  "tanda_access_token": "access_token",
  "tanda_org_id": "org-456"
}
```

### Get Summary
```python
GET /api/onboarding/summary/venue-123
```

## Testing

All files compile successfully:
```bash
python -m py_compile services/onboarding.py
python -m py_compile routes/onboarding.py
python -m py_compile database.py
python -m py_compile api.py
```

## Future Enhancements

1. **Webhook Support:** Listen for Tanda roster.published webhooks during import
2. **Progress Notifications:** Send updates via email/SMS/webhook
3. **Parallel Execution:** Run independent steps concurrently (e.g., employees + departments)
4. **Batch Operations:** Optimize large imports with bulk DB inserts
5. **Rollback Capability:** Undo venue config if onboarding fails
6. **Performance Metrics:** Track import speed, API response times
7. **POS Integration:** Auto-detect SwiftPOS/other POS systems
8. **Data Validation:** Validate employee data quality before saving

## Database Schema

For PostgreSQL deployments, an `onboarding_states` table is needed:
```sql
CREATE TABLE onboarding_states (
    venue_id TEXT PRIMARY KEY,
    state_data JSONB NOT NULL,
    updated_at TIMESTAMP DEFAULT now()
);
```

This table stores the complete OnboardingState as JSON for recovery/debugging.
