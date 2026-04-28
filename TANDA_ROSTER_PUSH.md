# Tanda Roster Push Service

## Overview

The `TandaRosterPush` service pushes RosterIQ-optimised rosters back to Tanda via their API. This enables a complete bidirectional sync workflow:
- Pull current state from Tanda → optimize in RosterIQ → push optimized roster back to Tanda

## Architecture

### Location
- **Service**: `services/tanda_roster_push.py`
- **API Endpoints**: Added to `api.py` at `POST /tanda/push-roster` and `POST /tanda/diff-roster`

### Key Classes

#### `TandaRosterPush`
Main service class. Reuses an existing `TandaAdapter` instance for auth and HTTP handling.

**Constructor**:
```python
pusher = TandaRosterPush(tanda_adapter_instance)
```

**Core Methods**:
- `async push_roster(roster, venue_id, dry_run=True) -> PushResult` — Push all shifts in a roster
- `async diff_roster(roster, venue_id) -> RosterDiff` — Compare against current Tanda state
- `async push_shift(shift, tanda_roster_id) -> dict` — Push a single shift
- `async delete_shift(tanda_shift_id) -> bool` — Delete a shift from Tanda

#### `PushResult`
Result of pushing a roster to Tanda.

**Fields**:
- `success_count: int` — Number of shifts successfully pushed
- `failed_count: int` — Number of failed pushes
- `errors: List[str]` — Error messages from failed pushes
- `dry_run: bool` — Whether this was a dry-run
- `pushed_shift_ids: List[str]` — IDs of shifts that were pushed
- `timestamp: datetime` — When the push occurred

**Methods**:
- `to_dict() -> dict` — Convert to JSON-serializable dict

#### `RosterDiff`
Comparison between RosterIQ and Tanda rosters.

**Fields**:
- `new_shifts: List[Shift]` — Shifts in RosterIQ but not in Tanda
- `removed_shifts: List[Dict]` — Shifts in Tanda but not in RosterIQ
- `changed_shifts: List[Dict]` — Shifts that exist in both but differ
- `unchanged_count: int` — Number of unchanged shifts

**Methods**:
- `to_dict() -> dict` — Convert to JSON-serializable dict

#### `ShiftMapping`
Maps a RosterIQ shift ID to a Tanda schedule ID (for reference).

#### `PushRateLimiter`
Internal rate limiter (10 requests/second max to Tanda API).

## Usage

### Programmatic (Python)

```python
from rosteriq.tanda_adapter import TandaAdapter
from rosteriq.services.tanda_roster_push import TandaRosterPush
from rosteriq.models import Roster

# Initialize Tanda adapter with credentials
credentials = TandaCredentials(...)
async with TandaAdapter(credentials, state=State.vic) as tanda:
    # Create pusher
    pusher = TandaRosterPush(tanda)
    
    # (Optional) Set custom employee ID mapping
    # pusher.set_employee_id_mapping({"rosteriq_emp_1": "tanda_user_123"})
    
    # Compare rosters to see what would change
    diff = await pusher.diff_roster(roster, venue_id="tanda-venue-id")
    print(f"Would add {len(diff.new_shifts)} shifts")
    print(f"Would remove {len(diff.removed_shifts)} shifts")
    
    # Push with dry-run (default—validates without pushing)
    result = await pusher.push_roster(roster, venue_id, dry_run=True)
    if result.failed_count == 0:
        print(f"Validated {result.success_count} shifts")
    
    # Push for real (dry_run=False)
    result = await pusher.push_roster(roster, venue_id, dry_run=False)
    print(f"Pushed {result.success_count} shifts, {result.failed_count} failed")
```

### REST API

#### 1. Diff a Roster
```bash
POST /tanda/diff-roster?roster_id=abc123&venue_id=tanda-venue-123
```

Response:
```json
{
  "new_shifts_count": 5,
  "removed_shifts_count": 2,
  "changed_shifts_count": 1,
  "unchanged_count": 12,
  "summary": {
    "new": [
      {
        "shift_id": "rosteriq-shift-1",
        "employee_id": "emp-1",
        "date": "2026-04-15",
        "start": "09:00:00",
        "end": "17:00:00"
      }
    ],
    "removed": [
      {
        "tanda_shift_id": "tanda-shift-999",
        "user_id": "user-123",
        "date": "2026-04-15"
      }
    ]
  }
}
```

#### 2. Push a Roster (Dry-Run)
```bash
POST /tanda/push-roster?roster_id=abc123&venue_id=tanda-venue-123&dry_run=true
```

Response:
```json
{
  "success_count": 20,
  "failed_count": 0,
  "errors": [],
  "dry_run": true,
  "pushed_shift_ids": ["shift-1", "shift-2", ...],
  "timestamp": "2026-04-24T10:30:00.123456"
}
```

#### 3. Push a Roster (For Real)
```bash
POST /tanda/push-roster?roster_id=abc123&venue_id=tanda-venue-123&dry_run=false
```

Same response format as dry-run, but with `"dry_run": false`.

## Safety Features

### Default Dry-Run
All `push_roster()` calls default to `dry_run=True`, so you must explicitly set `dry_run=False` to actually push:
```python
# Safe—validates only
result = await pusher.push_roster(roster, venue_id)

# Actually pushes
result = await pusher.push_roster(roster, venue_id, dry_run=False)
```

### Validation
Before pushing, all shifts are validated to ensure:
- Employee IDs can be mapped to Tanda user IDs
- Shift data is well-formed and complete

### Rate Limiting
The service enforces a max of 10 requests/second to Tanda API, with automatic backoff.

### Logging
Every API call is logged via the standard `logging` module. Enable debug logging to see detailed output:
```python
import logging
logging.getLogger('rosteriq.services.tanda_roster_push').setLevel(logging.DEBUG)
```

## Data Mapping

### Shift Mapping (RosterIQ → Tanda)

RosterIQ `Shift` objects are mapped to Tanda schedule format:

```python
{
    "user_id": 123,                    # Tanda user ID (from employee mapping)
    "start": "2026-04-15T09:00:00+10:00",  # ISO datetime
    "finish": "2026-04-15T17:00:00+10:00", # ISO datetime
    "department_id": 1,                # Default department (customize as needed)
    "roster_id": 789,                  # Tanda roster ID (from venue)
    "role": "barista",                 # From shift.role
    "break_minutes": 30                # From shift.break_minutes
}
```

**Timezone Handling**:
- Currently assumes Australia/Melbourne timezone
- To customize, modify the timezone in `_build_tanda_shift_payload()`

### Employee ID Mapping

By default, RosterIQ employee IDs are used directly as Tanda user IDs. For custom mapping:

```python
pusher.set_employee_id_mapping({
    "rosteriq_emp_1": "tanda_user_123",
    "rosteriq_emp_2": "tanda_user_456",
})
```

## Error Handling

The service provides detailed error information:

```python
result = await pusher.push_roster(roster, venue_id)
if result.failed_count > 0:
    for error in result.errors:
        print(f"Error: {error}")
```

Common error scenarios:
- **Missing employee mapping**: "No Tanda user ID mapping for employee {id}"
- **API errors**: "Tanda API error on POST /schedules: {detail}"
- **Rate limit**: Automatic retry with exponential backoff

## Compliance & Auditing

- All push operations are logged with timestamps and outcome
- Dry-run mode allows review before committing changes
- `pushed_shift_ids` in result enables audit trail of what was pushed
- Errors are captured and reported for manual review

## Testing

The module includes comprehensive error handling suitable for production:

1. **Validate with diff first**:
   ```python
   diff = await pusher.diff_roster(roster, venue_id)
   if len(diff.new_shifts) > 10:
       print("Large change detected, review before pushing")
   ```

2. **Always start with dry-run**:
   ```python
   dry_result = await pusher.push_roster(roster, venue_id, dry_run=True)
   if dry_result.failed_count == 0:
       # Safe to push for real
       real_result = await pusher.push_roster(roster, venue_id, dry_run=False)
   ```

## Future Enhancements

Potential improvements for future versions:
- Batch push operations for better performance
- Custom field mapping (e.g., notes, tags)
- Conflict resolution strategy (merge, override, etc.)
- Webhook notifications on push completion
- Cost impact preview before pushing
- Rollback capability for failed pushes
