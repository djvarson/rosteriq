# RosterIQ SMS Notifications System

This document describes the SMS notification system added to RosterIQ, including SMS service, notification preferences, and API routes.

## Overview

The SMS notification system enables RosterIQ to send SMS messages via Twilio alongside the existing email notification system. Features include:

- **Multi-channel notifications**: Email, SMS, and push (framework for future)
- **User preferences**: Per-user control over notification channels and types
- **Quiet hours**: Respect user quiet hours (e.g., 22:00-07:00 AEST) for non-urgent notifications
- **Rate limiting**: Max 1 SMS per phone number per 5 minutes to prevent flooding
- **Graceful degradation**: SMS notifications are skipped if Twilio is not configured
- **Australian phone numbers**: Automatic formatting to +61 E.164 format

## Architecture

### 1. SMS Service (`services/sms.py`)

The `SMSService` class handles all SMS operations:

```python
from rosteriq.services.sms import get_sms_service

sms = get_sms_service()
success = await sms.send_sms(to_number="+61412345678", message="Hello")
```

**Key Methods:**
- `send_sms(to_number, message)` - Core SMS sending
- `send_shift_reminder(employee, shift)` - "Reminder: You have a shift at {venue} from {start} to {end} in 2 hours"
- `send_swap_notification(employee, swap)` - "Your shift swap request was {approved/rejected}"
- `send_roster_published(employee, venue_name, week_start)` - "New roster published for {venue} week of {date}"
- `send_urgent_alert(phone, message)` - For compliance alerts

**Features:**
- Rate limiting: 1 SMS per number per 5 minutes (in-memory tracking)
- Message truncation: Auto-truncates to 160 chars with "..."
- Phone number formatting: Converts any format to +61... E.164 format
- Graceful no-op: Returns False silently if Twilio not configured

**Environment Variables:**
```
TWILIO_ACCOUNT_SID=your_account_sid
TWILIO_AUTH_TOKEN=your_auth_token
TWILIO_FROM_NUMBER=+61412345678
```

### 2. Notification Preferences (`services/notification_preferences.py`)

The `NotificationPreferences` class manages per-user notification settings:

```python
from rosteriq.services.notification_preferences import get_preferences_service

prefs = get_preferences_service()
should_send = prefs.should_notify(user_id, channel="sms", notification_type="shift_reminder")
is_quiet = prefs.is_in_quiet_hours(user_id)
```

**User Preference Structure:**
```python
{
    "channels": {
        "email": True,
        "sms": False,  # Disabled by default
        "push": False
    },
    "notification_types": {
        "shift_reminders": True,
        "roster_published": True,
        "swap_updates": True,
        "compliance_alerts": True
    },
    "quiet_hours": {
        "enabled": True,
        "start": "22:00",  # 10 PM AEST
        "end": "07:00"     # 7 AM AEST
    }
}
```

**Default Behavior:**
- Email: enabled by default
- SMS: disabled by default (users must opt-in)
- All notification types: enabled by default
- Quiet hours: 22:00-07:00 AEST
- Compliance alerts: always sent (bypass quiet hours, cannot disable)

**Key Methods:**
- `get_preferences(user_id)` - Get user's preferences or defaults
- `update_preferences(user_id, prefs)` - Save updated preferences
- `should_notify(user_id, channel, notification_type)` - Check if notification should be sent
- `is_in_quiet_hours(user_id)` - Check if currently in quiet hours

### 3. API Routes (`routes/notification_prefs.py`)

REST endpoints for managing notification preferences:

#### GET `/api/notifications/preferences`
Get current user's notification preferences.

**Response:**
```json
{
    "channels": {
        "email": true,
        "sms": false,
        "push": false
    },
    "notification_types": {
        "shift_reminders": true,
        "roster_published": true,
        "swap_updates": true,
        "compliance_alerts": true
    },
    "quiet_hours": {
        "enabled": true,
        "start": "22:00",
        "end": "07:00"
    }
}
```

#### PUT `/api/notifications/preferences`
Update notification preferences (partial updates supported).

**Request:**
```json
{
    "channels": {
        "sms": true
    },
    "quiet_hours": {
        "start": "23:00",
        "end": "08:00"
    }
}
```

**Response:** Updated preferences (same as GET)

#### POST `/api/notifications/test-sms`
Send test SMS to verify phone number configuration.

**Request:**
```json
{
    "phone": "0412345678"
}
```

**Response:**
```json
{
    "success": true,
    "message": "Test SMS sent successfully. Check your phone."
}
```

### 4. Database Integration (`database.py`)

Added to `BaseStore` interface:
- `save_notification_preferences(user_id: str, prefs: dict) -> None`
- `get_notification_preferences(user_id: str) -> Optional[dict]`

Implemented in `MemoryStore`:
- In-memory dict-based storage for development/testing
- Keyed by user_id

### 5. Notification Service Integration (`services/notifications.py`)

Added new method to integrate SMS with existing email service:

```python
async def send_with_preferences(
    user_id: str,
    email: str,
    notification_type: str,
    email_subject: str,
    html_content: str,
    sms_func: Optional[Any] = None,
) -> Dict[str, bool]:
    """
    Send notification respecting user preferences and quiet hours.
    
    Returns:
        {"email": bool, "sms": bool} indicating success of each channel
    """
```

This method:
- Checks user preferences before sending
- Respects quiet hours (no SMS between 22:00-07:00)
- Handles dual-channel sending (email + SMS)
- Skips compliance alerts from quiet hour restrictions

## Setup Instructions

### 1. Install Twilio

```bash
pip install twilio
```

### 2. Configure Environment Variables

Add to `.env`:
```
TWILIO_ACCOUNT_SID=your_twilio_account_sid
TWILIO_AUTH_TOKEN=your_twilio_auth_token
TWILIO_FROM_NUMBER=+61412345678
```

### 3. Verify Configuration

```bash
# Test SMS sending
curl -X POST http://localhost:8000/api/notifications/test-sms \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"phone": "0412345678"}'
```

## Usage Examples

### 1. Send Shift Reminder SMS

```python
from rosteriq.services.sms import get_sms_service

sms = get_sms_service()
employee = {
    "id": "emp_123",
    "name": "John Doe",
    "phone": "0412345678"
}
shift = {
    "venue_name": "The Paddock",
    "start_time": "17:30",
    "end_time": "22:00"
}

success = await sms.send_shift_reminder(employee, shift)
# SMS: "Reminder: You have a shift at The Paddock from 17:30 to 22:00 in 2 hours."
```

### 2. Check if User Should Get SMS

```python
from rosteriq.services.notification_preferences import get_preferences_service

prefs = get_preferences_service()

# Check if user wants shift reminders via SMS
if prefs.should_notify(user_id="user_123", channel="sms", notification_type="shift_reminder"):
    # Send SMS
    await sms.send_shift_reminder(employee, shift)
```

### 3. Send Notification Respecting Preferences

```python
from rosteriq.services.notifications import get_notification_service

notif = get_notification_service()

# Send with dual email + SMS based on preferences
results = await notif.send_with_preferences(
    user_id="user_123",
    email="john@example.com",
    notification_type="roster_published",
    email_subject="Roster Published",
    html_content="<p>Your roster has been published</p>",
    sms_func=lambda uid: sms.send_roster_published(employee, "The Paddock", "2026-04-28")
)

print(results)  # {"email": True, "sms": True}
```

### 4. Update User Preferences

```python
from rosteriq.services.notification_preferences import get_preferences_service

prefs = get_preferences_service()

# Enable SMS notifications and set quiet hours
prefs.update_preferences(
    user_id="user_123",
    prefs={
        "channels": {"sms": True},
        "quiet_hours": {
            "enabled": True,
            "start": "23:00",
            "end": "08:00"
        }
    }
)
```

## Testing

### 1. Unit Tests for SMS Service

```python
import pytest
from rosteriq.services.sms import SMSService

@pytest.mark.asyncio
async def test_sms_rate_limiting():
    sms = SMSService()
    
    # First SMS should succeed
    result1 = await sms.send_sms("+61412345678", "Message 1")
    assert result1 is True
    
    # Second SMS to same number within 5 minutes should fail
    result2 = await sms.send_sms("+61412345678", "Message 2")
    assert result2 is False

def test_phone_number_formatting():
    sms = SMSService()
    
    assert sms._format_phone_number("0412345678") == "+61412345678"
    assert sms._format_phone_number("412345678") == "+61412345678"
    assert sms._format_phone_number("+61412345678") == "+61412345678"

def test_message_truncation():
    sms = SMSService()
    long_msg = "x" * 200
    truncated = sms._truncate_message(long_msg)
    
    assert len(truncated) == 160
    assert truncated.endswith("...")
```

### 2. Integration Tests

```python
@pytest.mark.asyncio
async def test_send_shift_reminder():
    sms = SMSService()
    
    employee = {"id": "emp1", "phone": "0412345678", "name": "John"}
    shift = {"venue_name": "Pub", "start_time": "17:00", "end_time": "22:00"}
    
    result = await sms.send_shift_reminder(employee, shift)
    # Result depends on Twilio configuration
```

## Troubleshooting

### SMS Not Sending

1. **Check Twilio Configuration**
   ```bash
   echo $TWILIO_ACCOUNT_SID
   echo $TWILIO_AUTH_TOKEN
   echo $TWILIO_FROM_NUMBER
   ```

2. **Check User Preferences**
   ```bash
   curl http://localhost:8000/api/notifications/preferences \
     -H "Authorization: Bearer $JWT_TOKEN"
   ```

3. **Check Quiet Hours**
   ```python
   prefs = get_preferences_service()
   print(prefs.is_in_quiet_hours(user_id))
   ```

4. **Test SMS Directly**
   ```bash
   curl -X POST http://localhost:8000/api/notifications/test-sms \
     -H "Authorization: Bearer $JWT_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"phone": "0412345678"}'
   ```

### Rate Limiting Issues

Rate limits are in-memory and reset on app restart. To bypass for testing:
```python
sms = get_sms_service()
sms._rate_limit_tracker.clear()  # Clear rate limit tracking
```

## Migration Path for Database

When migrating to PostgreSQL, implement in `PostgresStore`:

```python
def save_notification_preferences(self, user_id: str, prefs: dict) -> None:
    with self._cursor() as cur:
        cur.execute("""
            INSERT INTO notification_preferences (user_id, prefs)
            VALUES (%s, %s)
            ON CONFLICT (user_id) DO UPDATE SET prefs=EXCLUDED.prefs
        """, (user_id, json.dumps(prefs)))

def get_notification_preferences(self, user_id: str) -> Optional[dict]:
    with self._cursor() as cur:
        cur.execute("SELECT prefs FROM notification_preferences WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
        return json.loads(row['prefs']) if row else None
```

## Files Modified

1. **Created:**
   - `/RosterIQ/services/sms.py` - SMS service (~290 lines)
   - `/RosterIQ/services/notification_preferences.py` - Preferences service (~200 lines)
   - `/RosterIQ/routes/notification_prefs.py` - API routes (~130 lines)

2. **Modified:**
   - `/RosterIQ/database.py` - Added preference methods to BaseStore & MemoryStore
   - `/RosterIQ/services/notifications.py` - Added SMS import and integrated send method
   - `/RosterIQ/api.py` - Registered notification preferences router

## Future Enhancements

1. **Push Notifications**: Implement Firebase Cloud Messaging for web/mobile push
2. **Bulk SMS**: Send batch SMS for roster changes
3. **SMS Reply Parsing**: Handle SMS replies (e.g., "ACCEPT" to approve swap)
4. **Delivery Webhooks**: Track SMS delivery status via Twilio webhooks
5. **Template System**: Parameterized SMS templates with fallback translations
6. **Analytics**: Track SMS delivery rates, bounces, opt-outs
