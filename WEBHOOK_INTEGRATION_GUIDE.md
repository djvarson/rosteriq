# Webhook Integration Guide for RosterIQ

Quick reference for integrating webhook events into your application code.

## Basic Event Firing

```python
from rosteriq.services.outbound_webhooks import get_outbound_webhook_service

service = get_outbound_webhook_service()

# Fire event (async - call with await)
await service.fire_event(
    event_type="roster.published",
    venue_id="venue-123",
    payload={
        "roster_id": "roster-456",
        "week_start": "2026-04-27",
        "week_end": "2026-05-03",
        "total_cost": 5000.00,
        "shifts_count": 42,
    }
)
```

## Event Type Reference

### Roster Events

**roster.published**
Fired when a roster is published/finalized.
```python
await service.fire_event(
    "roster.published",
    venue_id,
    {
        "roster_id": roster.id,
        "week_start": roster.week_start.isoformat(),
        "week_end": roster.week_end.isoformat(),
        "total_cost": float(roster.total_cost),
        "shifts_count": len(roster.shifts),
        "venues_affected": [venue_id],
    }
)
```

**roster.updated**
Fired when an existing roster is modified.
```python
await service.fire_event(
    "roster.updated",
    venue_id,
    {
        "roster_id": roster.id,
        "previous_cost": 4900.00,
        "new_cost": 5000.00,
        "shifts_changed": 5,
        "reason": "shift_swap_approved",
    }
)
```

### Shift Events

**shift.created**
Fired when a new shift is created.
```python
await service.fire_event(
    "shift.created",
    venue_id,
    {
        "shift_id": shift.id,
        "employee_id": shift.employee_id,
        "employee_name": employee.name,
        "date": shift.date.isoformat(),
        "start_time": shift.start_time.isoformat(),
        "end_time": shift.end_time.isoformat(),
        "role": shift.role,
        "cost": float(shift.cost) if shift.cost else None,
    }
)
```

**shift.swapped**
Fired when two employees swap shifts.
```python
await service.fire_event(
    "shift.swapped",
    venue_id,
    {
        "shift_id_1": shift1.id,
        "shift_id_2": shift2.id,
        "employee_id_1": shift1.employee_id,
        "employee_id_2": shift2.employee_id,
        "date": shift1.date.isoformat(),
        "approved_by": user_id,
        "approved_at": datetime.utcnow().isoformat(),
    }
)
```

**shift.cancelled**
Fired when a shift is cancelled.
```python
await service.fire_event(
    "shift.cancelled",
    venue_id,
    {
        "shift_id": shift.id,
        "employee_id": shift.employee_id,
        "date": shift.date.isoformat(),
        "reason": "business_need" | "employee_request" | "other",
        "cancelled_by": user_id,
        "cancelled_at": datetime.utcnow().isoformat(),
    }
)
```

### Employee Events

**employee.added**
Fired when a new employee is added to a venue.
```python
await service.fire_event(
    "employee.added",
    venue_id,
    {
        "employee_id": employee.id,
        "name": employee.name,
        "email": employee.email,
        "phone": employee.phone,
        "employment_type": employee.employment_type.value,
        "award_level": employee.award_level.value,
        "created_at": datetime.utcnow().isoformat(),
    }
)
```

**employee.updated**
Fired when employee information is updated.
```python
await service.fire_event(
    "employee.updated",
    venue_id,
    {
        "employee_id": employee.id,
        "name": employee.name,
        "email": employee.email,
        "phone": employee.phone,
        "skills": employee.skills,
        "availability": employee.availability,
        "updated_at": datetime.utcnow().isoformat(),
        "changed_fields": ["phone", "availability"],
    }
)
```

### Alert Events

**alert.compliance**
Fired when a compliance issue is detected.
```python
await service.fire_event(
    "alert.compliance",
    venue_id,
    {
        "alert_id": f"compliance_{uuid4().hex}",
        "severity": "warning" | "critical",
        "type": "award_violation" | "minimum_rest" | "consecutive_days",
        "employee_id": employee_id,
        "employee_name": employee_name,
        "details": "Employee scheduled beyond award limits",
        "date": date.isoformat(),
        "created_at": datetime.utcnow().isoformat(),
    }
)
```

**alert.variance**
Fired when actual vs forecast variance exceeds threshold.
```python
await service.fire_event(
    "alert.variance",
    venue_id,
    {
        "alert_id": f"variance_{uuid4().hex}",
        "severity": "warning" | "critical",
        "date": date.isoformat(),
        "hour": 18,
        "forecast_covers": 45,
        "actual_covers": 32,
        "variance_percent": -28.9,
        "variance_threshold": 20,
        "created_at": datetime.utcnow().isoformat(),
    }
)
```

### Forecast Events

**forecast.updated**
Fired when demand forecast is updated.
```python
await service.fire_event(
    "forecast.updated",
    venue_id,
    {
        "forecast_date": date.isoformat(),
        "forecast_hours": [
            {
                "hour": 12,
                "predicted_covers": 30,
                "confidence": 0.87,
                "signals_used": ["bom_weather", "historical_trend"],
            },
            {
                "hour": 18,
                "predicted_covers": 45,
                "confidence": 0.92,
                "signals_used": ["bom_weather", "foot_traffic", "booking"],
            },
        ],
        "model_version": "v2.3.1",
        "updated_at": datetime.utcnow().isoformat(),
    }
)
```

## Where to Add Event Calls

### Roster Routes
In `routes/rosters.py` (or similar):
```python
async def publish_roster(roster_id: str):
    roster = get_roster(roster_id)
    # ... publication logic ...
    
    service = get_outbound_webhook_service()
    await service.fire_event("roster.published", venue_id, {...})
```

### Shift Management
In `routes/shifts.py` or business logic:
```python
async def create_shift(...):
    shift = create_shift_record(...)
    
    service = get_outbound_webhook_service()
    await service.fire_event("shift.created", venue_id, {...})

async def swap_shifts(shift1_id, shift2_id):
    shift1, shift2 = swap_shifts_impl(shift1_id, shift2_id)
    
    service = get_outbound_webhook_service()
    await service.fire_event("shift.swapped", venue_id, {...})
```

### Employee Management
In `routes/employees.py`:
```python
async def add_employee(venue_id, employee_data):
    employee = save_employee(venue_id, employee_data)
    
    service = get_outbound_webhook_service()
    await service.fire_event("employee.added", venue_id, {...})

async def update_employee(employee_id, updates):
    old_employee = get_employee(employee_id)
    new_employee = apply_updates(old_employee, updates)
    
    service = get_outbound_webhook_service()
    await service.fire_event("employee.updated", venue_id, {...})
```

### Alerts/Compliance
In `services/compliance.py` or `services/alerts.py`:
```python
async def check_compliance(venue_id, shifts):
    violations = detect_violations(shifts)
    
    service = get_outbound_webhook_service()
    for violation in violations:
        await service.fire_event(
            "alert.compliance",
            venue_id,
            {
                "alert_id": str(uuid4()),
                "severity": "critical" if severe else "warning",
                "type": violation.type,
                "employee_id": violation.employee_id,
                "details": violation.description,
                "date": violation.date.isoformat(),
            }
        )
```

### Forecasting
In `services/forecasting.py` or `ensemble.py`:
```python
async def update_forecast(venue_id, forecast_date):
    forecasts = calculate_forecasts(venue_id, forecast_date)
    save_forecasts(forecasts)
    
    service = get_outbound_webhook_service()
    await service.fire_event(
        "forecast.updated",
        venue_id,
        {
            "forecast_date": forecast_date.isoformat(),
            "forecast_hours": [
                {
                    "hour": f.hour,
                    "predicted_covers": f.predicted_covers,
                    "confidence": f.confidence,
                    "signals_used": f.signals_used,
                }
                for f in forecasts
            ],
        }
    )
```

## Testing Events

### In Python
```python
import asyncio
from rosteriq.services.outbound_webhooks import get_outbound_webhook_service

async def test():
    service = get_outbound_webhook_service()
    
    # Register test subscription
    sub_id = service.register_subscription(
        venue_id="test-venue",
        callback_url="https://httpbin.org/post",
        events=["roster.published"],
        secret="test-secret"
    )
    
    # Fire test event
    matched = await service.fire_event(
        "roster.published",
        "test-venue",
        {"test": True}
    )
    
    print(f"Matched {matched} subscriptions")
    
    # Check delivery log
    log = service.get_delivery_log(sub_id)
    print(f"Deliveries: {log}")

asyncio.run(test())
```

### Via cURL
```bash
# 1. Create subscription for test URL
curl -X POST http://localhost:8000/api/webhooks/subscribe \
  -H "Content-Type: application/json" \
  -d '{
    "venue_id": "test-venue",
    "callback_url": "https://httpbin.org/post",
    "events": ["roster.published"],
    "secret": "test-secret"
  }' > /tmp/sub.json

SUBSCRIPTION_ID=$(cat /tmp/sub.json | jq -r .subscription_id)

# 2. Send test event
curl -X POST http://localhost:8000/api/webhooks/test/$SUBSCRIPTION_ID \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "roster.published",
    "data": {
      "roster_id": "test-roster-1",
      "week_start": "2026-04-27",
      "total_cost": 5000
    }
  }'

# 3. Check delivery status
curl http://localhost:8000/api/webhooks/deliveries/$SUBSCRIPTION_ID
```

## Error Handling

```python
try:
    matched = await service.fire_event(
        "roster.published",
        venue_id,
        payload
    )
    logger.info(f"Webhook fired to {matched} subscriptions")
except Exception as e:
    logger.error(f"Failed to fire webhook: {e}")
    # Continue with operation - webhook failure is non-blocking
```

## Performance Notes

- Event firing is async and non-blocking
- Delivery happens in background with retries
- Failures don't block application logic
- Each delivery attempt times out after 10 seconds
- Maximum 3 retry attempts per event

## Best Practices

1. **Always include timestamps:** Use `datetime.utcnow().isoformat()` for consistency
2. **Use UUIDs for IDs:** For uniqueness across systems
3. **Keep payloads lean:** Include only essential data
4. **Use enums for types:** "critical" vs 1, "warning" vs 2
5. **Document your events:** Comment what triggers each event type
6. **Test with httpbin.org:** Use for testing webhook delivery
7. **Monitor delivery log:** Check for failed deliveries regularly
8. **Verify signatures:** Always verify HMAC on receiver side
9. **Handle retries:** Use delivery_id for idempotency
10. **Log webhook calls:** Add debug logging at fire_event call sites

## Debugging

Check delivery log for failed webhooks:
```python
from rosteriq.services.outbound_webhooks import get_outbound_webhook_service

service = get_outbound_webhook_service()
log = service.get_delivery_log(subscription_id, limit=100)

# Find failures
failures = [d for d in log if d['status'] == 'failed']
for delivery in failures:
    print(f"Failed: {delivery['event_type']} at {delivery['last_attempt_at']}")
    print(f"  Attempts: {delivery['attempts']}")
    print(f"  Response: {delivery['response_code']}")
```
