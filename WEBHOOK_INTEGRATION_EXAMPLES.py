"""
Webhook Integration Examples - Code snippets showing where to fire events.

This file demonstrates how to integrate webhook event firing into various
parts of the RosterIQ application. Copy and adapt these examples to your code.

Note: This is example code, not meant to be executed directly.
"""

from datetime import datetime
from uuid import uuid4
from rosteriq.services.outbound_webhooks import get_outbound_webhook_service


# ============================================================================
# Example 1: Fire Event After Roster Publishing
# ============================================================================

async def publish_roster_example(roster_id: str, venue_id: str):
    """
    Example: Fire roster.published event after roster is finalized.

    Add this to your roster publishing endpoint.
    """
    # ... existing roster publishing logic ...
    roster = get_roster(roster_id)
    publish_roster_to_system(roster)

    # Fire webhook event
    service = get_outbound_webhook_service()

    await service.fire_event(
        event_type="roster.published",
        venue_id=venue_id,
        payload={
            "roster_id": roster.id,
            "week_start": roster.week_start.isoformat(),
            "week_end": roster.week_end.isoformat(),
            "total_cost": float(roster.total_cost) if roster.total_cost else 0.0,
            "shifts_count": len(roster.shifts),
            "venues_affected": [venue_id],
            "published_at": datetime.utcnow().isoformat(),
        }
    )


# ============================================================================
# Example 2: Fire Event When Shift Is Created
# ============================================================================

async def create_shift_example(
    venue_id: str,
    employee_id: str,
    shift_date,
    start_time,
    end_time,
):
    """
    Example: Fire shift.created event after creating a shift.

    Add this to your shift creation endpoint.
    """
    # ... existing shift creation logic ...
    shift = create_shift_record(
        venue_id=venue_id,
        employee_id=employee_id,
        shift_date=shift_date,
        start_time=start_time,
        end_time=end_time,
    )

    employee = get_employee(employee_id)

    # Fire webhook event
    service = get_outbound_webhook_service()

    await service.fire_event(
        event_type="shift.created",
        venue_id=venue_id,
        payload={
            "shift_id": shift.id,
            "employee_id": shift.employee_id,
            "employee_name": employee.name,
            "date": shift.date.isoformat(),
            "start_time": shift.start_time.isoformat(),
            "end_time": shift.end_time.isoformat(),
            "role": shift.role,
            "cost": float(shift.cost) if shift.cost else None,
            "created_at": datetime.utcnow().isoformat(),
        }
    )


# ============================================================================
# Example 3: Fire Event When Shifts Are Swapped
# ============================================================================

async def swap_shifts_example(shift1_id: str, shift2_id: str, venue_id: str):
    """
    Example: Fire shift.swapped event after swapping two shifts.

    Add this to your shift swap endpoint.
    """
    # ... existing shift swap logic ...
    shift1 = get_shift(shift1_id)
    shift2 = get_shift(shift2_id)

    swap_shifts_impl(shift1_id, shift2_id)

    # Fire webhook event
    service = get_outbound_webhook_service()

    await service.fire_event(
        event_type="shift.swapped",
        venue_id=venue_id,
        payload={
            "shift_id_1": shift1.id,
            "shift_id_2": shift2.id,
            "employee_id_1": shift1.employee_id,
            "employee_id_2": shift2.employee_id,
            "date": shift1.date.isoformat(),
            "approved_by": "current_user_id",
            "approved_at": datetime.utcnow().isoformat(),
        }
    )


# ============================================================================
# Example 4: Fire Event When Employee Is Added
# ============================================================================

async def add_employee_example(venue_id: str, employee_data: dict):
    """
    Example: Fire employee.added event after adding new employee.

    Add this to your employee creation endpoint.
    """
    # ... existing employee creation logic ...
    employee = save_employee(venue_id=venue_id, **employee_data)

    # Fire webhook event
    service = get_outbound_webhook_service()

    await service.fire_event(
        event_type="employee.added",
        venue_id=venue_id,
        payload={
            "employee_id": employee.id,
            "name": employee.name,
            "email": employee.email,
            "phone": employee.phone,
            "employment_type": employee.employment_type.value,
            "award_level": employee.award_level.value,
            "skills": employee.skills,
            "created_at": datetime.utcnow().isoformat(),
        }
    )


# ============================================================================
# Example 5: Fire Event When Employee Is Updated
# ============================================================================

async def update_employee_example(employee_id: str, venue_id: str, updates: dict):
    """
    Example: Fire employee.updated event after updating employee info.

    Add this to your employee update endpoint.
    """
    # ... existing employee update logic ...
    old_employee = get_employee(employee_id)
    updated_employee = apply_updates(old_employee, updates)
    save_employee(updated_employee)

    changed_fields = [key for key in updates.keys() if updates[key] != getattr(old_employee, key, None)]

    # Fire webhook event
    service = get_outbound_webhook_service()

    await service.fire_event(
        event_type="employee.updated",
        venue_id=venue_id,
        payload={
            "employee_id": updated_employee.id,
            "name": updated_employee.name,
            "email": updated_employee.email,
            "phone": updated_employee.phone,
            "skills": updated_employee.skills,
            "availability": updated_employee.availability,
            "updated_at": datetime.utcnow().isoformat(),
            "changed_fields": changed_fields,
        }
    )


# ============================================================================
# Example 6: Fire Event When Compliance Issue Detected
# ============================================================================

async def compliance_alert_example(
    venue_id: str,
    employee_id: str,
    violation_type: str,
    details: str,
):
    """
    Example: Fire alert.compliance event when compliance issue is detected.

    Add this to your compliance checking logic.
    """
    employee = get_employee(employee_id)

    # Fire webhook event
    service = get_outbound_webhook_service()

    await service.fire_event(
        event_type="alert.compliance",
        venue_id=venue_id,
        payload={
            "alert_id": f"compliance_{uuid4().hex}",
            "severity": "critical",  # or "warning"
            "type": violation_type,  # e.g., "award_violation", "minimum_rest"
            "employee_id": employee_id,
            "employee_name": employee.name,
            "details": details,
            "date": datetime.utcnow().date().isoformat(),
            "created_at": datetime.utcnow().isoformat(),
        }
    )


# ============================================================================
# Example 7: Fire Event When Variance Alert Triggered
# ============================================================================

async def variance_alert_example(
    venue_id: str,
    alert_date,
    hour: int,
    forecast_covers: int,
    actual_covers: int,
):
    """
    Example: Fire alert.variance event when actual vs forecast exceeds threshold.

    Add this to your variance detection logic.
    """
    variance = actual_covers - forecast_covers
    variance_pct = (variance / forecast_covers * 100) if forecast_covers > 0 else 0

    # Fire webhook event
    service = get_outbound_webhook_service()

    await service.fire_event(
        event_type="alert.variance",
        venue_id=venue_id,
        payload={
            "alert_id": f"variance_{uuid4().hex}",
            "severity": "critical" if abs(variance_pct) > 30 else "warning",
            "date": alert_date.isoformat(),
            "hour": hour,
            "forecast_covers": forecast_covers,
            "actual_covers": actual_covers,
            "variance": variance,
            "variance_percent": round(variance_pct, 1),
            "variance_threshold": 20,
            "created_at": datetime.utcnow().isoformat(),
        }
    )


# ============================================================================
# Example 8: Fire Event When Forecast Is Updated
# ============================================================================

async def forecast_update_example(venue_id: str, forecast_date):
    """
    Example: Fire forecast.updated event after updating demand forecast.

    Add this to your forecasting logic.
    """
    # ... existing forecast calculation logic ...
    forecasts = calculate_hourly_forecast(venue_id, forecast_date)
    save_forecasts(forecasts)

    # Fire webhook event
    service = get_outbound_webhook_service()

    forecast_hours = []
    for forecast in forecasts:
        forecast_hours.append({
            "hour": forecast.hour,
            "predicted_covers": forecast.predicted_covers,
            "confidence": forecast.confidence,
            "signals_used": forecast.signals_used,
        })

    await service.fire_event(
        event_type="forecast.updated",
        venue_id=venue_id,
        payload={
            "forecast_date": forecast_date.isoformat(),
            "forecast_hours": forecast_hours,
            "model_version": "v2.3.1",
            "updated_at": datetime.utcnow().isoformat(),
        }
    )


# ============================================================================
# Example 9: Handling Background Tasks in FastAPI Endpoint
# ============================================================================

from fastapi import BackgroundTasks

async def publish_roster_endpoint(
    roster_id: str,
    background_tasks: BackgroundTasks,
):
    """
    Example: Use BackgroundTasks to fire webhook without blocking response.

    Add this to your FastAPI endpoint.
    """
    # Publish roster
    roster = get_roster(roster_id)
    publish_roster_to_system(roster)

    # Queue webhook event to fire in background (return immediately)
    service = get_outbound_webhook_service()
    background_tasks.add_task(
        service.fire_event,
        event_type="roster.published",
        venue_id=roster.venue_id,
        payload={
            "roster_id": roster.id,
            "week_start": roster.week_start.isoformat(),
            "total_cost": float(roster.total_cost),
        }
    )

    return {"status": "published", "roster_id": roster_id}


# ============================================================================
# Example 10: Error Handling When Firing Events
# ============================================================================

async def safe_fire_event_example(venue_id: str):
    """
    Example: Safely fire events with error handling.

    Don't let webhook failures block your business logic.
    """
    import logging

    logger = logging.getLogger(__name__)
    service = get_outbound_webhook_service()

    try:
        matched = await service.fire_event(
            event_type="roster.published",
            venue_id=venue_id,
            payload={"roster_id": "r123"}
        )
        logger.info(f"Webhook fired to {matched} subscriptions")
    except Exception as e:
        # Log error but don't fail the operation
        logger.error(f"Failed to fire webhook: {e}", exc_info=True)
        # Continue with application logic


# ============================================================================
# Example 11: Using Test Endpoint During Development
# ============================================================================

async def test_webhook_integration_example():
    """
    Example: Test webhook delivery during development.

    Use httpbin.org for testing without real callback server.
    """
    import httpx

    # 1. Register test subscription
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "http://localhost:8000/api/webhooks/subscribe",
            json={
                "venue_id": "test-venue",
                "callback_url": "https://httpbin.org/post",
                "events": ["roster.published"],
                "secret": "test-secret"
            }
        )
        subscription = response.json()
        subscription_id = subscription["subscription_id"]

    # 2. Send test event
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"http://localhost:8000/api/webhooks/test/{subscription_id}",
            json={
                "event_type": "roster.published",
                "data": {"test": True}
            }
        )
        print(response.json())

    # 3. Check delivery log
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"http://localhost:8000/api/webhooks/deliveries/{subscription_id}"
        )
        deliveries = response.json()
        print(deliveries)


# ============================================================================
# Integration Checklist
# ============================================================================

"""
Before shipping webhooks to production:

1. Data Layer
   [ ] Create webhook_subscriptions table in PostgreSQL
   [ ] Create webhook_deliveries table in PostgreSQL
   [ ] Add foreign key constraints
   [ ] Create performance indexes

2. Service Integration
   [ ] Add fire_event calls to roster publishing logic
   [ ] Add fire_event calls to shift creation/modification logic
   [ ] Add fire_event calls to employee management logic
   [ ] Add fire_event calls to compliance checking logic
   [ ] Add fire_event calls to forecasting logic

3. Testing
   [ ] Test subscription registration via API
   [ ] Test event firing from Python code
   [ ] Verify webhook delivery to test endpoint (httpbin.org)
   [ ] Verify HMAC signature matches on receiver side
   [ ] Test retry logic with failing endpoint
   [ ] Check delivery log for success/failure records

4. Documentation
   [ ] Document webhook payload formats for customers
   [ ] Provide signature verification code examples
   [ ] Document all event types and payload structures
   [ ] Provide webhook testing guide
   [ ] Document retry behavior and timing

5. Operations
   [ ] Set up monitoring for failed deliveries
   [ ] Set up alerts for consistently failing subscriptions
   [ ] Plan delivery log cleanup/archival strategy
   [ ] Configure logging for webhook operations
   [ ] Document how to troubleshoot webhook issues

6. Security
   [ ] Review HTTPS enforcement on callback URLs
   [ ] Review signature verification examples
   [ ] Document secret rotation procedures
   [ ] Add rate limiting to webhook endpoints
   [ ] Add authentication/authorization to webhook API
"""
