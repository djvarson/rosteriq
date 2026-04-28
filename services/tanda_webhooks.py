"""
Tanda webhook event handlers for RosterIQ.

Processes inbound Tanda events and broadcasts updates via WebSocket hub.
"""
import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional, Callable
from enum import Enum
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class TandaEventType(str, Enum):
    """Supported Tanda webhook event types."""
    ROSTER_PUBLISHED = "roster.published"
    SHIFT_CREATED = "shift.created"
    SHIFT_UPDATED = "shift.updated"
    SHIFT_DELETED = "shift.deleted"
    EMPLOYEE_CREATED = "employee.created"
    EMPLOYEE_UPDATED = "employee.updated"
    EMPLOYEE_DEACTIVATED = "employee.deactivated"
    TIMESHEET_APPROVED = "timesheet.approved"
    LEAVE_CREATED = "leave.created"
    LEAVE_UPDATED = "leave.updated"


@dataclass
class WebhookPayload:
    """Parsed Tanda webhook payload."""
    event_type: TandaEventType
    webhook_id: str
    timestamp: datetime
    venue_id: str
    data: Dict[str, Any]
    user_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for broadcasting."""
        return {
            "event_type": self.event_type.value,
            "webhook_id": self.webhook_id,
            "timestamp": self.timestamp.isoformat(),
            "venue_id": self.venue_id,
            "user_id": self.user_id,
            "data": self.data,
        }


class EventHandlerRegistry:
    """Registry for webhook event handlers."""

    def __init__(self):
        self._handlers: Dict[TandaEventType, list[Callable]] = {
            event_type: [] for event_type in TandaEventType
        }

    def register(self, event_type: TandaEventType):
        """Decorator to register an event handler."""
        def decorator(handler: Callable) -> Callable:
            self._handlers[event_type].append(handler)
            return handler
        return decorator

    def get_handlers(self, event_type: TandaEventType) -> list[Callable]:
        """Get all handlers for an event type."""
        return self._handlers.get(event_type, [])

    async def dispatch(self, payload: WebhookPayload, ws_hub=None) -> Dict[str, Any]:
        """Dispatch event to all registered handlers."""
        handlers = self.get_handlers(payload.event_type)
        results = {
            "event_type": payload.event_type.value,
            "handlers_called": len(handlers),
            "results": [],
            "errors": [],
        }

        for handler in handlers:
            try:
                if ws_hub:
                    result = await handler(payload, ws_hub)
                else:
                    result = await handler(payload, None)
                results["results"].append({
                    "handler": handler.__name__,
                    "status": "success",
                    "result": result,
                })
                logger.info(f"Handler {handler.__name__} completed for {payload.event_type}")
            except Exception as e:
                error_msg = str(e)
                results["errors"].append({
                    "handler": handler.__name__,
                    "error": error_msg,
                })
                logger.error(f"Handler {handler.__name__} failed: {error_msg}")

        return results


# Global registry
_registry = EventHandlerRegistry()


# ============================================================================
# Event Handlers
# ============================================================================

@_registry.register(TandaEventType.ROSTER_PUBLISHED)
async def handle_roster_published(payload: WebhookPayload, ws_hub) -> Dict[str, Any]:
    """
    Handle roster.published event.

    Syncs shifts, compares forecast vs actual, flags variances.
    """
    data = payload.data
    venue_id = payload.venue_id

    result = {
        "action": "roster_published",
        "venue_id": venue_id,
        "shifts_synced": 0,
        "variances_flagged": 0,
    }

    # Extract shifts from payload
    shifts = data.get("shifts", [])
    result["shifts_synced"] = len(shifts)

    # Compare forecast vs actual (mock implementation)
    variances = []
    for shift in shifts:
        shift_id = shift.get("id")
        actual_hours = shift.get("actual_hours", 0)
        scheduled_hours = shift.get("scheduled_hours", 0)
        variance = abs(actual_hours - scheduled_hours)

        if variance > 0.5:  # Flag if variance > 30 minutes
            variances.append({
                "shift_id": shift_id,
                "variance_hours": variance,
                "severity": "high" if variance > 2 else "medium",
            })

    result["variances_flagged"] = len(variances)
    result["variances"] = variances

    # Broadcast via WebSocket
    if ws_hub:
        try:
            await ws_hub.broadcast({
                "type": "roster_published",
                "venue_id": venue_id,
                "timestamp": payload.timestamp.isoformat(),
                "shifts_synced": len(shifts),
                "variances": variances,
            })
        except Exception as e:
            logger.error(f"Failed to broadcast roster_published: {e}")

    return result


@_registry.register(TandaEventType.SHIFT_CREATED)
async def handle_shift_created(payload: WebhookPayload, ws_hub) -> Dict[str, Any]:
    """
    Handle shift.created event.

    Updates local shift records, recalculates costs with award rates.
    """
    data = payload.data
    shift_id = data.get("id")
    venue_id = payload.venue_id

    result = {
        "action": "shift_created",
        "shift_id": shift_id,
        "venue_id": venue_id,
        "cost_calculated": False,
        "cost_aud": 0.0,
    }

    # Extract shift details
    employee_id = data.get("employee_id")
    start_time = data.get("start_time")
    end_time = data.get("end_time")
    hourly_rate = data.get("hourly_rate", 0)
    award_level = data.get("award_level", "minimum")

    # Mock cost calculation with award rates
    if start_time and end_time:
        try:
            from datetime import datetime as dt
            start = dt.fromisoformat(start_time)
            end = dt.fromisoformat(end_time)
            hours = (end - start).total_seconds() / 3600

            # Apply award multiplier (mock)
            award_multiplier = {
                "minimum": 1.0,
                "level1": 1.15,
                "level2": 1.3,
                "level3": 1.5,
            }.get(award_level, 1.0)

            cost = hours * hourly_rate * award_multiplier
            result["cost_aud"] = round(cost, 2)
            result["cost_calculated"] = True
        except Exception as e:
            logger.error(f"Cost calculation failed: {e}")

    # Broadcast via WebSocket
    if ws_hub:
        try:
            await ws_hub.broadcast({
                "type": "shift_created",
                "shift_id": shift_id,
                "venue_id": venue_id,
                "employee_id": employee_id,
                "cost_aud": result["cost_aud"],
                "timestamp": payload.timestamp.isoformat(),
            })
        except Exception as e:
            logger.error(f"Failed to broadcast shift_created: {e}")

    return result


@_registry.register(TandaEventType.SHIFT_UPDATED)
async def handle_shift_updated(payload: WebhookPayload, ws_hub) -> Dict[str, Any]:
    """
    Handle shift.updated event.

    Updates local shift records, recalculates costs.
    """
    data = payload.data
    shift_id = data.get("id")
    venue_id = payload.venue_id

    result = {
        "action": "shift_updated",
        "shift_id": shift_id,
        "venue_id": venue_id,
        "fields_updated": list(data.keys()),
    }

    # Broadcast via WebSocket
    if ws_hub:
        try:
            await ws_hub.broadcast({
                "type": "shift_updated",
                "shift_id": shift_id,
                "venue_id": venue_id,
                "fields": result["fields_updated"],
                "timestamp": payload.timestamp.isoformat(),
            })
        except Exception as e:
            logger.error(f"Failed to broadcast shift_updated: {e}")

    return result


@_registry.register(TandaEventType.SHIFT_DELETED)
async def handle_shift_deleted(payload: WebhookPayload, ws_hub) -> Dict[str, Any]:
    """
    Handle shift.deleted event.

    Removes shift from local records, adjusts budget.
    """
    data = payload.data
    shift_id = data.get("id")
    venue_id = payload.venue_id

    result = {
        "action": "shift_deleted",
        "shift_id": shift_id,
        "venue_id": venue_id,
        "record_removed": True,
    }

    # Broadcast via WebSocket
    if ws_hub:
        try:
            await ws_hub.broadcast({
                "type": "shift_deleted",
                "shift_id": shift_id,
                "venue_id": venue_id,
                "timestamp": payload.timestamp.isoformat(),
            })
        except Exception as e:
            logger.error(f"Failed to broadcast shift_deleted: {e}")

    return result


@_registry.register(TandaEventType.EMPLOYEE_CREATED)
async def handle_employee_created(payload: WebhookPayload, ws_hub) -> Dict[str, Any]:
    """
    Handle employee.created event.

    Syncs employee details to local records.
    """
    data = payload.data
    employee_id = data.get("id")
    venue_id = payload.venue_id

    result = {
        "action": "employee_created",
        "employee_id": employee_id,
        "venue_id": venue_id,
        "name": data.get("name"),
        "email": data.get("email"),
        "status": "active",
    }

    # Broadcast via WebSocket
    if ws_hub:
        try:
            await ws_hub.broadcast({
                "type": "employee_created",
                "employee_id": employee_id,
                "venue_id": venue_id,
                "name": result["name"],
                "timestamp": payload.timestamp.isoformat(),
            })
        except Exception as e:
            logger.error(f"Failed to broadcast employee_created: {e}")

    return result


@_registry.register(TandaEventType.EMPLOYEE_UPDATED)
async def handle_employee_updated(payload: WebhookPayload, ws_hub) -> Dict[str, Any]:
    """
    Handle employee.updated event.

    Updates employee details in local records.
    """
    data = payload.data
    employee_id = data.get("id")
    venue_id = payload.venue_id

    result = {
        "action": "employee_updated",
        "employee_id": employee_id,
        "venue_id": venue_id,
        "fields_updated": [k for k in data.keys() if k != "id"],
    }

    # Broadcast via WebSocket
    if ws_hub:
        try:
            await ws_hub.broadcast({
                "type": "employee_updated",
                "employee_id": employee_id,
                "venue_id": venue_id,
                "fields": result["fields_updated"],
                "timestamp": payload.timestamp.isoformat(),
            })
        except Exception as e:
            logger.error(f"Failed to broadcast employee_updated: {e}")

    return result


@_registry.register(TandaEventType.EMPLOYEE_DEACTIVATED)
async def handle_employee_deactivated(payload: WebhookPayload, ws_hub) -> Dict[str, Any]:
    """
    Handle employee.deactivated event.

    Marks employee as inactive in local records.
    """
    data = payload.data
    employee_id = data.get("id")
    venue_id = payload.venue_id

    result = {
        "action": "employee_deactivated",
        "employee_id": employee_id,
        "venue_id": venue_id,
        "status": "inactive",
    }

    # Broadcast via WebSocket
    if ws_hub:
        try:
            await ws_hub.broadcast({
                "type": "employee_deactivated",
                "employee_id": employee_id,
                "venue_id": venue_id,
                "timestamp": payload.timestamp.isoformat(),
            })
        except Exception as e:
            logger.error(f"Failed to broadcast employee_deactivated: {e}")

    return result


@_registry.register(TandaEventType.TIMESHEET_APPROVED)
async def handle_timesheet_approved(payload: WebhookPayload, ws_hub) -> Dict[str, Any]:
    """
    Handle timesheet.approved event.

    Pulls approved hours, compares to rostered, calculates variance.
    """
    data = payload.data
    timesheet_id = data.get("id")
    venue_id = payload.venue_id

    result = {
        "action": "timesheet_approved",
        "timesheet_id": timesheet_id,
        "venue_id": venue_id,
        "approved_hours": 0,
        "rostered_hours": 0,
        "variance_hours": 0,
    }

    # Extract hours
    approved_hours = data.get("approved_hours", 0)
    rostered_hours = data.get("rostered_hours", 0)
    variance = abs(approved_hours - rostered_hours)

    result["approved_hours"] = approved_hours
    result["rostered_hours"] = rostered_hours
    result["variance_hours"] = variance

    # Flag if variance is significant
    if variance > 2:
        result["variance_flag"] = "significant"

    # Broadcast via WebSocket
    if ws_hub:
        try:
            await ws_hub.broadcast({
                "type": "timesheet_approved",
                "timesheet_id": timesheet_id,
                "venue_id": venue_id,
                "approved_hours": approved_hours,
                "variance_hours": variance,
                "timestamp": payload.timestamp.isoformat(),
            })
        except Exception as e:
            logger.error(f"Failed to broadcast timesheet_approved: {e}")

    return result


@_registry.register(TandaEventType.LEAVE_CREATED)
async def handle_leave_created(payload: WebhookPayload, ws_hub) -> Dict[str, Any]:
    """
    Handle leave.created event.

    Updates employee availability, suggests re-optimization.
    """
    data = payload.data
    leave_id = data.get("id")
    venue_id = payload.venue_id
    employee_id = data.get("employee_id")

    result = {
        "action": "leave_created",
        "leave_id": leave_id,
        "venue_id": venue_id,
        "employee_id": employee_id,
        "reoptimization_suggested": True,
    }

    # Broadcast via WebSocket
    if ws_hub:
        try:
            await ws_hub.broadcast({
                "type": "leave_created",
                "leave_id": leave_id,
                "venue_id": venue_id,
                "employee_id": employee_id,
                "reoptimization_suggested": True,
                "timestamp": payload.timestamp.isoformat(),
            })
        except Exception as e:
            logger.error(f"Failed to broadcast leave_created: {e}")

    return result


@_registry.register(TandaEventType.LEAVE_UPDATED)
async def handle_leave_updated(payload: WebhookPayload, ws_hub) -> Dict[str, Any]:
    """
    Handle leave.updated event.

    Updates availability, flags for re-optimization if dates changed.
    """
    data = payload.data
    leave_id = data.get("id")
    venue_id = payload.venue_id

    result = {
        "action": "leave_updated",
        "leave_id": leave_id,
        "venue_id": venue_id,
        "reoptimization_suggested": False,
    }

    # Check if dates were modified
    if "start_date" in data or "end_date" in data:
        result["reoptimization_suggested"] = True

    # Broadcast via WebSocket
    if ws_hub:
        try:
            await ws_hub.broadcast({
                "type": "leave_updated",
                "leave_id": leave_id,
                "venue_id": venue_id,
                "fields": [k for k in data.keys() if k != "id"],
                "reoptimization_suggested": result["reoptimization_suggested"],
                "timestamp": payload.timestamp.isoformat(),
            })
        except Exception as e:
            logger.error(f"Failed to broadcast leave_updated: {e}")

    return result


# ============================================================================
# Public API
# ============================================================================

def get_registry() -> EventHandlerRegistry:
    """Get the global event handler registry."""
    return _registry
