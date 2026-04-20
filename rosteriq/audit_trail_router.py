"""REST API endpoints for audit trail / activity log.

Provides:
- GET /api/v1/audit/{venue_id} — Query audit log with filters
- GET /api/v1/audit/{venue_id}/entity/{entity_type}/{entity_id} — History for specific entity
- GET /api/v1/audit/{venue_id}/actor/{actor_id} — Activity by specific user
- GET /api/v1/audit/{venue_id}/summary — Aggregated stats (OWNER only)
- GET /api/v1/audit/{venue_id}/recent — Last 50 events (quick dashboard view)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    from fastapi import APIRouter, Depends, HTTPException, Request, status
    from pydantic import BaseModel, Field
except ImportError:
    APIRouter = None
    Depends = None
    HTTPException = None
    Request = None
    status = None
    BaseModel = None
    Field = None

from rosteriq.audit_trail import (
    AuditAction,
    AuditEntityType,
    AuditEntry,
    AuditQuery,
    build_audit_summary,
    format_audit_entry,
    get_actor_activity,
    get_entity_history,
    query_audit,
)

try:
    from rosteriq.auth import AccessLevel, require_access
except ImportError:
    AccessLevel = None
    require_access = None

logger = logging.getLogger("rosteriq.audit_trail_router")


# ============================================================================
# Response Models (only defined if pydantic/fastapi available)
# ============================================================================

if BaseModel is not None:

    class AuditEntryResponse(BaseModel):
        """Response model for a single audit entry."""

        entry_id: str
        venue_id: str
        timestamp: str
        actor_id: str
        actor_name: str
        action: str
        entity_type: str
        entity_id: str
        description: str
        changes: Optional[Dict[str, Any]] = None
        ip_address: Optional[str] = None
        metadata: Optional[Dict[str, Any]] = None

        class Config:
            json_schema_extra = {
                "example": {
                    "entry_id": "550e8400-e29b-41d4-a716-446655440000",
                    "venue_id": "venue-001",
                    "timestamp": "2026-04-20T14:30:00+00:00",
                    "actor_id": "user-123",
                    "actor_name": "Dale Ingvarson",
                    "action": "approve",
                    "entity_type": "leave_request",
                    "entity_id": "LR-001",
                    "description": "Approved leave request for Alice",
                    "changes": None,
                    "ip_address": "192.168.1.100",
                    "metadata": None,
                }
            }

    class AuditQueryResponse(BaseModel):
        """Response for querying audit log."""

        total_count: int = Field(..., description="Total matching entries (before pagination)")
        entries: List[AuditEntryResponse]
        limit: int
        offset: int

    class AuditSummaryResponse(BaseModel):
        """Response for audit summary endpoint."""

        venue_id: str
        period_start: str
        period_end: str
        total_entries: int
        by_action: Dict[str, int]
        by_entity_type: Dict[str, int]
        by_actor: Dict[str, int]
        most_active_actor: Optional[str]
        most_changed_entity_type: Optional[str]

        class Config:
            json_schema_extra = {
                "example": {
                    "venue_id": "venue-001",
                    "period_start": "2026-04-01T00:00:00+00:00",
                    "period_end": "2026-04-30T23:59:59+00:00",
                    "total_entries": 150,
                    "by_action": {"create": 45, "update": 60, "approve": 30, "reject": 15},
                    "by_entity_type": {"shift": 80, "leave_request": 40, "roster": 30},
                    "by_actor": {"Dale Ingvarson": 85, "Alice Manager": 65},
                    "most_active_actor": "Dale Ingvarson",
                    "most_changed_entity_type": "shift",
                }
            }

    class RecentActivityResponse(BaseModel):
        """Response for recent activity endpoint."""

        venue_id: str
        count: int
        entries: List[AuditEntryResponse]

        class Config:
            json_schema_extra = {
                "example": {
                    "venue_id": "venue-001",
                    "count": 50,
                    "entries": [],
                }
            }

    class EntityHistoryResponse(BaseModel):
        """Response for entity history endpoint."""

        entity_type: str
        entity_id: str
        count: int
        entries: List[AuditEntryResponse]

    class ActorActivityResponse(BaseModel):
        """Response for actor activity endpoint."""

        actor_id: str
        actor_name: str
        count: int
        entries: List[AuditEntryResponse]

else:
    # Stubs for when pydantic/fastapi not available
    AuditEntryResponse = None
    AuditQueryResponse = None
    AuditSummaryResponse = None
    RecentActivityResponse = None
    EntityHistoryResponse = None
    ActorActivityResponse = None


# ============================================================================
# Helper Functions
# ============================================================================


def _gate(request: Request, level_name: str) -> None:
    """
    Gate access to a resource based on auth level.

    In demo mode (no auth), allows all access.
    In live mode, requires valid JWT with appropriate level.

    Args:
        request: FastAPI request
        level_name: "l1", "l2", or "owner"

    Raises:
        HTTPException: If unauthorized
    """
    try:
        from rosteriq.auth import AUTH_ENABLED, _get_user_from_request
    except ImportError:
        return

    if not AUTH_ENABLED:
        return  # Demo mode: allow all

    user = _get_user_from_request(request)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")

    level_rank = {"l1": 1, "l2": 2, "owner": 3}
    user_rank = level_rank.get(user.access_level.value, 0)
    required_rank = level_rank.get(level_name, 0)

    if user_rank < required_rank:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient access level")


def _entry_to_response(entry: AuditEntry) -> AuditEntryResponse:
    """Convert AuditEntry to response model."""
    return AuditEntryResponse(
        entry_id=entry.entry_id,
        venue_id=entry.venue_id,
        timestamp=entry.timestamp.isoformat(),
        actor_id=entry.actor_id,
        actor_name=entry.actor_name,
        action=entry.action.value,
        entity_type=entry.entity_type.value,
        entity_id=entry.entity_id,
        description=entry.description,
        changes=entry.changes,
        ip_address=entry.ip_address,
        metadata=entry.metadata,
    )


# ============================================================================
# Router (only wired if fastapi available)
# ============================================================================

if APIRouter is not None:

    audit_trail_router = APIRouter(prefix="/api/v1/audit", tags=["audit"])

    @audit_trail_router.get("/{venue_id}", response_model=AuditQueryResponse)
    async def query_audit_log(
        venue_id: str,
        actor_id: Optional[str] = None,
        entity_type: Optional[str] = None,
        entity_id: Optional[str] = None,
        action: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        request: Request = None,
    ) -> AuditQueryResponse:
        """
        Query audit log with filters and pagination.

        Requires: L2 (Roster Maker) or higher

        Query Parameters:
        - actor_id: Filter by actor/user ID (optional)
        - entity_type: Filter by entity type (optional)
        - entity_id: Filter by entity ID (optional)
        - action: Filter by action (optional)
        - date_from: ISO timestamp for start date (optional)
        - date_to: ISO timestamp for end date (optional)
        - limit: Number of entries to return (default 100, max 1000)
        - offset: Number of entries to skip (default 0)

        Returns:
            List of audit entries matching filters, with pagination info
        """
        _gate(request, "l2")

        if limit > 1000:
            limit = 1000

        # Parse optional filters
        entity_type_enum = None
        if entity_type:
            try:
                entity_type_enum = AuditEntityType(entity_type)
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid entity_type '{entity_type}'",
                )

        action_enum = None
        if action:
            try:
                action_enum = AuditAction(action)
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid action '{action}'",
                )

        date_from_dt = None
        if date_from:
            try:
                date_from_dt = datetime.fromisoformat(date_from)
                if date_from_dt.tzinfo is None:
                    date_from_dt = date_from_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid date_from format (use ISO 8601)",
                )

        date_to_dt = None
        if date_to:
            try:
                date_to_dt = datetime.fromisoformat(date_to)
                if date_to_dt.tzinfo is None:
                    date_to_dt = date_to_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid date_to format (use ISO 8601)",
                )

        # Build query
        query = AuditQuery(
            venue_id=venue_id,
            actor_id=actor_id,
            entity_type=entity_type_enum,
            entity_id=entity_id,
            action=action_enum,
            date_from=date_from_dt,
            date_to=date_to_dt,
            limit=limit,
            offset=offset,
        )

        # Execute query
        entries = query_audit(query)

        return AuditQueryResponse(
            total_count=len(entries) + offset,
            entries=[_entry_to_response(e) for e in entries],
            limit=limit,
            offset=offset,
        )

    @audit_trail_router.get("/{venue_id}/entity/{entity_type}/{entity_id}", response_model=EntityHistoryResponse)
    async def get_entity_audit_history(
        venue_id: str,
        entity_type: str,
        entity_id: str,
        request: Request = None,
    ) -> EntityHistoryResponse:
        """
        Get audit history for a specific entity.

        Requires: L2 (Roster Maker) or higher

        Args:
            venue_id: Venue identifier
            entity_type: Type of entity (roster, shift, employee, etc.)
            entity_id: ID of the entity

        Returns:
            All audit entries for this entity, chronologically ordered
        """
        _gate(request, "l2")

        try:
            entity_type_enum = AuditEntityType(entity_type)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid entity_type '{entity_type}'",
            )

        entries = get_entity_history(entity_type_enum, entity_id)
        # Filter by venue_id
        entries = [e for e in entries if e.venue_id == venue_id]

        return EntityHistoryResponse(
            entity_type=entity_type,
            entity_id=entity_id,
            count=len(entries),
            entries=[_entry_to_response(e) for e in entries],
        )

    @audit_trail_router.get("/{venue_id}/actor/{actor_id}", response_model=ActorActivityResponse)
    async def get_actor_audit_activity(
        venue_id: str,
        actor_id: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        request: Request = None,
    ) -> ActorActivityResponse:
        """
        Get audit activity for a specific user/actor.

        Requires: L2 (Roster Maker) or higher

        Args:
            venue_id: Venue identifier
            actor_id: ID of the actor/user
            date_from: Optional ISO timestamp for start date
            date_to: Optional ISO timestamp for end date

        Returns:
            All audit entries for this actor
        """
        _gate(request, "l2")

        date_from_dt = None
        if date_from:
            try:
                date_from_dt = datetime.fromisoformat(date_from)
                if date_from_dt.tzinfo is None:
                    date_from_dt = date_from_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid date_from format (use ISO 8601)",
                )

        date_to_dt = None
        if date_to:
            try:
                date_to_dt = datetime.fromisoformat(date_to)
                if date_to_dt.tzinfo is None:
                    date_to_dt = date_to_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid date_to format (use ISO 8601)",
                )

        entries = get_actor_activity(actor_id, date_from_dt, date_to_dt)
        # Filter by venue_id
        entries = [e for e in entries if e.venue_id == venue_id]

        # Get actor name from entries (if any)
        actor_name = entries[0].actor_name if entries else actor_id

        return ActorActivityResponse(
            actor_id=actor_id,
            actor_name=actor_name,
            count=len(entries),
            entries=[_entry_to_response(e) for e in entries],
        )

    @audit_trail_router.get("/{venue_id}/summary", response_model=AuditSummaryResponse)
    async def get_audit_summary(
        venue_id: str,
        date_from: str,
        date_to: str,
        request: Request = None,
    ) -> AuditSummaryResponse:
        """
        Get aggregated audit statistics for a period.

        Requires: OWNER access level

        Query Parameters:
        - date_from: ISO timestamp for start date (required)
        - date_to: ISO timestamp for end date (required)

        Returns:
            Summary with counts by action, entity type, actor, and most active/changed
        """
        _gate(request, "owner")

        try:
            date_from_dt = datetime.fromisoformat(date_from)
            if date_from_dt.tzinfo is None:
                date_from_dt = date_from_dt.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid date_from format (use ISO 8601)",
            )

        try:
            date_to_dt = datetime.fromisoformat(date_to)
            if date_to_dt.tzinfo is None:
                date_to_dt = date_to_dt.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid date_to format (use ISO 8601)",
            )

        summary = build_audit_summary(venue_id, date_from_dt, date_to_dt)

        return AuditSummaryResponse(
            venue_id=summary.venue_id,
            period_start=summary.period_start.isoformat(),
            period_end=summary.period_end.isoformat(),
            total_entries=summary.total_entries,
            by_action=summary.by_action,
            by_entity_type=summary.by_entity_type,
            by_actor=summary.by_actor,
            most_active_actor=summary.most_active_actor,
            most_changed_entity_type=summary.most_changed_entity_type,
        )

    @audit_trail_router.get("/{venue_id}/recent", response_model=RecentActivityResponse)
    async def get_recent_activity(
        venue_id: str,
        limit: int = 50,
        request: Request = None,
    ) -> RecentActivityResponse:
        """
        Get recent audit entries (quick dashboard view).

        Requires: L1 (Supervisor) or higher

        Query Parameters:
        - limit: Number of recent entries to return (default 50, max 200)

        Returns:
            Last N audit entries for this venue, newest first
        """
        _gate(request, "l1")

        if limit > 200:
            limit = 200

        query = AuditQuery(
            venue_id=venue_id,
            limit=limit,
            offset=0,
        )
        entries = query_audit(query)
        # Reverse to get newest first
        entries = list(reversed(entries[-limit:]))

        return RecentActivityResponse(
            venue_id=venue_id,
            count=len(entries),
            entries=[_entry_to_response(e) for e in entries],
        )

else:
    # Stub when fastapi not available
    audit_trail_router = None
