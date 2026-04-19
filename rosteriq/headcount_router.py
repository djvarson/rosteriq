"""FastAPI router for headcount clicker and shift notes.

Endpoints:
- POST   /api/v1/headcount/{venue_id}           — record a headcount tap
- GET    /api/v1/headcount/{venue_id}           — list headcount entries (by shift)
- GET    /api/v1/headcount/{venue_id}/latest    — get latest headcount for venue
- POST   /api/v1/shifts/{venue_id}/notes        — add a shift note
- GET    /api/v1/shifts/{venue_id}/notes        — list shift notes (by tag/shift)

All endpoints require L1+ access (duty managers + roster makers).
"""

from __future__ import annotations

import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

# Auth gating — fall back to no-op in demo/sandbox when auth stack unavailable
try:
    from rosteriq.auth import require_access, AccessLevel  # type: ignore
except Exception:  # pragma: no cover — demo/sandbox path
    require_access = None  # type: ignore
    AccessLevel = None  # type: ignore

logger = logging.getLogger("rosteriq.headcount_router")

router = APIRouter(prefix="/api/v1", tags=["headcount", "shift-notes"])


async def _gate(request: Request, level_name: str) -> None:
    """Apply role gating if auth stack is present; no-op in demo."""
    if require_access is None or AccessLevel is None:
        return
    level = getattr(AccessLevel, level_name)
    await require_access(level)(request=request)


# ---------------------------------------------------------------------------
# Pydantic Models
# ---------------------------------------------------------------------------


class HeadcountRecordRequest(BaseModel):
    """Body for POST /api/v1/headcount/{venue_id}."""

    shift_id: str = Field(..., description="Shift ID")
    count: int = Field(..., description="Current patron count")
    note: Optional[str] = Field(None, description="Optional note on the count")


class HeadcountEntryResponse(BaseModel):
    """Single headcount entry response."""

    entry_id: str
    venue_id: str
    shift_id: str
    count: int
    delta: int
    recorded_at: str
    recorded_by: str
    note: Optional[str]


class HeadcountListResponse(BaseModel):
    """List of headcount entries."""

    entries: List[HeadcountEntryResponse]
    venue_id: str
    count: int


class HeadcountLatestResponse(BaseModel):
    """Latest headcount entry for a venue."""

    entry: Optional[HeadcountEntryResponse]
    venue_id: str


class ShiftNoteAddRequest(BaseModel):
    """Body for POST /api/v1/shifts/{venue_id}/notes."""

    shift_id: str = Field(..., description="Shift ID")
    content: str = Field(..., description="Note content")
    tags: List[str] = Field(
        default_factory=list,
        description="Tags: weather, event, staffing, incident, etc.",
    )


class ShiftNoteResponse(BaseModel):
    """Single shift note response."""

    note_id: str
    venue_id: str
    shift_id: str
    author_id: str
    author_name: str
    content: str
    tags: List[str]
    created_at: str


class ShiftNoteListResponse(BaseModel):
    """List of shift notes."""

    notes: List[ShiftNoteResponse]
    venue_id: str
    count: int


# ---------------------------------------------------------------------------
# Headcount Endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/headcount/{venue_id}",
    response_model=HeadcountEntryResponse,
    summary="Record a headcount tap",
)
async def record_headcount(
    venue_id: str, body: HeadcountRecordRequest, request: Request
) -> HeadcountEntryResponse:
    """Record a patron count tap. Returns the entry with delta."""
    await _gate(request, "L1_DUTY_MANAGER")

    from rosteriq.headcount import get_headcount_store

    store = get_headcount_store()

    # Get recorded_by from request (auth context)
    recorded_by = "unknown"
    try:
        # Try to extract from auth context if available
        if hasattr(request.state, "user_id"):
            recorded_by = request.state.user_id
        elif hasattr(request.state, "user"):
            recorded_by = getattr(request.state.user, "id", "unknown")
    except Exception:
        pass

    entry = store.record(
        venue_id=venue_id,
        shift_id=body.shift_id,
        count=body.count,
        recorded_by=recorded_by,
        note=body.note,
    )

    return HeadcountEntryResponse(
        entry_id=entry.entry_id,
        venue_id=entry.venue_id,
        shift_id=entry.shift_id,
        count=entry.count,
        delta=entry.delta,
        recorded_at=entry.recorded_at.isoformat(),
        recorded_by=entry.recorded_by,
        note=entry.note,
    )


@router.get(
    "/headcount/{venue_id}",
    response_model=HeadcountListResponse,
    summary="List headcount entries",
)
async def list_headcount(
    venue_id: str,
    shift_id: Optional[str] = None,
    limit: int = 100,
    request: Optional[Request] = None,
) -> HeadcountListResponse:
    """List headcount entries for a venue. Optionally filter by shift_id."""
    if request:
        await _gate(request, "L1_DUTY_MANAGER")

    from rosteriq.headcount import get_headcount_store

    store = get_headcount_store()

    if shift_id:
        entries = store.get_shift_entries(shift_id)
    else:
        entries = store.get_venue_entries(venue_id, limit=limit)

    return HeadcountListResponse(
        entries=[
            HeadcountEntryResponse(
                entry_id=e.entry_id,
                venue_id=e.venue_id,
                shift_id=e.shift_id,
                count=e.count,
                delta=e.delta,
                recorded_at=e.recorded_at.isoformat(),
                recorded_by=e.recorded_by,
                note=e.note,
            )
            for e in entries
        ],
        venue_id=venue_id,
        count=len(entries),
    )


@router.get(
    "/headcount/{venue_id}/latest",
    response_model=HeadcountLatestResponse,
    summary="Get latest headcount",
)
async def get_latest_headcount(
    venue_id: str, request: Optional[Request] = None
) -> HeadcountLatestResponse:
    """Get the latest headcount entry for a venue."""
    if request:
        await _gate(request, "L1_DUTY_MANAGER")

    from rosteriq.headcount import get_headcount_store

    store = get_headcount_store()
    entry = store.get_latest(venue_id)

    return HeadcountLatestResponse(
        entry=HeadcountEntryResponse(
            entry_id=entry.entry_id,
            venue_id=entry.venue_id,
            shift_id=entry.shift_id,
            count=entry.count,
            delta=entry.delta,
            recorded_at=entry.recorded_at.isoformat(),
            recorded_by=entry.recorded_by,
            note=entry.note,
        ) if entry else None,
        venue_id=venue_id,
    )


# ---------------------------------------------------------------------------
# Shift Note Endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/shifts/{venue_id}/notes",
    response_model=ShiftNoteResponse,
    summary="Add a shift note",
)
async def add_shift_note(
    venue_id: str, body: ShiftNoteAddRequest, request: Request
) -> ShiftNoteResponse:
    """Add an end-of-shift observation. Returns the note."""
    await _gate(request, "L1_DUTY_MANAGER")

    from rosteriq.headcount import get_shift_note_store

    store = get_shift_note_store()

    # Get author from request (auth context)
    author_id = "unknown"
    author_name = "Unknown"
    try:
        if hasattr(request.state, "user_id"):
            author_id = request.state.user_id
        if hasattr(request.state, "user"):
            author_name = getattr(request.state.user, "name", "Unknown")
    except Exception:
        pass

    note = store.add(
        venue_id=venue_id,
        shift_id=body.shift_id,
        author_id=author_id,
        author_name=author_name,
        content=body.content,
        tags=body.tags,
    )

    return ShiftNoteResponse(
        note_id=note.note_id,
        venue_id=note.venue_id,
        shift_id=note.shift_id,
        author_id=note.author_id,
        author_name=note.author_name,
        content=note.content,
        tags=note.tags,
        created_at=note.created_at.isoformat(),
    )


@router.get(
    "/shifts/{venue_id}/notes",
    response_model=ShiftNoteListResponse,
    summary="List shift notes",
)
async def list_shift_notes(
    venue_id: str,
    shift_id: Optional[str] = None,
    tag: Optional[str] = None,
    limit: int = 50,
    request: Optional[Request] = None,
) -> ShiftNoteListResponse:
    """List shift notes for a venue. Optionally filter by shift_id or tag."""
    if request:
        await _gate(request, "L1_DUTY_MANAGER")

    from rosteriq.headcount import get_shift_note_store

    store = get_shift_note_store()

    if tag:
        notes = store.search_by_tag(venue_id, tag)
    elif shift_id:
        notes = store.get_shift_notes(shift_id)
    else:
        notes = store.get_venue_notes(venue_id, limit=limit)

    return ShiftNoteListResponse(
        notes=[
            ShiftNoteResponse(
                note_id=n.note_id,
                venue_id=n.venue_id,
                shift_id=n.shift_id,
                author_id=n.author_id,
                author_name=n.author_name,
                content=n.content,
                tags=n.tags,
                created_at=n.created_at.isoformat(),
            )
            for n in notes
        ],
        venue_id=venue_id,
        count=len(notes),
    )
