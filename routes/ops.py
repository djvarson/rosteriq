"""
Night-time tools for the duty manager, behind the On-Shift page.

    GET /api/sos?venue_id&date&start&end&area&include_unavailable&include_salaried
        who can I call in (services/sos)
    GET /api/ops/cut-advice?venue_id&area&takings&covers
        cut or hold, right now (services/cut_advice)

Managers of the venue only; a department manager only for their own areas
(with no area picked they get their areas, never the whole venue). Every
answer shown is kept in the event log with its inputs and the viewer.
"""

from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from rosteriq.database import get_db
from rosteriq.middleware.tenant import (
    enforce_section_manager,
    enforce_venue_manager_any,
    get_tenant_context_optional,
)
from rosteriq.services import cut_advice, sos

router = APIRouter(tags=["ops"])


def _scope(venue_id: str, area: Optional[str]) -> list:
    """Areas this request may look at: the one asked for, else the caller's
    own sections (department manager), else [] = the whole venue."""
    enforce_venue_manager_any(venue_id)
    area = (area or "").strip()
    if area and area.lower() != "all":
        enforce_section_manager(venue_id, area)
        return [area]
    sections = sos.viewer_sections(venue_id)
    if sections is None:
        return []
    if not sections:
        raise HTTPException(status_code=403, detail="You don't manage any area of this venue.")
    return sections


def _viewer() -> Optional[str]:
    tenant = get_tenant_context_optional()
    return getattr(tenant, "user_id", None) if tenant else None


@router.get("/api/sos")
async def sos_search(
    venue_id: str = Query(...),
    date_: Optional[str] = Query(None, alias="date", description="YYYY-MM-DD; default today"),
    start: Optional[str] = Query(None, description="HH:MM; default now (rounded up to the half hour)"),
    end: Optional[str] = Query(None, description="HH:MM; default the venue's close"),
    area: Optional[str] = Query(None),
    include_unavailable: bool = Query(False),
    include_salaried: bool = Query(False),
) -> dict:
    areas = _scope(venue_id, area)
    try:
        day = date.fromisoformat(date_) if date_ else None
    except ValueError:
        raise HTTPException(status_code=422, detail="date must be YYYY-MM-DD")
    db = get_db()
    try:
        result = sos.find_cover(db, venue_id, day=day, start=start, end=end, areas=areas,
                                include_unavailable=include_unavailable,
                                include_salaried=include_salaried)
    except LookupError:
        raise HTTPException(status_code=404, detail="Venue not found")
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    sos.record_shown(venue_id, {
        "date": date_, "start": start, "end": end, "area": area or "all",
        "include_unavailable": include_unavailable, "include_salaried": include_salaried,
    }, result, source="on_shift", user_id=_viewer())
    return result


@router.get("/api/ops/cut-advice")
async def cut_or_hold(
    venue_id: str = Query(...),
    area: Optional[str] = Query(None),
    takings: Optional[float] = Query(None, ge=0, le=1_000_000,
                                     description="Last hour's takings in AUD, inc GST"),
    covers: Optional[int] = Query(None, ge=0, le=100_000),
) -> dict:
    areas = _scope(venue_id, area)
    db = get_db()
    try:
        result = await cut_advice.advise(db, venue_id, areas=areas, takings=takings, covers=covers)
    except LookupError:
        raise HTTPException(status_code=404, detail="Venue not found")
    cut_advice.record_shown(venue_id, {"area": area or "all", "takings": takings, "covers": covers},
                            result, source="on_shift", user_id=_viewer())
    return result
