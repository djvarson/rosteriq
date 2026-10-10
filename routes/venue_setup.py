"""
Areas & minimums — what the Settings card and the setup wizard's areas step
read before they save through POST /venues.

    GET /api/venue-setup?venue_id=
        areas            the venue's areas with per-weekday minimums;
                         follows_demand_set says whether follows_demand is
                         the venue's own setting or the default
        trading_hours    the hours the venue recorded
        effective_hours  every weekday's hours as forecasting uses them, with
                         their source (recorded / roster history / default)
        candidates       the staff skills and imported shift roles to offer as
                         areas, with how many staff and shifts carry each
"""

from fastapi import APIRouter, HTTPException, Query

from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_venue_manager
from rosteriq.services.clock import venue_today
from rosteriq.services.trading_hours import DEFAULT_CLOSE, DEFAULT_OPEN, venue_trading_hours
from rosteriq.services.venue_areas import DAY_KEYS, venue_areas
from rosteriq.services.venue_setup import area_candidates

router = APIRouter(prefix="/api/venue-setup", tags=["venue-setup"])


@router.get("")
async def venue_setup(venue_id: str = Query(...)) -> dict:
    enforce_venue_manager(venue_id)
    db = get_db()
    venue = db.get_venue(venue_id)
    if venue is None:
        raise HTTPException(404, "Venue not found")
    today = venue_today(venue_id, db)
    recorded = {" ".join(str(a.get("name", "")).lower().split()) for a in (venue.areas or [])
                if isinstance(a, dict) and a.get("follows_demand") is not None}
    areas = venue_areas(venue)
    for a in areas:
        a["follows_demand_set"] = a["name"] in recorded
    return {
        "venue_id": venue_id,
        "name": venue.name,
        "days": list(DAY_KEYS),
        "areas": areas,
        "trading_hours": dict(venue.trading_hours or {}),
        "effective_hours": venue_trading_hours(db, venue, today),
        "default_hours": {"open": DEFAULT_OPEN, "close": DEFAULT_CLOSE},
        "candidates": area_candidates(db, venue_id, today),
    }
