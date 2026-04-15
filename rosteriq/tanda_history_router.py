"""REST endpoints for Tanda historical ingestion + queries (Round 8 Track B).

Endpoints:
  POST /api/v1/tanda/history/ingest
    body: {venue_id, org_id, from, to}
    OWNER access required. Pulls the date range from Tanda and stores
    rolled-up actuals.

  GET /api/v1/tanda/history/daily?venue_id=&from=&to=
    L1+ access. Returns daily aggregates inclusive of from/to.

  GET /api/v1/tanda/history/hourly?venue_id=&day=
    L1+ access. Returns hourly buckets for one day.

  GET /api/v1/tanda/history/variance?venue_id=&days=14
    L1+ access. Returns a digest comparing rostered vs worked vs revenue.

  GET /api/v1/tanda/history/status?venue_id=
    L1+ access. Returns last_ingested timestamp.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request

from rosteriq.tanda_history import (
    TandaHistoryIngestor,
    get_history_store,
    variance_summary,
)

# Auth gating — fall back to no-op in demo/sandbox when auth stack unavailable
try:
    from rosteriq.auth import require_access, AccessLevel  # type: ignore
except Exception:  # pragma: no cover — demo/sandbox path
    require_access = None  # type: ignore
    AccessLevel = None  # type: ignore


async def _gate(request: Request, level_name: str) -> None:
    if require_access is None or AccessLevel is None:
        return
    level = getattr(AccessLevel, level_name)
    await require_access(level)(request=request)


logger = logging.getLogger("rosteriq.tanda_history_router")
AU_TZ = timezone(timedelta(hours=10))

router = APIRouter(tags=["tanda_history"])


def _parse_date(s: Optional[str], default: date) -> date:
    if not s:
        return default
    try:
        return date.fromisoformat(s)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"invalid date {s!r}: {e}")


@router.post("/api/v1/tanda/history/ingest")
async def ingest_history(request: Request) -> dict:
    """Trigger an ingest run for one venue/org over a date range."""
    await _gate(request, "OWNER")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid JSON body")

    venue_id = body.get("venue_id")
    org_id = body.get("org_id") or venue_id
    from_str = body.get("from")
    to_str = body.get("to")
    if not venue_id:
        raise HTTPException(status_code=400, detail="venue_id is required")

    today = date.today()
    start = _parse_date(from_str, today - timedelta(days=14))
    end = _parse_date(to_str, today)
    if start > end:
        raise HTTPException(status_code=400, detail="from must be <= to")

    # Lazy import to keep this module loadable in sandbox
    try:
        from rosteriq.tanda_integration import get_tanda_adapter  # type: ignore
    except Exception:
        get_tanda_adapter = None  # type: ignore

    if get_tanda_adapter is None:
        # Demo path — return a stub run so the UI can wire up
        return {
            "status": "demo",
            "venue_id": venue_id,
            "from": start.isoformat(),
            "to": end.isoformat(),
            "message": "tanda adapter unavailable in demo mode",
        }

    try:
        adapter = get_tanda_adapter()
    except Exception as e:
        logger.warning("tanda adapter init failed: %s", e)
        raise HTTPException(status_code=502, detail=f"tanda adapter unavailable: {e}")

    ingestor = TandaHistoryIngestor(adapter=adapter)
    try:
        return await ingestor.ingest_range(venue_id, org_id, start, end)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("history ingest failed: %s", e)
        raise HTTPException(status_code=502, detail="ingest failed")


@router.get("/api/v1/tanda/history/daily")
async def get_daily(
    request: Request,
    venue_id: str = Query(...),
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
) -> dict:
    await _gate(request, "L1_SUPERVISOR")
    today = date.today()
    start = _parse_date(from_date, today - timedelta(days=14))
    end = _parse_date(to_date, today)
    if start > end:
        raise HTTPException(status_code=400, detail="from must be <= to")
    rows = get_history_store().daily_range(venue_id, start, end)
    return {
        "venue_id": venue_id,
        "from": start.isoformat(),
        "to": end.isoformat(),
        "count": len(rows),
        "rows": [r.to_dict() for r in rows],
    }


@router.get("/api/v1/tanda/history/hourly")
async def get_hourly(
    request: Request,
    venue_id: str = Query(...),
    day: Optional[str] = Query(None),
) -> dict:
    await _gate(request, "L1_SUPERVISOR")
    target = _parse_date(day, date.today())
    rows = get_history_store().hourly_for_day(venue_id, target)
    return {
        "venue_id": venue_id,
        "day": target.isoformat(),
        "count": len(rows),
        "rows": [r.to_dict() for r in rows],
    }


@router.get("/api/v1/tanda/history/variance")
async def get_variance(
    request: Request,
    venue_id: str = Query(...),
    days: int = Query(14, ge=1, le=120),
) -> dict:
    await _gate(request, "L1_SUPERVISOR")
    return variance_summary(venue_id, days=days)


@router.get("/api/v1/tanda/history/status")
async def get_status(
    request: Request,
    venue_id: str = Query(...),
) -> dict:
    await _gate(request, "L1_SUPERVISOR")
    last = get_history_store().last_ingested(venue_id)
    return {
        "venue_id": venue_id,
        "last_ingested": last.isoformat() if last else None,
        "venues_known": get_history_store().venues(),
    }
