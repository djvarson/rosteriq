"""
Generic booking ingestion — for any booking/ticketing system without a native
adapter (the pilot's theatre/booking program, or a venue's own system).

  POST /api/reservations/direct/ingest   — JSON {venue_id, bookings:[{date, party_size, time?}]}
  POST /api/reservations/direct/upload    — {venue_id, csv_data(base64)} with columns date, party_size|covers, time?
  GET  /api/reservations/direct/signals   — venue_id,start,end -> demand signal derived from stored bookings

Bookings are PERSISTED (database.save_direct_bookings) so they survive restarts and
feed forecasting via the same DirectBookingImporter signal path the in-memory
adapter uses — closing the gap where the importer existed but was never wired up.
"""

import base64
import csv
import io
import logging
from datetime import date, datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from rosteriq.database import BaseStore, get_db
from rosteriq.data_feeds.reservations import DirectBookingImporter
from rosteriq.data_feeds.base import Location

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/reservations/direct", tags=["reservations"])


class DirectBooking(BaseModel):
    date: str = Field(..., description="Booking date, YYYY-MM-DD")
    party_size: float = Field(default=0, description="Number of covers/guests")
    time: Optional[str] = Field(default=None, description="Optional time, HH:MM")


class IngestRequest(BaseModel):
    venue_id: str
    bookings: List[DirectBooking]


class UploadRequest(BaseModel):
    venue_id: str
    csv_data: str = Field(..., description="Base64-encoded CSV with columns: date, party_size (or covers), time (optional)")


def _normalise(b: dict) -> Optional[dict]:
    """Coerce a raw booking dict to {date, party_size, time} or None if unusable."""
    raw_date = (b.get("date") or "").strip() if isinstance(b.get("date"), str) else b.get("date")
    if not raw_date:
        return None
    # Validate the date parses (accept ISO date or datetime).
    try:
        if isinstance(raw_date, str):
            datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
        iso = str(raw_date)[:10]
    except ValueError:
        try:
            iso = datetime.strptime(str(raw_date), "%Y-%m-%d").date().isoformat()
        except ValueError:
            return None
    party = b.get("party_size")
    if party in (None, ""):
        party = b.get("covers")
    try:
        party = float(party) if party not in (None, "") else 0.0
    except (ValueError, TypeError):
        party = 0.0
    time_val = b.get("time") or None
    return {"date": iso, "party_size": party, "time": time_val}


@router.post("/ingest")
async def ingest(body: IngestRequest, db: BaseStore = Depends(get_db)):
    """Ingest bookings as JSON (e.g. from a venue's booking-system webhook)."""
    rows = [_normalise(b.model_dump()) for b in body.bookings]
    rows = [r for r in rows if r]
    if not rows:
        raise HTTPException(status_code=400, detail="No valid bookings (need at least a date per row)")
    stored = db.save_direct_bookings(body.venue_id, rows)
    total = db.count_direct_bookings(body.venue_id)
    return {"status": "success", "stored": stored, "total_bookings": total}


@router.post("/upload")
async def upload(body: UploadRequest, db: BaseStore = Depends(get_db)):
    """Ingest bookings from a base64-encoded CSV (date, party_size|covers, time?)."""
    try:
        decoded = base64.b64decode(body.csv_data).decode("utf-8-sig")
    except Exception:
        raise HTTPException(status_code=400, detail="csv_data must be valid base64-encoded UTF-8 text")

    reader = csv.DictReader(io.StringIO(decoded))
    if not reader.fieldnames:
        raise HTTPException(status_code=400, detail="CSV has no header row")
    # Case-insensitive header lookup.
    lower = {(f or "").strip().lower(): f for f in reader.fieldnames}
    date_col = lower.get("date")
    party_col = lower.get("party_size") or lower.get("covers") or lower.get("guests") or lower.get("pax")
    time_col = lower.get("time")
    if not date_col:
        raise HTTPException(status_code=400, detail="CSV must have a 'date' column")

    rows = []
    for r in reader:
        row = _normalise({
            "date": r.get(date_col),
            "party_size": r.get(party_col) if party_col else None,
            "time": r.get(time_col) if time_col else None,
        })
        if row:
            rows.append(row)
    if not rows:
        raise HTTPException(status_code=400, detail="No valid rows parsed from CSV")

    stored = db.save_direct_bookings(body.venue_id, rows)
    total = db.count_direct_bookings(body.venue_id)
    return {"status": "success", "stored": stored, "rows_seen": stored, "total_bookings": total}


@router.get("/signals")
async def signals(
    venue_id: str = Query(...),
    start: str = Query(..., description="Start date YYYY-MM-DD"),
    end: str = Query(..., description="End date YYYY-MM-DD"),
    db: BaseStore = Depends(get_db),
):
    """Demand signals derived from the venue's stored direct bookings."""
    try:
        start_d = date.fromisoformat(start)
        end_d = date.fromisoformat(end)
    except ValueError:
        raise HTTPException(status_code=400, detail="start/end must be YYYY-MM-DD")

    bookings = db.get_direct_bookings(venue_id, start, end)
    if not bookings:
        return {"venue_id": venue_id, "signals": [], "total_bookings": 0}

    importer = DirectBookingImporter()
    importer.ingest_bookings(venue_id, bookings)
    feed_signals = await importer.fetch_signals(Location(latitude=0, longitude=0), start_d, end_d, venue_id)

    out = []
    for s in feed_signals:
        snap = (s.raw_data or {}).get("snapshot", {})
        out.append({
            "date": getattr(s, "signal_date", None).isoformat() if getattr(s, "signal_date", None) else None,
            "covers": snap.get("total_covers"),
            "bookings": snap.get("total_bookings"),
            "confidence": getattr(s, "confidence", None),
        })
    return {"venue_id": venue_id, "signals": out, "total_bookings": len(bookings)}
