"""
Business snapshot — the one screen a venue owner opens every morning.

Joins the two halves of the Venue OS that already exist and are verified:
labour (from the roster) and trade (from dish sales) into the ratios that
actually run a hospitality business:

    food cost %  = COGS / net sales           (kitchen efficiency)
    labour %     = wages / net sales          (the roster's real question)
    prime cost % = (labour + COGS) / net sales   (the number that makes or
                   breaks a venue — target ~60-65%)

Honesty rules baked in:
- Net sales are ex-GST (you don't keep the GST), and the report says so.
- Labour is ROSTERED cost (what the plan will cost), clearly labelled —
  never dressed up as actual paid wages. Actuals come from the payroll
  batch once timesheets are approved; this is the live planning view.
- Divide-by-zero (no sales yet) yields null ratios, not a fake 0% or a
  crash.

Route:
    GET /api/snapshot?venue_id=&start_date=&end_date=  (default: last 7 days)
"""

import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_venue_access
from rosteriq.routes.menu_costing import GST_RATE

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/snapshot", tags=["snapshot"])

# Prime cost rule-of-thumb bands for AU hospitality (net sales basis).
_PRIME_GOOD = 60.0
_PRIME_WATCH = 65.0


def _shift_paid_minutes(shift) -> int:
    start = datetime.combine(shift.date, shift.start_time)
    end = datetime.combine(shift.date, shift.end_time)
    if end <= start:
        end += timedelta(days=1)  # overnight
    return max(0, int((end - start).total_seconds() // 60) - int(getattr(shift, "break_minutes", 0) or 0))


def _labour_by_day(db, venue_id: str, start: date, end: date) -> dict:
    """Rostered labour $ per day. Prefers each shift's stored cost; falls back
    to paid hours x the employee's base rate so a shift with no cached cost
    still counts (never silently zero)."""
    rates = {}
    for e in (db.get_employees(venue_id) or []):
        try:
            rates[e.id] = float(e.hourly_base_rate)
        except Exception:
            rates[e.id] = 0.0
    by_day: dict = {}
    try:
        rosters = db.get_rosters_by_date_range(venue_id, start, end) or []
    except Exception as ex:
        logger.warning(f"Roster lookup failed for {venue_id}: {ex}")
        rosters = []
    for roster in rosters:
        for s in roster.shifts:
            if not (start <= s.date <= end):
                continue
            cost = s.cost
            if cost is not None:
                cost = float(cost)
            else:
                cost = round(_shift_paid_minutes(s) / 60 * rates.get(s.employee_id, 0.0), 2)
            by_day[s.date.isoformat()] = round(by_day.get(s.date.isoformat(), 0.0) + cost, 2)
    return by_day


@router.get("")
async def business_snapshot(venue_id: str = Query(...),
                            start_date: Optional[date] = Query(None),
                            end_date: Optional[date] = Query(None)) -> dict:
    enforce_venue_access(venue_id)
    db = get_db()
    end = end_date or date.today()
    start = start_date or (end - timedelta(days=6))
    if start > end:
        raise HTTPException(status_code=422, detail="start_date is after end_date")

    # Trade: revenue + COGS from recorded/imported sales
    sales = db.list_dish_sales(venue_id, start, end) or []
    rev_by_day: dict = {}
    cogs_by_day: dict = {}
    for s in sales:
        d = str(s.get("sale_date"))[:10]
        rev_by_day[d] = round(rev_by_day.get(d, 0.0) + float(s.get("revenue_inc_gst") or 0), 2)
        cogs_by_day[d] = round(cogs_by_day.get(d, 0.0) + float(s.get("cogs") or 0), 2)

    labour_by_day = _labour_by_day(db, venue_id, start, end)

    total_rev_inc = round(sum(rev_by_day.values()), 2)
    total_rev_net = round(total_rev_inc / (1 + GST_RATE), 2)
    total_cogs = round(sum(cogs_by_day.values()), 2)
    total_labour = round(sum(labour_by_day.values()), 2)

    def _pct(part: float) -> Optional[float]:
        if total_rev_net <= 0:
            return None  # no sales -> ratio is undefined, not a fake 0
        return round(part / total_rev_net * 100, 1)

    prime = round(total_labour + total_cogs, 2)
    prime_pct = _pct(prime)
    if prime_pct is None:
        prime_band = "no_sales"
    elif prime_pct <= _PRIME_GOOD:
        prime_band = "healthy"
    elif prime_pct <= _PRIME_WATCH:
        prime_band = "watch"
    else:
        prime_band = "high"

    # Per-day series across the full window (zero-filled) for the trend row
    days = []
    cur = start
    while cur <= end:
        k = cur.isoformat()
        rev_inc = rev_by_day.get(k, 0.0)
        net = round(rev_inc / (1 + GST_RATE), 2)
        lab = labour_by_day.get(k, 0.0)
        cog = cogs_by_day.get(k, 0.0)
        days.append({
            "date": k,
            "revenue_inc_gst": round(rev_inc, 2),
            "revenue_net": net,
            "labour": round(lab, 2),
            "cogs": round(cog, 2),
            "prime_pct": (round((lab + cog) / net * 100, 1) if net > 0 else None),
        })
        cur += timedelta(days=1)

    return {
        "venue_id": venue_id,
        "from": start.isoformat(),
        "to": end.isoformat(),
        "labour_basis": "rostered",  # planning view; actuals via payroll batch
        "net_sales_basis": "ex_gst",
        "revenue_inc_gst": total_rev_inc,
        "net_sales": total_rev_net,
        "cogs": total_cogs,
        "labour": total_labour,
        "gross_profit": round(total_rev_net - total_cogs - total_labour, 2),
        "food_cost_pct": _pct(total_cogs),
        "labour_pct": _pct(total_labour),
        "prime_cost_pct": prime_pct,
        "prime_cost_band": prime_band,
        "days": days,
    }
