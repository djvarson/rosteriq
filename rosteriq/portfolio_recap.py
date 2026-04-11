"""Multi-venue portfolio roll-up for group operators (Moment 9 / Tier 3).

Takes a list of per-venue shift recaps (each already produced by
``rosteriq.shift_recap.compose_recap``) and rolls them into a single
portfolio view with:

* Aggregated revenue, wages, wage %, peak head count, and accountability
* A worst-of traffic light across all venues
* A one-line English summary deterministic enough to use in a report
* A per-venue mini-recap array for the dashboard's sub-cards

Pure stdlib. No FastAPI, no Pydantic, no IO. Tests live in
``tests/test_portfolio_recap.py``.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

LIGHT_RANK = {"green": 0, "amber": 1, "red": 2, "unknown": -1}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe(d: Dict[str, Any], *path: str, default: float = 0.0) -> float:
    """Walk a nested dict path, returning ``default`` if any hop is missing
    or not castable to float."""
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
    if cur is None:
        return default
    try:
        return float(cur)
    except (TypeError, ValueError):
        return default


def _fmt_money(n: float) -> str:
    n = float(n or 0.0)
    if abs(n) >= 1000:
        return f"${n / 1000:.1f}k"
    return f"${round(n):,.0f}"


def _fmt_pct(decimal: float, *, signed: bool = False) -> str:
    pts = (decimal or 0.0) * 100
    sign = "+" if (signed and pts >= 0) else ""
    return f"{sign}{pts:.1f}%"


def _worst_light(lights: Iterable[str]) -> str:
    """Return the highest-severity light from a list (worst-of).
    Unknown lights do not count toward the worst-of — an all-unknown
    portfolio returns 'unknown'."""
    ranks = [LIGHT_RANK.get((l or "").lower(), -1) for l in lights]
    known = [r for r in ranks if r >= 0]
    if not known:
        return "unknown"
    top = max(known)
    for name, rank in LIGHT_RANK.items():
        if rank == top:
            return name
    return "unknown"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Totals + accountability aggregation
# ---------------------------------------------------------------------------

def aggregate_totals(venue_recaps: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Sum revenue, wages, headcount across venues and derive group-level %s.

    Returns a dict shaped like a single-venue recap's numeric block so
    the dashboard can reuse its KPI tile renderer unchanged.
    """
    revenue_actual = 0.0
    revenue_forecast = 0.0
    wages_actual = 0.0
    wages_forecast = 0.0
    # For wage target we compute a weighted avg by forecast revenue so a
    # $50k venue at 28% and a $5k venue at 25% roll up as a sensible
    # 27.7%-ish target rather than a naive mean.
    weighted_target_num = 0.0
    weighted_target_den = 0.0
    peak_headcount = 0
    venue_count = 0

    for r in venue_recaps:
        venue_count += 1
        revenue_actual += _safe(r, "revenue", "actual")
        revenue_forecast += _safe(r, "revenue", "forecast")
        wages_actual += _safe(r, "wages", "actual")
        wages_forecast += _safe(r, "wages", "forecast")
        tgt = _safe(r, "wages", "pct_of_revenue_target")
        fc = _safe(r, "revenue", "forecast")
        if tgt > 0 and fc > 0:
            weighted_target_num += tgt * fc
            weighted_target_den += fc
        hc = _safe(r, "headcount", "peak")
        if hc > peak_headcount:
            peak_headcount = int(hc)

    revenue_delta = revenue_actual - revenue_forecast
    revenue_delta_pct = (revenue_delta / revenue_forecast) if revenue_forecast > 0 else 0.0
    wage_pct_actual = (wages_actual / revenue_actual) if revenue_actual > 0 else 0.0
    wage_pct_target = (
        weighted_target_num / weighted_target_den
        if weighted_target_den > 0
        else 0.0
    )
    wage_pct_delta = wage_pct_actual - wage_pct_target

    return {
        "venue_count": venue_count,
        "revenue": {
            "actual": round(revenue_actual, 2),
            "forecast": round(revenue_forecast, 2),
            "delta": round(revenue_delta, 2),
            "delta_pct": round(revenue_delta_pct, 4),
        },
        "wages": {
            "actual": round(wages_actual, 2),
            "forecast": round(wages_forecast, 2),
            "pct_of_revenue_actual": round(wage_pct_actual, 4),
            "pct_of_revenue_target": round(wage_pct_target, 4),
            "pct_delta": round(wage_pct_delta, 4),
        },
        "headcount": {
            "peak_across_portfolio": int(peak_headcount),
        },
    }


def aggregate_accountability(venue_recaps: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Sum the per-venue accountability blocks into a portfolio total."""
    total = pending = accepted = dismissed = 0
    missed = 0.0
    pending_impact = 0.0
    top_missed: List[Dict[str, Any]] = []

    for r in venue_recaps:
        acct = r.get("accountability") or {}
        if not isinstance(acct, dict):
            continue
        total += int(_safe(acct, "total"))
        pending += int(_safe(acct, "pending"))
        accepted += int(_safe(acct, "accepted"))
        dismissed += int(_safe(acct, "dismissed"))
        missed += _safe(acct, "estimated_impact_missed_aud")
        pending_impact += _safe(acct, "estimated_impact_pending_aud")
        venue_id = r.get("venue_id") or "?"
        for m in (acct.get("top_missed") or [])[:5]:
            top_missed.append({**m, "venue_id": venue_id})

    acceptance_rate = (
        accepted / (accepted + dismissed)
        if (accepted + dismissed) > 0
        else 0.0
    )
    # Sort the portfolio's top_missed by impact and take the top 5
    top_missed.sort(key=lambda m: float(m.get("impact_estimate_aud") or 0), reverse=True)
    top_missed = top_missed[:5]

    return {
        "total": total,
        "pending": pending,
        "accepted": accepted,
        "dismissed": dismissed,
        "estimated_impact_missed_aud": round(missed, 2),
        "estimated_impact_pending_aud": round(pending_impact, 2),
        "acceptance_rate": round(acceptance_rate, 4),
        "top_missed": top_missed,
    }


# ---------------------------------------------------------------------------
# Per-venue mini-summary for dashboard sub-cards
# ---------------------------------------------------------------------------

def _venue_mini(r: Dict[str, Any], label: Optional[str]) -> Dict[str, Any]:
    return {
        "venue_id": r.get("venue_id") or "",
        "label": label or (r.get("venue_id") or ""),
        "traffic_light": r.get("traffic_light") or "unknown",
        "revenue_actual": _safe(r, "revenue", "actual"),
        "revenue_delta_pct": _safe(r, "revenue", "delta_pct"),
        "wage_pct_actual": _safe(r, "wages", "pct_of_revenue_actual"),
        "wage_pct_delta": _safe(r, "wages", "pct_delta"),
        "peak_headcount": int(_safe(r, "headcount", "peak")),
        "accountability": {
            "dismissed": int(_safe(r.get("accountability") or {}, "dismissed")),
            "missed_aud": _safe(r.get("accountability") or {}, "estimated_impact_missed_aud"),
        },
        "summary": r.get("summary") or "",
    }


# ---------------------------------------------------------------------------
# Portfolio summary line
# ---------------------------------------------------------------------------

def _compose_summary(
    *,
    venue_recaps: List[Dict[str, Any]],
    totals: Dict[str, Any],
    accountability: Dict[str, Any],
) -> str:
    n = len(venue_recaps)
    if n == 0:
        return "No venues reporting yet."

    # Count lights
    lights = [(r.get("traffic_light") or "unknown").lower() for r in venue_recaps]
    red = sum(1 for l in lights if l == "red")
    amber = sum(1 for l in lights if l == "amber")
    green = sum(1 for l in lights if l == "green")

    parts: List[str] = []
    parts.append(f"{n} venues: {red} red, {amber} amber, {green} green.")

    rev_actual = _safe(totals, "revenue", "actual")
    rev_fc = _safe(totals, "revenue", "forecast")
    rev_delta_pct = _safe(totals, "revenue", "delta_pct")
    wage_pct_actual = _safe(totals, "wages", "pct_of_revenue_actual")
    wage_pct_target = _safe(totals, "wages", "pct_of_revenue_target")
    wage_pct_delta = _safe(totals, "wages", "pct_delta")

    if rev_fc > 0:
        parts.append(
            f"Portfolio revenue {_fmt_money(rev_actual)} vs "
            f"{_fmt_money(rev_fc)} ({_fmt_pct(rev_delta_pct, signed=True)})."
        )
    if wage_pct_actual > 0 and wage_pct_target > 0:
        parts.append(
            f"Group wage % {_fmt_pct(wage_pct_actual)} "
            f"({_fmt_pct(wage_pct_delta, signed=True).replace('%', 'pt')} vs "
            f"{_fmt_pct(wage_pct_target)} target)."
        )

    dismissed = int(_safe(accountability, "dismissed"))
    missed = _safe(accountability, "estimated_impact_missed_aud")
    if dismissed > 0:
        word = "rec" if dismissed == 1 else "recs"
        if missed > 0:
            parts.append(
                f"{dismissed} {word} dismissed across the group "
                f"(~{_fmt_money(missed)} at stake)."
            )
        else:
            parts.append(f"{dismissed} {word} dismissed across the group.")

    return " ".join(parts)


# ---------------------------------------------------------------------------
# Main composer
# ---------------------------------------------------------------------------

def compose_portfolio(
    venue_recaps: List[Dict[str, Any]],
    *,
    portfolio_id: Optional[str] = None,
    shift_date: Optional[str] = None,
    venue_labels: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Roll a list of per-venue recap dicts up into a portfolio view.

    Args:
        venue_recaps: The output of ``shift_recap.compose_recap`` for
            each venue in the portfolio. An empty list is valid — the
            composer returns a zero-state recap in that case.
        portfolio_id: Optional group identifier (free-form string).
        shift_date: Optional override; defaults to the first recap's
            ``shift_date`` when present.
        venue_labels: Optional ``{venue_id: human_name}`` mapping so the
            mini-card displays 'Mojo's Bar' rather than 'venue_001'.

    Returns:
        Dict with keys: ``portfolio_id``, ``shift_date``, ``generated_at``,
        ``traffic_light`` (worst-of), ``summary``, ``totals``,
        ``accountability``, ``venues`` (list of mini summaries).
    """
    recaps = list(venue_recaps or [])
    labels = dict(venue_labels or {})

    totals = aggregate_totals(recaps)
    accountability = aggregate_accountability(recaps)
    lights = [r.get("traffic_light") or "unknown" for r in recaps]
    traffic_light = _worst_light(lights)

    summary = _compose_summary(
        venue_recaps=recaps,
        totals=totals,
        accountability=accountability,
    )

    mini_venues = [_venue_mini(r, labels.get(r.get("venue_id") or "")) for r in recaps]
    # Sort so red-light venues float to the top (most urgent first)
    mini_venues.sort(
        key=lambda v: -LIGHT_RANK.get((v.get("traffic_light") or "unknown").lower(), -1)
    )

    # Shift date: prefer the explicit arg; fall back to the first recap.
    resolved_date = shift_date or (
        recaps[0].get("shift_date") if recaps and isinstance(recaps[0], dict) else None
    ) or ""

    return {
        "portfolio_id": portfolio_id or "",
        "shift_date": resolved_date,
        "generated_at": _now_iso(),
        "traffic_light": traffic_light,
        "summary": summary,
        "totals": totals,
        "accountability": accountability,
        "venues": mini_venues,
    }
