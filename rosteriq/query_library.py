"""
Query library — deterministic, pre-written answers to the questions a
duty manager actually asks.

This is the *foundation* of RosterIQ's data-mining layer. The hidden
philosophy: duty managers ask a small set of predictable questions over
and over, and they want the same answer every time. An LLM agent is the
wrong tool for that — token cost, prompt drift, non-determinism, and
procurement-conversation landmines. A structured library of named
queries is the right one.

Design rules:

  1. No external deps. Pure stdlib. Reusable from the API endpoint, the
     chatbot router, a CLI, a nightly report, and tests — no pydantic,
     no FastAPI, no asyncpg.

  2. Duck-typed inputs. Every query takes a `QueryContext` built from
     whatever your actual stores look like. Tests build it with toy
     dataclasses; the real API endpoint builds it from the rosteriq
     pydantic models. They share attribute names but not types.

  3. Deterministic. Given the same context and the same period, every
     query returns byte-identical output. No randomness, no "temperature",
     no "model versions".

  4. Structured output. `QueryResult` has a headline number, a one-line
     answer, a per-row breakdown, and an optional comparison — enough
     for both the on-shift tile and the chatbot reply bubble.

  5. Period-aware. The `resolve_period` helper handles the common
     English expressions a duty manager types ("last saturday", "last
     week", "last 4 saturdays", "last month", "this month so far").

The top of file lists the 8 foundational queries. Add more as the pilot
surfaces real questions — every new query is ~20 lines and a test.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Iterable, List, Optional, Protocol


# ---------------------------------------------------------------------------
# Duck-typed protocols — any object matching these shapes works as input
# ---------------------------------------------------------------------------

class _ShiftLike(Protocol):
    employee_id: str
    date: date
    @property
    def net_hours(self) -> float: ...
    cost: Any            # Decimal | float | None


class _RosterLike(Protocol):
    venue_id: str
    shifts: list


class _VendorForecastLike(Protocol):
    """Mirrors the VendorForecast shape: one row per metric per bucket."""
    bucket_start: datetime
    bucket_end: datetime
    metric: Any          # ForecastMetric or str
    amount: Any          # Decimal | float
    department: Optional[str]


class _EmployeeLike(Protocol):
    id: str
    employment_type: Any  # EmploymentType enum or str


class _HeadCountLike(Protocol):
    counted_at: datetime
    count: int
    zone: Optional[str]


# ---------------------------------------------------------------------------
# Context + result
# ---------------------------------------------------------------------------

@dataclass
class VendorForecastRow:
    """Lightweight mirror of ext_vendor_forecasts.

    The query library duck-types its input, so callers can supply any
    object with these attribute names. `VendorForecastRow` exists so the
    API layer has a concrete type to map asyncpg rows into, and so the
    router/tests have a canonical shape to assert against.

    Fields intentionally mirror the columns of ext_vendor_forecasts
    (bucket_start, bucket_end, metric, amount, department) — if the
    table grows new columns, add them here as optional fields to keep
    backward-compat.
    """
    bucket_start: datetime
    bucket_end: datetime
    metric: str
    amount: Decimal
    department: Optional[str] = None


def vendor_forecast_row_from_dict(row) -> VendorForecastRow:
    """Build a VendorForecastRow from a dict-like row (asyncpg Record,
    plain dict, whatever). Handles Decimal/float/str for `amount` and
    tolerates a missing `department` column.
    """
    def _get(key, default=None):
        # asyncpg.Record, plain dict, and sqlite3.Row all support [key]
        try:
            return row[key]
        except (KeyError, IndexError, TypeError):
            return default

    bucket_start = _get("bucket_start")
    bucket_end = _get("bucket_end")
    metric = _get("metric")
    amount_raw = _get("amount")
    department = _get("department")

    if bucket_start is None or bucket_end is None or metric is None or amount_raw is None:
        raise ValueError(
            "vendor_forecast_row_from_dict requires bucket_start, "
            "bucket_end, metric, amount"
        )

    # Metric might be an Enum, a string, or bytes
    if hasattr(metric, "value"):
        metric = metric.value
    metric = str(metric)

    # Amount could be Decimal, float, int, or string
    try:
        amount = Decimal(str(amount_raw))
    except Exception as e:
        raise ValueError(f"Invalid amount: {amount_raw!r}") from e

    return VendorForecastRow(
        bucket_start=bucket_start,
        bucket_end=bucket_end,
        metric=metric,
        amount=amount,
        department=department,
    )


@dataclass
class QueryContext:
    """Everything a query might need, gathered once per request.

    Callers build this by reading from the real stores (pydantic models,
    asyncpg rows, whatever). Tests build it from toy dataclasses.
    """
    venue_id: str
    today: date
    shifts: list = field(default_factory=list)
    rosters: list = field(default_factory=list)
    vendor_forecasts: list = field(default_factory=list)
    head_counts: list = field(default_factory=list)
    employees: dict = field(default_factory=dict)

    # Optional: a timezone label ("Australia/Melbourne") for display only.
    timezone_label: Optional[str] = None


@dataclass
class DateRange:
    """Half-open date range [start, end).  `label` is the human name
    for the period ('last Saturday', 'last 4 weeks', etc.)."""
    start: date
    end: date    # exclusive
    label: str

    def contains(self, d: date) -> bool:
        return self.start <= d < self.end

    def days(self) -> int:
        return (self.end - self.start).days

    def to_dict(self) -> dict:
        return {"start": self.start.isoformat(), "end": self.end.isoformat(),
                "label": self.label}


@dataclass
class QueryResult:
    """Uniform result envelope returned by every query function.

    Fields:
      query            — the canonical name of the query ("total_sales")
      question         — the human-readable question that was asked
      period           — the resolved DateRange
      headline_value   — the single number or text that answers it
      headline_unit    — "$" | "%" | "hours" | "people" | "" etc.
      summary          — one-sentence natural answer, ready for the UI
      breakdown        — list of per-row dicts (per-day, per-zone)
      comparison       — optional dict: {"period_label": "...", "value": ...,
                                          "delta": ..., "delta_pct": ...}
      raw_row_count    — how many source rows powered the answer
      notes            — strings to caveat or explain the number
    """
    query: str
    question: str
    period: DateRange
    headline_value: Any
    headline_unit: str = ""
    summary: str = ""
    breakdown: list = field(default_factory=list)
    comparison: Optional[dict] = None
    raw_row_count: int = 0
    notes: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "query": self.query,
            "question": self.question,
            "period": self.period.to_dict(),
            "headline_value": _jsonable(self.headline_value),
            "headline_unit": self.headline_unit,
            "summary": self.summary,
            "breakdown": [
                {k: _jsonable(v) for k, v in row.items()}
                for row in self.breakdown
            ],
            "comparison": (
                {k: _jsonable(v) for k, v in self.comparison.items()}
                if self.comparison else None
            ),
            "raw_row_count": self.raw_row_count,
            "notes": list(self.notes),
        }


def _jsonable(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    return value


# ---------------------------------------------------------------------------
# Period resolver — English → DateRange
# ---------------------------------------------------------------------------

_WEEKDAYS = {
    "monday": 0, "mon": 0,
    "tuesday": 1, "tue": 1, "tues": 1,
    "wednesday": 2, "wed": 2,
    "thursday": 3, "thu": 3, "thurs": 3,
    "friday": 4, "fri": 4,
    "saturday": 5, "sat": 5,
    "sunday": 6, "sun": 6,
}


def _most_recent_weekday(today: date, weekday: int) -> date:
    """Return the most recent date <= today with that weekday (0=Mon)."""
    delta = (today.weekday() - weekday) % 7
    return today - timedelta(days=delta)


def resolve_period(expression: str, today: date) -> DateRange:
    """Resolve a human phrase to a DateRange. Half-open [start, end).

    Supported:
      - 'today', 'yesterday'
      - 'last_week', 'last week', 'this_week', 'this week'
      - 'last_month', 'last month', 'this_month', 'this month'
      - 'last_<weekday>' e.g. 'last_saturday', 'last saturday'
      - 'last_<n>_<weekday>s' e.g. 'last_4_saturdays', 'last 4 saturdays'
      - 'last_<n>_days'
      - Explicit ISO range: '2026-04-01:2026-04-08' (end exclusive)

    Anything unrecognised raises ValueError.
    """
    if not expression:
        raise ValueError("period expression is required")
    expr = expression.strip().lower().replace("-", "_").replace(" ", "_")
    while "__" in expr:
        expr = expr.replace("__", "_")

    if expr == "today":
        return DateRange(today, today + timedelta(days=1), "today")
    if expr == "yesterday":
        y = today - timedelta(days=1)
        return DateRange(y, today, "yesterday")
    if expr in ("this_week", "week_to_date"):
        start = today - timedelta(days=today.weekday())  # Monday
        return DateRange(start, today + timedelta(days=1), "this week so far")
    if expr == "last_week":
        this_monday = today - timedelta(days=today.weekday())
        last_monday = this_monday - timedelta(days=7)
        return DateRange(last_monday, this_monday, "last week")
    if expr in ("this_month", "month_to_date"):
        start = today.replace(day=1)
        return DateRange(start, today + timedelta(days=1), "this month so far")
    if expr == "last_month":
        first_this = today.replace(day=1)
        last_day_prev = first_this - timedelta(days=1)
        first_prev = last_day_prev.replace(day=1)
        return DateRange(first_prev, first_this, "last month")

    # last_<weekday> (single most recent instance)
    m = re.fullmatch(r"last_(\w+)", expr)
    if m and m.group(1) in _WEEKDAYS:
        wd = _WEEKDAYS[m.group(1)]
        most_recent = _most_recent_weekday(today - timedelta(days=1), wd)
        return DateRange(most_recent, most_recent + timedelta(days=1),
                          f"last {m.group(1)}")

    # last_<n>_days
    m = re.fullmatch(r"last_(\d+)_days?", expr)
    if m:
        n = int(m.group(1))
        if n < 1:
            raise ValueError("n must be >= 1")
        return DateRange(today - timedelta(days=n), today, f"last {n} days")

    # last_<n>_<weekday>s (e.g., last_4_saturdays)
    m = re.fullmatch(r"last_(\d+)_(\w+?)s?", expr)
    if m and m.group(2) in _WEEKDAYS:
        n = int(m.group(1))
        if n < 1:
            raise ValueError("n must be >= 1")
        wd = _WEEKDAYS[m.group(2)]
        latest = _most_recent_weekday(today - timedelta(days=1), wd)
        earliest = latest - timedelta(days=7 * (n - 1))
        return DateRange(earliest, latest + timedelta(days=1),
                          f"last {n} {m.group(2)}s")

    # Explicit ISO range "YYYY-MM-DD:YYYY-MM-DD"
    if ":" in expression:
        try:
            a, b = expression.split(":", 1)
            start = date.fromisoformat(a.strip())
            end = date.fromisoformat(b.strip())
            return DateRange(start, end, f"{a.strip()} to {b.strip()}")
        except Exception:
            pass

    raise ValueError(f"Unrecognised period expression: {expression!r}")


# ---------------------------------------------------------------------------
# Low-level aggregators (shared by multiple queries)
# ---------------------------------------------------------------------------

def _filter_vendor_forecasts_by_metric(
    ctx: QueryContext, period: DateRange, metric_name: str,
    department: Optional[str] = None,
) -> list:
    """Return VendorForecast-like rows in [period.start, period.end) for
    a given metric. Metric comparison is string-based to duck-type around
    both enums and plain strings."""
    rows = []
    for vf in ctx.vendor_forecasts:
        bucket_date = vf.bucket_start.date() if hasattr(vf.bucket_start, "date") else vf.bucket_start
        if not period.contains(bucket_date):
            continue
        m = vf.metric
        m_name = m.value if hasattr(m, "value") else str(m)
        if m_name != metric_name:
            continue
        if department is not None and getattr(vf, "department", None) != department:
            continue
        rows.append(vf)
    return rows


def _filter_shifts_by_period(
    ctx: QueryContext, period: DateRange,
) -> list:
    rows = []
    for roster in ctx.rosters:
        if getattr(roster, "venue_id", None) != ctx.venue_id:
            continue
        for shift in roster.shifts:
            if period.contains(shift.date):
                rows.append(shift)
    for shift in ctx.shifts:  # loose shifts not wrapped in a roster
        if period.contains(shift.date):
            rows.append(shift)
    return rows


def _sum_shift_cost(shifts: Iterable) -> Decimal:
    total = Decimal("0")
    for s in shifts:
        if s.cost is None:
            continue
        total += Decimal(str(s.cost))
    return total


def _sum_shift_hours(shifts: Iterable) -> float:
    return round(sum(s.net_hours for s in shifts), 2)


# ---------------------------------------------------------------------------
# The 8 foundational queries
# ---------------------------------------------------------------------------

def total_sales(
    ctx: QueryContext, period: DateRange,
    department: Optional[str] = None,
    question: str = "",
) -> QueryResult:
    """Sum of revenue forecasts (or actuals once we have them) in period."""
    rows = _filter_vendor_forecasts_by_metric(ctx, period, "revenue", department)
    total = sum((Decimal(str(r.amount)) for r in rows), Decimal("0"))

    # Per-day breakdown
    per_day: dict[date, Decimal] = {}
    for r in rows:
        d = r.bucket_start.date() if hasattr(r.bucket_start, "date") else r.bucket_start
        per_day[d] = per_day.get(d, Decimal("0")) + Decimal(str(r.amount))
    breakdown = [
        {"date": d, "amount": v}
        for d, v in sorted(per_day.items())
    ]

    return QueryResult(
        query="total_sales",
        question=question or f"What were the sales for {period.label}?",
        period=period,
        headline_value=float(total),
        headline_unit="$",
        summary=f"${float(total):,.2f} in forecast revenue over {period.label}.",
        breakdown=breakdown,
        raw_row_count=len(rows),
        notes=["Based on vendor-supplied forecast rows (revenue metric)."],
    )


def total_wage_cost(
    ctx: QueryContext, period: DateRange, question: str = "",
) -> QueryResult:
    """Sum of shift costs over period."""
    shifts = _filter_shifts_by_period(ctx, period)
    total = _sum_shift_cost(shifts)
    hours = _sum_shift_hours(shifts)

    per_day: dict[date, Decimal] = {}
    for s in shifts:
        if s.cost is None:
            continue
        per_day[s.date] = per_day.get(s.date, Decimal("0")) + Decimal(str(s.cost))
    breakdown = [
        {"date": d, "wage_cost": v} for d, v in sorted(per_day.items())
    ]

    return QueryResult(
        query="total_wage_cost",
        question=question or f"What was the wage cost for {period.label}?",
        period=period,
        headline_value=float(total),
        headline_unit="$",
        summary=(
            f"${float(total):,.2f} total wage cost across {len(shifts)} shifts "
            f"({hours} hours) for {period.label}."
        ),
        breakdown=breakdown,
        raw_row_count=len(shifts),
    )


def wage_cost_percentage(
    ctx: QueryContext, period: DateRange, question: str = "",
) -> QueryResult:
    """wage_cost / sales * 100 over the period."""
    sales_res = total_sales(ctx, period)
    wage_res = total_wage_cost(ctx, period)
    sales_amount = sales_res.headline_value
    wage_amount = wage_res.headline_value

    if sales_amount and sales_amount > 0:
        pct = round(wage_amount / sales_amount * 100, 2)
    else:
        pct = None

    summary = (
        f"Wage cost was {pct}% of sales over {period.label}."
        if pct is not None
        else f"Can't compute wage% — no sales data for {period.label}."
    )

    return QueryResult(
        query="wage_cost_percentage",
        question=question or f"What was the wage % for {period.label}?",
        period=period,
        headline_value=pct,
        headline_unit="%",
        summary=summary,
        breakdown=[
            {"metric": "total_sales", "value": sales_amount},
            {"metric": "total_wage_cost", "value": wage_amount},
        ],
        raw_row_count=sales_res.raw_row_count + wage_res.raw_row_count,
    )


def days_over_wage_pct(
    ctx: QueryContext, period: DateRange, threshold_pct: float,
    question: str = "",
) -> QueryResult:
    """List each day in the period where wage% exceeded `threshold_pct`."""
    if threshold_pct <= 0:
        raise ValueError("threshold_pct must be > 0")

    per_day_sales: dict[date, Decimal] = {}
    for r in _filter_vendor_forecasts_by_metric(ctx, period, "revenue"):
        d = r.bucket_start.date() if hasattr(r.bucket_start, "date") else r.bucket_start
        per_day_sales[d] = per_day_sales.get(d, Decimal("0")) + Decimal(str(r.amount))
    per_day_wage: dict[date, Decimal] = {}
    for s in _filter_shifts_by_period(ctx, period):
        if s.cost is None:
            continue
        per_day_wage[s.date] = per_day_wage.get(s.date, Decimal("0")) + Decimal(str(s.cost))

    offenders = []
    for d in sorted(set(per_day_sales) | set(per_day_wage)):
        sales = per_day_sales.get(d, Decimal("0"))
        wage = per_day_wage.get(d, Decimal("0"))
        if sales <= 0:
            continue
        pct = float(wage / sales * 100)
        if pct > threshold_pct:
            offenders.append({
                "date": d,
                "sales": sales,
                "wage_cost": wage,
                "wage_pct": round(pct, 2),
            })

    return QueryResult(
        query="days_over_wage_pct",
        question=(
            question
            or f"Which days in {period.label} went over {threshold_pct}% wage cost?"
        ),
        period=period,
        headline_value=len(offenders),
        headline_unit="days",
        summary=(
            f"{len(offenders)} day(s) in {period.label} ran above "
            f"{threshold_pct}% wage cost."
        ),
        breakdown=offenders,
        raw_row_count=len(per_day_sales) + len(per_day_wage),
    )


def last_n_same_weekdays(
    ctx: QueryContext, weekday: int, n: int,
    metric: str = "revenue",
    question: str = "",
) -> QueryResult:
    """'Show me the last 4 Saturdays' — pulls the same metric for the
    most-recent N matching weekdays. Weekday is 0=Mon..6=Sun.

    Returns one breakdown row per weekday date, newest first.
    """
    if not (0 <= weekday <= 6):
        raise ValueError("weekday must be 0..6 (Mon..Sun)")
    if n < 1:
        raise ValueError("n must be >= 1")

    latest = _most_recent_weekday(ctx.today - timedelta(days=1), weekday)
    dates = [latest - timedelta(days=7 * i) for i in range(n)]
    full_range = DateRange(
        start=dates[-1], end=latest + timedelta(days=1),
        label=f"last {n} {_weekday_name(weekday)}s",
    )

    breakdown = []
    totals = []
    for d in dates:  # newest-first
        day_range = DateRange(d, d + timedelta(days=1), d.isoformat())
        sales = sum(
            Decimal(str(r.amount))
            for r in _filter_vendor_forecasts_by_metric(ctx, day_range, metric)
        )
        wage = _sum_shift_cost(_filter_shifts_by_period(ctx, day_range))
        pct = (wage / sales * 100) if sales > 0 else None
        row = {
            "date": d,
            "sales": sales,
            "wage_cost": wage,
            "wage_pct": float(round(pct, 2)) if pct is not None else None,
        }
        breakdown.append(row)
        totals.append((sales, wage))

    headline = float(sum(t[0] for t in totals))
    return QueryResult(
        query="last_n_same_weekdays",
        question=(
            question
            or f"Show me the last {n} {_weekday_name(weekday)}s"
        ),
        period=full_range,
        headline_value=headline,
        headline_unit="$",
        summary=(
            f"Last {n} {_weekday_name(weekday)}s averaged "
            f"${headline / n:,.2f} in sales."
        ),
        breakdown=breakdown,
        raw_row_count=len(breakdown),
    )


def busiest_day(
    ctx: QueryContext, period: DateRange, question: str = "",
) -> QueryResult:
    """Day in period with the highest forecast sales."""
    per_day: dict[date, Decimal] = {}
    for r in _filter_vendor_forecasts_by_metric(ctx, period, "revenue"):
        d = r.bucket_start.date() if hasattr(r.bucket_start, "date") else r.bucket_start
        per_day[d] = per_day.get(d, Decimal("0")) + Decimal(str(r.amount))

    if not per_day:
        return QueryResult(
            query="busiest_day",
            question=question or f"What was the busiest day in {period.label}?",
            period=period,
            headline_value=None,
            headline_unit="$",
            summary=f"No sales data for {period.label}.",
            breakdown=[],
            raw_row_count=0,
        )

    top = max(per_day.items(), key=lambda kv: kv[1])
    return QueryResult(
        query="busiest_day",
        question=question or f"What was the busiest day in {period.label}?",
        period=period,
        headline_value=float(top[1]),
        headline_unit="$",
        summary=(
            f"{top[0].isoformat()} ({_weekday_name(top[0].weekday())}) "
            f"was the busiest day at ${float(top[1]):,.2f}."
        ),
        breakdown=[{"date": d, "sales": v} for d, v in sorted(per_day.items())],
        raw_row_count=len(per_day),
    )


def peak_head_count(
    ctx: QueryContext, period: DateRange, zone: Optional[str] = None,
    question: str = "",
) -> QueryResult:
    """Highest observed head count in the window."""
    rows = [
        hc for hc in ctx.head_counts
        if period.contains(
            hc.counted_at.date() if hasattr(hc.counted_at, "date") else hc.counted_at
        )
        and (zone is None or hc.zone == zone)
    ]
    if not rows:
        return QueryResult(
            query="peak_head_count",
            question=question or f"What was the peak head count in {period.label}?",
            period=period,
            headline_value=None,
            headline_unit="people",
            summary="No head-count observations recorded in this window.",
            raw_row_count=0,
        )

    top = max(rows, key=lambda r: r.count)
    return QueryResult(
        query="peak_head_count",
        question=question or f"What was the peak head count in {period.label}?",
        period=period,
        headline_value=top.count,
        headline_unit="people",
        summary=(
            f"Peak was {top.count} people at "
            f"{top.counted_at.isoformat()}"
            + (f" in {top.zone}" if top.zone else "")
            + "."
        ),
        breakdown=[
            {
                "counted_at": r.counted_at,
                "count": r.count,
                "zone": r.zone,
            }
            for r in sorted(rows, key=lambda r: r.counted_at)
        ],
        raw_row_count=len(rows),
    )


def staff_hours_summary(
    ctx: QueryContext, period: DateRange, question: str = "",
) -> QueryResult:
    """Total rostered hours, shift count, unique employees in the period."""
    shifts = _filter_shifts_by_period(ctx, period)
    hours = _sum_shift_hours(shifts)
    unique_emps = len({s.employee_id for s in shifts})

    return QueryResult(
        query="staff_hours_summary",
        question=question or f"How many hours did we roster in {period.label}?",
        period=period,
        headline_value=hours,
        headline_unit="hours",
        summary=(
            f"{hours} rostered hours across {len(shifts)} shifts "
            f"and {unique_emps} employees for {period.label}."
        ),
        breakdown=[],
        raw_row_count=len(shifts),
    )


# ---------------------------------------------------------------------------
# Pilot-driven extensions — add to this list as real venues ask real
# questions. Every new query keeps the same structured-envelope contract.
# ---------------------------------------------------------------------------

def worst_day(
    ctx: QueryContext, period: DateRange, question: str = "",
) -> QueryResult:
    """Day in period with the *lowest* positive revenue.

    Zero-revenue days are ignored because most venues are closed 1-2 days
    a week and including those would always win — unhelpful.
    """
    per_day: dict[date, Decimal] = {}
    for r in _filter_vendor_forecasts_by_metric(ctx, period, "revenue"):
        d = r.bucket_start.date() if hasattr(r.bucket_start, "date") else r.bucket_start
        per_day[d] = per_day.get(d, Decimal("0")) + Decimal(str(r.amount))

    positive = {d: v for d, v in per_day.items() if v > 0}
    if not positive:
        return QueryResult(
            query="worst_day",
            question=question or f"What was the slowest day in {period.label}?",
            period=period,
            headline_value=None,
            headline_unit="$",
            summary=f"No sales data for {period.label}.",
            breakdown=[],
            raw_row_count=0,
        )

    bottom = min(positive.items(), key=lambda kv: kv[1])
    return QueryResult(
        query="worst_day",
        question=question or f"What was the slowest day in {period.label}?",
        period=period,
        headline_value=float(bottom[1]),
        headline_unit="$",
        summary=(
            f"{bottom[0].isoformat()} ({_weekday_name(bottom[0].weekday())}) "
            f"was the slowest day at ${float(bottom[1]):,.2f}."
        ),
        breakdown=[{"date": d, "sales": v} for d, v in sorted(positive.items())],
        raw_row_count=len(positive),
    )


def overtime_hours(
    ctx: QueryContext, period: DateRange,
    overtime_threshold: float = 8.0,
    question: str = "",
) -> QueryResult:
    """Sum of shift hours above `overtime_threshold` per shift.

    This is the *per-shift* overtime rule used by most Aussie hospitality
    awards — anything worked past 8 hours on a single shift counts as OT.
    Weekly-cap overtime (e.g., over 38 hours/week) is a separate query
    that needs the full employee ledger and will land later.

    Breakdown is per-employee with hours_regular, hours_overtime,
    and a flag for the worst offender.
    """
    if overtime_threshold <= 0:
        raise ValueError("overtime_threshold must be > 0")

    shifts = _filter_shifts_by_period(ctx, period)
    per_emp: dict[str, dict[str, float]] = {}
    for s in shifts:
        hrs = float(s.net_hours)
        ot = round(max(0.0, hrs - overtime_threshold), 2)
        reg = round(hrs - ot, 2)
        bucket = per_emp.setdefault(
            s.employee_id,
            {"hours_regular": 0.0, "hours_overtime": 0.0, "shift_count": 0},
        )
        bucket["hours_regular"] = round(bucket["hours_regular"] + reg, 2)
        bucket["hours_overtime"] = round(bucket["hours_overtime"] + ot, 2)
        bucket["shift_count"] += 1

    total_ot = round(sum(v["hours_overtime"] for v in per_emp.values()), 2)
    breakdown = [
        {
            "employee_id": emp,
            "hours_regular": v["hours_regular"],
            "hours_overtime": v["hours_overtime"],
            "shift_count": v["shift_count"],
        }
        for emp, v in sorted(
            per_emp.items(),
            key=lambda kv: kv[1]["hours_overtime"],
            reverse=True,
        )
    ]

    if total_ot == 0:
        summary = (
            f"No overtime in {period.label} — all {len(shifts)} shifts "
            f"were at or under {overtime_threshold} hours."
        )
    else:
        top = breakdown[0]
        summary = (
            f"{total_ot} overtime hours in {period.label} "
            f"across {sum(1 for v in per_emp.values() if v['hours_overtime'] > 0)} "
            f"employees. Worst: {top['employee_id']} "
            f"at {top['hours_overtime']} OT hours."
        )

    return QueryResult(
        query="overtime_hours",
        question=question or f"How much overtime in {period.label}?",
        period=period,
        headline_value=total_ot,
        headline_unit="hours",
        summary=summary,
        breakdown=breakdown,
        raw_row_count=len(shifts),
        notes=[f"Overtime threshold: {overtime_threshold} hours per shift"],
    )


def average_wage_pct_per_day(
    ctx: QueryContext, period: DateRange, question: str = "",
) -> QueryResult:
    """Unweighted average of daily wage% across days with sales.

    Different from `wage_cost_percentage` which is (total wage / total
    sales) — that's a *weighted* average dominated by your big nights.
    This query tells you how consistent your control is: a venue with
    18% every day and one with 8%, 30%, 8%, 30% can both hit 19%
    overall but one is way better run.
    """
    shifts = _filter_shifts_by_period(ctx, period)
    wage_by_day: dict[date, Decimal] = {}
    for s in shifts:
        wage_by_day[s.date] = wage_by_day.get(s.date, Decimal("0")) + (
            Decimal(str(s.cost)) if s.cost is not None else Decimal("0")
        )

    sales_by_day: dict[date, Decimal] = {}
    for r in _filter_vendor_forecasts_by_metric(ctx, period, "revenue"):
        d = r.bucket_start.date() if hasattr(r.bucket_start, "date") else r.bucket_start
        sales_by_day[d] = sales_by_day.get(d, Decimal("0")) + Decimal(str(r.amount))

    day_pcts: list[tuple[date, Decimal]] = []
    for d, sales in sales_by_day.items():
        if sales <= 0:
            continue
        wage = wage_by_day.get(d, Decimal("0"))
        day_pcts.append((d, (wage / sales) * Decimal("100")))

    if not day_pcts:
        return QueryResult(
            query="average_wage_pct_per_day",
            question=question or f"What was the average daily wage % in {period.label}?",
            period=period,
            headline_value=None,
            headline_unit="%",
            summary=f"No wage/sales overlap to average in {period.label}.",
            breakdown=[],
            raw_row_count=0,
        )

    mean = sum((p for _, p in day_pcts), Decimal("0")) / Decimal(len(day_pcts))
    mean_f = float(round(mean, 2))
    worst = max(day_pcts, key=lambda kv: kv[1])
    best = min(day_pcts, key=lambda kv: kv[1])

    return QueryResult(
        query="average_wage_pct_per_day",
        question=question or f"What was the average daily wage % in {period.label}?",
        period=period,
        headline_value=mean_f,
        headline_unit="%",
        summary=(
            f"Average daily wage cost was {mean_f}% across {len(day_pcts)} "
            f"trading days in {period.label}. "
            f"Best: {best[0].isoformat()} at {float(round(best[1], 2))}%. "
            f"Worst: {worst[0].isoformat()} at {float(round(worst[1], 2))}%."
        ),
        breakdown=[
            {"date": d, "wage_pct": float(round(p, 2))}
            for d, p in sorted(day_pcts)
        ],
        raw_row_count=len(day_pcts),
    )


def hours_by_employee(
    ctx: QueryContext, period: DateRange,
    employee_query: Optional[str] = None,
    question: str = "",
) -> QueryResult:
    """Hours rostered per employee in the period.

    If `employee_query` is passed, filter to employees whose id or
    (optional) name contains the query, case-insensitive. This is how
    the chatbot answers "how many hours did John work last week" — the
    name is pulled out of the question and passed through.
    """
    shifts = _filter_shifts_by_period(ctx, period)

    def _matches(shift, q: str) -> bool:
        q = q.lower().strip()
        if not q:
            return True
        if q in str(shift.employee_id).lower():
            return True
        name = getattr(shift, "employee_name", None)
        if name and q in str(name).lower():
            return True
        return False

    filtered = [s for s in shifts if _matches(s, employee_query or "")]

    per_emp: dict[str, dict[str, Any]] = {}
    for s in filtered:
        bucket = per_emp.setdefault(
            s.employee_id,
            {"hours": 0.0, "shift_count": 0, "cost": Decimal("0")},
        )
        bucket["hours"] = round(bucket["hours"] + float(s.net_hours), 2)
        bucket["shift_count"] += 1
        if s.cost is not None:
            bucket["cost"] += Decimal(str(s.cost))

    total_hours = round(sum(v["hours"] for v in per_emp.values()), 2)
    breakdown = [
        {
            "employee_id": emp,
            "hours": v["hours"],
            "shift_count": v["shift_count"],
            "cost": v["cost"],
        }
        for emp, v in sorted(
            per_emp.items(),
            key=lambda kv: kv[1]["hours"],
            reverse=True,
        )
    ]

    if not breakdown:
        if employee_query:
            summary = (
                f"No shifts matching '{employee_query}' in {period.label}."
            )
        else:
            summary = f"No shifts rostered in {period.label}."
        return QueryResult(
            query="hours_by_employee",
            question=question or f"Hours per employee in {period.label}?",
            period=period,
            headline_value=0.0,
            headline_unit="hours",
            summary=summary,
            breakdown=[],
            raw_row_count=0,
        )

    if employee_query and len(breakdown) == 1:
        row = breakdown[0]
        summary = (
            f"{row['employee_id']} worked {row['hours']} hours across "
            f"{row['shift_count']} shifts in {period.label}."
        )
    else:
        top = breakdown[0]
        summary = (
            f"{total_hours} hours across {len(breakdown)} employees in "
            f"{period.label}. Top: {top['employee_id']} at {top['hours']} hours."
        )

    return QueryResult(
        query="hours_by_employee",
        question=question or f"Hours per employee in {period.label}?",
        period=period,
        headline_value=total_hours,
        headline_unit="hours",
        summary=summary,
        breakdown=breakdown,
        raw_row_count=len(filtered),
        notes=(
            [f"Filtered by '{employee_query}'"] if employee_query else []
        ),
    )


# ---------------------------------------------------------------------------
# Comparison helper — "vs last week" etc.
# ---------------------------------------------------------------------------

def compare_to_previous_period(
    current: QueryResult, previous: QueryResult,
    previous_label: str,
) -> QueryResult:
    """Attach a comparison block to the current result. Returns a *new*
    QueryResult (doesn't mutate current)."""
    curr = current.headline_value
    prev = previous.headline_value
    delta = None
    delta_pct = None
    if curr is not None and prev is not None and prev:
        delta = round(curr - prev, 2)
        delta_pct = round(((curr - prev) / prev) * 100, 2) if prev else None
    new = QueryResult(
        query=current.query,
        question=current.question,
        period=current.period,
        headline_value=current.headline_value,
        headline_unit=current.headline_unit,
        summary=current.summary,
        breakdown=list(current.breakdown),
        raw_row_count=current.raw_row_count,
        notes=list(current.notes),
        comparison={
            "period_label": previous_label,
            "value": prev,
            "delta": delta,
            "delta_pct": delta_pct,
        },
    )
    return new


# ---------------------------------------------------------------------------
# NL-lite router — phrases → query function
# ---------------------------------------------------------------------------

def _weekday_name(weekday: int) -> str:
    return ["monday", "tuesday", "wednesday", "thursday",
            "friday", "saturday", "sunday"][weekday]


# The router is a list of (pattern, handler) pairs. First match wins.
# Patterns are compiled once at import time. Every handler takes
# (ctx, match, default_period) and returns a QueryResult.

def _handler_total_sales(ctx, match, period):
    return total_sales(ctx, period, question=match.string)


def _handler_total_wage(ctx, match, period):
    return total_wage_cost(ctx, period, question=match.string)


def _handler_wage_pct(ctx, match, period):
    return wage_cost_percentage(ctx, period, question=match.string)


def _handler_days_over(ctx, match, period):
    threshold = float(match.group("pct"))
    return days_over_wage_pct(ctx, period, threshold, question=match.string)


def _handler_last_n_weekdays(ctx, match, period):
    n = int(match.group("n") or 1)
    weekday = _WEEKDAYS[match.group("weekday")]
    return last_n_same_weekdays(ctx, weekday, n, question=match.string)


def _handler_busiest(ctx, match, period):
    return busiest_day(ctx, period, question=match.string)


def _handler_peak_head_count(ctx, match, period):
    return peak_head_count(ctx, period, question=match.string)


def _handler_hours(ctx, match, period):
    return staff_hours_summary(ctx, period, question=match.string)


def _handler_worst(ctx, match, period):
    return worst_day(ctx, period, question=match.string)


def _handler_overtime(ctx, match, period):
    return overtime_hours(ctx, period, question=match.string)


def _handler_avg_wage_pct(ctx, match, period):
    return average_wage_pct_per_day(ctx, period, question=match.string)


def _handler_hours_by_employee(ctx, match, period):
    name = match.group("name").strip(" ?.,")
    # Strip trailing period-words so "john last week" filters on "john"
    for stop in (
        " last week", " this week", " last month", " this month",
        " yesterday", " today", " last ",
    ):
        i = name.lower().find(stop)
        if i >= 0:
            name = name[:i].strip()
    return hours_by_employee(ctx, period, employee_query=name,
                              question=match.string)


# Ordering matters: more-specific patterns must come before more-general
# ones. "last 4 saturdays" must match _handler_last_n_weekdays before
# _handler_total_sales picks up "sales".
_PATTERNS: list[tuple[re.Pattern, Callable]] = [
    # "last 4 saturdays", "last saturday"
    (re.compile(
        r"(?:show me|give me|what (?:was|were))?\s*last\s+(?:(?P<n>\d+)\s+)?"
        r"(?P<weekday>monday|tuesday|wednesday|thursday|friday|saturday|sunday)s?",
        re.IGNORECASE,
    ), _handler_last_n_weekdays),

    # "days over 20%", "which days over 18% wage cost"
    (re.compile(
        r"(?:days?|which days?).+over\s*(?P<pct>\d+(?:\.\d+)?)\s*%",
        re.IGNORECASE,
    ), _handler_days_over),

    # "average wage %", "mean wage cost %", "avg labour %"
    # Must precede _handler_wage_pct so "average wage %" doesn't fall
    # through to the overall (weighted) wage_cost_percentage.
    (re.compile(
        r"(?:average|avg|mean)\s+(?:daily\s+)?(?:wage|labour)\s*(?:cost)?\s*"
        r"(?:%|percent(?:age)?)",
        re.IGNORECASE,
    ), _handler_avg_wage_pct),

    # "wage %", "wage cost %", "labour %"
    (re.compile(
        r"(?:wage|labour)\s*(?:cost)?\s*(?:%|percent(?:age)?)",
        re.IGNORECASE,
    ), _handler_wage_pct),

    # "wage cost", "labour cost" (without %)
    (re.compile(
        r"\b(?:wage|labour)\s*cost\b",
        re.IGNORECASE,
    ), _handler_total_wage),

    # "overtime", "OT hours" — must precede the generic "hours" handler
    (re.compile(
        r"\b(?:overtime|ot\s+hours?)\b",
        re.IGNORECASE,
    ), _handler_overtime),

    # "hours for <name>", "hours by <name>" — must precede the generic
    # "how many hours" handler
    (re.compile(
        r"\bhours?\s+(?:for|by)\s+(?P<name>[\w\-\.' ]+)",
        re.IGNORECASE,
    ), _handler_hours_by_employee),

    # "how many hours did <name> work"
    (re.compile(
        r"\bhours?\s+did\s+(?P<name>[\w\-\.' ]+?)\s+work",
        re.IGNORECASE,
    ), _handler_hours_by_employee),

    # "total sales", "what were my sales", "sales for last week"
    (re.compile(
        r"\b(?:sales|revenue|takings)\b",
        re.IGNORECASE,
    ), _handler_total_sales),

    # "busiest day", "best day"
    (re.compile(
        r"\b(?:busiest|biggest|best)\s+(?:day|night)\b",
        re.IGNORECASE,
    ), _handler_busiest),

    # "worst day", "slowest day", "quietest day"
    (re.compile(
        r"\b(?:worst|slowest|quietest|dead(?:est)?)\s+(?:day|night)\b",
        re.IGNORECASE,
    ), _handler_worst),

    # "peak headcount", "highest headcount", "max people"
    (re.compile(
        r"\b(?:peak|highest|max(?:imum)?)\s*(?:head\s*count|headcount|people)\b",
        re.IGNORECASE,
    ), _handler_peak_head_count),

    # "how many hours", "rostered hours", "total hours"
    (re.compile(
        r"\b(?:how many|total|rostered)\s*hours\b",
        re.IGNORECASE,
    ), _handler_hours),
]


def _extract_period_from_text(text: str, today: date) -> DateRange:
    """Look for a known period expression inside the text. Falls back to
    'last week' if nothing matches — the single most common implicit
    window a duty manager means."""
    text_l = text.lower()

    candidates = [
        ("last_week", "last week"),
        ("this_week", "this week"),
        ("last_month", "last month"),
        ("this_month", "this month"),
        ("yesterday", "yesterday"),
        ("today", "today"),
    ]
    for key, phrase in candidates:
        if phrase in text_l:
            return resolve_period(key, today)

    # last N days
    m = re.search(r"last\s+(\d+)\s+days?", text_l)
    if m:
        return resolve_period(f"last_{m.group(1)}_days", today)

    # last <weekday>
    m = re.search(
        r"last\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday)",
        text_l,
    )
    if m:
        return resolve_period(f"last_{m.group(1)}", today)

    # Default
    return resolve_period("last_week", today)


@dataclass
class RouterResult:
    matched: bool
    query_result: Optional[QueryResult] = None
    reason: Optional[str] = None


def route_question(question: str, ctx: QueryContext) -> RouterResult:
    """Entry point for the chatbot / search box. Returns a RouterResult
    with either a QueryResult or a reason the question couldn't be
    answered."""
    text = question.strip()
    if not text:
        return RouterResult(False, reason="empty question")
    period = _extract_period_from_text(text, ctx.today)

    for pattern, handler in _PATTERNS:
        m = pattern.search(text)
        if m:
            try:
                result = handler(ctx, m, period)
            except ValueError as e:
                return RouterResult(False, reason=str(e))
            return RouterResult(True, query_result=result)

    return RouterResult(
        False,
        reason=(
            "I don't have a built-in query for that yet. Try phrases like "
            "'sales last week', 'wage % last saturday', 'last 4 saturdays', "
            "'which days over 18% last month', or 'busiest day this month'."
        ),
    )


# Public helpers for tests / callers
def list_supported_queries() -> list[str]:
    return [
        "total_sales",
        "total_wage_cost",
        "wage_cost_percentage",
        "days_over_wage_pct",
        "last_n_same_weekdays",
        "busiest_day",
        "peak_head_count",
        "staff_hours_summary",
        "worst_day",
        "overtime_hours",
        "average_wage_pct_per_day",
        "hours_by_employee",
    ]
