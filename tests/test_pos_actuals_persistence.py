"""
POS sync now persists hourly sales into revenue_actuals so the forecast-accuracy
engine can grade forecasts against real takings. Previously synced sales were
returned to the caller and never stored, so accuracy had nothing to measure.
"""

from types import SimpleNamespace
from datetime import date

from rosteriq.database import MemoryStore
from rosteriq.routes.pos import _persist_pos_actuals
from rosteriq.models import DemandForecast, SignalType
from rosteriq.services.forecast_accuracy import ForecastAccuracyService


def _sig(d, hour, revenue, covers, txns):
    return SimpleNamespace(
        signal_date=d, signal_hour=hour,
        raw_data={"snapshot": {"total_revenue": revenue, "total_covers": covers, "transactions": txns}},
    )


def test_persist_pos_actuals_groups_by_day():
    db = MemoryStore()
    sigs = [
        _sig(date(2026, 6, 22), 19, 1800.0, 60, 30),
        _sig(date(2026, 6, 22), 20, 1200.0, 40, 20),
        _sig(date(2026, 6, 23), 19, 900.0, 30, 15),
    ]
    days = _persist_pos_actuals(db, "v1", "swiftpos", sigs)
    assert days == 2

    recs = db.list_revenue_actuals("v1", "2026-06-22", "2026-06-23")
    by_date = {r["date"][:10] if isinstance(r["date"], str) else r["date"]: r for r in recs}
    r22 = by_date["2026-06-22"]
    assert r22["daily_total"] == 3000.0
    assert {h["hour"] for h in r22["hourly_breakdown"]} == {19, 20}


def test_second_source_merges_not_clobbers_a_day():
    """A later persist for a day MERGES hourly_breakdown rather than overwriting it
    (review found CSV-import-after-API-sync silently lost the day's actuals)."""
    from rosteriq.routes.pos import _persist_pos_actuals
    db = MemoryStore()
    _persist_pos_actuals(db, "v1", "swiftpos", [_sig(date(2026, 6, 22), 19, 1800.0, 60, 30)])
    # a second source/import for the SAME day, a different hour
    _persist_pos_actuals(db, "v1", "csv", [_sig(date(2026, 6, 22), 20, 1200.0, 40, 20)])
    recs = db.list_revenue_actuals("v1", "2026-06-22", "2026-06-22")
    assert len(recs) == 1
    hours = {h["hour"] for h in recs[0]["hourly_breakdown"]}
    assert hours == {19, 20}, f"a source clobbered the other: {hours}"
    assert recs[0]["daily_total"] == 3000.0


def test_pos_actuals_make_forecast_accuracy_measurable():
    """The whole point: POS covers become the actuals the accuracy engine grades."""
    db = MemoryStore()
    _persist_pos_actuals(db, "v1", "swiftpos", [
        _sig(date(2026, 6, 22), 19, 1800.0, 60, 30),
        _sig(date(2026, 6, 22), 20, 1200.0, 40, 20),
    ])
    db.add_forecasts([
        DemandForecast(id="f19", venue_id="v1", date=date(2026, 6, 22), hour=19,
                       predicted_covers=60, confidence=0.8,
                       signals_used=[SignalType.historical], model_version="v1"),
        DemandForecast(id="f20", venue_id="v1", date=date(2026, 6, 22), hour=20,
                       predicted_covers=50, confidence=0.8,
                       signals_used=[SignalType.historical], model_version="v1"),
    ])
    rep = ForecastAccuracyService(db).calculate_accuracy("v1", date(2026, 6, 21), date(2026, 6, 23))
    assert rep.measured_samples == 2
    assert rep.data_coverage == 1.0
    # MAPE normalises by ACTUAL: hour19 |60-60|/60 = 0%, hour20 |40-50|/40 = 25% -> mean 12.5%
    assert round(rep.overall_mape, 1) == 12.5
