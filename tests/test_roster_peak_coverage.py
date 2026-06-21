"""
Regression test for the roster generator starving the demand peak.

The generator gives each employee one shift/day and used to process demand periods
in CLOCK order — so a quiet morning claimed every employee before the loop reached
the busy dinner peak, leaving the peak with ZERO staff. Periods are now staffed
highest-demand-first, so the busiest service is always covered.
"""

from datetime import date, datetime
from decimal import Decimal

from rosteriq.models import (
    Employee, State, EmploymentType, AwardLevel, DemandForecast, SignalType,
)
from rosteriq.roster_optimiser import generate_daily_roster


def _emp(i):
    return Employee(
        id=f"e{i}", name=f"Emp{i}", employment_type=EmploymentType.full_time,
        award_level=AwardLevel.level_1, state=State.vic, venue_id="v1",
        hourly_base_rate=Decimal("30.00"),
        created_at=datetime(2025, 1, 1), updated_at=datetime(2025, 1, 1),
    )


def _fc(hour, covers):
    return DemandForecast(
        id=f"f{hour}", venue_id="v1", date=date(2026, 6, 22), hour=hour,
        predicted_covers=float(covers), confidence=0.8,
        signals_used=[SignalType.historical], model_version="v1",
    )


def _coverage_by_hour(shifts):
    cov = {}
    for s in shifts:
        sh, eh = s.start_time.hour, s.end_time.hour
        if eh <= sh:
            eh += 24
        for h in range(sh, eh):
            cov[h % 24] = cov.get(h % 24, 0) + 1
    return cov


def test_dinner_peak_is_staffed_not_starved():
    # Quiet lunch (11-16, ~30 covers -> ~2 staff) and busy dinner (18-22,
    # ~90 covers -> ~6 staff). A 6-person pool can't cover both fully, so the
    # generator must prioritise the dinner PEAK.
    forecasts = [_fc(h, 30) for h in range(11, 16)] + [_fc(h, 90) for h in range(18, 23)]
    employees = [_emp(i) for i in range(6)]

    shifts = generate_daily_roster(
        target_date=date(2026, 6, 22), forecasts=forecasts, employees=employees,
        state=State.vic, venue_config=None,
    )
    cov = _coverage_by_hour(shifts)

    # The dinner peak (18:00-22:00) MUST have staff — this is the bug guard.
    for h in range(18, 22):
        assert cov.get(h, 0) > 0, f"dinner hour {h}:00 left unstaffed (peak starvation): {cov}"
    # And the peak should be the better-staffed service.
    peak = max(cov.get(h, 0) for h in range(18, 22))
    assert peak >= 3, f"dinner peak under-staffed: {cov}"
