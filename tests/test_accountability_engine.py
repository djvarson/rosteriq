"""Tests for rosteriq.accountability_engine — pure-stdlib, no pytest.

Runs with `python tests/test_accountability_engine.py`; every `test_` function
is executed and pass/fail is reported.
"""
from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, date, timedelta, timezone

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq import accountability_engine as acct_eng  # noqa: E402


def _reset():
    acct_eng.clear()


# ---------------------------------------------------------------------------
# DecisionLog creation and retrieval
# ---------------------------------------------------------------------------


def test_record_decision_creates_log_with_uuid():
    _reset()
    decision = acct_eng.record_decision(
        venue_id="mojos",
        shift_id="shift_2026-04-15_0900",
        manager_id="mgr_alice",
        manager_name="Alice Smith",
        decision_type=acct_eng.DecisionType.CUT_STAFF,
        signals_available={
            "forecast_revenue": 5000.0,
            "forecast_headcount": 8,
            "suggested_action": "cut",
        },
        notes="Slow early shift, cut 1 bartender",
    )

    assert decision.decision_id.startswith("dec_")
    assert decision.venue_id == "mojos"
    assert decision.shift_id == "shift_2026-04-15_0900"
    assert decision.manager_id == "mgr_alice"
    assert decision.manager_name == "Alice Smith"
    assert decision.decision_type == acct_eng.DecisionType.CUT_STAFF
    assert decision.signals_available["forecast_revenue"] == 5000.0
    assert decision.outcome_variance == {}
    assert decision.notes == "Slow early shift, cut 1 bartender"


def test_record_decision_stores_in_venue_bucket():
    _reset()
    d1 = acct_eng.record_decision(
        venue_id="earls",
        shift_id="shift_2026-04-15_0900",
        manager_id="mgr_bob",
        manager_name="Bob Jones",
        decision_type=acct_eng.DecisionType.KEPT_STAFF_ON,
        signals_available={},
    )
    d2 = acct_eng.record_decision(
        venue_id="earls",
        shift_id="shift_2026-04-15_1700",
        manager_id="mgr_bob",
        manager_name="Bob Jones",
        decision_type=acct_eng.DecisionType.CALLED_IN_STAFF,
        signals_available={},
    )

    decisions = acct_eng.list_decisions("earls")
    assert len(decisions) == 2
    # Newest first
    assert decisions[0].decision_id == d2.decision_id
    assert decisions[1].decision_id == d1.decision_id


def test_list_decisions_filter_by_manager():
    _reset()
    acct_eng.record_decision(
        venue_id="venue1",
        shift_id="shift_2026-04-15_0900",
        manager_id="mgr_alice",
        manager_name="Alice",
        decision_type=acct_eng.DecisionType.CUT_STAFF,
        signals_available={},
    )
    acct_eng.record_decision(
        venue_id="venue1",
        shift_id="shift_2026-04-15_1700",
        manager_id="mgr_bob",
        manager_name="Bob",
        decision_type=acct_eng.DecisionType.KEPT_STAFF_ON,
        signals_available={},
    )

    alice_decisions = acct_eng.list_decisions("venue1", manager_id="mgr_alice")
    assert len(alice_decisions) == 1
    assert alice_decisions[0].manager_id == "mgr_alice"

    bob_decisions = acct_eng.list_decisions("venue1", manager_id="mgr_bob")
    assert len(bob_decisions) == 1
    assert bob_decisions[0].manager_id == "mgr_bob"


def test_list_decisions_filter_by_since():
    _reset()
    now = datetime.now(timezone.utc)
    future = now + timedelta(hours=1)

    acct_eng.record_decision(
        venue_id="venue1",
        shift_id="shift_1",
        manager_id="mgr_a",
        manager_name="A",
        decision_type=acct_eng.DecisionType.CUT_STAFF,
        signals_available={},
    )

    # Filter: since now (should exclude past decisions if we had any)
    # But we just recorded one, so it should be included
    decisions = acct_eng.list_decisions("venue1", since=now)
    assert len(decisions) == 1

    # Filter: since future (should exclude our decision)
    decisions = acct_eng.list_decisions("venue1", since=future)
    assert len(decisions) == 0


def test_update_decision_variance():
    _reset()
    decision = acct_eng.record_decision(
        venue_id="venue1",
        shift_id="shift_2026-04-15_0900",
        manager_id="mgr_a",
        manager_name="A",
        decision_type=acct_eng.DecisionType.CUT_STAFF,
        signals_available={"forecast": 5000},
    )

    assert decision.outcome_variance == {}

    updated = acct_eng.update_decision_variance(
        venue_id="venue1",
        decision_id=decision.decision_id,
        outcome_variance={
            "actual_revenue": 4200,
            "variance_revenue_pct": -15.8,
        },
    )

    assert updated is not None
    assert updated.outcome_variance["variance_revenue_pct"] == -15.8


# ---------------------------------------------------------------------------
# Variance computation
# ---------------------------------------------------------------------------


def test_compute_variance_basic():
    _reset()
    record = acct_eng.compute_variance(
        venue_id="mojos",
        shift_id="shift_2026-04-15",
        shift_date=date(2026, 4, 15),
        forecast_revenue=5000.0,
        actual_revenue=4000.0,
        forecast_headcount_peak=10,
        actual_headcount_peak=8,
        forecast_staff_hours=80.0,
        actual_staff_hours=64.0,
    )

    assert record.venue_id == "mojos"
    assert record.shift_id == "shift_2026-04-15"
    assert record.forecast_revenue == 5000.0
    assert record.actual_revenue == 4000.0
    # Variance: (4000 - 5000) / 5000 * 100 = -20.0
    assert record.variance_revenue_pct == -20.0
    # Variance staff: (64 - 80) / 80 * 100 = -20.0
    assert record.variance_staff_hours_pct == -20.0


def test_compute_variance_safer_with_none_values():
    _reset()
    record = acct_eng.compute_variance(
        venue_id="mojos",
        shift_id="shift_2026-04-15",
        shift_date=date(2026, 4, 15),
        forecast_revenue=5000.0,
        actual_revenue=None,  # Missing POS data
        forecast_headcount_peak=10,
        actual_headcount_peak=None,  # No headcount recorded
    )

    assert record.variance_revenue_pct is None
    assert record.variance_staff_hours_pct is None
    assert record.forecast_revenue == 5000.0


def test_compute_variance_updates_existing_shift():
    _reset()
    record1 = acct_eng.compute_variance(
        venue_id="mojos",
        shift_id="shift_2026-04-15",
        shift_date=date(2026, 4, 15),
        forecast_revenue=5000.0,
        actual_revenue=4500.0,
    )

    # Update the same shift
    record2 = acct_eng.compute_variance(
        venue_id="mojos",
        shift_id="shift_2026-04-15",
        shift_date=date(2026, 4, 15),
        forecast_revenue=5000.0,
        actual_revenue=4000.0,  # Different actual
    )

    variances = acct_eng.list_variance("mojos")
    assert len(variances) == 1
    assert variances[0].variance_revenue_pct == -20.0


def test_list_variance_filter_by_shift():
    _reset()
    acct_eng.compute_variance(
        venue_id="mojos",
        shift_id="shift_1",
        shift_date=date(2026, 4, 15),
        forecast_revenue=5000.0,
        actual_revenue=4500.0,
    )
    acct_eng.compute_variance(
        venue_id="mojos",
        shift_id="shift_2",
        shift_date=date(2026, 4, 15),
        forecast_revenue=6000.0,
        actual_revenue=5500.0,
    )

    records = acct_eng.list_variance("mojos", shift_id="shift_1")
    assert len(records) == 1
    assert records[0].shift_id == "shift_1"


# ---------------------------------------------------------------------------
# Manager Scoring
# ---------------------------------------------------------------------------


def test_score_manager_basic():
    _reset()
    # Alice makes 2 decisions: cut staff (actionable) + ignored alert (not actionable)
    acct_eng.record_decision(
        venue_id="mojos",
        shift_id="shift_1",
        manager_id="mgr_alice",
        manager_name="Alice",
        decision_type=acct_eng.DecisionType.CUT_STAFF,
        signals_available={"suggested_action": "cut"},
    )
    acct_eng.record_decision(
        venue_id="mojos",
        shift_id="shift_2",
        manager_id="mgr_alice",
        manager_name="Alice",
        decision_type=acct_eng.DecisionType.IGNORED_ALERT,
        signals_available={"suggested_action": "call_in"},
    )

    score = acct_eng.score_manager("mojos", "mgr_alice")

    assert score.manager_id == "mgr_alice"
    assert score.manager_name == "Alice"
    assert score.decisions_total == 2
    # 1 actioned (cut) out of 2 = 50%
    assert score.alerts_actioned_pct == 50.0
    # 1 against signals (ignored alert)
    assert score.decisions_against_signals == 1


def test_score_manager_kept_staff_against_signals():
    _reset()
    # Manager kept staff on when signals suggested cut
    acct_eng.record_decision(
        venue_id="mojos",
        shift_id="shift_1",
        manager_id="mgr_bob",
        manager_name="Bob",
        decision_type=acct_eng.DecisionType.KEPT_STAFF_ON,
        signals_available={"suggested_action": "cut"},
    )

    score = acct_eng.score_manager("mojos", "mgr_bob")

    assert score.decisions_against_signals == 1
    assert score.alerts_actioned_pct == 0.0  # No actionable decisions


def test_score_manager_with_outcome_variance():
    _reset()
    decision = acct_eng.record_decision(
        venue_id="mojos",
        shift_id="shift_1",
        manager_id="mgr_alice",
        manager_name="Alice",
        decision_type=acct_eng.DecisionType.CUT_STAFF,
        signals_available={},
    )

    # Update with outcome
    acct_eng.update_decision_variance(
        venue_id="mojos",
        decision_id=decision.decision_id,
        outcome_variance={"variance_revenue_pct": -15.0},
    )

    score = acct_eng.score_manager("mojos", "mgr_alice")

    # Average variance: only one decision with -15.0
    assert score.avg_variance_revenue == -15.0


def test_score_manager_computes_average_variance():
    _reset()
    # Three decisions with different variances
    for i, variance in enumerate([-10.0, -20.0, -30.0]):
        decision = acct_eng.record_decision(
            venue_id="mojos",
            shift_id=f"shift_{i}",
            manager_id="mgr_alice",
            manager_name="Alice",
            decision_type=acct_eng.DecisionType.CUT_STAFF,
            signals_available={},
        )
        acct_eng.update_decision_variance(
            venue_id="mojos",
            decision_id=decision.decision_id,
            outcome_variance={"variance_revenue_pct": variance},
        )

    score = acct_eng.score_manager("mojos", "mgr_alice")

    # Average of [-10, -20, -30] = -20.0
    assert score.avg_variance_revenue == -20.0


# ---------------------------------------------------------------------------
# Leaderboard
# ---------------------------------------------------------------------------


def test_venue_leaderboard_sorts_by_alerts_actioned():
    _reset()
    # Alice: 2 actionable (100%)
    for i in range(2):
        acct_eng.record_decision(
            venue_id="mojos",
            shift_id=f"shift_a_{i}",
            manager_id="mgr_alice",
            manager_name="Alice",
            decision_type=acct_eng.DecisionType.CUT_STAFF,
            signals_available={},
        )

    # Bob: 1 actionable, 1 ignored (50%)
    acct_eng.record_decision(
        venue_id="mojos",
        shift_id="shift_b_1",
        manager_id="mgr_bob",
        manager_name="Bob",
        decision_type=acct_eng.DecisionType.CUT_STAFF,
        signals_available={},
    )
    acct_eng.record_decision(
        venue_id="mojos",
        shift_id="shift_b_2",
        manager_id="mgr_bob",
        manager_name="Bob",
        decision_type=acct_eng.DecisionType.IGNORED_ALERT,
        signals_available={},
    )

    leaderboard = acct_eng.venue_leaderboard(["mojos"])

    assert len(leaderboard) == 2
    # Alice first (100% > 50%)
    assert leaderboard[0].manager_id == "mgr_alice"
    assert leaderboard[0].alerts_actioned_pct == 100.0
    # Bob second
    assert leaderboard[1].manager_id == "mgr_bob"
    assert leaderboard[1].alerts_actioned_pct == 50.0


def test_venue_leaderboard_multi_venue():
    _reset()
    # Alice at mojos: 1 decision
    acct_eng.record_decision(
        venue_id="mojos",
        shift_id="shift_1",
        manager_id="mgr_alice",
        manager_name="Alice",
        decision_type=acct_eng.DecisionType.CUT_STAFF,
        signals_available={},
    )

    # Alice also at earls: 1 decision
    acct_eng.record_decision(
        venue_id="earls",
        shift_id="shift_2",
        manager_id="mgr_alice",
        manager_name="Alice",
        decision_type=acct_eng.DecisionType.CALLED_IN_STAFF,
        signals_available={},
    )

    leaderboard = acct_eng.venue_leaderboard(["mojos", "earls"])

    # Should have entries for both venues
    # (they may be duplicated by manager_id, but each entry is venue-scoped)
    assert len(leaderboard) >= 2


# ---------------------------------------------------------------------------
# to_dict() serialization
# ---------------------------------------------------------------------------


def test_decision_log_to_dict():
    _reset()
    decision = acct_eng.record_decision(
        venue_id="mojos",
        shift_id="shift_2026-04-15",
        manager_id="mgr_a",
        manager_name="A",
        decision_type=acct_eng.DecisionType.CUT_STAFF,
        signals_available={"x": 1},
        notes="Test",
    )

    d = decision.to_dict()

    assert d["decision_id"] == decision.decision_id
    assert d["venue_id"] == "mojos"
    assert d["decision_type"] == "cut_staff"
    assert d["notes"] == "Test"
    assert d["signals_available"]["x"] == 1


def test_variance_record_to_dict():
    _reset()
    record = acct_eng.compute_variance(
        venue_id="mojos",
        shift_id="shift_2026-04-15",
        shift_date=date(2026, 4, 15),
        forecast_revenue=5000.0,
        actual_revenue=4500.0,
    )

    d = record.to_dict()

    assert d["venue_id"] == "mojos"
    assert d["shift_date"] == "2026-04-15"
    assert d["forecast_revenue"] == 5000.0


# ---------------------------------------------------------------------------
# Main test runner (stdlib, no pytest)
# ---------------------------------------------------------------------------


def run_all_tests():
    """Run all test_ functions and report results."""
    test_functions = [
        name
        for name in globals()
        if name.startswith("test_") and callable(globals()[name])
    ]

    passed = 0
    failed = 0
    errors = []

    for test_name in test_functions:
        try:
            globals()[test_name]()
            print(f"✓ {test_name}")
            passed += 1
        except AssertionError as e:
            print(f"✗ {test_name}: {e}")
            failed += 1
            errors.append((test_name, str(e)))
        except Exception as e:
            print(f"✗ {test_name}: EXCEPTION {e}")
            failed += 1
            errors.append((test_name, f"EXCEPTION: {e}"))

    print(f"\n{'='*60}")
    print(f"Tests passed: {passed}")
    print(f"Tests failed: {failed}")
    print(f"{'='*60}")

    if errors:
        print("\nFailures:")
        for test_name, err in errors:
            print(f"  {test_name}: {err}")

    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
