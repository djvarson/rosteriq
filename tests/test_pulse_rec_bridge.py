"""Tests for rosteriq.pulse_rec_bridge — pure stdlib, no pytest."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq import pulse_rec_bridge as prb  # noqa: E402


# ---------------------------------------------------------------------------
# Minimal fixture builders
# ---------------------------------------------------------------------------

def _pulse(**overrides):
    base = {
        "venue_id": "mojos",
        "timestamp": "2026-04-11T19:30:00",
        "current_hour": "19:30",
        "wages_burned_so_far": 2000.0,
        "wages_forecast_today": 4200.0,
        "wages_pct_of_forecast": 0.48,
        "revenue_so_far": 8000.0,
        "revenue_forecast_today": 15000.0,
        "current_wage_pct_of_revenue": 0.25,
        "projected_wage_pct_of_revenue": 0.28,
        "hourly_burn_rate": 300.0,
        "trend": "on_track",
        "minutes_remaining": 210,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# evaluate_pulse — no-op cases
# ---------------------------------------------------------------------------

def test_neutral_pulse_emits_nothing():
    pulse = _pulse()
    recs = prb.evaluate_pulse(pulse, target_wage_pct=0.28)
    assert recs == []


def test_missing_venue_id_emits_nothing():
    pulse = _pulse(venue_id="")
    assert prb.evaluate_pulse(pulse) == []


def test_unknown_timestamp_still_produces_valid_rec_id():
    pulse = _pulse(
        timestamp="",
        projected_wage_pct_of_revenue=0.34,  # +6pt over 0.28
    )
    recs = prb.evaluate_pulse(pulse)
    assert len(recs) >= 1
    assert recs[0]["rec_id"].startswith("rec_pulse_mojos_unknown_")


def test_tolerates_none_fields():
    pulse = _pulse(
        projected_wage_pct_of_revenue=None,
        current_wage_pct_of_revenue=None,
        revenue_forecast_today=None,
        hourly_burn_rate=None,
        wages_forecast_today=None,
    )
    # With everything None, nothing should fire.
    assert prb.evaluate_pulse(pulse) == []


# ---------------------------------------------------------------------------
# Wage-% tiers
# ---------------------------------------------------------------------------

def test_over_wage_med_fires_at_plus_3pt():
    pulse = _pulse(projected_wage_pct_of_revenue=0.31)  # +3pt
    recs = prb.evaluate_pulse(pulse, target_wage_pct=0.28)
    assert len(recs) == 1
    r = recs[0]
    assert r["priority"] == "med"
    assert r["source"] == "wage_pulse"
    assert r["rec_id"].endswith("_over_wage_med")
    assert r["impact_estimate_aud"] == round(0.03 * 15000, 0)


def test_over_wage_high_fires_at_plus_5pt_and_suppresses_med():
    pulse = _pulse(projected_wage_pct_of_revenue=0.335)  # +5.5pt
    recs = prb.evaluate_pulse(pulse, target_wage_pct=0.28)
    # only one over_wage rec — the high one, not also the med
    wage_buckets = [r for r in recs if "over_wage" in r["rec_id"]]
    assert len(wage_buckets) == 1
    assert wage_buckets[0]["priority"] == "high"
    assert wage_buckets[0]["rec_id"].endswith("_over_wage_high")


def test_under_wage_amber_fires_at_minus_3pt():
    pulse = _pulse(
        projected_wage_pct_of_revenue=0.24,  # -4pt under 0.28
        hourly_burn_rate=10.0,  # keep the burn rate alarm quiet
        wages_burned_so_far=50.0,
    )
    recs = prb.evaluate_pulse(pulse, target_wage_pct=0.28)
    under_buckets = [r for r in recs if "under_wage" in r["rec_id"]]
    assert len(under_buckets) == 1
    assert under_buckets[0]["priority"] == "med"
    # impact is +ve; we took abs(pct_delta)
    assert under_buckets[0]["impact_estimate_aud"] == round(0.04 * 15000, 0)


def test_at_target_emits_nothing_wage_tier():
    pulse = _pulse(projected_wage_pct_of_revenue=0.28)
    recs = prb.evaluate_pulse(pulse, target_wage_pct=0.28)
    assert [r for r in recs if "over_wage" in r["rec_id"] or "under_wage" in r["rec_id"]] == []


def test_uses_current_pct_when_projected_is_zero():
    pulse = _pulse(
        projected_wage_pct_of_revenue=0.0,
        current_wage_pct_of_revenue=0.34,  # +6pt
    )
    recs = prb.evaluate_pulse(pulse, target_wage_pct=0.28)
    assert any(r["rec_id"].endswith("_over_wage_high") for r in recs)


# ---------------------------------------------------------------------------
# Burn-rate alarm
# ---------------------------------------------------------------------------

def test_burn_rate_alarm_fires_on_runaway_burn():
    # wages_forecast=4200, burn=800/hr, 3h left → projected +2400 on top
    # of 2000 already burned = 4400 → overrun 200 > threshold 420? actually
    # threshold is max(100, 10% of 4200=420) → 420, and overrun is 4400-4200=200
    # so THIS case should not fire. Use a higher burn rate.
    pulse = _pulse(
        wages_burned_so_far=2000.0,
        wages_forecast_today=4200.0,
        hourly_burn_rate=1200.0,  # 3h * 1200 = 3600 → projected 5600 → overrun 1400 > 420
        minutes_remaining=180,
        projected_wage_pct_of_revenue=0.28,  # keep wage tier quiet
    )
    recs = prb.evaluate_pulse(pulse, target_wage_pct=0.28)
    burn = [r for r in recs if r["rec_id"].endswith("_burn_rate_high")]
    assert len(burn) == 1
    assert burn[0]["priority"] == "high"
    assert burn[0]["impact_estimate_aud"] == 1400.0


def test_burn_rate_alarm_quiet_below_threshold():
    pulse = _pulse(
        wages_burned_so_far=2000.0,
        wages_forecast_today=4200.0,
        hourly_burn_rate=700.0,  # 3h * 700 = 2100 → projected 4100 → no overrun
        minutes_remaining=180,
        projected_wage_pct_of_revenue=0.28,
    )
    recs = prb.evaluate_pulse(pulse, target_wage_pct=0.28)
    assert [r for r in recs if "burn_rate" in r["rec_id"]] == []


def test_burn_rate_alarm_quiet_when_less_than_60_min_remaining():
    pulse = _pulse(
        wages_burned_so_far=2000.0,
        wages_forecast_today=4200.0,
        hourly_burn_rate=9999.0,  # massive, but only 30 min left
        minutes_remaining=30,
    )
    recs = prb.evaluate_pulse(pulse, target_wage_pct=0.28)
    assert [r for r in recs if "burn_rate" in r["rec_id"]] == []


def test_burn_rate_and_wage_tier_can_both_fire():
    pulse = _pulse(
        projected_wage_pct_of_revenue=0.34,  # +6pt → over_wage_high
        wages_burned_so_far=2000.0,
        wages_forecast_today=4200.0,
        hourly_burn_rate=1500.0,
        minutes_remaining=180,
    )
    recs = prb.evaluate_pulse(pulse, target_wage_pct=0.28)
    buckets = {r["rec_id"].rsplit("_", 2)[-2] + "_" + r["rec_id"].rsplit("_", 2)[-1] for r in recs}
    # At minimum we expect over_wage_high and burn_rate_high
    assert any("over_wage_high" in r["rec_id"] for r in recs)
    assert any("burn_rate_high" in r["rec_id"] for r in recs)


# ---------------------------------------------------------------------------
# Deterministic rec_id
# ---------------------------------------------------------------------------

def test_same_pulse_produces_identical_rec_ids():
    p = _pulse(projected_wage_pct_of_revenue=0.31)
    r1 = prb.evaluate_pulse(p, target_wage_pct=0.28)
    r2 = prb.evaluate_pulse(p, target_wage_pct=0.28)
    assert [r["rec_id"] for r in r1] == [r["rec_id"] for r in r2]


def test_rec_id_encodes_venue_and_date():
    p = _pulse(venue_id="hamilton", timestamp="2026-06-15T12:00:00")
    p["projected_wage_pct_of_revenue"] = 0.32
    recs = prb.evaluate_pulse(p, target_wage_pct=0.28)
    assert len(recs) == 1
    assert recs[0]["rec_id"].startswith("rec_pulse_hamilton_2026-06-15_")


# ---------------------------------------------------------------------------
# record_pulse_recs store round-trip
# ---------------------------------------------------------------------------

class _FakeStore:
    def __init__(self):
        self.calls = []
        self.events = []

    def record(self, venue_id, *, text, source="manual",
               impact_estimate_aud=None, priority="med", rec_id=None):
        # Dedupe on rec_id (mimic real store)
        for ev in self.events:
            if ev.get("id") == rec_id:
                return ev
        ev = {
            "id": rec_id or f"rec_auto_{len(self.events)}",
            "venue_id": venue_id,
            "text": text,
            "source": source,
            "priority": priority,
            "impact_estimate_aud": impact_estimate_aud,
            "status": "pending",
        }
        self.events.append(ev)
        self.calls.append({"venue_id": venue_id, "rec_id": rec_id, "text": text})
        return ev


def test_record_pulse_recs_writes_events_to_store():
    pulse = _pulse(projected_wage_pct_of_revenue=0.31)
    store = _FakeStore()
    out = prb.record_pulse_recs(pulse, target_wage_pct=0.28, store=store)
    assert len(out) == 1
    assert len(store.events) == 1
    assert store.events[0]["source"] == "wage_pulse"
    assert store.events[0]["priority"] == "med"


def test_record_pulse_recs_idempotent_on_repeat():
    pulse = _pulse(projected_wage_pct_of_revenue=0.31)
    store = _FakeStore()
    prb.record_pulse_recs(pulse, target_wage_pct=0.28, store=store)
    prb.record_pulse_recs(pulse, target_wage_pct=0.28, store=store)
    prb.record_pulse_recs(pulse, target_wage_pct=0.28, store=store)
    assert len(store.events) == 1  # deduped


def test_record_pulse_recs_escalates_to_new_event_on_severity_jump():
    store = _FakeStore()
    # First pulse: +3pt → med
    pulse1 = _pulse(projected_wage_pct_of_revenue=0.31)
    prb.record_pulse_recs(pulse1, target_wage_pct=0.28, store=store)
    # Later pulse: +6pt → high. Different bucket, so NEW event.
    pulse2 = _pulse(projected_wage_pct_of_revenue=0.34)
    prb.record_pulse_recs(pulse2, target_wage_pct=0.28, store=store)
    assert len(store.events) == 2
    severities = {ev["priority"] for ev in store.events}
    assert severities == {"med", "high"}


def test_record_pulse_recs_empty_on_neutral_pulse():
    store = _FakeStore()
    out = prb.record_pulse_recs(_pulse(), target_wage_pct=0.28, store=store)
    assert out == []
    assert store.events == []


# ---------------------------------------------------------------------------
# Integration with real accountability_store
# ---------------------------------------------------------------------------

def test_integration_with_real_store():
    from rosteriq import accountability_store as acct
    acct.clear()
    pulse = _pulse(projected_wage_pct_of_revenue=0.33)  # +5pt → high
    prb.record_pulse_recs(pulse, target_wage_pct=0.28)
    hist = acct.history("mojos")
    assert len(hist) == 1
    assert hist[0]["source"] == "wage_pulse"
    assert hist[0]["priority"] == "high"
    assert hist[0]["status"] == "pending"
    # Calling again is a no-op
    prb.record_pulse_recs(pulse, target_wage_pct=0.28)
    assert len(acct.history("mojos")) == 1


if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_") and callable(v)]
    passed = failed = 0
    for t in tests:
        try:
            t()
            passed += 1
            print(f"  PASS {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"  FAIL {t.__name__}: {e}")
        except Exception as e:
            failed += 1
            print(f"  ERROR {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{passed}/{passed+failed} tests passed")
    sys.exit(0 if failed == 0 else 1)
