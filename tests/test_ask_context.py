"""
Unit tests for the /ask endpoint pipeline: ask_context + query_library.

Philosophy: we don't test query_library's internal handlers here (each
handler has its own narrow test in the query library's own suite). What
we test is the *integration contract* — the thing that has to not break
for the /ask endpoint to work:

  1. build_demo_query_context is deterministic per (venue_id, today)
  2. The resulting context has enough data for every supported query to
     route cleanly (matched=True with a non-empty headline)
  3. Unknown questions return matched=False with a helpful reason
  4. Every QueryResult serialises to JSON-safe dicts (for FastAPI)

Run with: python -m pytest tests/test_ask_context.py -q
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq.ask_context import build_demo_query_context  # noqa: E402
from rosteriq.query_library import route_question, list_supported_queries  # noqa: E402


FIXED_TODAY = date(2026, 4, 11)  # a Saturday — gives the demo some weekend data
VENUE = "test_venue_001"


# ---------------------------------------------------------------------------
# 1. Determinism
# ---------------------------------------------------------------------------

def test_demo_context_is_deterministic_per_venue_and_today():
    """Same (venue_id, today) must produce byte-identical synthetic data.

    If this test flakes, the demo will show different numbers every
    refresh and look buggy in front of a pilot venue."""
    a = build_demo_query_context(VENUE, today=FIXED_TODAY)
    b = build_demo_query_context(VENUE, today=FIXED_TODAY)

    # Spot-check a few cardinal invariants
    assert len(a.rosters) == len(b.rosters)
    assert len(a.vendor_forecasts) == len(b.vendor_forecasts)
    assert len(a.head_counts) == len(b.head_counts)

    # Total revenue should be byte-identical
    a_total = sum(float(r.amount) for r in a.vendor_forecasts)
    b_total = sum(float(r.amount) for r in b.vendor_forecasts)
    assert a_total == b_total


def test_demo_context_varies_by_venue():
    """Two different venues on the same day must produce different
    numbers — otherwise our 'per-venue' story is a lie."""
    a = build_demo_query_context("venue_a", today=FIXED_TODAY)
    b = build_demo_query_context("venue_b", today=FIXED_TODAY)
    a_total = sum(float(r.amount) for r in a.vendor_forecasts)
    b_total = sum(float(r.amount) for r in b.vendor_forecasts)
    assert a_total != b_total


# ---------------------------------------------------------------------------
# 2. Routing coverage
# ---------------------------------------------------------------------------

# Every supported query in query_library.list_supported_queries() should
# be reachable by at least one example phrasing. If a new query is added
# to the library without a sample phrasing here, this test fails loud.
# Keyed by canonical name rather than phrasing so we can also assert the
# routed query.
EXAMPLE_PHRASINGS = {
    "total_sales":            "sales last week",
    "total_wage_cost":        "total wage cost last week",
    "wage_cost_percentage":   "wage cost percentage last week",
    "days_over_wage_pct":     "which days over 30% last month",
    "last_n_same_weekdays":   "last 4 saturdays",
    "busiest_day":            "busiest day this month",
    "peak_head_count":        "peak head count yesterday",
    "staff_hours_summary":    "total hours last week",
    "worst_day":              "worst day last month",
    "overtime_hours":         "overtime hours last week",
    "average_wage_pct_per_day": "average wage % per day last week",
    "hours_by_employee":      "hours by employee last week",
}


def test_all_supported_queries_have_an_example_phrasing():
    """The EXAMPLE_PHRASINGS dict above must cover every query returned
    by list_supported_queries() — otherwise our demo chips list goes
    out of sync with what the library actually supports."""
    supported = set(list_supported_queries())
    covered = set(EXAMPLE_PHRASINGS.keys())
    missing = supported - covered
    assert not missing, (
        f"These queries have no sample phrasing in the test fixture: {missing}. "
        "Add one to EXAMPLE_PHRASINGS."
    )


def test_example_phrasings_route_cleanly():
    """Every example phrasing must return matched=True with a non-None
    query_result against the demo context. A failure means either (a)
    the router regex stopped matching, or (b) the demo context doesn't
    have enough data to produce a result for that query."""
    ctx = build_demo_query_context(VENUE, today=FIXED_TODAY)
    for name, phrasing in EXAMPLE_PHRASINGS.items():
        result = route_question(phrasing, ctx)
        assert result.matched, (
            f"{phrasing!r} ({name}) did not match. Reason: {result.reason}"
        )
        assert result.query_result is not None
        qr = result.query_result
        assert qr.query, f"query name must be set for {name}"
        assert qr.headline_value is not None, (
            f"{phrasing!r} routed to {qr.query} but returned None headline_value"
        )


def test_empty_question_returns_unmatched():
    ctx = build_demo_query_context(VENUE, today=FIXED_TODAY)
    r = route_question("   ", ctx)
    assert r.matched is False
    assert r.reason


def test_gibberish_returns_unmatched_with_helpful_reason():
    ctx = build_demo_query_context(VENUE, today=FIXED_TODAY)
    r = route_question("xkcd what is love baby don't hurt me", ctx)
    assert r.matched is False
    assert r.reason and len(r.reason) > 10  # should be a sentence, not empty


# ---------------------------------------------------------------------------
# 3. JSON-safety of QueryResult.to_dict()
# ---------------------------------------------------------------------------

def test_query_result_dict_is_json_serialisable():
    """Every QueryResult.to_dict() must be JSON-safe (no Decimal, no
    datetime objects). If this fails, FastAPI will 500 on the /ask
    endpoint."""
    ctx = build_demo_query_context(VENUE, today=FIXED_TODAY)
    for name, phrasing in EXAMPLE_PHRASINGS.items():
        result = route_question(phrasing, ctx)
        assert result.matched, f"{name} didn't match"
        d = result.query_result.to_dict()
        json_str = json.dumps(d)
        assert isinstance(json_str, str) and len(json_str) > 0, (
            f"{name} to_dict() not JSON-serialisable"
        )


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
    print(f"\n{passed}/{passed + failed} tests passed")
    sys.exit(0 if failed == 0 else 1)
