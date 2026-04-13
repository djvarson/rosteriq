"""
Tests for the extra Tanda reads added to tanda_adapter:

- Department categorisation (Tanda dept name → RosterIQ category)
- Employee enrichment with department_id/name/category
- get_forecast_revenue on both TandaAdapter (real) and DemoTandaAdapter

These reads let the Roster Maker split demand across kitchen/bar/floor
and benchmark RosterIQ's forecast against Tanda's own.
"""

import asyncio
from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq.tanda_adapter import (
    DepartmentCategory,
    DemoTandaAdapter,
    Employee,
    ForecastRevenue,
    TandaAdapter,
    TandaClient,
    categorise_department,
)


def _run(coro):
    """Small helper to run async tests without an asyncio plugin."""
    return asyncio.get_event_loop().run_until_complete(coro)


# ----------------------------------------------------------------------------
# Department categorisation
# ----------------------------------------------------------------------------


def test_categorise_kitchen_names():
    assert categorise_department("Main Kitchen") == DepartmentCategory.KITCHEN.value
    assert categorise_department("Pastry Prep") == DepartmentCategory.KITCHEN.value
    assert categorise_department("Head Chef") == DepartmentCategory.KITCHEN.value


def test_categorise_bar_names():
    assert categorise_department("Public Bar") == DepartmentCategory.BAR.value
    assert categorise_department("Cocktail Lounge") == DepartmentCategory.BAR.value
    assert categorise_department("Wine Service") == DepartmentCategory.BAR.value
    assert categorise_department("Barista Counter") == DepartmentCategory.BAR.value


def test_categorise_floor_names():
    assert categorise_department("Restaurant Floor") == DepartmentCategory.FLOOR.value
    assert categorise_department("Waitstaff") == DepartmentCategory.FLOOR.value
    assert categorise_department("Table Service") == DepartmentCategory.FLOOR.value


def test_categorise_foh_and_management():
    assert categorise_department("Host Stand") == DepartmentCategory.FOH.value
    assert categorise_department("Reservations") == DepartmentCategory.FOH.value
    assert categorise_department("Duty Manager") == DepartmentCategory.MANAGEMENT.value
    assert categorise_department("Administration") == DepartmentCategory.MANAGEMENT.value


def test_categorise_security():
    assert categorise_department("Security") == DepartmentCategory.SECURITY.value
    assert categorise_department("Crowd Control") == DepartmentCategory.SECURITY.value


def test_categorise_unknown_or_empty():
    assert categorise_department(None) == DepartmentCategory.OTHER.value
    assert categorise_department("") == DepartmentCategory.OTHER.value
    assert categorise_department("Xyzzy Team") == DepartmentCategory.OTHER.value


def test_categorise_is_case_insensitive():
    assert categorise_department("KITCHEN") == DepartmentCategory.KITCHEN.value
    assert categorise_department("kitchen") == DepartmentCategory.KITCHEN.value
    assert categorise_department("Kitchen") == DepartmentCategory.KITCHEN.value


# ----------------------------------------------------------------------------
# Demo adapter — employee enrichment + forecast revenue
# ----------------------------------------------------------------------------


def test_demo_employees_have_department_fields():
    """Every demo employee should have department_category populated."""
    demo = DemoTandaAdapter()
    employees = _run(demo.get_employees("demo_venue"))
    assert len(employees) > 0
    for emp in employees:
        assert emp.department_id is not None
        assert emp.department_name is not None
        assert emp.department_category in {
            c.value for c in DepartmentCategory
        }
    # Ensure we see real category variety across the demo cohort
    categories = {e.department_category for e in employees}
    assert DepartmentCategory.KITCHEN.value in categories
    assert DepartmentCategory.BAR.value in categories


def test_demo_forecast_revenue_shape_and_weekly_pattern():
    demo = DemoTandaAdapter()
    start = date(2026, 4, 13)  # Monday
    end = start + timedelta(days=6)
    forecasts = _run(demo.get_forecast_revenue("demo_venue", (start, end)))

    assert len(forecasts) == 7
    for f in forecasts:
        assert isinstance(f, ForecastRevenue)
        assert f.forecast > 0
        assert f.source == "tanda_demo"
        assert set(f.department_breakdown.keys()) == {"Kitchen", "Bar", "Floor", "Other"}
        # Breakdown should sum to (approximately) the headline forecast
        total = sum(f.department_breakdown.values())
        assert abs(total - f.forecast) < 1.0  # within rounding noise

    # Friday (index 4) and Saturday (index 5) should exceed Mon-Thu
    weekday_avg = sum(f.forecast for f in forecasts[:4]) / 4
    weekend_avg = (forecasts[4].forecast + forecasts[5].forecast) / 2
    assert weekend_avg > weekday_avg


def test_demo_forecast_single_day_range():
    demo = DemoTandaAdapter()
    d = date(2026, 4, 14)
    forecasts = _run(demo.get_forecast_revenue("demo_venue", (d, d)))
    assert len(forecasts) == 1
    assert forecasts[0].date == d


# ----------------------------------------------------------------------------
# Real TandaAdapter — mocked HTTP client
# ----------------------------------------------------------------------------


def _make_mock_client():
    """Build a TandaClient with paginate/get stubbed out."""
    client = MagicMock(spec=TandaClient)
    client.paginate = AsyncMock()
    client.get = AsyncMock()
    return client


def test_tanda_adapter_enriches_employees_with_department():
    client = _make_mock_client()

    # First paginate call: departments. Second: users.
    client.paginate.side_effect = [
        # Departments
        [
            {"id": "d1", "name": "Main Kitchen"},
            {"id": "d2", "name": "Public Bar"},
        ],
        # Users
        [
            {
                "id": "u1",
                "name": "Jack",
                "email": "j@e.com",
                "department_ids": ["d1"],
                "employment_type": "casual",
                "hourly_rate": 26.0,
            },
            {
                "id": "u2",
                "name": "Sophie",
                "email": "s@e.com",
                "department_ids": ["d2"],
                "employment_type": "part_time",
                "hourly_rate": 27.5,
            },
            {
                "id": "u3",
                "name": "Liam",
                "email": "l@e.com",
                # No department_ids, but role string should still
                # get picked up by the fallback in get_employees
                "role": "wine_service",
                "employment_type": "casual",
                "hourly_rate": 25.0,
            },
        ],
    ]

    adapter = TandaAdapter(client)
    employees = _run(adapter.get_employees("org_1"))

    assert len(employees) == 3
    by_id = {e.id: e for e in employees}

    assert by_id["u1"].department_name == "Main Kitchen"
    assert by_id["u1"].department_category == DepartmentCategory.KITCHEN.value
    assert by_id["u2"].department_name == "Public Bar"
    assert by_id["u2"].department_category == DepartmentCategory.BAR.value
    # u3 has no department_ids — falls back to role "wine_service" → bar
    assert by_id["u3"].department_name is None
    assert by_id["u3"].department_category == DepartmentCategory.BAR.value


def test_tanda_adapter_get_forecast_revenue_parses_response():
    client = _make_mock_client()

    # Departments (for name lookup)
    client.paginate.return_value = [
        {"id": "d1", "name": "Main Kitchen"},
        {"id": "d2", "name": "Public Bar"},
    ]

    # /revenue_forecast response
    client.get.return_value = {
        "data": [
            {
                "date": "2026-04-13",
                "forecast": 12500.0,
                "departments": {"d1": 7500.0, "d2": 5000.0},
            },
            {
                "date": "2026-04-14",
                "forecast": 9800.0,
                "departments": {"d1": 6000.0, "d2": 3800.0},
            },
        ]
    }

    adapter = TandaAdapter(client)
    start = date(2026, 4, 13)
    end = date(2026, 4, 14)
    forecasts = _run(adapter.get_forecast_revenue("org_1", (start, end)))

    assert len(forecasts) == 2
    assert forecasts[0].date == start
    assert forecasts[0].forecast == 12500.0
    # Department ids should have been remapped to their names
    assert forecasts[0].department_breakdown == {
        "Main Kitchen": 7500.0,
        "Public Bar": 5000.0,
    }
    assert forecasts[1].source == "tanda"

    # Confirm the adapter actually hit the revenue_forecast endpoint with
    # the right date range.
    args, kwargs = client.get.call_args
    assert args[0].endswith("/revenue_forecast")
    assert kwargs["params"]["from"] == "2026-04-13"
    assert kwargs["params"]["to"] == "2026-04-14"


def test_tanda_adapter_forecast_revenue_handles_missing_departments():
    """If Tanda omits the department breakdown we still return the headline."""
    client = _make_mock_client()
    client.paginate.return_value = []  # no departments
    client.get.return_value = {
        "data": [{"date": "2026-04-13", "forecast": 8000.0}]
    }

    adapter = TandaAdapter(client)
    d = date(2026, 4, 13)
    forecasts = _run(adapter.get_forecast_revenue("org_1", (d, d)))

    assert len(forecasts) == 1
    assert forecasts[0].forecast == 8000.0
    assert forecasts[0].department_breakdown == {}


def test_tanda_adapter_forecast_revenue_handles_single_dict_response():
    """Some Tanda tenants return a single object instead of a list."""
    client = _make_mock_client()
    client.paginate.return_value = []
    client.get.return_value = {"date": "2026-04-13", "forecast": 7200.0}

    adapter = TandaAdapter(client)
    d = date(2026, 4, 13)
    forecasts = _run(adapter.get_forecast_revenue("org_1", (d, d)))

    assert len(forecasts) == 1
    assert forecasts[0].date == d
    assert forecasts[0].forecast == 7200.0
