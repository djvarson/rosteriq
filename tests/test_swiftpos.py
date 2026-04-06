"""
Tests for SwiftPOS Data Feed Adapter
=====================================
"""

import asyncio
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rosteriq.data_feeds.swiftpos import (
    SwiftPOSCredentials,
    SwiftPOSClient,
    SwiftPOSAdapter,
    SalesAnalyser,
    SalesSnapshot,
    TradingPattern,
    DemandSignal,
    DEMAND_MULTIPLIERS,
    LocationDepartment,
    SwiftPOSError,
    SwiftPOSAuthError,
    create_swiftpos_adapter,
)

AU_TZ = timezone(timedelta(hours=10))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_credentials():
    return SwiftPOSCredentials(
        api_url="https://api.swiftpos.test/v1",
        clerk_id="CLERK001",
        client_id="CLIENT001",
        customer_id="CUST001",
    )


def make_transaction(hour=12, amount=45.50, category="Food", covers=2):
    dt = datetime(2026, 4, 6, hour, 30, 0, tzinfo=AU_TZ)
    return {
        "transactionTime": dt.isoformat(),
        "total": amount,
        "category": category,
        "covers": covers,
    }


def make_transactions(count=20, base_hour=11, category_mix=None):
    """Generate a list of test transactions."""
    txns = []
    categories = category_mix or ["Food", "Beverage", "Food", "Dessert"]
    for i in range(count):
        hour = base_hour + (i % 8)
        cat = categories[i % len(categories)]
        amount = 30 + (i * 2.5)
        txns.append(make_transaction(hour=hour, amount=amount, category=cat, covers=1 + i % 3))
    return txns


def make_snapshot(
    revenue=5000, txn_count=120, covers=80, hour=14,
    location_id="LOC001", day_offset=0,
):
    now = datetime(2026, 4, 6, hour, 0, 0, tzinfo=AU_TZ) - timedelta(days=day_offset)
    hourly = {h: revenue / 12 for h in range(10, 22)}
    return SalesSnapshot(
        timestamp=now,
        location_id=location_id,
        location_name="Test Venue",
        total_revenue=revenue,
        transaction_count=txn_count,
        avg_transaction_value=round(revenue / max(txn_count, 1), 2),
        covers=covers,
        top_categories={"Food": revenue * 0.6, "Beverage": revenue * 0.4},
        hourly_breakdown=hourly,
    )


# ---------------------------------------------------------------------------
# Credentials Tests
# ---------------------------------------------------------------------------

class TestCredentials:
    def test_url_trailing_slash_stripped(self):
        creds = SwiftPOSCredentials(
            api_url="https://api.test.com/v1/",
            clerk_id="C", client_id="CL", customer_id="CU",
        )
        assert creds.api_url == "https://api.test.com/v1"

    def test_all_fields_stored(self):
        creds = make_credentials()
        assert creds.clerk_id == "CLERK001"
        assert creds.client_id == "CLIENT001"
        assert creds.customer_id == "CUST001"


# ---------------------------------------------------------------------------
# SalesSnapshot Tests
# ---------------------------------------------------------------------------

class TestSalesSnapshot:
    def test_snapshot_creation(self):
        snap = make_snapshot()
        assert snap.total_revenue == 5000
        assert snap.transaction_count == 120
        assert snap.covers == 80
        assert snap.avg_transaction_value == round(5000 / 120, 2)

    def test_snapshot_hourly_breakdown(self):
        snap = make_snapshot()
        assert len(snap.hourly_breakdown) == 12
        assert all(v > 0 for v in snap.hourly_breakdown.values())

    def test_snapshot_top_categories(self):
        snap = make_snapshot()
        assert "Food" in snap.top_categories
        assert "Beverage" in snap.top_categories
        assert snap.top_categories["Food"] > snap.top_categories["Beverage"]


# ---------------------------------------------------------------------------
# SalesAnalyser Tests
# ---------------------------------------------------------------------------

class TestSalesAnalyser:
    def test_build_snapshot_from_transactions(self):
        analyser = SalesAnalyser()
        txns = make_transactions(count=10)
        snap = analyser.build_sales_snapshot("LOC001", "Test", txns)

        assert snap.transaction_count == 10
        assert snap.total_revenue > 0
        assert snap.avg_transaction_value > 0
        assert len(snap.top_categories) > 0

    def test_build_snapshot_empty_transactions(self):
        analyser = SalesAnalyser()
        snap = analyser.build_sales_snapshot("LOC001", "Test", [])

        assert snap.transaction_count == 0
        assert snap.total_revenue == 0
        assert snap.avg_transaction_value == 0

    def test_build_snapshot_covers_counted(self):
        analyser = SalesAnalyser()
        txns = [make_transaction(covers=3) for _ in range(5)]
        snap = analyser.build_sales_snapshot("LOC001", "Test", txns)
        assert snap.covers == 15

    def test_build_snapshot_hourly_breakdown(self):
        analyser = SalesAnalyser()
        txns = [make_transaction(hour=14, amount=100) for _ in range(5)]
        snap = analyser.build_sales_snapshot("LOC001", "Test", txns)
        assert 14 in snap.hourly_breakdown
        assert snap.hourly_breakdown[14] == 500

    def test_build_snapshot_category_totals(self):
        analyser = SalesAnalyser()
        txns = [
            make_transaction(category="Food", amount=100),
            make_transaction(category="Food", amount=50),
            make_transaction(category="Beverage", amount=75),
        ]
        snap = analyser.build_sales_snapshot("LOC001", "Test", txns)
        assert snap.top_categories["Food"] == 150
        assert snap.top_categories["Beverage"] == 75

    def test_analyse_trading_patterns(self):
        analyser = SalesAnalyser()
        # Create snapshots across multiple weeks
        snapshots = []
        for week in range(4):
            for day in range(7):
                snap = make_snapshot(
                    revenue=4000 + week * 200,
                    day_offset=week * 7 + day,
                )
                snapshots.append(snap)

        patterns = analyser.analyse_trading_patterns(snapshots)
        assert len(patterns) > 0
        assert all(isinstance(p, TradingPattern) for p in patterns)

    def test_trading_pattern_has_volatility(self):
        analyser = SalesAnalyser()
        snapshots = [make_snapshot(revenue=r, day_offset=i * 7)
                     for i, r in enumerate([3000, 5000, 4000, 6000])]
        patterns = analyser.analyse_trading_patterns(snapshots)
        # At least some patterns should have non-zero volatility
        has_volatility = any(p.volatility > 0 for p in patterns)
        assert has_volatility

    def test_demand_signal_surge(self):
        analyser = SalesAnalyser()
        # Pattern: avg 100 per hour
        pattern = TradingPattern(
            day_of_week=0, hour=14,
            avg_revenue=100, avg_transactions=10, avg_covers=8,
            volatility=10, trend=0,
        )
        # Current: 140 (40% above average)
        snap = SalesSnapshot(
            timestamp=datetime(2026, 4, 6, 14, 0, tzinfo=AU_TZ),  # Monday
            location_id="L", location_name="T",
            total_revenue=140, transaction_count=14,
            avg_transaction_value=10, covers=11,
            hourly_breakdown={14: 140},
        )
        signal, mult = analyser.get_demand_signal(snap, [pattern])
        assert signal == DemandSignal.SURGE
        assert mult == DEMAND_MULTIPLIERS[DemandSignal.SURGE]

    def test_demand_signal_quiet(self):
        analyser = SalesAnalyser()
        pattern = TradingPattern(
            day_of_week=0, hour=14,
            avg_revenue=100, avg_transactions=10, avg_covers=8,
            volatility=10, trend=0,
        )
        snap = SalesSnapshot(
            timestamp=datetime(2026, 4, 6, 14, 0, tzinfo=AU_TZ),
            location_id="L", location_name="T",
            total_revenue=50, transaction_count=5,
            avg_transaction_value=10, covers=4,
            hourly_breakdown={14: 50},
        )
        signal, mult = analyser.get_demand_signal(snap, [pattern])
        assert signal == DemandSignal.QUIET
        assert mult < 0

    def test_demand_signal_normal(self):
        analyser = SalesAnalyser()
        pattern = TradingPattern(
            day_of_week=0, hour=14,
            avg_revenue=100, avg_transactions=10, avg_covers=8,
            volatility=10, trend=0,
        )
        snap = SalesSnapshot(
            timestamp=datetime(2026, 4, 6, 14, 0, tzinfo=AU_TZ),
            location_id="L", location_name="T",
            total_revenue=105, transaction_count=10,
            avg_transaction_value=10.5, covers=8,
            hourly_breakdown={14: 105},
        )
        signal, mult = analyser.get_demand_signal(snap, [pattern])
        assert signal == DemandSignal.NORMAL
        assert mult == 0.0

    def test_demand_signal_no_matching_pattern(self):
        analyser = SalesAnalyser()
        # Pattern for Tuesday, but snapshot is Monday
        pattern = TradingPattern(
            day_of_week=1, hour=14,
            avg_revenue=100, avg_transactions=10, avg_covers=8,
            volatility=10, trend=0,
        )
        snap = SalesSnapshot(
            timestamp=datetime(2026, 4, 6, 14, 0, tzinfo=AU_TZ),  # Monday=0
            location_id="L", location_name="T",
            total_revenue=200, transaction_count=20,
            avg_transaction_value=10, covers=15,
            hourly_breakdown={14: 200},
        )
        signal, mult = analyser.get_demand_signal(snap, [pattern])
        assert signal == DemandSignal.NORMAL
        assert mult == 0.0


# ---------------------------------------------------------------------------
# DemandSignal Tests
# ---------------------------------------------------------------------------

class TestDemandSignal:
    def test_all_signals_have_multipliers(self):
        for signal in DemandSignal:
            assert signal in DEMAND_MULTIPLIERS

    def test_surge_is_positive(self):
        assert DEMAND_MULTIPLIERS[DemandSignal.SURGE] > 0

    def test_quiet_is_negative(self):
        assert DEMAND_MULTIPLIERS[DemandSignal.QUIET] < 0

    def test_normal_is_zero(self):
        assert DEMAND_MULTIPLIERS[DemandSignal.NORMAL] == 0.0


# ---------------------------------------------------------------------------
# SwiftPOSAdapter Tests
# ---------------------------------------------------------------------------

class TestSwiftPOSAdapter:
    def test_create_adapter_factory(self):
        adapter = create_swiftpos_adapter(
            api_url="https://api.test.com",
            clerk_id="C", client_id="CL",
            customer_id="CU", location_id="L",
            location_name="Test Venue",
        )
        assert isinstance(adapter, SwiftPOSAdapter)
        assert adapter.location_id == "L"
        assert adapter.location_name == "Test Venue"

    def test_adapter_default_settings(self):
        adapter = create_swiftpos_adapter(
            api_url="https://api.test.com",
            clerk_id="C", client_id="CL",
            customer_id="CU", location_id="L",
        )
        assert adapter.fetch_interval_minutes == 15
        assert adapter.analyser.lookback_weeks == 8

    def test_confidence_calculation_base(self):
        adapter = create_swiftpos_adapter(
            api_url="https://api.test.com",
            clerk_id="C", client_id="CL",
            customer_id="CU", location_id="L",
        )
        # Empty snapshot
        snap = SalesSnapshot(
            timestamp=datetime.now(AU_TZ),
            location_id="L", location_name="T",
            total_revenue=0, transaction_count=0,
            avg_transaction_value=0, covers=0,
        )
        conf = adapter._calculate_confidence(snap)
        assert conf == 0.5  # base score only

    def test_confidence_calculation_full_data(self):
        adapter = create_swiftpos_adapter(
            api_url="https://api.test.com",
            clerk_id="C", client_id="CL",
            customer_id="CU", location_id="L",
        )
        adapter._patterns = list(range(60))  # >50 patterns
        snap = SalesSnapshot(
            timestamp=datetime.now(AU_TZ),
            location_id="L", location_name="T",
            total_revenue=5000, transaction_count=100,
            avg_transaction_value=50, covers=80,
            top_categories={"Food": 3000, "Bev": 1500, "Dessert": 500},
        )
        conf = adapter._calculate_confidence(snap)
        assert conf == 1.0  # all bonuses

    def test_category_shift_beverage_heavy(self):
        adapter = create_swiftpos_adapter(
            api_url="https://api.test.com",
            clerk_id="C", client_id="CL",
            customer_id="CU", location_id="L",
        )
        snap = SalesSnapshot(
            timestamp=datetime.now(AU_TZ),
            location_id="L", location_name="T",
            total_revenue=5000, transaction_count=100,
            avg_transaction_value=50, covers=80,
            top_categories={"Beer": 2500, "Wine": 1500, "Food": 1000},
        )
        signal = adapter._analyse_category_shift(snap)
        assert signal is not None
        assert signal["metadata"]["pattern"] == "beverage_heavy"

    def test_category_shift_balanced_returns_none(self):
        adapter = create_swiftpos_adapter(
            api_url="https://api.test.com",
            clerk_id="C", client_id="CL",
            customer_id="CU", location_id="L",
        )
        snap = SalesSnapshot(
            timestamp=datetime.now(AU_TZ),
            location_id="L", location_name="T",
            total_revenue=5000, transaction_count=100,
            avg_transaction_value=50, covers=80,
            top_categories={"Food": 3000, "Beverage": 2000},
        )
        signal = adapter._analyse_category_shift(snap)
        assert signal is None  # Not beverage-heavy enough

    def test_velocity_signal_rush_detected(self):
        adapter = create_swiftpos_adapter(
            api_url="https://api.test.com",
            clerk_id="C", client_id="CL",
            customer_id="CU", location_id="L",
        )
        # Set up pattern: 60 transactions per hour average on Monday at 2pm
        adapter._patterns = [
            TradingPattern(
                day_of_week=0, hour=14,
                avg_revenue=3000, avg_transactions=60, avg_covers=40,
                volatility=15, trend=0,
            )
        ]
        # Current: 30 txns in 15 minutes = 2/min (avg is 1/min) → rush
        snap = SalesSnapshot(
            timestamp=datetime(2026, 4, 6, 14, 15, tzinfo=AU_TZ),  # Monday
            location_id="L", location_name="T",
            total_revenue=1500, transaction_count=30,
            avg_transaction_value=50, covers=25,
            hourly_breakdown={14: 1500},
        )
        signal = adapter._analyse_velocity(snap)
        assert signal is not None
        assert signal["metadata"]["alert"] is not None

    def test_velocity_signal_normal_returns_none(self):
        adapter = create_swiftpos_adapter(
            api_url="https://api.test.com",
            clerk_id="C", client_id="CL",
            customer_id="CU", location_id="L",
        )
        adapter._patterns = [
            TradingPattern(
                day_of_week=0, hour=14,
                avg_revenue=3000, avg_transactions=60, avg_covers=40,
                volatility=15, trend=0,
            )
        ]
        # Normal velocity: 15 txns in 15 mins = 1/min (avg is 1/min)
        snap = SalesSnapshot(
            timestamp=datetime(2026, 4, 6, 14, 15, tzinfo=AU_TZ),
            location_id="L", location_name="T",
            total_revenue=750, transaction_count=15,
            avg_transaction_value=50, covers=12,
            hourly_breakdown={14: 750},
        )
        signal = adapter._analyse_velocity(snap)
        assert signal is None


# ---------------------------------------------------------------------------
# LocationDepartment Tests
# ---------------------------------------------------------------------------

class TestLocationDepartment:
    def test_creation(self):
        loc = LocationDepartment(
            location_id="L001",
            location_name="Main Bar",
            department_id="D001",
            department_name="Bar",
        )
        assert loc.is_active is True
        assert loc.location_name == "Main Bar"


# ---------------------------------------------------------------------------
# Run tests
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
