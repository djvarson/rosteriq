"""
Comprehensive test suite for Lightspeed Restaurant K-Series adapter.

Tests cover:
  - Credentials validation
  - OAuth token caching and refresh
  - Sales snapshot building
  - Trading pattern analysis
  - Demand signal calculation
  - Revenue centre analysis
  - Covers velocity analysis
  - Factory function
  - Health check
  - Error handling

No network calls required - all tests use mocked httpx client.
"""

import sys
import asyncio
from pathlib import Path
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import dataclass

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from rosteriq.data_feeds.lightspeed import (
    LightspeedCredentials,
    LightspeedClient,
    LightspeedAdapter,
    LightspeedError,
    LightspeedAuthError,
    LightspeedRateLimitError,
    SalesSnapshot,
    TradingPattern,
    DemandSignal,
    SalesAnalyser,
    RevenueCenterBreakdown,
    create_lightspeed_adapter,
    AU_TZ,
)


# ---------------------------------------------------------------------------
# Helper Functions (formerly fixtures)
# ---------------------------------------------------------------------------

def _valid_credentials():
    """Valid test credentials."""
    return LightspeedCredentials(
        client_id="test_client_id",
        client_secret="test_client_secret",
        refresh_token="test_refresh_token",
        business_location_id="loc_12345",
    )


def _sample_sales_data():
    """Sample Lightspeed API sales response."""
    now = datetime.now(AU_TZ)
    return [
        {
            "id": "sale_001",
            "total": 125.50,
            "tax": 12.55,
            "covers": 2,
            "timeClosed": now.isoformat(),
            "revenueCenter": "Dining",
            "staff": {"id": "staff_001", "name": "John"},
        },
        {
            "id": "sale_002",
            "total": 85.00,
            "covers": 1,
            "timeClosed": (now - timedelta(minutes=5)).isoformat(),
            "revenueCenter": "Bar",
            "staff": {"id": "staff_002", "name": "Jane"},
        },
        {
            "id": "sale_003",
            "total": 45.75,
            "covers": 1,
            "timeClosed": (now - timedelta(minutes=10)).isoformat(),
            "revenueCenter": "Takeaway",
        },
    ]


def _sample_historical_sales():
    """Generate 8 weeks of sample historical sales."""
    base_date = datetime.now(AU_TZ) - timedelta(weeks=8)
    sales = []

    for week in range(8):
        for day in range(7):
            current_date = base_date + timedelta(weeks=week, days=day)
            # Generate sales for each hour (10am to 10pm)
            for hour in range(10, 22):
                hour_time = current_date.replace(hour=hour, minute=30)
                # Mon-Fri busier than Sat-Sun
                base_txn_count = 5 if day < 5 else 8
                txn_count = base_txn_count + (week % 2)  # Slight variation

                for txn in range(txn_count):
                    sales.append({
                        "id": f"sale_{week}_{day}_{hour}_{txn}",
                        "total": 75.00 + (txn * 5),
                        "covers": 1 if txn % 2 == 0 else 2,
                        "timeClosed": hour_time.isoformat(),
                        "revenueCenter": "Dining",
                    })

    return sales


# ---------------------------------------------------------------------------
# Credentials Tests
# ---------------------------------------------------------------------------

class TestLightspeedCredentials:
    """Test credential validation."""

    def test_valid_credentials(self):
        """Test valid credentials creation."""
        valid_credentials = _valid_credentials()
        assert valid_credentials.client_id == "test_client_id"
        assert valid_credentials.client_secret == "test_client_secret"
        assert valid_credentials.refresh_token == "test_refresh_token"
        assert valid_credentials.business_location_id == "loc_12345"

    def test_empty_client_id_raises_error(self):
        """Test that empty client_id raises ValueError."""
        try:
            LightspeedCredentials(
                client_id="",
                client_secret="secret",
                refresh_token="token",
                business_location_id="loc_123",
            )
            raise AssertionError("expected ValueError")
        except ValueError:
            pass

    def test_empty_client_secret_raises_error(self):
        """Test that empty client_secret raises ValueError."""
        try:
            LightspeedCredentials(
                client_id="client",
                client_secret="",
                refresh_token="token",
                business_location_id="loc_123",
            )
            raise AssertionError("expected ValueError")
        except ValueError:
            pass

    def test_empty_refresh_token_raises_error(self):
        """Test that empty refresh_token raises ValueError."""
        try:
            LightspeedCredentials(
                client_id="client",
                client_secret="secret",
                refresh_token="",
                business_location_id="loc_123",
            )
            raise AssertionError("expected ValueError")
        except ValueError:
            pass

    def test_empty_location_id_raises_error(self):
        """Test that empty business_location_id raises ValueError."""
        try:
            LightspeedCredentials(
                client_id="client",
                client_secret="secret",
                refresh_token="token",
                business_location_id="",
            )
            raise AssertionError("expected ValueError")
        except ValueError:
            pass


# ---------------------------------------------------------------------------
# OAuth Token Cache Tests
# ---------------------------------------------------------------------------

class TestLightspeedClient:
    """Test low-level Lightspeed API client."""

    def test_client_initialization(self):
        """Test client initializes correctly."""
        valid_credentials = _valid_credentials()
        client = LightspeedClient(valid_credentials)
        assert client.creds == valid_credentials
        assert client._token_cache.token is None
        asyncio.run(client.close())

    def test_token_cache_validity(self):
        """Test token cache expiry tracking."""
        valid_credentials = _valid_credentials()
        client = LightspeedClient(valid_credentials)

        # Token not yet set
        assert not client._token_cache.is_valid

        # Set valid token (expires in 1 hour)
        client._token_cache.token = "test_token"
        client._token_cache.expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
        assert client._token_cache.is_valid

        # Set expired token
        client._token_cache.expires_at = datetime.now(timezone.utc) - timedelta(minutes=1)
        assert not client._token_cache.is_valid

        asyncio.run(client.close())

    def test_token_refresh_success(self):
        """Test successful OAuth token refresh."""
        valid_credentials = _valid_credentials()
        client = LightspeedClient(valid_credentials)

        # Mock httpx response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "new_access_token",
            "expires_in": 3600,
            "token_type": "Bearer",
        }

        with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_response

            token = asyncio.run(client._refresh_oauth_token())
            assert token == "new_access_token"
            assert client._token_cache.token == "new_access_token"

        asyncio.run(client.close())

    def test_token_refresh_auth_error(self):
        """Test token refresh auth error handling."""
        valid_credentials = _valid_credentials()
        client = LightspeedClient(valid_credentials)

        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.json.return_value = {"error": "invalid_credentials"}

        with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_response

            try:
                asyncio.run(client._refresh_oauth_token())
                raise AssertionError("expected LightspeedAuthError")
            except LightspeedAuthError:
                pass

        asyncio.run(client.close())

    def test_token_cache_reuse(self):
        """Test that valid cached token is reused."""
        valid_credentials = _valid_credentials()
        client = LightspeedClient(valid_credentials)

        # Set valid token in cache
        client._token_cache.token = "cached_token"
        client._token_cache.expires_at = datetime.now(timezone.utc) + timedelta(hours=1)

        # Should not make HTTP call
        with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
            token = asyncio.run(client._refresh_oauth_token())
            assert token == "cached_token"
            mock_post.assert_not_called()

        asyncio.run(client.close())


# ---------------------------------------------------------------------------
# Sales Snapshot Tests
# ---------------------------------------------------------------------------

class TestSalesSnapshot:
    """Test SalesSnapshot building."""

    def test_snapshot_initialization(self):
        """Test snapshot initializes with required fields."""
        now = datetime.now(AU_TZ)
        snapshot = SalesSnapshot(
            timestamp=now,
            location_id="loc_123",
            location_name="Test Venue",
            total_revenue=500.00,
            transaction_count=5,
            avg_transaction_value=100.00,
            covers=10,
        )
        assert snapshot.location_id == "loc_123"
        assert snapshot.total_revenue == 500.00
        assert snapshot.transaction_count == 5
        assert snapshot.covers == 10


class TestSalesAnalyser:
    """Test sales data analysis."""

    def test_analyser_initialization(self):
        """Test analyser initializes with lookback period."""
        analyser = SalesAnalyser(lookback_weeks=12)
        assert analyser.lookback_weeks == 12

    def test_build_sales_snapshot_empty(self):
        """Test snapshot building with empty sales list."""
        analyser = SalesAnalyser()
        snapshot = analyser.build_sales_snapshot(
            location_id="loc_123",
            location_name="Test Venue",
            sales=[],
        )
        assert snapshot.transaction_count == 0
        assert snapshot.total_revenue == 0.0
        assert snapshot.covers == 0

    def test_build_sales_snapshot_single_sale(self):
        """Test snapshot building with single sale."""
        analyser = SalesAnalyser()
        now = datetime.now(AU_TZ)
        sales = [{
            "total": 100.00,
            "covers": 2,
            "timeClosed": now.isoformat(),
            "revenueCenter": "Dining",
        }]

        snapshot = analyser.build_sales_snapshot(
            location_id="loc_123",
            location_name="Test Venue",
            sales=sales,
        )
        assert snapshot.transaction_count == 1
        assert snapshot.total_revenue == 100.00
        assert snapshot.avg_transaction_value == 100.00
        assert snapshot.covers == 2

    def test_build_sales_snapshot_multiple_sales(self):
        """Test snapshot building with multiple sales."""
        sample_sales_data = _sample_sales_data()
        analyser = SalesAnalyser()
        snapshot = analyser.build_sales_snapshot(
            location_id="loc_123",
            location_name="Test Venue",
            sales=sample_sales_data,
        )
        assert snapshot.transaction_count == 3
        # Use custom approx check instead of pytest.approx
        assert abs(snapshot.total_revenue - 256.25) / max(abs(256.25), 1e-9) < 0.01
        assert snapshot.covers == 4

    def test_build_sales_snapshot_revenue_centres(self):
        """Test revenue centre breakdown in snapshot."""
        sample_sales_data = _sample_sales_data()
        analyser = SalesAnalyser()
        snapshot = analyser.build_sales_snapshot(
            location_id="loc_123",
            location_name="Test Venue",
            sales=sample_sales_data,
        )
        # Should have Bar, Dining, Takeaway
        assert len(snapshot.top_revenue_centers) == 3
        assert "Bar" in snapshot.top_revenue_centers
        assert "Dining" in snapshot.top_revenue_centers
        assert "Takeaway" in snapshot.top_revenue_centers

    def test_build_sales_snapshot_hourly_breakdown(self):
        """Test hourly breakdown in snapshot."""
        sample_sales_data = _sample_sales_data()
        analyser = SalesAnalyser()
        snapshot = analyser.build_sales_snapshot(
            location_id="loc_123",
            location_name="Test Venue",
            sales=sample_sales_data,
        )
        # Should have hourly data
        assert len(snapshot.hourly_breakdown) > 0
        assert len(snapshot.hourly_covers) > 0

    def test_analyse_trading_patterns_basic(self):
        """Test trading pattern analysis."""
        sample_historical_sales = _sample_historical_sales()
        analyser = SalesAnalyser()

        # Build snapshots from historical sales
        snapshots = []
        base_date = datetime.now(AU_TZ) - timedelta(weeks=8)

        for week in range(8):
            for day in range(7):
                current_date = base_date + timedelta(weeks=week, days=day)
                day_sales = [s for s in sample_historical_sales
                             if s["timeClosed"].startswith(current_date.strftime("%Y-%m-%d"))]
                if day_sales:
                    snap = analyser.build_sales_snapshot(
                        location_id="loc_123",
                        location_name="Test Venue",
                        sales=day_sales,
                        timestamp=current_date,
                    )
                    snapshots.append(snap)

        patterns = analyser.analyse_trading_patterns(snapshots)

        # Should have patterns for multiple day/hour combinations
        assert len(patterns) > 0

        # Each pattern should have required fields
        for pattern in patterns:
            assert pattern.day_of_week in range(7)
            assert pattern.hour in range(24)
            assert pattern.avg_revenue >= 0
            assert pattern.avg_transactions >= 0

    def test_demand_signal_surge(self):
        """Test demand signal calculation for surge."""
        analyser = SalesAnalyser()

        # Pattern: avg 100 per hour
        pattern = TradingPattern(
            day_of_week=1,
            hour=12,
            avg_revenue=100.0,
            avg_transactions=5,
            avg_covers=10,
            volatility=10.0,
            trend=0.0,
        )

        # Current: 150 (50% above, surge)
        now = datetime(2024, 2, 6, 12, 0, tzinfo=AU_TZ)  # Tuesday
        snapshot = SalesSnapshot(
            timestamp=now,
            location_id="loc_123",
            location_name="Test",
            total_revenue=150.0,
            transaction_count=5,
            avg_transaction_value=30.0,
            covers=10,
            hourly_breakdown={12: 150.0},
        )

        signal, multiplier = analyser.get_demand_signal(snapshot, [pattern])
        assert signal == DemandSignal.SURGE
        assert multiplier == 0.35

    def test_demand_signal_high(self):
        """Test demand signal calculation for high."""
        analyser = SalesAnalyser()

        pattern = TradingPattern(
            day_of_week=1,
            hour=12,
            avg_revenue=100.0,
            avg_transactions=5,
            avg_covers=10,
            volatility=10.0,
            trend=0.0,
        )

        # Current: 115 (15% above, high)
        now = datetime(2024, 2, 6, 12, 0, tzinfo=AU_TZ)
        snapshot = SalesSnapshot(
            timestamp=now,
            location_id="loc_123",
            location_name="Test",
            total_revenue=115.0,
            transaction_count=5,
            avg_transaction_value=23.0,
            covers=10,
            hourly_breakdown={12: 115.0},
        )

        signal, multiplier = analyser.get_demand_signal(snapshot, [pattern])
        assert signal == DemandSignal.HIGH
        assert multiplier == 0.15

    def test_demand_signal_normal(self):
        """Test demand signal calculation for normal."""
        analyser = SalesAnalyser()

        pattern = TradingPattern(
            day_of_week=1,
            hour=12,
            avg_revenue=100.0,
            avg_transactions=5,
            avg_covers=10,
            volatility=10.0,
            trend=0.0,
        )

        # Current: 105 (5% above, normal)
        now = datetime(2024, 2, 6, 12, 0, tzinfo=AU_TZ)
        snapshot = SalesSnapshot(
            timestamp=now,
            location_id="loc_123",
            location_name="Test",
            total_revenue=105.0,
            transaction_count=5,
            avg_transaction_value=21.0,
            covers=10,
            hourly_breakdown={12: 105.0},
        )

        signal, multiplier = analyser.get_demand_signal(snapshot, [pattern])
        assert signal == DemandSignal.NORMAL
        assert multiplier == 0.0

    def test_demand_signal_low(self):
        """Test demand signal calculation for low."""
        analyser = SalesAnalyser()

        pattern = TradingPattern(
            day_of_week=1,
            hour=12,
            avg_revenue=100.0,
            avg_transactions=5,
            avg_covers=10,
            volatility=10.0,
            trend=0.0,
        )

        # Current: 80 (20% below, low)
        now = datetime(2024, 2, 6, 12, 0, tzinfo=AU_TZ)
        snapshot = SalesSnapshot(
            timestamp=now,
            location_id="loc_123",
            location_name="Test",
            total_revenue=80.0,
            transaction_count=4,
            avg_transaction_value=20.0,
            covers=8,
            hourly_breakdown={12: 80.0},
        )

        signal, multiplier = analyser.get_demand_signal(snapshot, [pattern])
        assert signal == DemandSignal.LOW
        assert multiplier == -0.15

    def test_demand_signal_quiet(self):
        """Test demand signal calculation for quiet."""
        analyser = SalesAnalyser()

        pattern = TradingPattern(
            day_of_week=1,
            hour=12,
            avg_revenue=100.0,
            avg_transactions=5,
            avg_covers=10,
            volatility=10.0,
            trend=0.0,
        )

        # Current: 50 (50% below, quiet)
        now = datetime(2024, 2, 6, 12, 0, tzinfo=AU_TZ)
        snapshot = SalesSnapshot(
            timestamp=now,
            location_id="loc_123",
            location_name="Test",
            total_revenue=50.0,
            transaction_count=2,
            avg_transaction_value=25.0,
            covers=5,
            hourly_breakdown={12: 50.0},
        )

        signal, multiplier = analyser.get_demand_signal(snapshot, [pattern])
        assert signal == DemandSignal.QUIET
        assert multiplier == -0.30


# ---------------------------------------------------------------------------
# Adapter Tests
# ---------------------------------------------------------------------------

class TestLightspeedAdapter:
    """Test high-level Lightspeed adapter."""

    def test_adapter_initialization(self):
        """Test adapter initializes correctly."""
        valid_credentials = _valid_credentials()
        adapter = LightspeedAdapter(
            credentials=valid_credentials,
            location_name="The Eagle Hotel",
            lookback_weeks=8,
            fetch_interval_minutes=15,
        )
        assert adapter.location_id == "loc_12345"
        assert adapter.location_name == "The Eagle Hotel"
        assert adapter.lookback_weeks == 8
        assert adapter.fetch_interval_minutes == 15

    def test_calculate_confidence_no_data(self):
        """Test confidence calculation with no data."""
        valid_credentials = _valid_credentials()
        adapter = LightspeedAdapter(valid_credentials)

        snapshot = SalesSnapshot(
            timestamp=datetime.now(AU_TZ),
            location_id="loc_123",
            location_name="Test",
            total_revenue=0.0,
            transaction_count=0,
            avg_transaction_value=0.0,
        )

        confidence = adapter._calculate_confidence(snapshot)
        assert confidence == 0.5  # base confidence only

    def test_calculate_confidence_full_data(self):
        """Test confidence calculation with complete data."""
        valid_credentials = _valid_credentials()
        sample_sales_data = _sample_sales_data()
        adapter = LightspeedAdapter(valid_credentials)

        now = datetime.now(AU_TZ)
        snapshot = SalesSnapshot(
            timestamp=now,
            location_id="loc_123",
            location_name="Test",
            total_revenue=256.25,
            transaction_count=3,
            avg_transaction_value=85.42,
            covers=4,
            top_revenue_centers={"Dining": 100.0, "Bar": 85.0},
            hourly_breakdown={now.hour: 256.25},
        )

        # Add patterns for higher confidence
        adapter._patterns = [
            TradingPattern(
                day_of_week=now.weekday(),
                hour=now.hour,
                avg_revenue=250.0,
                avg_transactions=3,
                avg_covers=4,
                volatility=5.0,
                trend=0.0,
            ) for _ in range(60)  # 60 patterns to trigger +0.1
        ]

        confidence = adapter._calculate_confidence(snapshot)
        assert confidence > 0.5
        assert confidence <= 1.0

    def test_analyse_revenue_centres_dominant(self):
        """Test revenue centre analysis with one dominant centre."""
        valid_credentials = _valid_credentials()
        adapter = LightspeedAdapter(valid_credentials)

        now = datetime.now(AU_TZ)
        snapshot = SalesSnapshot(
            timestamp=now,
            location_id="loc_123",
            location_name="Test",
            total_revenue=100.0,
            transaction_count=2,
            avg_transaction_value=50.0,
            covers=2,
            top_revenue_centers={"Bar": 75.0, "Dining": 25.0},
            hourly_breakdown={now.hour: 100.0},
        )

        signal = adapter._analyse_revenue_centres(snapshot)
        assert signal is not None
        assert signal["category"] == "pos_revenue_centre"
        assert signal["metadata"]["dominant_centre"] == "Bar"
        assert signal["metadata"]["centre_revenue_pct"] == 75.0

    def test_analyse_revenue_centres_balanced(self):
        """Test revenue centre analysis with balanced centres."""
        valid_credentials = _valid_credentials()
        adapter = LightspeedAdapter(valid_credentials)

        now = datetime.now(AU_TZ)
        snapshot = SalesSnapshot(
            timestamp=now,
            location_id="loc_123",
            location_name="Test",
            total_revenue=100.0,
            transaction_count=2,
            avg_transaction_value=50.0,
            covers=2,
            top_revenue_centers={"Bar": 50.0, "Dining": 50.0},
            hourly_breakdown={now.hour: 100.0},
        )

        signal = adapter._analyse_revenue_centres(snapshot)
        assert signal is None  # No dominant centre

    def test_analyse_covers_velocity_high(self):
        """Test covers velocity analysis with high velocity."""
        valid_credentials = _valid_credentials()
        adapter = LightspeedAdapter(valid_credentials)

        now = datetime.now(AU_TZ)
        snapshot = SalesSnapshot(
            timestamp=now,
            location_id="loc_123",
            location_name="Test",
            total_revenue=200.0,
            transaction_count=10,
            avg_transaction_value=20.0,
            covers=30,  # High covers
            top_revenue_centers={"Dining": 200.0},
            hourly_breakdown={now.hour: 200.0},
            hourly_covers={now.hour: 30},
        )

        # Add pattern with lower covers for contrast
        adapter._patterns = [
            TradingPattern(
                day_of_week=now.weekday(),
                hour=now.hour,
                avg_revenue=150.0,
                avg_transactions=8,
                avg_covers=10,  # Average 10 covers/hour
                volatility=5.0,
                trend=0.0,
            )
        ]

        signal = adapter._analyse_covers_velocity(snapshot)
        assert signal is not None
        assert signal["category"] == "pos_covers_velocity"
        assert "velocity_ratio" in signal["metadata"]

    def test_analyse_covers_velocity_normal(self):
        """Test covers velocity analysis with normal velocity."""
        valid_credentials = _valid_credentials()
        adapter = LightspeedAdapter(valid_credentials)

        now = datetime.now(AU_TZ)
        snapshot = SalesSnapshot(
            timestamp=now,
            location_id="loc_123",
            location_name="Test",
            total_revenue=150.0,
            transaction_count=6,
            avg_transaction_value=25.0,
            covers=10,  # Normal covers
            top_revenue_centers={"Dining": 150.0},
            hourly_breakdown={now.hour: 150.0},
            hourly_covers={now.hour: 10},
        )

        # Add pattern matching current traffic
        adapter._patterns = [
            TradingPattern(
                day_of_week=now.weekday(),
                hour=now.hour,
                avg_revenue=150.0,
                avg_transactions=6,
                avg_covers=10,
                volatility=5.0,
                trend=0.0,
            )
        ]

        signal = adapter._analyse_covers_velocity(snapshot)
        assert signal is None  # No high velocity alert

    def test_health_check_success(self):
        """Test health check success."""
        valid_credentials = _valid_credentials()
        adapter = LightspeedAdapter(valid_credentials)

        # Mock successful auth and venue fetch
        with patch.object(adapter.client, "_refresh_oauth_token", new_callable=AsyncMock) as mock_auth:
            with patch.object(adapter.client, "get_venue", new_callable=AsyncMock) as mock_venue:
                mock_auth.return_value = "test_token"
                mock_venue.return_value = {"name": "The Eagle Hotel"}

                # Run async health check in sync test
                result = asyncio.run(adapter.health_check())

                assert result["status"] == "healthy"
                assert result["connected"] is True
                assert result["venue_id"] == "loc_12345"

    def test_health_check_auth_failure(self):
        """Test health check with auth failure."""
        valid_credentials = _valid_credentials()
        adapter = LightspeedAdapter(valid_credentials)

        with patch.object(adapter.client, "_refresh_oauth_token", new_callable=AsyncMock) as mock_auth:
            mock_auth.side_effect = LightspeedAuthError("Auth failed")

            result = asyncio.run(adapter.health_check())

            assert result["status"] == "auth_failed"
            assert result["connected"] is False


# ---------------------------------------------------------------------------
# Factory Function Tests
# ---------------------------------------------------------------------------

class TestFactoryFunction:
    """Test factory function."""

    def test_create_lightspeed_adapter(self):
        """Test adapter creation via factory function."""
        adapter = create_lightspeed_adapter(
            client_id="test_client",
            client_secret="test_secret",
            refresh_token="test_token",
            business_location_id="loc_123",
            location_name="Test Venue",
        )

        assert isinstance(adapter, LightspeedAdapter)
        assert adapter.location_id == "loc_123"
        assert adapter.location_name == "Test Venue"
        assert adapter.client.creds.client_id == "test_client"

    def test_create_lightspeed_adapter_default_name(self):
        """Test adapter creation with default location name."""
        adapter = create_lightspeed_adapter(
            client_id="test_client",
            client_secret="test_secret",
            refresh_token="test_token",
            business_location_id="loc_123",
        )

        assert adapter.location_name == "Venue"


# ---------------------------------------------------------------------------
# Integration Tests (Mock HTTP)
# ---------------------------------------------------------------------------

class TestLightspeedIntegration:
    """Integration tests with mocked HTTP."""

    def test_fetch_signals_success(self):
        """Test successful signal fetching."""
        valid_credentials = _valid_credentials()
        sample_sales_data = _sample_sales_data()
        adapter = LightspeedAdapter(valid_credentials)

        # Mock initialise and client methods
        with patch.object(adapter, "initialise", new_callable=AsyncMock):
            adapter._patterns = [
                TradingPattern(
                    day_of_week=datetime.now(AU_TZ).weekday(),
                    hour=datetime.now(AU_TZ).hour,
                    avg_revenue=200.0,
                    avg_transactions=2,
                    avg_covers=4,
                    volatility=10.0,
                    trend=0.0,
                )
            ]

            with patch.object(adapter.client, "get_sales", new_callable=AsyncMock) as mock_get_sales:
                mock_get_sales.return_value = sample_sales_data

                signals = asyncio.run(adapter.fetch_signals())

                assert len(signals) > 0
                # Should have primary demand signal
                assert any(s["source"] == "lightspeed" for s in signals)

        asyncio.run(adapter.close())

    def test_fetch_signals_no_sales(self):
        """Test signal fetching with no current sales."""
        valid_credentials = _valid_credentials()
        adapter = LightspeedAdapter(valid_credentials)

        with patch.object(adapter, "initialise", new_callable=AsyncMock):
            adapter._patterns = []

            with patch.object(adapter.client, "get_sales", new_callable=AsyncMock) as mock_get_sales:
                mock_get_sales.return_value = []

                signals = asyncio.run(adapter.fetch_signals())

                assert signals == []

        asyncio.run(adapter.close())

    def test_get_revenue_centres(self):
        """Test revenue centre breakdown."""
        valid_credentials = _valid_credentials()
        sample_sales_data = _sample_sales_data()
        adapter = LightspeedAdapter(valid_credentials)

        with patch.object(adapter.client, "get_sales", new_callable=AsyncMock) as mock_get_sales:
            mock_get_sales.return_value = sample_sales_data

            centres = asyncio.run(adapter.get_revenue_centres())

            assert len(centres) == 3
            assert all(isinstance(c, RevenueCenterBreakdown) for c in centres)
            # Should be sorted by revenue descending
            assert centres[0].revenue >= centres[-1].revenue

        asyncio.run(adapter.close())


# ---------------------------------------------------------------------------
# Error Handling Tests
# ---------------------------------------------------------------------------

class TestErrorHandling:
    """Test error handling."""

    def test_lightspeed_error_on_get_sales(self):
        """Test LightspeedError handling during sales fetch."""
        valid_credentials = _valid_credentials()
        adapter = LightspeedAdapter(valid_credentials)

        with patch.object(adapter.client, "get_sales", new_callable=AsyncMock) as mock_get_sales:
            mock_get_sales.side_effect = LightspeedError("API error")

            signals = asyncio.run(adapter.fetch_signals())

            assert signals == []

        asyncio.run(adapter.close())

    def test_rate_limit_error(self):
        """Test RateLimitError exception."""
        try:
            raise LightspeedRateLimitError("Rate limit hit")
        except LightspeedRateLimitError:
            pass

    def test_auth_error(self):
        """Test AuthError exception."""
        try:
            raise LightspeedAuthError("Auth failed")
        except LightspeedAuthError:
            pass

    def test_base_error(self):
        """Test base LightspeedError exception."""
        try:
            raise LightspeedError("Generic error")
        except LightspeedError:
            pass


# ---------------------------------------------------------------------------
# Custom Test Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    passed = failed = 0
    for name, obj in list(globals().items()):
        if isinstance(obj, type) and name.startswith("Test"):
            inst = obj()
            for mname in sorted(dir(inst)):
                if mname.startswith("test_"):
                    try:
                        result = getattr(inst, mname)()
                        if asyncio.iscoroutine(result):
                            asyncio.run(result)
                        passed += 1
                        print(f"  PASS {name}.{mname}")
                    except AssertionError as e:
                        failed += 1
                        print(f"  FAIL {name}.{mname}: {e}")
                    except Exception as e:
                        failed += 1
                        print(f"  ERROR {name}.{mname}: {type(e).__name__}: {e}")
    print(f"\n{passed}/{passed + failed} tests passed")
    sys.exit(0 if failed == 0 else 1)
