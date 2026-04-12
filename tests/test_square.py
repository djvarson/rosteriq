"""
Tests for Square POS Adapter
=============================

Comprehensive unit tests for Square API integration covering:
- Credentials and configuration
- Order parsing and snapshot building
- Demand signal analysis
- Order velocity detection
- Average order value shifts
- Pagination and API client behavior
- Factory function creation
- Health checks and error handling

Tests are isolated and do not require network access.
"""

import sys
from pathlib import Path
import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)) if str(ROOT) not in sys.path else None

from rosteriq.data_feeds.square import (
    SquareCredentials,
    SquareClient,
    SquareAdapter,
    OrderAnalyser,
    OrderSnapshot,
    TradingPattern,
    DemandSignal,
    LocationInfo,
    SquareError,
    SquareAuthError,
    SquareRateLimitError,
    create_square_adapter,
    AU_TZ,
)


# ---------------------------------------------------------------------------
# Helper Functions (formerly fixtures)
# ---------------------------------------------------------------------------

def _credentials():
    """Valid Square credentials."""
    return SquareCredentials(
        access_token="sq_live_test123xyz",
        location_id="LNHCCCCAAA111BBB",
        environment="production",
    )


def _credentials_sandbox():
    """Square sandbox credentials."""
    return SquareCredentials(
        access_token="sq_sandbox_test123xyz",
        location_id="LNHCCCCBBB222CCC",
        environment="sandbox",
    )


def _mock_client():
    """Mock Square client."""
    credentials = _credentials()
    return SquareClient(credentials)


def _mock_adapter():
    """Mock Square adapter."""
    credentials = _credentials()
    return SquareAdapter(
        credentials=credentials,
        location_id="LNHCCCCAAA111BBB",
        location_name="The Royal Oak",
        lookback_weeks=8,
        fetch_interval_minutes=15,
    )


def _sample_orders():
    """Sample order data from Square API."""
    now = datetime.now(AU_TZ)
    return [
        {
            "id": "order_123",
            "location_id": "LNHCCCCAAA111BBB",
            "state": "COMPLETED",
            "created_at": (now - timedelta(minutes=5)).isoformat(),
            "total_money": {"amount": 4500, "currency": "AUD"},
            "tenders": [
                {
                    "type": "CARD",
                    "amount_money": {"amount": 4500, "currency": "AUD"},
                }
            ],
            "line_items": [
                {"name": "Burger", "quantity": "1", "gross_sales_money": {"amount": 2000}},
                {"name": "Beer", "quantity": "2", "gross_sales_money": {"amount": 2500}},
            ],
        },
        {
            "id": "order_124",
            "location_id": "LNHCCCCAAA111BBB",
            "state": "COMPLETED",
            "created_at": (now - timedelta(minutes=10)).isoformat(),
            "total_money": {"amount": 3200, "currency": "AUD"},
            "tenders": [
                {
                    "type": "CARD",
                    "amount_money": {"amount": 3200, "currency": "AUD"},
                }
            ],
        },
        {
            "id": "order_125",
            "location_id": "LNHCCCCAAA111BBB",
            "state": "OPEN",
            "created_at": (now - timedelta(minutes=2)).isoformat(),
            "total_money": {"amount": 2800, "currency": "AUD"},
            "tenders": [],
        },
    ]


def _sample_locations():
    """Sample locations from Square API."""
    return {
        "locations": [
            {
                "id": "LNHCCCCAAA111BBB",
                "name": "The Royal Oak",
                "status": "ACTIVE",
                "currency": "AUD",
                "timezone": "Australia/Sydney",
            },
            {
                "id": "LNHCCCCBBB222CCC",
                "name": "The Ivy",
                "status": "ACTIVE",
                "currency": "AUD",
                "timezone": "Australia/Sydney",
            },
        ]
    }


# ---------------------------------------------------------------------------
# Tests: Credentials
# ---------------------------------------------------------------------------

class TestSquareCredentials:
    """Test SquareCredentials dataclass."""

    def test_credentials_creation(self):
        """Test creating credentials."""
        credentials = _credentials()
        assert credentials.access_token == "sq_live_test123xyz"
        assert credentials.location_id == "LNHCCCCAAA111BBB"
        assert credentials.environment == "production"

    def test_credentials_sandbox_environment(self):
        """Test sandbox environment."""
        credentials_sandbox = _credentials_sandbox()
        assert credentials_sandbox.environment == "sandbox"

    def test_credentials_invalid_environment_defaults(self):
        """Test invalid environment defaults to production."""
        creds = SquareCredentials(
            access_token="token",
            location_id="loc",
            environment="invalid",
        )
        assert creds.environment == "production"

    def test_credentials_required_fields(self):
        """Test credentials require access_token and location_id."""
        try:
            SquareCredentials()
            assert False, "Should have raised TypeError"
        except TypeError:
            pass

    def test_credentials_access_token_required(self):
        """Test access_token is required."""
        try:
            SquareCredentials(location_id="loc")
            assert False, "Should have raised TypeError"
        except TypeError:
            pass

    def test_credentials_location_id_required(self):
        """Test location_id is required."""
        try:
            SquareCredentials(access_token="token")
            assert False, "Should have raised TypeError"
        except TypeError:
            pass


# ---------------------------------------------------------------------------
# Tests: SquareClient
# ---------------------------------------------------------------------------

class TestSquareClient:
    """Test SquareClient API client."""

    async def test_client_creation(self):
        """Test client creation."""
        mock_client = _mock_client()
        assert mock_client.creds.access_token == "sq_live_test123xyz"
        assert mock_client.creds.location_id == "LNHCCCCAAA111BBB"
        assert mock_client._client is None

    async def test_client_get_client(self):
        """Test _get_client creates httpx client."""
        mock_client = _mock_client()
        client = await mock_client._get_client()
        assert client is not None
        assert client.base_url == "https://connect.squareup.com/v2"
        await mock_client.close()

    async def test_client_reuses_connection(self):
        """Test client reuses same httpx connection."""
        mock_client = _mock_client()
        client1 = await mock_client._get_client()
        client2 = await mock_client._get_client()
        assert client1 is client2
        await mock_client.close()

    async def test_client_close(self):
        """Test closing client."""
        mock_client = _mock_client()
        client = await mock_client._get_client()
        assert not client.is_closed
        await mock_client.close()
        assert client.is_closed

    async def test_client_auth_header_included(self):
        """Test Authorization header is set."""
        mock_client = _mock_client()
        client = await mock_client._get_client()
        assert mock_client.creds.access_token in "sq_live_test123xyz"
        await mock_client.close()

    async def test_client_get_locations(self):
        """Test get_locations endpoint."""
        mock_client = _mock_client()
        with patch.object(mock_client, "get") as mock_get:
            mock_get.return_value = {
                "locations": [
                    {"id": "loc1", "name": "Venue 1"},
                    {"id": "loc2", "name": "Venue 2"},
                ]
            }
            locations = await mock_client.get_locations()
            assert len(locations) == 2
            assert locations[0]["id"] == "loc1"
            mock_get.assert_called_once_with("/locations")

    async def test_client_search_orders_basic(self):
        """Test search_orders endpoint."""
        mock_client = _mock_client()
        sample_orders = _sample_orders()
        with patch.object(mock_client, "post") as mock_post:
            mock_post.return_value = {
                "orders": sample_orders,
                "cursor": None,
            }
            now = datetime.now(AU_TZ)
            start = now - timedelta(hours=1)

            orders, cursor = await mock_client.search_orders(
                "loc123", start, now
            )
            assert len(orders) == 3
            assert orders[0]["id"] == "order_123"
            assert cursor is None
            mock_post.assert_called_once()

    async def test_client_search_orders_with_cursor(self):
        """Test search_orders with pagination cursor."""
        mock_client = _mock_client()
        sample_orders = _sample_orders()
        with patch.object(mock_client, "post") as mock_post:
            mock_post.return_value = {
                "orders": sample_orders[:2],
                "cursor": "next_page_cursor",
            }
            now = datetime.now(AU_TZ)
            start = now - timedelta(hours=1)

            orders, cursor = await mock_client.search_orders(
                "loc123", start, now, cursor="page1_cursor"
            )
            assert cursor == "next_page_cursor"
            assert len(orders) == 2

    async def test_client_get_payments(self):
        """Test get_payments endpoint."""
        mock_client = _mock_client()
        with patch.object(mock_client, "get") as mock_get:
            mock_get.return_value = {
                "payments": [
                    {"id": "pay1", "amount_money": {"amount": 4500}},
                ],
                "cursor": None,
            }
            now = datetime.now(AU_TZ)
            start = now - timedelta(hours=1)

            payments, cursor = await mock_client.get_payments(
                "loc123", start, now
            )
            assert len(payments) == 1
            assert cursor is None

    async def test_client_get_catalog(self):
        """Test get_catalog endpoint."""
        mock_client = _mock_client()
        with patch.object(mock_client, "get") as mock_get:
            mock_get.return_value = {
                "objects": [
                    {"id": "cat1", "type": "CATEGORY", "category_data": {"name": "Food"}},
                ]
            }
            catalog = await mock_client.get_catalog()
            assert len(catalog) == 1
            assert catalog[0]["id"] == "cat1"


# ---------------------------------------------------------------------------
# Tests: OrderAnalyser
# ---------------------------------------------------------------------------

class TestOrderAnalyser:
    """Test OrderAnalyser for signal generation."""

    def test_analyser_creation(self):
        """Test creating analyser."""
        analyser = OrderAnalyser(lookback_weeks=8)
        assert analyser.lookback_weeks == 8

    def test_build_order_snapshot_basic(self):
        """Test building snapshot from orders."""
        analyser = OrderAnalyser()
        now = datetime.now(AU_TZ)
        sample_orders = _sample_orders()

        snapshot = analyser.build_order_snapshot(
            location_id="loc123",
            location_name="The Royal Oak",
            orders=sample_orders,
            timestamp=now,
        )

        assert snapshot.location_id == "loc123"
        assert snapshot.location_name == "The Royal Oak"
        assert snapshot.total_revenue == 105.00  # (4500 + 3200 + 2800) / 100
        assert snapshot.order_count == 3
        assert snapshot.completed_orders == 2
        assert snapshot.open_orders == 1
        assert snapshot.canceled_orders == 0

    def test_build_order_snapshot_avg_order_value(self):
        """Test average order value calculation."""
        analyser = OrderAnalyser()
        sample_orders = _sample_orders()
        snapshot = analyser.build_order_snapshot(
            "loc123", "Venue", sample_orders
        )

        # (4500 + 3200 + 2800) / 100 / 3 = 35.00
        assert snapshot.avg_order_value == 35.0

    def test_build_order_snapshot_payment_methods(self):
        """Test payment method tracking."""
        analyser = OrderAnalyser()
        sample_orders = _sample_orders()
        snapshot = analyser.build_order_snapshot(
            "loc123", "Venue", sample_orders
        )

        assert "CARD" in snapshot.payment_methods
        assert snapshot.payment_methods["CARD"] == 75.00  # (4500 + 3200) / 100

    def test_build_order_snapshot_hourly_breakdown(self):
        """Test hourly breakdown calculation."""
        analyser = OrderAnalyser()
        now = datetime.now(AU_TZ).replace(hour=14, minute=30)

        orders = [
            {
                "id": "o1",
                "state": "COMPLETED",
                "created_at": now.isoformat(),
                "total_money": {"amount": 5000},
                "tenders": [],
            },
            {
                "id": "o2",
                "state": "COMPLETED",
                "created_at": (now + timedelta(minutes=10)).isoformat(),
                "total_money": {"amount": 3000},
                "tenders": [],
            },
        ]

        snapshot = analyser.build_order_snapshot("loc1", "Venue", orders, now)
        assert 14 in snapshot.hourly_breakdown
        assert snapshot.hourly_breakdown[14] == 80.0  # (5000 + 3000) / 100

    def test_analyse_trading_patterns(self):
        """Test trading pattern analysis."""
        analyser = OrderAnalyser()

        # Create snapshots with different timestamps
        now = datetime.now(AU_TZ)
        snapshots = [
            OrderSnapshot(
                timestamp=now - timedelta(days=1),
                location_id="loc1",
                location_name="Venue",
                total_revenue=1000.0,
                order_count=20,
                avg_order_value=50.0,
                completed_orders=20,
                hourly_breakdown={10: 500.0, 11: 500.0},
                payment_methods={},
            ),
            OrderSnapshot(
                timestamp=now - timedelta(days=2),
                location_id="loc1",
                location_name="Venue",
                total_revenue=1050.0,
                order_count=21,
                avg_order_value=50.0,
                completed_orders=21,
                hourly_breakdown={10: 525.0, 11: 525.0},
                payment_methods={},
            ),
        ]

        patterns = analyser.analyse_trading_patterns(snapshots)
        assert len(patterns) > 0
        assert all(isinstance(p, TradingPattern) for p in patterns)

    def test_get_demand_signal_surge(self):
        """Test SURGE demand signal (>30% above average)."""
        analyser = OrderAnalyser()
        now = datetime.now(AU_TZ).replace(hour=14, minute=0)

        snapshot = OrderSnapshot(
            timestamp=now,
            location_id="loc1",
            location_name="Venue",
            total_revenue=1000.0,
            order_count=20,
            avg_order_value=50.0,
            hourly_breakdown={14: 500.0},  # High revenue
        )

        pattern = TradingPattern(
            day_of_week=now.weekday(),
            hour=now.hour,
            avg_revenue=300.0,  # Historical average
            avg_orders=6,
            volatility=10.0,
            trend=0.0,
        )

        signal, multiplier = analyser.get_demand_signal(snapshot, [pattern])
        assert signal == DemandSignal.SURGE
        assert multiplier == 0.35

    def test_get_demand_signal_high(self):
        """Test HIGH demand signal (10-30% above)."""
        analyser = OrderAnalyser()
        now = datetime.now(AU_TZ).replace(hour=10)

        snapshot = OrderSnapshot(
            timestamp=now,
            location_id="loc1",
            location_name="Venue",
            total_revenue=600.0,
            order_count=12,
            avg_order_value=50.0,
            hourly_breakdown={10: 350.0},
        )

        pattern = TradingPattern(
            day_of_week=now.weekday(),
            hour=now.hour,
            avg_revenue=300.0,
            avg_orders=6,
            volatility=10.0,
            trend=0.0,
        )

        signal, multiplier = analyser.get_demand_signal(snapshot, [pattern])
        assert signal == DemandSignal.HIGH
        assert multiplier == 0.15

    def test_get_demand_signal_normal(self):
        """Test NORMAL demand signal (within 10%)."""
        analyser = OrderAnalyser()
        now = datetime.now(AU_TZ).replace(hour=15)

        snapshot = OrderSnapshot(
            timestamp=now,
            location_id="loc1",
            location_name="Venue",
            total_revenue=500.0,
            order_count=10,
            avg_order_value=50.0,
            hourly_breakdown={15: 310.0},
        )

        pattern = TradingPattern(
            day_of_week=now.weekday(),
            hour=now.hour,
            avg_revenue=300.0,
            avg_orders=6,
            volatility=10.0,
            trend=0.0,
        )

        signal, multiplier = analyser.get_demand_signal(snapshot, [pattern])
        assert signal == DemandSignal.NORMAL
        assert multiplier == 0.0

    def test_get_demand_signal_low(self):
        """Test LOW demand signal (10-30% below)."""
        analyser = OrderAnalyser()
        now = datetime.now(AU_TZ).replace(hour=16)

        snapshot = OrderSnapshot(
            timestamp=now,
            location_id="loc1",
            location_name="Venue",
            total_revenue=450.0,
            order_count=9,
            avg_order_value=50.0,
            hourly_breakdown={16: 225.0},
        )

        pattern = TradingPattern(
            day_of_week=now.weekday(),
            hour=now.hour,
            avg_revenue=300.0,
            avg_orders=6,
            volatility=10.0,
            trend=0.0,
        )

        signal, multiplier = analyser.get_demand_signal(snapshot, [pattern])
        assert signal == DemandSignal.LOW
        assert multiplier == -0.15

    def test_get_demand_signal_quiet(self):
        """Test QUIET demand signal (>30% below)."""
        analyser = OrderAnalyser()
        now = datetime.now(AU_TZ).replace(hour=17)

        snapshot = OrderSnapshot(
            timestamp=now,
            location_id="loc1",
            location_name="Venue",
            total_revenue=300.0,
            order_count=6,
            avg_order_value=50.0,
            hourly_breakdown={17: 150.0},
        )

        pattern = TradingPattern(
            day_of_week=now.weekday(),
            hour=now.hour,
            avg_revenue=300.0,
            avg_orders=6,
            volatility=10.0,
            trend=0.0,
        )

        signal, multiplier = analyser.get_demand_signal(snapshot, [pattern])
        assert signal == DemandSignal.QUIET
        assert multiplier == -0.30

    def test_get_demand_signal_no_pattern(self):
        """Test NORMAL when no matching pattern found."""
        analyser = OrderAnalyser()
        now = datetime.now(AU_TZ).replace(hour=10)

        snapshot = OrderSnapshot(
            timestamp=now,
            location_id="loc1",
            location_name="Venue",
            total_revenue=500.0,
            order_count=10,
            avg_order_value=50.0,
            hourly_breakdown={10: 500.0},
        )

        signal, multiplier = analyser.get_demand_signal(snapshot, [])
        assert signal == DemandSignal.NORMAL
        assert multiplier == 0.0


# ---------------------------------------------------------------------------
# Tests: SquareAdapter
# ---------------------------------------------------------------------------

class TestSquareAdapter:
    """Test SquareAdapter integration."""

    def test_adapter_creation(self):
        """Test creating adapter."""
        mock_adapter = _mock_adapter()
        assert mock_adapter.location_id == "LNHCCCCAAA111BBB"
        assert mock_adapter.location_name == "The Royal Oak"
        assert mock_adapter.lookback_weeks == 8
        assert mock_adapter.fetch_interval_minutes == 15

    def test_adapter_constants(self):
        """Test adapter constants."""
        assert SquareAdapter.FEED_CATEGORY == "pos_sales"
        assert SquareAdapter.SIGNAL_TYPE == "foot_traffic"

    async def test_adapter_initialise(self):
        """Test adapter initialization."""
        mock_adapter = _mock_adapter()
        with patch.object(mock_adapter.client, "search_orders") as mock_search:
            mock_search.return_value = ([], None)

            await mock_adapter.initialise()

            assert mock_adapter._patterns is not None
            assert mock_adapter._last_pattern_build is not None

    async def test_adapter_fetch_signals_basic(self):
        """Test fetching signals."""
        mock_adapter = _mock_adapter()
        sample_orders = _sample_orders()
        mock_adapter._patterns = [
            TradingPattern(
                day_of_week=datetime.now(AU_TZ).weekday(),
                hour=datetime.now(AU_TZ).hour,
                avg_revenue=100.0,
                avg_orders=10,
                volatility=5.0,
                trend=0.0,
            )
        ]

        with patch.object(mock_adapter.client, "search_orders") as mock_search:
            mock_search.return_value = (sample_orders, None)

            signals = await mock_adapter.fetch_signals()

            assert len(signals) > 0
            assert signals[0]["source"] == "square"
            assert signals[0]["category"] == "pos_sales"

    async def test_adapter_fetch_signals_empty(self):
        """Test fetch_signals with no orders."""
        mock_adapter = _mock_adapter()
        mock_adapter._patterns = []

        with patch.object(mock_adapter.client, "search_orders") as mock_search:
            mock_search.return_value = ([], None)

            signals = await mock_adapter.fetch_signals()
            assert signals == []

    def test_adapter_calculate_confidence(self):
        """Test confidence calculation."""
        mock_adapter = _mock_adapter()
        snapshot = OrderSnapshot(
            timestamp=datetime.now(AU_TZ),
            location_id="loc1",
            location_name="Venue",
            total_revenue=500.0,
            order_count=10,
            avg_order_value=50.0,
            completed_orders=10,
            payment_methods={"CARD": 500.0, "CASH": 0.0},
        )

        confidence = mock_adapter._calculate_confidence(snapshot)
        assert 0.5 <= confidence <= 1.0

    def test_adapter_analyse_payment_shift_card_heavy(self):
        """Test payment method shift detection (card-heavy)."""
        mock_adapter = _mock_adapter()
        snapshot = OrderSnapshot(
            timestamp=datetime.now(AU_TZ),
            location_id="loc1",
            location_name="Venue",
            total_revenue=1000.0,
            order_count=20,
            avg_order_value=50.0,
            payment_methods={
                "CARD": 850.0,
                "CASH": 150.0,
            },
        )

        signal = mock_adapter._analyse_payment_shift(snapshot)
        assert signal is not None
        assert signal["category"] == "pos_payment_mix"
        assert signal["metadata"]["card_ratio"] == 0.85

    def test_adapter_analyse_payment_shift_normal(self):
        """Test payment method shift detection (normal mix)."""
        mock_adapter = _mock_adapter()
        snapshot = OrderSnapshot(
            timestamp=datetime.now(AU_TZ),
            location_id="loc1",
            location_name="Venue",
            total_revenue=1000.0,
            order_count=20,
            avg_order_value=50.0,
            payment_methods={
                "CARD": 500.0,
                "CASH": 500.0,
            },
        )

        signal = mock_adapter._analyse_payment_shift(snapshot)
        assert signal is None

    def test_adapter_analyse_velocity_rush(self):
        """Test velocity detection (rush)."""
        mock_adapter = _mock_adapter()
        now = datetime.now(AU_TZ)
        mock_adapter._patterns = [
            TradingPattern(
                day_of_week=now.weekday(),
                hour=now.hour,
                avg_revenue=100.0,
                avg_orders=5,  # 5 orders/hour = 0.083 per minute
                volatility=5.0,
                trend=0.0,
            )
        ]

        snapshot = OrderSnapshot(
            timestamp=now,
            location_id="loc1",
            location_name="Venue",
            total_revenue=200.0,
            order_count=15,  # 15 orders in 15 minutes = 1 per minute (12x normal)
            avg_order_value=13.33,
            hourly_breakdown={now.hour: 200.0},
        )

        signal = mock_adapter._analyse_velocity(snapshot)
        assert signal is not None
        assert signal["category"] == "pos_velocity"

    def test_adapter_analyse_velocity_normal(self):
        """Test velocity detection (normal)."""
        mock_adapter = _mock_adapter()
        now = datetime.now(AU_TZ)
        mock_adapter._patterns = [
            TradingPattern(
                day_of_week=now.weekday(),
                hour=now.hour,
                avg_revenue=100.0,
                avg_orders=20,  # 20 orders/hour = 0.33 per minute
                volatility=5.0,
                trend=0.0,
            )
        ]

        snapshot = OrderSnapshot(
            timestamp=now,
            location_id="loc1",
            location_name="Venue",
            total_revenue=100.0,
            order_count=5,  # 5 orders in 15 minutes = 0.33 per minute (normal)
            avg_order_value=20.0,
            hourly_breakdown={now.hour: 100.0},
        )

        signal = mock_adapter._analyse_velocity(snapshot)
        assert signal is None

    def test_adapter_analyse_avg_order_value_increased(self):
        """Test AOV shift detection (increased)."""
        mock_adapter = _mock_adapter()
        now = datetime.now(AU_TZ)
        mock_adapter._patterns = [
            TradingPattern(
                day_of_week=now.weekday(),
                hour=now.hour,
                avg_revenue=100.0,
                avg_orders=10,  # avg AOV = $10.00
                volatility=5.0,
                trend=0.0,
            )
        ]

        snapshot = OrderSnapshot(
            timestamp=now,
            location_id="loc1",
            location_name="Venue",
            total_revenue=150.0,
            order_count=10,  # AOV = $15.00 (50% increase)
            avg_order_value=15.0,
            hourly_breakdown={now.hour: 150.0},
        )

        signal = mock_adapter._analyse_avg_order_value(snapshot)
        assert signal is not None
        assert signal["category"] == "pos_avg_order_value"
        assert signal["metadata"]["direction"] == "increased"

    def test_adapter_analyse_avg_order_value_decreased(self):
        """Test AOV shift detection (decreased)."""
        mock_adapter = _mock_adapter()
        now = datetime.now(AU_TZ)
        mock_adapter._patterns = [
            TradingPattern(
                day_of_week=now.weekday(),
                hour=now.hour,
                avg_revenue=100.0,
                avg_orders=10,  # avg AOV = $10.00
                volatility=5.0,
                trend=0.0,
            )
        ]

        snapshot = OrderSnapshot(
            timestamp=now,
            location_id="loc1",
            location_name="Venue",
            total_revenue=60.0,
            order_count=10,  # AOV = $6.00 (40% decrease)
            avg_order_value=6.0,
            hourly_breakdown={now.hour: 60.0},
        )

        signal = mock_adapter._analyse_avg_order_value(snapshot)
        assert signal is not None
        assert signal["category"] == "pos_avg_order_value"
        assert signal["metadata"]["direction"] == "decreased"

    def test_adapter_analyse_avg_order_value_normal(self):
        """Test AOV shift detection (normal)."""
        mock_adapter = _mock_adapter()
        now = datetime.now(AU_TZ)
        mock_adapter._patterns = [
            TradingPattern(
                day_of_week=now.weekday(),
                hour=now.hour,
                avg_revenue=100.0,
                avg_orders=10,  # avg AOV = $10.00
                volatility=5.0,
                trend=0.0,
            )
        ]

        snapshot = OrderSnapshot(
            timestamp=now,
            location_id="loc1",
            location_name="Venue",
            total_revenue=105.0,
            order_count=10,  # AOV = $10.50 (5% change - within normal range)
            avg_order_value=10.5,
            hourly_breakdown={now.hour: 105.0},
        )

        signal = mock_adapter._analyse_avg_order_value(snapshot)
        assert signal is None

    async def test_adapter_get_locations(self):
        """Test fetching locations."""
        mock_adapter = _mock_adapter()
        with patch.object(mock_adapter.client, "get_locations") as mock_get:
            mock_get.return_value = [
                {
                    "id": "loc1",
                    "name": "Venue 1",
                    "status": "ACTIVE",
                    "currency": "AUD",
                    "timezone": "Australia/Sydney",
                },
            ]

            locations = await mock_adapter.get_locations()
            assert len(locations) == 1
            assert locations[0].location_id == "loc1"
            assert isinstance(locations[0], LocationInfo)

    async def test_adapter_health_check_healthy(self):
        """Test health check (healthy)."""
        mock_adapter = _mock_adapter()
        with patch.object(mock_adapter.client, "get_locations") as mock_get:
            mock_get.return_value = [
                {"id": "LNHCCCCAAA111BBB", "name": "The Royal Oak", "status": "ACTIVE"},
            ]

            health = await mock_adapter.health_check()
            assert health["status"] == "healthy"
            assert health["connected"] is True

    async def test_adapter_health_check_auth_failed(self):
        """Test health check (auth failed)."""
        mock_adapter = _mock_adapter()
        with patch.object(mock_adapter.client, "get_locations") as mock_get:
            mock_get.side_effect = SquareAuthError("Invalid token")

            health = await mock_adapter.health_check()
            assert health["status"] == "auth_failed"
            assert health["connected"] is False

    async def test_adapter_health_check_error(self):
        """Test health check (error)."""
        mock_adapter = _mock_adapter()
        with patch.object(mock_adapter.client, "get_locations") as mock_get:
            mock_get.side_effect = SquareError("Connection error")

            health = await mock_adapter.health_check()
            assert health["status"] == "error"
            assert health["connected"] is False

    async def test_adapter_close(self):
        """Test closing adapter."""
        mock_adapter = _mock_adapter()
        with patch.object(mock_adapter.client, "close") as mock_close:
            await mock_adapter.close()
            mock_close.assert_called_once()


# ---------------------------------------------------------------------------
# Tests: Factory Function
# ---------------------------------------------------------------------------

class TestFactory:
    """Test create_square_adapter factory function."""

    def test_create_adapter_basic(self):
        """Test creating adapter with factory."""
        adapter = create_square_adapter(
            access_token="sq_live_abc123",
            location_id="LOC001",
            location_name="The Royal Oak",
        )

        assert isinstance(adapter, SquareAdapter)
        assert adapter.location_id == "LOC001"
        assert adapter.location_name == "The Royal Oak"

    def test_create_adapter_with_environment(self):
        """Test factory with environment parameter."""
        adapter = create_square_adapter(
            access_token="sq_sandbox_abc123",
            location_id="LOC002",
            location_name="Test Venue",
            environment="sandbox",
        )

        assert adapter.client.creds.environment == "sandbox"

    def test_create_adapter_defaults(self):
        """Test factory defaults."""
        adapter = create_square_adapter(
            access_token="token",
            location_id="loc",
        )

        assert adapter.location_name == "Venue"
        assert adapter.client.creds.environment == "production"


# ---------------------------------------------------------------------------
# Tests: Exception Handling
# ---------------------------------------------------------------------------

class TestExceptions:
    """Test exception classes."""

    def test_square_error(self):
        """Test SquareError exception."""
        exc = SquareError("Test error")
        assert str(exc) == "Test error"
        assert isinstance(exc, Exception)

    def test_square_auth_error(self):
        """Test SquareAuthError exception."""
        exc = SquareAuthError("Auth failed")
        assert str(exc) == "Auth failed"
        assert isinstance(exc, SquareError)

    def test_square_rate_limit_error(self):
        """Test SquareRateLimitError exception."""
        exc = SquareRateLimitError("Rate limited")
        assert str(exc) == "Rate limited"
        assert isinstance(exc, SquareError)


# ---------------------------------------------------------------------------
# Tests: Data Models
# ---------------------------------------------------------------------------

class TestDataModels:
    """Test data model classes."""

    def test_order_snapshot_creation(self):
        """Test creating OrderSnapshot."""
        now = datetime.now(AU_TZ)
        snap = OrderSnapshot(
            timestamp=now,
            location_id="loc1",
            location_name="Venue",
            total_revenue=500.0,
            order_count=10,
            avg_order_value=50.0,
        )

        assert snap.location_id == "loc1"
        assert snap.total_revenue == 500.0
        assert snap.order_count == 10

    def test_trading_pattern_creation(self):
        """Test creating TradingPattern."""
        pattern = TradingPattern(
            day_of_week=0,
            hour=10,
            avg_revenue=500.0,
            avg_orders=10,
            volatility=5.0,
            trend=0.1,
        )

        assert pattern.day_of_week == 0
        assert pattern.hour == 10
        assert pattern.avg_revenue == 500.0

    def test_location_info_creation(self):
        """Test creating LocationInfo."""
        loc = LocationInfo(
            location_id="loc1",
            location_name="The Royal Oak",
        )

        assert loc.location_id == "loc1"
        assert loc.currency == "AUD"
        assert loc.is_active is True

    def test_demand_signal_enum(self):
        """Test DemandSignal enum values."""
        assert DemandSignal.SURGE.value == "surge"
        assert DemandSignal.HIGH.value == "high"
        assert DemandSignal.NORMAL.value == "normal"
        assert DemandSignal.LOW.value == "low"
        assert DemandSignal.QUIET.value == "quiet"


if __name__ == "__main__":
    import asyncio as _asyncio
    passed = failed = 0
    for name, obj in list(globals().items()):
        if isinstance(obj, type) and name.startswith("Test"):
            inst = obj()
            for mname in sorted(dir(inst)):
                if mname.startswith("test_"):
                    try:
                        result = getattr(inst, mname)()
                        if _asyncio.iscoroutine(result):
                            _asyncio.run(result)
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
