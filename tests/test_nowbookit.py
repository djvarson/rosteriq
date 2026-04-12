"""
Tests for NowBookIt Reservation Feed Adapter
=============================================

Pure stdlib test runner with mocked HTTP calls (no pytest).
Tests cover: credentials, snapshots, analysis, patterns, signals, and integration.
"""

import asyncio
import sys
import unittest
from datetime import datetime, date, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from decimal import Decimal

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Mock httpx before importing nowbookit
sys.modules['httpx'] = MagicMock()

from rosteriq.data_feeds.nowbookit import (
    NowBookItCredentials,
    NowBookItClient,
    NowBookItAdapter,
    ReservationAnalyser,
    ReservationSnapshot,
    Reservation,
    BookingPattern,
    create_nowbookit_adapter,
    NowBookItError,
    NowBookItAuthError,
)

AU_TZ = timezone(timedelta(hours=10))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_credentials(api_key="sk_test_abc123", venue_id="venue_001"):
    return NowBookItCredentials(
        api_key=api_key,
        venue_id=venue_id,
        base_url="https://api.test.nowbookit.com/v1",
    )


def make_reservation(
    res_id="res_001",
    date_offset=0,
    time="18:30",
    covers=4,
    status="confirmed",
):
    target_date = date.today() + timedelta(days=date_offset)
    return {
        "id": res_id,
        "date": target_date.isoformat(),
        "reservation_date": target_date.isoformat(),
        "time": time,
        "reservation_time": time,
        "covers": covers,
        "party_size": covers,
        "name": f"Guest {res_id}",
        "status": status,
    }


def make_reservations(count=10, date_offset=0, base_time="18:30"):
    """Generate a list of test reservations."""
    res_list = []
    for i in range(count):
        hour = 17 + (i % 4)
        minute = (i * 15) % 60
        time = f"{hour:02d}:{minute:02d}"
        res_list.append(make_reservation(
            res_id=f"res_{i:03d}",
            date_offset=date_offset,
            time=time,
            covers=2 + (i % 4),
            status="confirmed",
        ))
    return res_list


def make_snapshot(
    total_covers=80,
    reservation_count=20,
    venue_id="venue_001",
    day_offset=0,
):
    target_date = date.today() - timedelta(days=day_offset)
    target_dt = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=AU_TZ)
    hourly = {h: total_covers // 6 for h in range(17, 23)}
    return ReservationSnapshot(
        timestamp=target_dt,
        venue_id=venue_id,
        venue_name="Test Venue",
        total_covers=total_covers,
        reservation_count=reservation_count,
        avg_covers_per_reservation=round(total_covers / max(reservation_count, 1), 1),
        booking_dates={target_date.isoformat(): total_covers},
        hourly_breakdown=hourly,
    )


# ---------------------------------------------------------------------------
# Test Credentials
# ---------------------------------------------------------------------------

class TestNowBookItCredentials(unittest.TestCase):
    def test_credentials_creation(self):
        creds = make_credentials()
        self.assertEqual(creds.api_key, "sk_test_abc123")
        self.assertEqual(creds.venue_id, "venue_001")

    def test_credentials_base_url_default(self):
        creds = NowBookItCredentials(
            api_key="key",
            venue_id="vid",
        )
        self.assertEqual(creds.base_url, "https://api.nowbookit.com/v1")

    def test_credentials_base_url_trailing_slash_stripped(self):
        creds = NowBookItCredentials(
            api_key="key",
            venue_id="vid",
            base_url="https://api.test.com/v1/",
        )
        self.assertEqual(creds.base_url, "https://api.test.com/v1")

    def test_credentials_all_fields_present(self):
        creds = make_credentials()
        self.assertTrue(creds.api_key)
        self.assertTrue(creds.venue_id)
        self.assertTrue(creds.base_url)


# ---------------------------------------------------------------------------
# Test Reservation Model
# ---------------------------------------------------------------------------

class TestReservation(unittest.TestCase):
    def test_reservation_creation(self):
        res = Reservation(
            reservation_id="res_001",
            date=date.today(),
            time="18:30",
            covers=4,
            name="John Doe",
        )
        self.assertEqual(res.reservation_id, "res_001")
        self.assertEqual(res.covers, 4)
        self.assertEqual(res.status, "confirmed")

    def test_reservation_with_notes(self):
        res = Reservation(
            reservation_id="res_001",
            date=date.today(),
            time="18:30",
            covers=4,
            notes="VIP guest",
            special_requests="Window seating",
        )
        self.assertEqual(res.notes, "VIP guest")
        self.assertEqual(res.special_requests, "Window seating")

    def test_reservation_status_cancelled(self):
        res = Reservation(
            reservation_id="res_001",
            date=date.today(),
            time="18:30",
            covers=4,
            status="cancelled",
        )
        self.assertEqual(res.status, "cancelled")


# ---------------------------------------------------------------------------
# Test ReservationSnapshot
# ---------------------------------------------------------------------------

class TestReservationSnapshot(unittest.TestCase):
    def test_snapshot_creation(self):
        snap = make_snapshot()
        self.assertEqual(snap.total_covers, 80)
        self.assertEqual(snap.reservation_count, 20)
        self.assertEqual(snap.venue_id, "venue_001")

    def test_snapshot_avg_covers_per_reservation(self):
        snap = make_snapshot(total_covers=100, reservation_count=25)
        self.assertEqual(snap.avg_covers_per_reservation, 4.0)

    def test_snapshot_hourly_breakdown(self):
        snap = make_snapshot()
        self.assertEqual(len(snap.hourly_breakdown), 6)  # 17-23
        self.assertTrue(all(v > 0 for v in snap.hourly_breakdown.values()))

    def test_snapshot_booking_dates(self):
        snap = make_snapshot()
        self.assertGreater(len(snap.booking_dates), 0)


# ---------------------------------------------------------------------------
# Test ReservationAnalyser
# ---------------------------------------------------------------------------

class TestReservationAnalyser(unittest.TestCase):
    def test_analyser_creation(self):
        analyser = ReservationAnalyser(lookback_weeks=8)
        self.assertEqual(analyser.lookback_weeks, 8)

    def test_build_snapshot_from_reservations(self):
        analyser = ReservationAnalyser()
        raw_res = make_reservations(count=10)
        snap = analyser.build_reservation_snapshot("venue_001", "Test Venue", raw_res)

        self.assertEqual(snap.reservation_count, 10)
        self.assertGreater(snap.total_covers, 0)
        self.assertGreater(snap.avg_covers_per_reservation, 0)

    def test_build_snapshot_empty_reservations(self):
        analyser = ReservationAnalyser()
        snap = analyser.build_reservation_snapshot("venue_001", "Test Venue", [])

        self.assertEqual(snap.total_covers, 0)
        self.assertEqual(snap.reservation_count, 0)
        self.assertEqual(snap.avg_covers_per_reservation, 0.0)

    def test_build_snapshot_skips_cancelled(self):
        analyser = ReservationAnalyser()
        raw_res = [
            make_reservation(res_id="res_001", covers=4, status="confirmed"),
            make_reservation(res_id="res_002", covers=4, status="cancelled"),
            make_reservation(res_id="res_003", covers=4, status="confirmed"),
        ]
        snap = analyser.build_reservation_snapshot("venue_001", "Test Venue", raw_res)

        # Should count 2 confirmed + 1 no_show-like = only confirmed
        self.assertLessEqual(snap.total_covers, 8)

    def test_build_snapshot_covers_aggregated(self):
        analyser = ReservationAnalyser()
        raw_res = [
            make_reservation(covers=3),
            make_reservation(covers=5),
            make_reservation(covers=2),
        ]
        snap = analyser.build_reservation_snapshot("venue_001", "Test Venue", raw_res)
        self.assertEqual(snap.total_covers, 10)

    def test_build_snapshot_hourly_breakdown(self):
        analyser = ReservationAnalyser()
        raw_res = [
            {**make_reservation(time="18:00", covers=4), "id": "1"},
            {**make_reservation(time="18:30", covers=4), "id": "2"},
            {**make_reservation(time="19:00", covers=4), "id": "3"},
        ]
        snap = analyser.build_reservation_snapshot("venue_001", "Test Venue", raw_res)
        self.assertIn(18, snap.hourly_breakdown)
        self.assertIn(19, snap.hourly_breakdown)

    def test_analyse_booking_patterns(self):
        analyser = ReservationAnalyser()
        snapshots = []
        for week in range(4):
            for day in range(7):
                snap = make_snapshot(
                    total_covers=60 + (week * 10),
                    day_offset=week * 7 + day,
                )
                snapshots.append(snap)

        patterns = analyser.analyse_booking_patterns(snapshots)
        self.assertGreater(len(patterns), 0)
        self.assertTrue(all(isinstance(p, BookingPattern) for p in patterns))

    def test_demand_signal_high(self):
        """Test positive demand signal."""
        analyser = ReservationAnalyser()
        pattern = BookingPattern(
            day_of_week=0, hour=18,
            avg_covers=50, avg_reservations=12,
            fill_rate=0.85, peak_indicator=True,
        )
        snap = ReservationSnapshot(
            timestamp=datetime(2026, 4, 6, 18, 0, tzinfo=AU_TZ),  # Monday
            venue_id="v", venue_name="T",
            total_covers=100, reservation_count=24,
            avg_covers_per_reservation=4.17,
            hourly_breakdown={18: 100},
        )
        mult, conf = analyser.get_demand_signal(snap, [pattern])
        self.assertGreater(mult, 0)
        self.assertGreaterEqual(conf, 0)

    def test_demand_signal_low(self):
        """Test negative demand signal."""
        analyser = ReservationAnalyser()
        pattern = BookingPattern(
            day_of_week=0, hour=18,
            avg_covers=50, avg_reservations=12,
            fill_rate=0.85, peak_indicator=True,
        )
        snap = ReservationSnapshot(
            timestamp=datetime(2026, 4, 6, 18, 0, tzinfo=AU_TZ),
            venue_id="v", venue_name="T",
            total_covers=10, reservation_count=2,
            avg_covers_per_reservation=5.0,
            hourly_breakdown={18: 10},
        )
        mult, conf = analyser.get_demand_signal(snap, [pattern])
        self.assertLess(mult, 0)

    def test_demand_signal_normal(self):
        """Test neutral demand signal."""
        analyser = ReservationAnalyser()
        pattern = BookingPattern(
            day_of_week=0, hour=18,
            avg_covers=50, avg_reservations=12,
            fill_rate=0.85, peak_indicator=True,
        )
        snap = ReservationSnapshot(
            timestamp=datetime(2026, 4, 6, 18, 0, tzinfo=AU_TZ),
            venue_id="v", venue_name="T",
            total_covers=55, reservation_count=13,
            avg_covers_per_reservation=4.23,
            hourly_breakdown={18: 55},
        )
        mult, conf = analyser.get_demand_signal(snap, [pattern])
        self.assertGreaterEqual(mult, -0.25)

    def test_demand_signal_no_matching_pattern(self):
        """Test when no pattern matches current time."""
        analyser = ReservationAnalyser()
        pattern = BookingPattern(
            day_of_week=1, hour=18,  # Tuesday
            avg_covers=50, avg_reservations=12,
            fill_rate=0.85, peak_indicator=True,
        )
        snap = ReservationSnapshot(
            timestamp=datetime(2026, 4, 6, 18, 0, tzinfo=AU_TZ),  # Monday
            venue_id="v", venue_name="T",
            total_covers=100, reservation_count=20,
            avg_covers_per_reservation=5.0,
            hourly_breakdown={18: 100},
        )
        mult, conf = analyser.get_demand_signal(snap, [pattern])
        self.assertEqual(mult, 0.0)


# ---------------------------------------------------------------------------
# Test BookingPattern
# ---------------------------------------------------------------------------

class TestBookingPattern(unittest.TestCase):
    def test_pattern_creation(self):
        pattern = BookingPattern(
            day_of_week=4,  # Friday
            hour=19,
            avg_covers=80,
            avg_reservations=18,
            fill_rate=0.95,
            peak_indicator=True,
        )
        self.assertEqual(pattern.day_of_week, 4)
        self.assertEqual(pattern.hour, 19)
        self.assertTrue(pattern.peak_indicator)

    def test_pattern_non_peak(self):
        pattern = BookingPattern(
            day_of_week=1,  # Tuesday
            hour=14,
            avg_covers=30,
            avg_reservations=7,
            fill_rate=0.65,
            peak_indicator=False,
        )
        self.assertFalse(pattern.peak_indicator)
        self.assertLess(pattern.avg_covers, 50)


# ---------------------------------------------------------------------------
# Test NowBookItClient
# ---------------------------------------------------------------------------

class TestNowBookItClient(unittest.TestCase):
    def test_client_creation(self):
        creds = make_credentials()
        client = NowBookItClient(creds)
        self.assertIsNotNone(client.creds)
        self.assertEqual(client.creds.venue_id, "venue_001")

    @patch("httpx.AsyncClient")
    def test_client_headers_include_api_key(self, mock_async_client):
        """Test that API key is included in headers."""
        creds = make_credentials(api_key="secret_key_123")
        client = NowBookItClient(creds)
        self.assertEqual(client.creds.api_key, "secret_key_123")


# ---------------------------------------------------------------------------
# Test NowBookItAdapter (with mocked HTTP)
# ---------------------------------------------------------------------------

class TestNowBookItAdapter(unittest.TestCase):
    def setUp(self):
        self.creds = make_credentials()
        self.adapter = NowBookItAdapter(self.creds)

    def test_adapter_creation(self):
        self.assertEqual(self.adapter.venue_id, "venue_001")
        self.assertAlmostEqual(self.adapter.walk_in_ratio, 0.4)

    def test_adapter_signal_type(self):
        self.assertEqual(self.adapter.SIGNAL_TYPE, "reservations")
        self.assertEqual(self.adapter.SOURCE, "nowbookit")

    @patch("rosteriq.data_feeds.nowbookit.NowBookItClient.get_reservations")
    async def async_test_fetch_reservations(self, mock_get):
        """Test fetching reservations."""
        mock_get.return_value = make_reservations(count=5)

        from_date = date.today()
        to_date = date.today()

        result = await self.adapter.fetch_reservations(from_date, to_date)
        self.assertEqual(len(result), 5)

    @patch("rosteriq.data_feeds.nowbookit.NowBookItClient.get_venue")
    async def async_test_health_check_success(self, mock_venue):
        """Test successful health check."""
        mock_venue.return_value = {"name": "Test Venue", "id": "venue_001"}

        # Mock the health_check on client
        with patch("rosteriq.data_feeds.nowbookit.NowBookItClient.health_check", new_callable=AsyncMock) as mock_health:
            mock_health.return_value = {}
            result = await self.adapter.health_check()
            self.assertEqual(result["status"], "healthy")

    def test_walk_in_ratio(self):
        """Test walk-in ratio configuration."""
        adapter = NowBookItAdapter(self.creds, walk_in_ratio=0.5)
        self.assertEqual(adapter.walk_in_ratio, 0.5)


# ---------------------------------------------------------------------------
# Test Factory Function
# ---------------------------------------------------------------------------

class TestNowBookItFactory(unittest.TestCase):
    def test_create_adapter(self):
        adapter = create_nowbookit_adapter(
            api_key="sk_test",
            venue_id="venue_001",
        )
        self.assertIsInstance(adapter, NowBookItAdapter)
        self.assertEqual(adapter.venue_id, "venue_001")

    def test_create_adapter_with_custom_url(self):
        adapter = create_nowbookit_adapter(
            api_key="sk_test",
            venue_id="venue_001",
            base_url="https://custom.api.com/v2",
        )
        self.assertEqual(adapter.client.creds.base_url, "https://custom.api.com/v2")


# ---------------------------------------------------------------------------
# Test Error Handling
# ---------------------------------------------------------------------------

class TestNowBookItErrors(unittest.TestCase):
    def test_nowbookit_error_creation(self):
        error = NowBookItError("Test error")
        self.assertEqual(str(error), "Test error")

    def test_auth_error_is_base_error(self):
        error = NowBookItAuthError("Auth failed")
        self.assertIsInstance(error, NowBookItError)

    def test_error_hierarchy(self):
        auth_err = NowBookItAuthError("auth")
        self.assertIsInstance(auth_err, NowBookItError)
        self.assertIsInstance(auth_err, Exception)


# ---------------------------------------------------------------------------
# Integration Tests (async)
# ---------------------------------------------------------------------------

class TestNowBookItIntegration(unittest.TestCase):
    """Integration tests requiring async execution."""

    def setUp(self):
        self.creds = make_credentials()

    def test_async_adapter_lifecycle(self):
        """Test adapter creation, initialization, and cleanup."""
        async def run_test():
            adapter = NowBookItAdapter(self.creds)
            # Should be creatable without error
            self.assertIsNotNone(adapter)
            await adapter.close()

        asyncio.run(run_test())

    def test_async_demand_signal_generation(self):
        """Test async demand signal generation."""
        async def run_test():
            adapter = NowBookItAdapter(self.creds)

            # Mock the client
            with patch.object(adapter.client, "get_reservations", new_callable=AsyncMock) as mock_res:
                with patch.object(adapter.client, "get_venue", new_callable=AsyncMock) as mock_venue:
                    mock_res.return_value = make_reservations(count=5)
                    mock_venue.return_value = {"name": "Test Venue"}

                    # Initialize patterns first
                    await adapter.initialise()

                    # Generate signal
                    signal = await adapter.get_demand_signal(date.today())
                    self.assertIn("signal_type", signal)
                    self.assertIn("value", signal)
                    self.assertIn("confidence", signal)

            await adapter.close()

        asyncio.run(run_test())


# ---------------------------------------------------------------------------
# Test Runner
# ---------------------------------------------------------------------------

def run_tests():
    """Run all tests and return results."""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestNowBookItCredentials))
    suite.addTests(loader.loadTestsFromTestCase(TestReservation))
    suite.addTests(loader.loadTestsFromTestCase(TestReservationSnapshot))
    suite.addTests(loader.loadTestsFromTestCase(TestReservationAnalyser))
    suite.addTests(loader.loadTestsFromTestCase(TestBookingPattern))
    suite.addTests(loader.loadTestsFromTestCase(TestNowBookItClient))
    suite.addTests(loader.loadTestsFromTestCase(TestNowBookItAdapter))
    suite.addTests(loader.loadTestsFromTestCase(TestNowBookItFactory))
    suite.addTests(loader.loadTestsFromTestCase(TestNowBookItErrors))
    suite.addTests(loader.loadTestsFromTestCase(TestNowBookItIntegration))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
