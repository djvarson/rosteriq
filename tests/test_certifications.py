"""Tests for rosteriq.certifications — pure-stdlib, no pytest.

Runs with `PYTHONPATH=. python3 -m unittest tests.test_certifications -v`
Tests cover status computation, expiry alerts at various thresholds,
venue compliance, missing certs, calendar, store persistence, and edge cases.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from datetime import date, timedelta

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq.certifications import (  # noqa: E402
    Certification,
    CertType,
    CertStatus,
    CertAlert,
    VenueComplianceStatus,
    compute_cert_status,
    check_expiry_alerts,
    check_venue_compliance,
    get_missing_certs,
    get_expiry_calendar,
    get_certification_store,
    _reset_for_tests,
)


def _reset():
    """Reset the store singleton for tests."""
    _reset_for_tests()


# ============================================================================
# Tests: compute_cert_status
# ============================================================================


class TestCertStatus(unittest.TestCase):
    """Tests for cert status computation."""

    def test_status_valid_when_expiry_far_away(self):
        """Certification is VALID when expiry > 60 days away."""
        future_date = date.today() + timedelta(days=90)
        cert = Certification(
            cert_id="cert_1",
            employee_id="emp1",
            employee_name="Alice",
            venue_id="venue1",
            cert_type=CertType.RSA,
            cert_number="RSA123",
            issued_date=date.today(),
            expiry_date=future_date,
            state="QLD",
        )
        self.assertEqual(cert.status, CertStatus.VALID)

    def test_status_expiring_soon_at_59_days(self):
        """Certification is EXPIRING_SOON when 59 days away (just under 60)."""
        future_date = date.today() + timedelta(days=59)
        cert = Certification(
            cert_id="cert_1",
            employee_id="emp1",
            employee_name="Alice",
            venue_id="venue1",
            cert_type=CertType.FOOD_SAFETY,
            cert_number="FS123",
            issued_date=date.today(),
            expiry_date=future_date,
            state="NSW",
        )
        self.assertEqual(cert.status, CertStatus.EXPIRING_SOON)

    def test_status_expiring_soon_at_30_days(self):
        """Certification is EXPIRING_SOON when 30 days away."""
        future_date = date.today() + timedelta(days=30)
        cert = Certification(
            cert_id="cert_1",
            employee_id="emp1",
            employee_name="Alice",
            venue_id="venue1",
            cert_type=CertType.FIRST_AID,
            cert_number="FA123",
            issued_date=date.today(),
            expiry_date=future_date,
            state="VIC",
        )
        self.assertEqual(cert.status, CertStatus.EXPIRING_SOON)

    def test_status_expired_past_date(self):
        """Certification is EXPIRED when past expiry date."""
        past_date = date.today() - timedelta(days=1)
        cert = Certification(
            cert_id="cert_1",
            employee_id="emp1",
            employee_name="Alice",
            venue_id="venue1",
            cert_type=CertType.RSA,
            cert_number="RSA123",
            issued_date=past_date - timedelta(days=365),
            expiry_date=past_date,
            state="QLD",
        )
        self.assertEqual(cert.status, CertStatus.EXPIRED)

    def test_status_valid_with_no_expiry_date(self):
        """Certification is VALID when expiry_date is None (e.g. VIC RSA)."""
        cert = Certification(
            cert_id="cert_1",
            employee_id="emp1",
            employee_name="Alice",
            venue_id="venue1",
            cert_type=CertType.RSA,
            cert_number="RSA_VIC_123",
            issued_date=date.today(),
            expiry_date=None,  # VIC RSA has no expiry
            state="VIC",
        )
        self.assertEqual(cert.status, CertStatus.VALID)


# ============================================================================
# Tests: check_expiry_alerts
# ============================================================================


class TestExpiryAlerts(unittest.TestCase):
    """Tests for expiry alert checking."""

    def test_alerts_empty_when_no_certs(self):
        """No alerts when cert list is empty."""
        alerts = check_expiry_alerts([])
        self.assertEqual(len(alerts), 0)

    def test_alerts_no_warning_far_away(self):
        """No alert when expiry is far away (>60 days)."""
        future = date.today() + timedelta(days=120)
        cert = Certification(
            cert_id="cert_1",
            employee_id="emp1",
            employee_name="Alice",
            venue_id="venue1",
            cert_type=CertType.RSA,
            cert_number="RSA123",
            issued_date=date.today(),
            expiry_date=future,
            state="QLD",
        )
        alerts = check_expiry_alerts([cert])
        self.assertEqual(len(alerts), 0)

    def test_alert_warning_at_45_days(self):
        """WARNING alert when 45 days away (60 > days > 30)."""
        future = date.today() + timedelta(days=45)
        cert = Certification(
            cert_id="cert_1",
            employee_id="emp1",
            employee_name="Alice",
            venue_id="venue1",
            cert_type=CertType.FOOD_SAFETY,
            cert_number="FS123",
            issued_date=date.today(),
            expiry_date=future,
            state="NSW",
        )
        alerts = check_expiry_alerts([cert])
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].severity, "WARNING")
        self.assertEqual(alerts[0].days_until_expiry, 45)

    def test_alert_urgent_at_20_days(self):
        """URGENT alert when 20 days away (<30)."""
        future = date.today() + timedelta(days=20)
        cert = Certification(
            cert_id="cert_1",
            employee_id="emp1",
            employee_name="Alice",
            venue_id="venue1",
            cert_type=CertType.FIRST_AID,
            cert_number="FA123",
            issued_date=date.today(),
            expiry_date=future,
            state="VIC",
        )
        alerts = check_expiry_alerts([cert])
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].severity, "URGENT")

    def test_alert_expired_past_date(self):
        """EXPIRED alert when past expiry date."""
        past = date.today() - timedelta(days=5)
        cert = Certification(
            cert_id="cert_1",
            employee_id="emp1",
            employee_name="Alice",
            venue_id="venue1",
            cert_type=CertType.RSA,
            cert_number="RSA123",
            issued_date=past - timedelta(days=365),
            expiry_date=past,
            state="QLD",
        )
        alerts = check_expiry_alerts([cert])
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].severity, "EXPIRED")
        self.assertEqual(alerts[0].days_until_expiry, -5)

    def test_alerts_sorted_by_urgency(self):
        """Alerts sorted by urgency: EXPIRED, URGENT, WARNING."""
        today = date.today()
        certs = [
            Certification(
                cert_id="cert_warning",
                employee_id="emp1",
                employee_name="Alice",
                venue_id="venue1",
                cert_type=CertType.RSA,
                cert_number="RSA123",
                issued_date=today,
                expiry_date=today + timedelta(days=45),  # WARNING
                state="QLD",
            ),
            Certification(
                cert_id="cert_urgent",
                employee_id="emp2",
                employee_name="Bob",
                venue_id="venue1",
                cert_type=CertType.FOOD_SAFETY,
                cert_number="FS123",
                issued_date=today,
                expiry_date=today + timedelta(days=20),  # URGENT
                state="NSW",
            ),
            Certification(
                cert_id="cert_expired",
                employee_id="emp3",
                employee_name="Charlie",
                venue_id="venue1",
                cert_type=CertType.FIRST_AID,
                cert_number="FA123",
                issued_date=today,
                expiry_date=today - timedelta(days=5),  # EXPIRED
                state="VIC",
            ),
        ]
        alerts = check_expiry_alerts(certs)
        self.assertEqual(len(alerts), 3)
        self.assertEqual(alerts[0].severity, "EXPIRED")
        self.assertEqual(alerts[1].severity, "URGENT")
        self.assertEqual(alerts[2].severity, "WARNING")

    def test_alert_ignores_none_expiry_date(self):
        """No alert for certs with expiry_date=None."""
        cert = Certification(
            cert_id="cert_1",
            employee_id="emp1",
            employee_name="Alice",
            venue_id="venue1",
            cert_type=CertType.RSA,
            cert_number="RSA_VIC_123",
            issued_date=date.today(),
            expiry_date=None,
            state="VIC",
        )
        alerts = check_expiry_alerts([cert])
        self.assertEqual(len(alerts), 0)


# ============================================================================
# Tests: check_venue_compliance
# ============================================================================


class TestVenueCompliance(unittest.TestCase):
    """Tests for venue compliance checking."""

    def test_compliance_zero_staff(self):
        """Compliance report for venue with no staff."""
        compliance = check_venue_compliance("venue1", [], [])
        self.assertEqual(compliance.total_staff, 0)
        self.assertEqual(compliance.certs_valid, 0)
        self.assertEqual(compliance.compliance_pct, 0.0)
        self.assertFalse(compliance.food_safety_covered)

    def test_compliance_all_valid(self):
        """Compliance when all staff have valid certs."""
        future = date.today() + timedelta(days=90)
        certs = [
            Certification(
                cert_id="cert_1",
                employee_id="emp1",
                employee_name="Alice",
                venue_id="venue1",
                cert_type=CertType.RSA,
                cert_number="RSA123",
                issued_date=date.today(),
                expiry_date=future,
                state="QLD",
            ),
            Certification(
                cert_id="cert_2",
                employee_id="emp2",
                employee_name="Bob",
                venue_id="venue1",
                cert_type=CertType.RSA,
                cert_number="RSA456",
                issued_date=date.today(),
                expiry_date=future,
                state="QLD",
            ),
        ]
        compliance = check_venue_compliance("venue1", certs, ["emp1", "emp2"])
        self.assertEqual(compliance.total_staff, 2)
        self.assertEqual(compliance.certs_valid, 2)
        self.assertEqual(compliance.compliance_pct, 100.0)

    def test_compliance_with_expired_certs(self):
        """Compliance tracks expired vs missing."""
        future = date.today() + timedelta(days=90)
        past = date.today() - timedelta(days=5)
        certs = [
            Certification(
                cert_id="cert_1",
                employee_id="emp1",
                employee_name="Alice",
                venue_id="venue1",
                cert_type=CertType.RSA,
                cert_number="RSA123",
                issued_date=date.today(),
                expiry_date=future,
                state="QLD",
            ),
            Certification(
                cert_id="cert_2",
                employee_id="emp2",
                employee_name="Bob",
                venue_id="venue1",
                cert_type=CertType.RSA,
                cert_number="RSA456",
                issued_date=past - timedelta(days=365),
                expiry_date=past,
                state="QLD",
            ),
        ]
        compliance = check_venue_compliance("venue1", certs, ["emp1", "emp2", "emp3"])
        self.assertEqual(compliance.total_staff, 3)
        self.assertEqual(compliance.certs_valid, 1)
        self.assertEqual(compliance.certs_expired, 1)
        self.assertEqual(compliance.certs_missing, 1)
        # Check compliance is approximately 33.33% (1 valid out of 3 total)
        self.assertLess(compliance.compliance_pct, 34)
        self.assertGreater(compliance.compliance_pct, 33)

    def test_compliance_food_safety_covered(self):
        """food_safety_covered is True when at least one valid food safety supervisor."""
        future = date.today() + timedelta(days=90)
        certs = [
            Certification(
                cert_id="cert_1",
                employee_id="emp1",
                employee_name="Alice",
                venue_id="venue1",
                cert_type=CertType.FOOD_SAFETY,
                cert_number="FS123",
                issued_date=date.today(),
                expiry_date=future,
                state="NSW",
            ),
        ]
        compliance = check_venue_compliance("venue1", certs, ["emp1"])
        self.assertTrue(compliance.food_safety_covered)

    def test_compliance_food_safety_not_covered(self):
        """food_safety_covered is False when no valid food safety supervisor."""
        future = date.today() + timedelta(days=90)
        certs = [
            Certification(
                cert_id="cert_1",
                employee_id="emp1",
                employee_name="Alice",
                venue_id="venue1",
                cert_type=CertType.RSA,
                cert_number="RSA123",
                issued_date=date.today(),
                expiry_date=future,
                state="QLD",
            ),
        ]
        compliance = check_venue_compliance("venue1", certs, ["emp1"])
        self.assertFalse(compliance.food_safety_covered)

    def test_compliance_includes_alerts(self):
        """Compliance status includes expiry alerts."""
        future = date.today() + timedelta(days=45)
        certs = [
            Certification(
                cert_id="cert_1",
                employee_id="emp1",
                employee_name="Alice",
                venue_id="venue1",
                cert_type=CertType.RSA,
                cert_number="RSA123",
                issued_date=date.today(),
                expiry_date=future,
                state="QLD",
            ),
        ]
        compliance = check_venue_compliance("venue1", certs, ["emp1"])
        self.assertEqual(len(compliance.alerts), 1)
        self.assertEqual(compliance.alerts[0].severity, "WARNING")


# ============================================================================
# Tests: get_missing_certs
# ============================================================================


class TestMissingCerts(unittest.TestCase):
    """Tests for missing cert detection."""

    def test_missing_certs_none_held(self):
        """All required certs are missing when none held."""
        required = [CertType.RSA, CertType.FOOD_SAFETY, CertType.FIRST_AID]
        missing = get_missing_certs("emp1", [], required)
        self.assertEqual(set(missing), set(required))

    def test_missing_certs_some_held(self):
        """Only unheld certs are missing."""
        held = [
            Certification(
                cert_id="cert_1",
                employee_id="emp1",
                employee_name="Alice",
                venue_id="venue1",
                cert_type=CertType.RSA,
                cert_number="RSA123",
                issued_date=date.today(),
                expiry_date=date.today() + timedelta(days=90),
                state="QLD",
            ),
        ]
        required = [CertType.RSA, CertType.FOOD_SAFETY, CertType.FIRST_AID]
        missing = get_missing_certs("emp1", held, required)
        self.assertEqual(set(missing), {CertType.FOOD_SAFETY, CertType.FIRST_AID})

    def test_missing_certs_all_held(self):
        """No missing certs when all required are held."""
        future = date.today() + timedelta(days=90)
        held = [
            Certification(
                cert_id="cert_1",
                employee_id="emp1",
                employee_name="Alice",
                venue_id="venue1",
                cert_type=CertType.RSA,
                cert_number="RSA123",
                issued_date=date.today(),
                expiry_date=future,
                state="QLD",
            ),
            Certification(
                cert_id="cert_2",
                employee_id="emp1",
                employee_name="Alice",
                venue_id="venue1",
                cert_type=CertType.FOOD_SAFETY,
                cert_number="FS123",
                issued_date=date.today(),
                expiry_date=future,
                state="NSW",
            ),
        ]
        required = [CertType.RSA, CertType.FOOD_SAFETY]
        missing = get_missing_certs("emp1", held, required)
        self.assertEqual(len(missing), 0)


# ============================================================================
# Tests: get_expiry_calendar
# ============================================================================


class TestExpiryCalendar(unittest.TestCase):
    """Tests for expiry calendar generation."""

    def test_calendar_empty_when_no_certs(self):
        """Empty calendar when no certs."""
        calendar = get_expiry_calendar([], days_ahead=90)
        self.assertEqual(len(calendar), 0)

    def test_calendar_excludes_far_away_dates(self):
        """Calendar excludes certs expiring beyond days_ahead."""
        future = date.today() + timedelta(days=120)
        cert = Certification(
            cert_id="cert_1",
            employee_id="emp1",
            employee_name="Alice",
            venue_id="venue1",
            cert_type=CertType.RSA,
            cert_number="RSA123",
            issued_date=date.today(),
            expiry_date=future,
            state="QLD",
        )
        calendar = get_expiry_calendar([cert], days_ahead=90)
        self.assertEqual(len(calendar), 0)

    def test_calendar_includes_within_range(self):
        """Calendar includes certs expiring within days_ahead."""
        future = date.today() + timedelta(days=45)
        cert = Certification(
            cert_id="cert_1",
            employee_id="emp1",
            employee_name="Alice",
            venue_id="venue1",
            cert_type=CertType.RSA,
            cert_number="RSA123",
            issued_date=date.today(),
            expiry_date=future,
            state="QLD",
        )
        calendar = get_expiry_calendar([cert], days_ahead=90)
        self.assertEqual(len(calendar), 1)
        self.assertEqual(calendar[0]["employee_id"], "emp1")
        self.assertEqual(calendar[0]["cert_type"], "rsa")
        self.assertEqual(calendar[0]["days_remaining"], 45)

    def test_calendar_sorted_by_expiry_date(self):
        """Calendar items sorted by expiry date (soonest first)."""
        today = date.today()
        certs = [
            Certification(
                cert_id="cert_1",
                employee_id="emp1",
                employee_name="Alice",
                venue_id="venue1",
                cert_type=CertType.RSA,
                cert_number="RSA123",
                issued_date=today,
                expiry_date=today + timedelta(days=50),
                state="QLD",
            ),
            Certification(
                cert_id="cert_2",
                employee_id="emp2",
                employee_name="Bob",
                venue_id="venue1",
                cert_type=CertType.FOOD_SAFETY,
                cert_number="FS123",
                issued_date=today,
                expiry_date=today + timedelta(days=20),
                state="NSW",
            ),
        ]
        calendar = get_expiry_calendar(certs, days_ahead=90)
        self.assertEqual(len(calendar), 2)
        self.assertEqual(calendar[0]["employee_id"], "emp2")  # 20 days (Bob)
        self.assertEqual(calendar[1]["employee_id"], "emp1")  # 50 days (Alice)

    def test_calendar_ignores_none_expiry(self):
        """Calendar ignores certs with expiry_date=None."""
        future = date.today() + timedelta(days=45)
        certs = [
            Certification(
                cert_id="cert_1",
                employee_id="emp1",
                employee_name="Alice",
                venue_id="venue1",
                cert_type=CertType.RSA,
                cert_number="RSA_VIC_123",
                issued_date=date.today(),
                expiry_date=None,
                state="VIC",
            ),
            Certification(
                cert_id="cert_2",
                employee_id="emp2",
                employee_name="Bob",
                venue_id="venue1",
                cert_type=CertType.RSA,
                cert_number="RSA123",
                issued_date=date.today(),
                expiry_date=future,
                state="QLD",
            ),
        ]
        calendar = get_expiry_calendar(certs, days_ahead=90)
        self.assertEqual(len(calendar), 1)
        self.assertEqual(calendar[0]["employee_id"], "emp2")


# ============================================================================
# Tests: CertificationStore
# ============================================================================


class TestCertStore(unittest.TestCase):
    """Tests for certification store operations."""

    def setUp(self):
        """Reset store before each test."""
        _reset()

    def test_store_add_and_retrieve(self):
        """Store can add and retrieve a certification."""
        store = get_certification_store()
        future = date.today() + timedelta(days=90)
        cert = Certification(
            cert_id="cert_1",
            employee_id="emp1",
            employee_name="Alice",
            venue_id="venue1",
            cert_type=CertType.RSA,
            cert_number="RSA123",
            issued_date=date.today(),
            expiry_date=future,
            state="QLD",
        )
        stored = store.add(cert)
        self.assertEqual(stored.cert_id, "cert_1")

        retrieved = store.get("cert_1")
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.employee_name, "Alice")

    def test_store_update(self):
        """Store can update a certification."""
        store = get_certification_store()
        future = date.today() + timedelta(days=90)
        cert = Certification(
            cert_id="cert_1",
            employee_id="emp1",
            employee_name="Alice",
            venue_id="venue1",
            cert_type=CertType.RSA,
            cert_number="RSA123",
            issued_date=date.today(),
            expiry_date=future,
            state="QLD",
        )
        store.add(cert)

        # Renew with new cert number and expiry
        new_future = date.today() + timedelta(days=1095)  # 3 years
        updated = store.update("cert_1", cert_number="RSA789", expiry_date=new_future)
        self.assertEqual(updated.cert_number, "RSA789")
        self.assertEqual(updated.expiry_date, new_future)
        self.assertEqual(updated.status, CertStatus.VALID)

    def test_store_delete(self):
        """Store can delete a certification."""
        store = get_certification_store()
        future = date.today() + timedelta(days=90)
        cert = Certification(
            cert_id="cert_1",
            employee_id="emp1",
            employee_name="Alice",
            venue_id="venue1",
            cert_type=CertType.RSA,
            cert_number="RSA123",
            issued_date=date.today(),
            expiry_date=future,
            state="QLD",
        )
        store.add(cert)
        self.assertIsNotNone(store.get("cert_1"))

        store.delete("cert_1")
        self.assertIsNone(store.get("cert_1"))

    def test_store_list_by_venue(self):
        """Store can list certs by venue."""
        store = get_certification_store()
        future = date.today() + timedelta(days=90)

        certs = [
            Certification(
                cert_id="cert_1",
                employee_id="emp1",
                employee_name="Alice",
                venue_id="venue1",
                cert_type=CertType.RSA,
                cert_number="RSA123",
                issued_date=date.today(),
                expiry_date=future,
                state="QLD",
            ),
            Certification(
                cert_id="cert_2",
                employee_id="emp2",
                employee_name="Bob",
                venue_id="venue1",
                cert_type=CertType.FOOD_SAFETY,
                cert_number="FS123",
                issued_date=date.today(),
                expiry_date=future,
                state="NSW",
            ),
            Certification(
                cert_id="cert_3",
                employee_id="emp3",
                employee_name="Charlie",
                venue_id="venue2",
                cert_type=CertType.FIRST_AID,
                cert_number="FA123",
                issued_date=date.today(),
                expiry_date=future,
                state="VIC",
            ),
        ]
        for cert in certs:
            store.add(cert)

        venue1_certs = store.list_by_venue("venue1")
        self.assertEqual(len(venue1_certs), 2)
        self.assertTrue(all(c.venue_id == "venue1" for c in venue1_certs))

    def test_store_list_by_venue_filter_employee(self):
        """Store list_by_venue can filter by employee."""
        store = get_certification_store()
        future = date.today() + timedelta(days=90)

        certs = [
            Certification(
                cert_id="cert_1",
                employee_id="emp1",
                employee_name="Alice",
                venue_id="venue1",
                cert_type=CertType.RSA,
                cert_number="RSA123",
                issued_date=date.today(),
                expiry_date=future,
                state="QLD",
            ),
            Certification(
                cert_id="cert_2",
                employee_id="emp1",
                employee_name="Alice",
                venue_id="venue1",
                cert_type=CertType.FOOD_SAFETY,
                cert_number="FS123",
                issued_date=date.today(),
                expiry_date=future,
                state="NSW",
            ),
        ]
        for cert in certs:
            store.add(cert)

        emp1_certs = store.list_by_venue("venue1", employee_id="emp1")
        self.assertEqual(len(emp1_certs), 2)

    def test_store_list_by_venue_filter_cert_type(self):
        """Store list_by_venue can filter by cert type."""
        store = get_certification_store()
        future = date.today() + timedelta(days=90)

        certs = [
            Certification(
                cert_id="cert_1",
                employee_id="emp1",
                employee_name="Alice",
                venue_id="venue1",
                cert_type=CertType.RSA,
                cert_number="RSA123",
                issued_date=date.today(),
                expiry_date=future,
                state="QLD",
            ),
            Certification(
                cert_id="cert_2",
                employee_id="emp2",
                employee_name="Bob",
                venue_id="venue1",
                cert_type=CertType.FOOD_SAFETY,
                cert_number="FS123",
                issued_date=date.today(),
                expiry_date=future,
                state="NSW",
            ),
        ]
        for cert in certs:
            store.add(cert)

        rsa_certs = store.list_by_venue("venue1", cert_type=CertType.RSA)
        self.assertEqual(len(rsa_certs), 1)
        self.assertEqual(rsa_certs[0].cert_type, CertType.RSA)

    def test_store_list_by_employee(self):
        """Store can list certs by employee across venues."""
        store = get_certification_store()
        future = date.today() + timedelta(days=90)

        certs = [
            Certification(
                cert_id="cert_1",
                employee_id="emp1",
                employee_name="Alice",
                venue_id="venue1",
                cert_type=CertType.RSA,
                cert_number="RSA123",
                issued_date=date.today(),
                expiry_date=future,
                state="QLD",
            ),
            Certification(
                cert_id="cert_2",
                employee_id="emp1",
                employee_name="Alice",
                venue_id="venue2",
                cert_type=CertType.FOOD_SAFETY,
                cert_number="FS123",
                issued_date=date.today(),
                expiry_date=future,
                state="NSW",
            ),
        ]
        for cert in certs:
            store.add(cert)

        emp1_certs = store.list_by_employee("emp1")
        self.assertEqual(len(emp1_certs), 2)

    def test_store_thread_safety(self):
        """Store operations are thread-safe."""
        store = get_certification_store()
        import threading

        future = date.today() + timedelta(days=90)
        results = []

        def add_cert(i):
            cert = Certification(
                cert_id=f"cert_{i}",
                employee_id=f"emp{i}",
                employee_name=f"Person{i}",
                venue_id="venue1",
                cert_type=CertType.RSA,
                cert_number=f"RSA{i}",
                issued_date=date.today(),
                expiry_date=future,
                state="QLD",
            )
            store.add(cert)
            results.append(cert)

        threads = [threading.Thread(target=add_cert, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(store.list_all()), 10)


# ============================================================================
# Helper
# ============================================================================


def pytest_approx(a, b, rel=1e-2):
    """Simple approximation check for floats."""
    return abs(a - b) <= rel * max(abs(a), abs(b))


if __name__ == "__main__":
    unittest.main()
