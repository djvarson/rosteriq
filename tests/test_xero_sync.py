"""Tests for Xero Bidirectional Revenue Sync module."""

import unittest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rosteriq.xero_sync import (
    XeroSyncStore, get_xero_sync_store, _reset_for_tests,
    RevenueCategory, PLLineType, SyncStatus, DEFAULT_ACCOUNT_MAPPING,
)


class TestXeroConnection(unittest.TestCase):
    """Tests for Xero OAuth connection management."""

    def setUp(self):
        _reset_for_tests()
        self.store = get_xero_sync_store()

    def test_save_connection(self):
        conn = self.store.save_connection({
            "venue_id": "v1",
            "tenant_id": "xero-tenant-123",
            "access_token": "tok_abc",
            "refresh_token": "ref_xyz",
            "token_expires_at": "2026-05-01T00:00:00+00:00",
            "organisation_name": "The Pub Co",
        })
        self.assertEqual(conn.venue_id, "v1")
        self.assertEqual(conn.tenant_id, "xero-tenant-123")
        self.assertEqual(conn.organisation_name, "The Pub Co")
        self.assertTrue(conn.is_active)

    def test_get_connection(self):
        self.store.save_connection({
            "venue_id": "v1",
            "tenant_id": "t1",
            "access_token": "a",
            "refresh_token": "r",
            "token_expires_at": "2026-05-01T00:00:00+00:00",
        })
        conn = self.store.get_connection("v1")
        self.assertIsNotNone(conn)
        self.assertEqual(conn.venue_id, "v1")

    def test_get_connection_not_found(self):
        conn = self.store.get_connection("nonexistent")
        self.assertIsNone(conn)

    def test_disconnect(self):
        self.store.save_connection({
            "venue_id": "v1",
            "tenant_id": "t1",
            "access_token": "a",
            "refresh_token": "r",
            "token_expires_at": "2026-05-01T00:00:00+00:00",
        })
        result = self.store.disconnect("v1")
        self.assertTrue(result)
        conn = self.store.get_connection("v1")
        self.assertIsNone(conn)  # inactive now

    def test_disconnect_not_found(self):
        result = self.store.disconnect("nonexistent")
        self.assertFalse(result)

    def test_update_tokens(self):
        conn = self.store.save_connection({
            "venue_id": "v1",
            "tenant_id": "t1",
            "access_token": "old",
            "refresh_token": "old_ref",
            "token_expires_at": "2026-04-01T00:00:00+00:00",
        })
        updated = self.store.update_tokens(
            conn.id, "new_tok", "new_ref", "2026-06-01T00:00:00+00:00")
        self.assertIsNotNone(updated)
        self.assertEqual(updated.access_token, "new_tok")
        self.assertEqual(updated.refresh_token, "new_ref")

    def test_is_token_expired(self):
        self.store.save_connection({
            "venue_id": "v1",
            "tenant_id": "t1",
            "access_token": "a",
            "refresh_token": "r",
            "token_expires_at": "2020-01-01T00:00:00+00:00",  # past
        })
        self.assertTrue(self.store.is_token_expired("v1"))

    def test_is_token_not_expired(self):
        self.store.save_connection({
            "venue_id": "v1",
            "tenant_id": "t1",
            "access_token": "a",
            "refresh_token": "r",
            "token_expires_at": "2030-01-01T00:00:00+00:00",  # future
        })
        self.assertFalse(self.store.is_token_expired("v1"))

    def test_is_token_expired_no_connection(self):
        self.assertTrue(self.store.is_token_expired("nonexistent"))


class TestRevenueRecords(unittest.TestCase):
    """Tests for revenue record management."""

    def setUp(self):
        _reset_for_tests()
        self.store = get_xero_sync_store()

    def test_add_revenue(self):
        rec = self.store.add_revenue({
            "venue_id": "v1",
            "date": "2026-04-20",
            "category": RevenueCategory.FOOD.value,
            "amount": 5000.00,
            "tax_amount": 454.55,
            "source": "xero_invoice",
        })
        self.assertEqual(rec.venue_id, "v1")
        self.assertEqual(rec.amount, 5000.00)
        self.assertEqual(rec.tax_amount, 454.55)
        self.assertEqual(rec.net_amount, 4545.45)
        self.assertEqual(rec.category, "FOOD")

    def test_add_revenue_default_gst(self):
        """When no tax_amount provided, default to 1/11 (AU GST)."""
        rec = self.store.add_revenue({
            "venue_id": "v1",
            "date": "2026-04-20",
            "amount": 1100.00,
        })
        self.assertAlmostEqual(rec.tax_amount, 100.00, places=2)
        self.assertAlmostEqual(rec.net_amount, 1000.00, places=2)

    def test_get_revenue_filters(self):
        self.store.add_revenue({"venue_id": "v1", "date": "2026-04-18",
                                "amount": 1000, "category": "FOOD"})
        self.store.add_revenue({"venue_id": "v1", "date": "2026-04-19",
                                "amount": 2000, "category": "BEVERAGE"})
        self.store.add_revenue({"venue_id": "v1", "date": "2026-04-20",
                                "amount": 3000, "category": "FOOD"})
        self.store.add_revenue({"venue_id": "v2", "date": "2026-04-20",
                                "amount": 500, "category": "FOOD"})

        # All for v1
        all_v1 = self.store.get_revenue("v1")
        self.assertEqual(len(all_v1), 3)

        # Date range
        ranged = self.store.get_revenue("v1", date_from="2026-04-19")
        self.assertEqual(len(ranged), 2)

        # Category filter
        food = self.store.get_revenue("v1", category="FOOD")
        self.assertEqual(len(food), 2)

        # Venue isolation
        v2 = self.store.get_revenue("v2")
        self.assertEqual(len(v2), 1)

    def test_get_daily_revenue_total(self):
        self.store.add_revenue({"venue_id": "v1", "date": "2026-04-20",
                                "amount": 3000, "category": "FOOD",
                                "tax_amount": 272.73})
        self.store.add_revenue({"venue_id": "v1", "date": "2026-04-20",
                                "amount": 2000, "category": "BEVERAGE",
                                "tax_amount": 181.82})
        daily = self.store.get_daily_revenue_total("v1", "2026-04-20")
        self.assertEqual(daily["total_revenue"], 5000.0)
        self.assertAlmostEqual(daily["total_tax"], 454.55, places=2)
        self.assertIn("FOOD", daily["by_category"])
        self.assertIn("BEVERAGE", daily["by_category"])
        self.assertEqual(daily["record_count"], 2)

    def test_delete_revenue(self):
        rec = self.store.add_revenue({"venue_id": "v1", "date": "2026-04-20",
                                      "amount": 1000})
        self.assertTrue(self.store.delete_revenue(rec.id))
        self.assertEqual(len(self.store.get_revenue("v1")), 0)

    def test_delete_revenue_not_found(self):
        self.assertFalse(self.store.delete_revenue("nonexistent"))

    def test_bulk_add_revenue(self):
        records = self.store.bulk_add_revenue([
            {"venue_id": "v1", "date": "2026-04-20", "amount": 1000},
            {"venue_id": "v1", "date": "2026-04-20", "amount": 2000},
            {"venue_id": "v1", "date": "2026-04-20", "amount": 3000},
        ])
        self.assertEqual(len(records), 3)
        self.assertEqual(len(self.store.get_revenue("v1")), 3)

    def test_revenue_sorted_by_date(self):
        self.store.add_revenue({"venue_id": "v1", "date": "2026-04-22",
                                "amount": 1000})
        self.store.add_revenue({"venue_id": "v1", "date": "2026-04-18",
                                "amount": 2000})
        self.store.add_revenue({"venue_id": "v1", "date": "2026-04-20",
                                "amount": 3000})
        records = self.store.get_revenue("v1")
        dates = [r.date for r in records]
        self.assertEqual(dates, ["2026-04-18", "2026-04-20", "2026-04-22"])


class TestPLSummary(unittest.TestCase):
    """Tests for P&L summary calculations."""

    def setUp(self):
        _reset_for_tests()
        self.store = get_xero_sync_store()
        # Seed revenue data
        self.store.add_revenue({"venue_id": "v1", "date": "2026-04-14",
                                "amount": 5000, "tax_amount": 454.55})
        self.store.add_revenue({"venue_id": "v1", "date": "2026-04-15",
                                "amount": 6000, "tax_amount": 545.45})
        self.store.add_revenue({"venue_id": "v1", "date": "2026-04-16",
                                "amount": 7000, "tax_amount": 636.36})

    def test_calculate_pl_basic(self):
        pl = self.store.calculate_pl(
            "v1", "2026-04-14", "2026-04-16",
            wage_cost=5400, cogs=3600, other_expenses=1000)
        self.assertEqual(pl.total_revenue, 18000.0)
        self.assertEqual(pl.cost_of_goods, 3600.0)
        self.assertEqual(pl.gross_profit, 14400.0)  # 18000 - 3600
        self.assertEqual(pl.total_wages, 5400.0)
        self.assertEqual(pl.net_profit, 8000.0)  # 14400 - 5400 - 1000
        self.assertEqual(pl.labour_cost_pct, 30.0)  # 5400/18000*100
        self.assertEqual(pl.cogs_pct, 20.0)  # 3600/18000*100

    def test_calculate_pl_with_line_items(self):
        pl = self.store.calculate_pl(
            "v1", "2026-04-14", "2026-04-16",
            line_items=[
                {"type": "COST_OF_GOODS", "amount": 2000},
                {"type": "WAGES", "amount": 4000},
                {"type": "OVERHEAD", "amount": 500},
            ])
        self.assertEqual(pl.cost_of_goods, 2000.0)
        self.assertEqual(pl.total_wages, 4000.0)
        self.assertEqual(pl.other_expenses, 500.0)

    def test_calculate_pl_zero_revenue(self):
        _reset_for_tests()
        store = get_xero_sync_store()
        pl = store.calculate_pl("v1", "2026-01-01", "2026-01-07",
                                wage_cost=1000)
        self.assertEqual(pl.total_revenue, 0)
        self.assertEqual(pl.labour_cost_pct, 0)

    def test_get_pl_summaries(self):
        self.store.calculate_pl("v1", "2026-04-14", "2026-04-14",
                                wage_cost=1000)
        self.store.calculate_pl("v1", "2026-04-15", "2026-04-16",
                                wage_cost=2000)
        summaries = self.store.get_pl_summaries("v1")
        self.assertEqual(len(summaries), 2)

    def test_get_latest_pl(self):
        self.store.calculate_pl("v1", "2026-04-14", "2026-04-14",
                                wage_cost=1000)
        self.store.calculate_pl("v1", "2026-04-15", "2026-04-16",
                                wage_cost=2000)
        latest = self.store.get_latest_pl("v1")
        self.assertIsNotNone(latest)
        self.assertEqual(latest.period_start, "2026-04-15")

    def test_get_latest_pl_none(self):
        self.assertIsNone(self.store.get_latest_pl("v2"))

    def test_compare_periods(self):
        # Period 1: Apr 14 = $5000
        # Period 2: Apr 15-16 = $13000
        result = self.store.compare_periods(
            "v1", "2026-04-14", "2026-04-14",
            "2026-04-15", "2026-04-16")
        self.assertEqual(result["period_1"]["revenue"], 5000.0)
        self.assertEqual(result["period_2"]["revenue"], 13000.0)
        self.assertEqual(result["revenue_change"], 8000.0)
        self.assertEqual(result["revenue_change_pct"], 160.0)


class TestSyncLog(unittest.TestCase):
    """Tests for sync operation logging."""

    def setUp(self):
        _reset_for_tests()
        self.store = get_xero_sync_store()

    def test_start_and_complete_sync(self):
        log = self.store.start_sync("v1", "revenue_pull")
        self.assertEqual(log.status, "IN_PROGRESS")
        self.assertEqual(log.sync_type, "revenue_pull")

        completed = self.store.complete_sync(log.id, records_synced=42)
        self.assertEqual(completed.status, "COMPLETED")
        self.assertEqual(completed.records_synced, 42)
        self.assertIsNotNone(completed.completed_at)

    def test_sync_failed(self):
        log = self.store.start_sync("v1", "revenue_pull")
        failed = self.store.complete_sync(log.id, error="API timeout")
        self.assertEqual(failed.status, "FAILED")
        self.assertEqual(failed.error_message, "API timeout")

    def test_get_sync_history(self):
        self.store.start_sync("v1", "revenue_pull")
        self.store.start_sync("v1", "payroll_push")
        self.store.start_sync("v1", "revenue_pull")
        self.store.start_sync("v2", "revenue_pull")

        v1_all = self.store.get_sync_history("v1")
        self.assertEqual(len(v1_all), 3)

        v1_revenue = self.store.get_sync_history("v1", sync_type="revenue_pull")
        self.assertEqual(len(v1_revenue), 2)

    def test_get_sync_history_limit(self):
        for i in range(5):
            self.store.start_sync("v1", "revenue_pull")
        history = self.store.get_sync_history("v1", limit=3)
        self.assertEqual(len(history), 3)

    def test_get_last_sync(self):
        log1 = self.store.start_sync("v1", "revenue_pull")
        self.store.complete_sync(log1.id, records_synced=10)
        log2 = self.store.start_sync("v1", "revenue_pull")
        self.store.complete_sync(log2.id, error="failed")

        last = self.store.get_last_sync("v1")
        self.assertIsNotNone(last)
        self.assertEqual(last.status, "COMPLETED")

    def test_get_last_sync_none(self):
        self.assertIsNone(self.store.get_last_sync("v1"))


class TestXeroInvoiceSync(unittest.TestCase):
    """Tests for simulated Xero invoice pull."""

    def setUp(self):
        _reset_for_tests()
        self.store = get_xero_sync_store()

    def test_simulate_revenue_pull(self):
        invoices = [
            {
                "invoice_number": "INV-001",
                "date": "2026-04-20",
                "line_items": [
                    {"account_code": "200", "amount": 3000,
                     "tax_amount": 272.73, "description": "Food sales"},
                    {"account_code": "201", "amount": 2000,
                     "tax_amount": 181.82, "description": "Beverage sales"},
                ],
            },
            {
                "invoice_number": "INV-002",
                "date": "2026-04-21",
                "line_items": [
                    {"account_code": "200", "amount": 3500,
                     "tax_amount": 318.18, "description": "Food sales"},
                ],
            },
        ]
        result = self.store.simulate_xero_revenue_pull(
            "v1", "2026-04-20", "2026-04-21", invoices)
        self.assertEqual(result["status"], "COMPLETED")
        self.assertEqual(result["records_synced"], 3)

        # Verify records were created
        records = self.store.get_revenue("v1")
        self.assertEqual(len(records), 3)

        # Check categories mapped correctly
        food = self.store.get_revenue("v1", category="FOOD")
        self.assertEqual(len(food), 2)

    def test_sync_ignores_expense_accounts(self):
        """Expense account codes should not create revenue records."""
        invoices = [{
            "invoice_number": "INV-003",
            "date": "2026-04-20",
            "line_items": [
                {"account_code": "200", "amount": 1000, "tax_amount": 90.91},
                {"account_code": "400", "amount": 500, "tax_amount": 0},  # COGS
                {"account_code": "420", "amount": 300, "tax_amount": 0},  # Wages
            ],
        }]
        result = self.store.simulate_xero_revenue_pull(
            "v1", "2026-04-20", "2026-04-20", invoices)
        self.assertEqual(result["records_synced"], 1)  # only the revenue line

    def test_sync_creates_log(self):
        result = self.store.simulate_xero_revenue_pull(
            "v1", "2026-04-20", "2026-04-20", [])
        history = self.store.get_sync_history("v1")
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0].status, "COMPLETED")

    def test_custom_account_mapping(self):
        self.store.set_account_mapping("v1", {
            "300": "GAMING",
            "301": "EVENTS",
        })
        invoices = [{
            "invoice_number": "INV-004",
            "date": "2026-04-20",
            "line_items": [
                {"account_code": "300", "amount": 5000, "tax_amount": 454.55},
            ],
        }]
        self.store.simulate_xero_revenue_pull(
            "v1", "2026-04-20", "2026-04-20", invoices)
        gaming = self.store.get_revenue("v1", category="GAMING")
        self.assertEqual(len(gaming), 1)
        self.assertEqual(gaming[0].amount, 5000)


class TestPayrollJournal(unittest.TestCase):
    """Tests for Xero payroll journal entry building."""

    def setUp(self):
        _reset_for_tests()
        self.store = get_xero_sync_store()

    def test_build_payroll_journal(self):
        payroll = [
            {"employee_name": "Alice", "gross_pay": 2000,
             "super_amount": 230, "tax_withheld": 400},
            {"employee_name": "Bob", "gross_pay": 1500,
             "super_amount": 172.50, "tax_withheld": 300},
        ]
        journal = self.store.build_payroll_journal(
            "v1", "2026-04-14", "2026-04-20", payroll)

        self.assertEqual(journal["Date"], "2026-04-20")
        self.assertIn("RosterIQ Payroll", journal["Narration"])
        self.assertEqual(journal["Status"], "DRAFT")

        summary = journal["summary"]
        self.assertEqual(summary["total_wages"], 3500.0)
        self.assertEqual(summary["total_super"], 402.50)
        self.assertEqual(summary["total_tax_withheld"], 700.0)
        self.assertEqual(summary["net_pay"], 2800.0)
        self.assertEqual(summary["employee_count"], 2)

        # Check journal lines balance
        debits = sum(l["LineAmount"] for l in journal["JournalLines"]
                     if l["IsDebit"])
        credits = sum(l["LineAmount"] for l in journal["JournalLines"]
                      if not l["IsDebit"])
        self.assertAlmostEqual(debits, credits, places=2)

    def test_payroll_journal_empty(self):
        journal = self.store.build_payroll_journal(
            "v1", "2026-04-14", "2026-04-20", [])
        self.assertEqual(journal["summary"]["total_wages"], 0)
        self.assertEqual(journal["summary"]["employee_count"], 0)


class TestLabourCostPct(unittest.TestCase):
    """Tests for real labour cost % calculation from Xero data."""

    def setUp(self):
        _reset_for_tests()
        self.store = get_xero_sync_store()

    def test_labour_cost_excellent(self):
        self.store.add_revenue({"venue_id": "v1", "date": "2026-04-20",
                                "amount": 10000, "tax_amount": 909.09})
        result = self.store.get_real_labour_cost_pct(
            "v1", "2026-04-20", "2026-04-20", total_wages=2000)
        self.assertEqual(result["labour_cost_pct"], 20.0)
        self.assertEqual(result["status"], "EXCELLENT")
        self.assertEqual(result["source"], "xero_actuals")

    def test_labour_cost_good(self):
        self.store.add_revenue({"venue_id": "v1", "date": "2026-04-20",
                                "amount": 10000, "tax_amount": 909.09})
        result = self.store.get_real_labour_cost_pct(
            "v1", "2026-04-20", "2026-04-20", total_wages=2800)
        self.assertEqual(result["status"], "GOOD")

    def test_labour_cost_warning(self):
        self.store.add_revenue({"venue_id": "v1", "date": "2026-04-20",
                                "amount": 10000, "tax_amount": 909.09})
        result = self.store.get_real_labour_cost_pct(
            "v1", "2026-04-20", "2026-04-20", total_wages=3200)
        self.assertEqual(result["status"], "WARNING")

    def test_labour_cost_critical(self):
        self.store.add_revenue({"venue_id": "v1", "date": "2026-04-20",
                                "amount": 10000, "tax_amount": 909.09})
        result = self.store.get_real_labour_cost_pct(
            "v1", "2026-04-20", "2026-04-20", total_wages=4000)
        self.assertEqual(result["status"], "CRITICAL")

    def test_labour_cost_no_revenue(self):
        result = self.store.get_real_labour_cost_pct(
            "v1", "2026-04-20", "2026-04-20", total_wages=1000)
        self.assertEqual(result["labour_cost_pct"], 0)
        self.assertEqual(result["status"], "NO_DATA")


class TestAccountMapping(unittest.TestCase):
    """Tests for Xero account code mapping."""

    def setUp(self):
        _reset_for_tests()
        self.store = get_xero_sync_store()

    def test_default_mapping(self):
        mapping = self.store.get_account_mapping("v1")
        self.assertIn("200", mapping)
        self.assertEqual(mapping["200"], "FOOD")

    def test_custom_mapping(self):
        custom = {"500": "GAMING", "501": "EVENTS"}
        self.store.set_account_mapping("v1", custom)
        mapping = self.store.get_account_mapping("v1")
        self.assertEqual(mapping["500"], "GAMING")
        self.assertNotIn("200", mapping)  # custom replaces defaults

    def test_venue_isolation(self):
        self.store.set_account_mapping("v1", {"500": "GAMING"})
        v1 = self.store.get_account_mapping("v1")
        v2 = self.store.get_account_mapping("v2")
        self.assertIn("500", v1)
        self.assertNotIn("500", v2)  # v2 gets defaults


class TestStoreReset(unittest.TestCase):
    """Tests for store reset and singleton behaviour."""

    def test_reset_clears_data(self):
        _reset_for_tests()
        store = get_xero_sync_store()
        store.add_revenue({"venue_id": "v1", "date": "2026-04-20",
                           "amount": 1000})
        self.assertEqual(len(store.get_revenue("v1")), 1)

        _reset_for_tests()
        store = get_xero_sync_store()
        self.assertEqual(len(store.get_revenue("v1")), 0)

    def test_singleton(self):
        _reset_for_tests()
        s1 = get_xero_sync_store()
        s2 = get_xero_sync_store()
        self.assertIs(s1, s2)


if __name__ == "__main__":
    unittest.main()
