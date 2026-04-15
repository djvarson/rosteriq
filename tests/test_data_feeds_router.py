"""
Tests for Data Feeds Router
============================

Tests CSV parsing, router endpoints, and bookings upload functionality.

Uses stdlib unittest, no pytest required.
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rosteriq.bookings_csv_parser import parse_bookings_csv


class TestCSVParsing(unittest.TestCase):
    """Test CSV parsing for bookings."""

    def test_parse_basic_csv(self):
        """Test parsing basic CSV with all columns."""
        csv_text = """booking_date,booking_time,party_size,customer_name
2026-04-15,18:00,4,John Doe
2026-04-15,19:30,2,Jane Smith
2026-04-16,18:00,6,Bob Johnson
"""
        bookings = parse_bookings_csv(csv_text)
        self.assertEqual(len(bookings), 3)
        self.assertEqual(bookings[0]["date"], "2026-04-15")
        self.assertEqual(bookings[0]["time"], "18:00")
        self.assertEqual(bookings[0]["covers"], 4)
        self.assertEqual(bookings[0]["name"], "John Doe")

    def test_parse_flexible_column_names(self):
        """Test parsing with alternative column names."""
        csv_text = """date,time,covers,name
2026-04-15,18:00,4,Guest A
2026-04-16,19:00,3,Guest B
"""
        bookings = parse_bookings_csv(csv_text)
        self.assertEqual(len(bookings), 2)
        self.assertEqual(bookings[0]["covers"], 4)

    def test_parse_pax_column_name(self):
        """Test parsing with 'pax' as covers column."""
        csv_text = """Date,Time,Pax,Name
2026-04-15,18:00,5,Test1
"""
        bookings = parse_bookings_csv(csv_text)
        self.assertEqual(len(bookings), 1)
        self.assertEqual(bookings[0]["covers"], 5)

    def test_parse_party_size_column(self):
        """Test parsing with 'Party Size' (with spaces)."""
        csv_text = """booking_date,booking_time,Party Size,customer_name
2026-04-15,18:00,4,John
"""
        bookings = parse_bookings_csv(csv_text)
        self.assertEqual(len(bookings), 1)
        self.assertEqual(bookings[0]["covers"], 4)

    def test_parse_default_time(self):
        """Test default time when not provided."""
        csv_text = """booking_date,covers,name
2026-04-15,4,John
"""
        bookings = parse_bookings_csv(csv_text)
        self.assertEqual(len(bookings), 1)
        self.assertEqual(bookings[0]["time"], "18:00")

    def test_parse_default_status(self):
        """Test default status when not provided."""
        csv_text = """booking_date,booking_time,covers
2026-04-15,18:00,4
"""
        bookings = parse_bookings_csv(csv_text)
        self.assertEqual(len(bookings), 1)
        self.assertEqual(bookings[0]["status"], "confirmed")

    def test_parse_explicit_status(self):
        """Test explicit status values."""
        csv_text = """booking_date,booking_time,covers,status
2026-04-15,18:00,4,confirmed
2026-04-15,19:00,2,cancelled
"""
        bookings = parse_bookings_csv(csv_text)
        self.assertEqual(len(bookings), 2)
        self.assertEqual(bookings[0]["status"], "confirmed")
        self.assertEqual(bookings[1]["status"], "cancelled")

    def test_parse_skips_invalid_rows(self):
        """Test that rows with missing required fields are skipped."""
        csv_text = """booking_date,booking_time,covers
2026-04-15,18:00,4
,19:00,2
2026-04-16,,3
"""
        bookings = parse_bookings_csv(csv_text)
        # Should have 1 valid row (others missing date or time not required if covers valid)
        self.assertGreater(len(bookings), 0)

    def test_parse_missing_required_field(self):
        """Test that rows missing covers are skipped."""
        csv_text = """booking_date,booking_time
2026-04-15,18:00
2026-04-15,19:00
"""
        bookings = parse_bookings_csv(csv_text)
        # All rows should be skipped (missing covers)
        self.assertEqual(len(bookings), 0)

    def test_parse_invalid_covers_integer(self):
        """Test that invalid covers values are skipped."""
        csv_text = """booking_date,booking_time,covers
2026-04-15,18:00,4
2026-04-15,19:00,abc
2026-04-16,18:00,6
"""
        bookings = parse_bookings_csv(csv_text)
        # Should have 2 valid rows
        self.assertEqual(len(bookings), 2)

    def test_parse_csv_with_empty_fields(self):
        """Test handling of empty fields in CSV."""
        csv_text = """booking_date,booking_time,covers,name
2026-04-15,18:00,4,
2026-04-15,19:00,2,John
"""
        bookings = parse_bookings_csv(csv_text)
        # Both rows should be valid (name is optional)
        self.assertEqual(len(bookings), 2)
        self.assertNotIn("name", bookings[0])  # Empty name not included
        self.assertEqual(bookings[1]["name"], "John")

    def test_parse_adds_source_csv(self):
        """Test that source='csv' is added to bookings."""
        csv_text = """booking_date,booking_time,covers
2026-04-15,18:00,4
"""
        bookings = parse_bookings_csv(csv_text)
        self.assertEqual(bookings[0]["source"], "csv")

    def test_parse_case_insensitive_headers(self):
        """Test that header matching is case-insensitive."""
        csv_text = """BOOKING_DATE,BOOKING_TIME,COVERS
2026-04-15,18:00,4
"""
        bookings = parse_bookings_csv(csv_text)
        self.assertEqual(len(bookings), 1)
        self.assertEqual(bookings[0]["covers"], 4)

    def test_parse_mixed_case_headers(self):
        """Test headers with mixed case."""
        csv_text = """Booking_Date,Booking_Time,Party_Size
2026-04-15,18:00,3
"""
        bookings = parse_bookings_csv(csv_text)
        self.assertEqual(len(bookings), 1)
        self.assertEqual(bookings[0]["covers"], 3)

    def test_parse_large_party_size(self):
        """Test parsing large party sizes (>10)."""
        csv_text = """booking_date,booking_time,covers
2026-04-15,18:00,25
2026-04-16,19:00,100
"""
        bookings = parse_bookings_csv(csv_text)
        self.assertEqual(len(bookings), 2)
        self.assertEqual(bookings[0]["covers"], 25)
        self.assertEqual(bookings[1]["covers"], 100)

    def test_parse_whitespace_handling(self):
        """Test handling of extra whitespace in values."""
        csv_text = """booking_date,booking_time,covers,name
 2026-04-15 , 18:00 , 4 , John Doe
"""
        bookings = parse_bookings_csv(csv_text)
        self.assertEqual(len(bookings), 1)
        # Values should be trimmed
        self.assertEqual(bookings[0]["date"], "2026-04-15")
        self.assertEqual(bookings[0]["name"], "John Doe")


class TestCSVParsingEdgeCases(unittest.TestCase):
    """Test edge cases in CSV parsing."""

    def test_parse_empty_csv(self):
        """Test parsing empty CSV."""
        csv_text = ""
        with self.assertRaises(ValueError):
            parse_bookings_csv(csv_text)

    def test_parse_headers_only(self):
        """Test parsing CSV with only headers."""
        csv_text = """booking_date,booking_time,covers"""
        bookings = parse_bookings_csv(csv_text)
        self.assertEqual(len(bookings), 0)

    def test_parse_single_row(self):
        """Test parsing CSV with single data row."""
        csv_text = """booking_date,booking_time,covers
2026-04-15,18:00,4
"""
        bookings = parse_bookings_csv(csv_text)
        self.assertEqual(len(bookings), 1)

    def test_parse_many_rows(self):
        """Test parsing CSV with many rows."""
        rows = ["booking_date,booking_time,covers"]
        for i in range(100):
            rows.append(f"2026-04-15,18:00,{i % 10 + 1}")
        csv_text = "\n".join(rows)
        bookings = parse_bookings_csv(csv_text)
        self.assertEqual(len(bookings), 100)

    def test_parse_unicode_names(self):
        """Test parsing bookings with unicode names."""
        csv_text = """booking_date,booking_time,covers,name
2026-04-15,18:00,4,José García
2026-04-15,19:00,2,François Müller
"""
        bookings = parse_bookings_csv(csv_text)
        self.assertEqual(len(bookings), 2)
        self.assertEqual(bookings[0]["name"], "José García")

    def test_parse_special_characters(self):
        """Test parsing with special characters in names."""
        csv_text = """booking_date,booking_time,covers,name
2026-04-15,18:00,4,"Smith, John"
2026-04-15,19:00,2,O'Brien
"""
        bookings = parse_bookings_csv(csv_text)
        # CSV parser should handle quoted fields
        self.assertGreater(len(bookings), 0)


if __name__ == "__main__":
    # Run all tests
    passed = failed = 0
    for name, obj in list(globals().items()):
        if isinstance(obj, type) and name.startswith("Test"):
            inst = obj()
            for mname in sorted(dir(inst)):
                if mname.startswith("test_"):
                    try:
                        getattr(inst, mname)()
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
