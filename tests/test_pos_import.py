"""Tests for POS data import and normalisation."""

import pytest
import tempfile
from datetime import date
from pathlib import Path

from rosteriq.pos_import import (
    detect_pos_system, import_pos_data, import_pos_string,
    summarise_import, _parse_date, _parse_time, _parse_float,
)


# ============================================================================
# Sample CSV data for each POS system
# ============================================================================

SAMPLE_HL_CSV = """Date,Time,Covers,Revenue,Items Sold,Table,Server
07/04/2026,09:30,4,$125.50,12,T1,Alice
07/04/2026,09:45,2,$67.80,6,T3,Bob
07/04/2026,12:15,6,$234.00,18,T2,Alice
07/04/2026,12:30,3,$145.20,9,T5,Carol
07/04/2026,18:00,8,$456.00,24,T1,Alice
07/04/2026,19:15,4,$198.50,15,T4,Bob
08/04/2026,10:00,2,$89.00,8,T1,Alice
08/04/2026,12:00,5,$210.00,14,T2,Carol
"""

SAMPLE_LIGHTSPEED_CSV = """Date,Time,Receipt Number,Total,Covers,Items
07/04/2026,09:30,R001,125.50,4,12
07/04/2026,12:15,R002,234.00,6,18
07/04/2026,18:00,R003,456.00,8,24
08/04/2026,10:00,R004,89.00,2,8
"""

SAMPLE_SQUARE_CSV = """Date,Time,Transaction ID,Gross Sales,Net Sales,Num Items,Card Brand,Fee
07/04/2026,09:30,TXN001,$125.50,$121.73,12,Visa,$3.77
07/04/2026,12:15,TXN002,$234.00,$226.98,18,Mastercard,$7.02
07/04/2026,18:00,TXN003,$456.00,$442.32,24,Visa,$13.68
08/04/2026,10:00,TXN004,$89.00,$86.33,8,Eftpos,$2.67
"""


# ============================================================================
# Helper to write CSV to temp file
# ============================================================================

def write_temp_csv(content: str) -> Path:
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    f.write(content)
    f.close()
    return Path(f.name)


# ============================================================================
# Date parsing tests
# ============================================================================

class TestParseDate:
    def test_au_format(self):
        assert _parse_date("07/04/2026") == date(2026, 4, 7)

    def test_iso_format(self):
        assert _parse_date("2026-04-07") == date(2026, 4, 7)

    def test_au_dash_format(self):
        assert _parse_date("07-04-2026") == date(2026, 4, 7)

    def test_short_year(self):
        assert _parse_date("07/04/26") == date(2026, 4, 7)

    def test_named_month(self):
        assert _parse_date("07 Apr 2026") == date(2026, 4, 7)

    def test_invalid(self):
        assert _parse_date("not a date") is None

    def test_empty(self):
        assert _parse_date("") is None


# ============================================================================
# Time parsing tests
# ============================================================================

class TestParseTime:
    def test_24h_format(self):
        assert _parse_time("14:30") == 14

    def test_24h_with_seconds(self):
        assert _parse_time("14:30:00") == 14

    def test_12h_pm(self):
        assert _parse_time("2:30 PM") == 14

    def test_12h_am(self):
        assert _parse_time("9:30 AM") == 9

    def test_midnight(self):
        assert _parse_time("00:00") == 0

    def test_invalid(self):
        assert _parse_time("not a time") is None


# ============================================================================
# Float parsing tests
# ============================================================================

class TestParseFloat:
    def test_plain_number(self):
        assert _parse_float("123.45") == 123.45

    def test_dollar_sign(self):
        assert _parse_float("$123.45") == 123.45

    def test_commas(self):
        assert _parse_float("$1,234.56") == 1234.56

    def test_negative_parentheses(self):
        assert _parse_float("($50.00)") == -50.0

    def test_empty(self):
        assert _parse_float("") == 0.0

    def test_spaces(self):
        assert _parse_float("  $45.00  ") == 45.0


# ============================================================================
# POS system detection tests
# ============================================================================

class TestDetectPosSystem:
    def test_detect_hl(self):
        path = write_temp_csv(SAMPLE_HL_CSV)
        try:
            assert detect_pos_system(path) == "hl"
        finally:
            path.unlink()

    def test_detect_lightspeed(self):
        path = write_temp_csv(SAMPLE_LIGHTSPEED_CSV)
        try:
            assert detect_pos_system(path) == "lightspeed"
        finally:
            path.unlink()

    def test_detect_square(self):
        path = write_temp_csv(SAMPLE_SQUARE_CSV)
        try:
            assert detect_pos_system(path) == "square"
        finally:
            path.unlink()

    def test_empty_file(self):
        path = write_temp_csv("")
        try:
            assert detect_pos_system(path) is None
        finally:
            path.unlink()

    def test_unknown_system(self):
        path = write_temp_csv("Foo,Bar,Baz\n1,2,3\n")
        try:
            result = detect_pos_system(path)
            # May return None or a best-guess
            assert result is None or result in ("hl", "lightspeed", "square")
        finally:
            path.unlink()


# ============================================================================
# H&L import tests
# ============================================================================

class TestHLImport:
    def test_basic_import(self):
        path = write_temp_csv(SAMPLE_HL_CSV)
        try:
            records = import_pos_data(path, system="hl")
            assert len(records) > 0
            assert all("date" in r for r in records)
            assert all("hour" in r for r in records)
            assert all("covers" in r for r in records)
        finally:
            path.unlink()

    def test_aggregates_by_hour(self):
        path = write_temp_csv(SAMPLE_HL_CSV)
        try:
            records = import_pos_data(path, system="hl")
            # 07/04 has transactions at hours 9, 12, 18, 19
            # 08/04 has transactions at hours 10, 12
            apr7_records = [r for r in records if r["date"] == date(2026, 4, 7)]
            assert len(apr7_records) == 4  # 4 distinct hours
        finally:
            path.unlink()

    def test_covers_summed(self):
        path = write_temp_csv(SAMPLE_HL_CSV)
        try:
            records = import_pos_data(path, system="hl")
            # Hour 9 on Apr 7: 4 + 2 = 6 covers
            hour9 = [r for r in records if r["date"] == date(2026, 4, 7) and r["hour"] == 9]
            assert len(hour9) == 1
            assert hour9[0]["covers"] == 6.0
        finally:
            path.unlink()

    def test_revenue_summed(self):
        path = write_temp_csv(SAMPLE_HL_CSV)
        try:
            records = import_pos_data(path, system="hl")
            hour9 = [r for r in records if r["date"] == date(2026, 4, 7) and r["hour"] == 9]
            assert hour9[0]["revenue"] == pytest.approx(193.30)  # 125.50 + 67.80
        finally:
            path.unlink()

    def test_source_tag(self):
        path = write_temp_csv(SAMPLE_HL_CSV)
        try:
            records = import_pos_data(path, system="hl")
            assert all(r["source"] == "H&L" for r in records)
        finally:
            path.unlink()

    def test_sorted_output(self):
        path = write_temp_csv(SAMPLE_HL_CSV)
        try:
            records = import_pos_data(path, system="hl")
            for i in range(1, len(records)):
                assert (records[i]["date"], records[i]["hour"]) >= \
                       (records[i-1]["date"], records[i-1]["hour"])
        finally:
            path.unlink()


# ============================================================================
# Lightspeed import tests
# ============================================================================

class TestLightspeedImport:
    def test_basic_import(self):
        path = write_temp_csv(SAMPLE_LIGHTSPEED_CSV)
        try:
            records = import_pos_data(path, system="lightspeed")
            assert len(records) > 0
            assert all(r["source"] == "Lightspeed" for r in records)
        finally:
            path.unlink()


# ============================================================================
# Square import tests
# ============================================================================

class TestSquareImport:
    def test_basic_import(self):
        path = write_temp_csv(SAMPLE_SQUARE_CSV)
        try:
            records = import_pos_data(path, system="square")
            assert len(records) > 0
            assert all(r["source"] == "Square" for r in records)
        finally:
            path.unlink()

    def test_dollar_signs_handled(self):
        path = write_temp_csv(SAMPLE_SQUARE_CSV)
        try:
            records = import_pos_data(path, system="square")
            assert all(r["revenue"] > 0 for r in records)
        finally:
            path.unlink()


# ============================================================================
# Auto-detection import tests
# ============================================================================

class TestAutoDetectImport:
    def test_hl_auto_detected(self):
        path = write_temp_csv(SAMPLE_HL_CSV)
        try:
            records = import_pos_data(path)  # No system specified
            assert all(r["source"] == "H&L" for r in records)
        finally:
            path.unlink()

    def test_square_auto_detected(self):
        path = write_temp_csv(SAMPLE_SQUARE_CSV)
        try:
            records = import_pos_data(path)
            assert all(r["source"] == "Square" for r in records)
        finally:
            path.unlink()


# ============================================================================
# Date filter tests
# ============================================================================

class TestDateFilters:
    def test_from_date_filter(self):
        path = write_temp_csv(SAMPLE_HL_CSV)
        try:
            records = import_pos_data(path, system="hl", date_from=date(2026, 4, 8))
            assert all(r["date"] >= date(2026, 4, 8) for r in records)
        finally:
            path.unlink()

    def test_to_date_filter(self):
        path = write_temp_csv(SAMPLE_HL_CSV)
        try:
            records = import_pos_data(path, system="hl", date_to=date(2026, 4, 7))
            assert all(r["date"] <= date(2026, 4, 7) for r in records)
        finally:
            path.unlink()

    def test_both_filters(self):
        path = write_temp_csv(SAMPLE_HL_CSV)
        try:
            records = import_pos_data(
                path, system="hl",
                date_from=date(2026, 4, 7), date_to=date(2026, 4, 7),
            )
            assert all(r["date"] == date(2026, 4, 7) for r in records)
        finally:
            path.unlink()


# ============================================================================
# Error handling tests
# ============================================================================

class TestErrorHandling:
    def test_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            import_pos_data("/nonexistent/file.csv")

    def test_unknown_system(self):
        path = write_temp_csv("Foo,Bar\n1,2\n")
        try:
            with pytest.raises(ValueError, match="Could not auto-detect"):
                import_pos_data(path)
        finally:
            path.unlink()

    def test_invalid_system_name(self):
        path = write_temp_csv(SAMPLE_HL_CSV)
        try:
            with pytest.raises(ValueError, match="Unknown POS system"):
                import_pos_data(path, system="invalid")
        finally:
            path.unlink()

    def test_empty_csv(self):
        path = write_temp_csv("")
        try:
            with pytest.raises(ValueError):
                import_pos_data(path, system="hl")
        finally:
            path.unlink()


# ============================================================================
# String import tests
# ============================================================================

class TestImportPosString:
    def test_basic(self):
        records = import_pos_string(SAMPLE_HL_CSV, system="hl")
        assert len(records) > 0

    def test_with_filters(self):
        records = import_pos_string(
            SAMPLE_HL_CSV, system="hl",
            date_from=date(2026, 4, 8),
        )
        assert all(r["date"] >= date(2026, 4, 8) for r in records)


# ============================================================================
# Summary tests
# ============================================================================

class TestSummariseImport:
    def test_empty_records(self):
        summary = summarise_import([])
        assert summary["total_records"] == 0

    def test_basic_summary(self):
        records = import_pos_string(SAMPLE_HL_CSV, system="hl")
        summary = summarise_import(records)
        assert summary["total_records"] > 0
        assert summary["total_covers"] > 0
        assert summary["total_revenue"] > 0
        assert summary["source"] == "H&L"

    def test_date_range(self):
        records = import_pos_string(SAMPLE_HL_CSV, system="hl")
        summary = summarise_import(records)
        assert summary["date_range"]["from"] == date(2026, 4, 7)
        assert summary["date_range"]["to"] == date(2026, 4, 8)

    def test_busiest_day(self):
        records = import_pos_string(SAMPLE_HL_CSV, system="hl")
        summary = summarise_import(records)
        # Apr 7 has more transactions than Apr 8
        assert summary["busiest_day"] == date(2026, 4, 7)
