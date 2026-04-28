"""
POS data import and normalisation tool for RosterIQ.

Takes CSV exports from the three most common Australian hospitality POS systems
(H&L, Lightspeed, Square) and normalises them into RosterIQ's internal format
for demand forecasting.

Supported POS systems:
- H&L (Hospitality & Leisure) — dominant in AU pubs and clubs
- Lightspeed Restaurant (K Series) — popular in cafés and restaurants
- Square — common in smaller venues and food trucks

Usage:
    from rosteriq.pos_import import import_pos_data, detect_pos_system

    # Auto-detect and import
    records = import_pos_data("sales_export.csv")

    # Or specify the system
    records = import_pos_data("sales_export.csv", system="square")

    # Feed into the ensemble forecaster
    forecaster.add_historical_data(records)
"""

import csv
import io
from datetime import date, datetime, time
from pathlib import Path
from typing import Optional


# ============================================================================
# Normalised output format
# ============================================================================

# Each record is a dict with:
#   date: date object
#   hour: int (0-23)
#   covers: float (customer/transaction count for that hour)
#   revenue: float (total revenue for that hour, AUD)
#   items: int (total items sold that hour)
#   source: str (POS system name)


# ============================================================================
# Column mapping for each POS system
# ============================================================================

# H&L exports: "Date", "Time", "Covers", "Revenue", "Items Sold", "Table", "Server"
HL_COLUMN_MAP = {
    "date": ["Date", "date", "DATE", "Trans Date", "Transaction Date"],
    "time": ["Time", "time", "TIME", "Trans Time", "Transaction Time"],
    "covers": ["Covers", "covers", "COVERS", "Guests", "Pax", "Cover Count"],
    "revenue": ["Revenue", "revenue", "REVENUE", "Total", "Net Total",
                 "Gross Total", "Amount", "Net Amount", "Sales"],
    "items": ["Items Sold", "Items", "items", "Item Count", "Qty"],
}

# Lightspeed exports: "Date", "Time", "Receipt Number", "Total", "Covers", "Items"
LIGHTSPEED_COLUMN_MAP = {
    "date": ["Date", "date", "Sale Date", "Receipt Date", "Completed"],
    "time": ["Time", "time", "Sale Time", "Receipt Time", "Completed Time"],
    "covers": ["Covers", "covers", "Guests", "Pax", "Customer Count"],
    "revenue": ["Total", "total", "Receipt Total", "Sale Total", "Revenue",
                 "Gross Sales", "Net Sales", "Amount"],
    "items": ["Items", "items", "Line Items", "Item Count", "Quantity"],
}

# Square exports: "Date", "Time", "Transaction ID", "Gross Sales", "Net Sales", "Num Items"
SQUARE_COLUMN_MAP = {
    "date": ["Date", "date", "Transaction Date", "Payment Date"],
    "time": ["Time", "time", "Transaction Time", "Payment Time"],
    "covers": ["Covers", "covers", "Customers"],  # Square often lacks covers
    "revenue": ["Gross Sales", "Net Sales", "Total Collected", "Amount",
                 "Revenue", "Total", "Gross Amount"],
    "items": ["Num Items", "Item Qty", "Items", "Quantity Sold", "Line Item Count"],
}

# SwiftPOS exports (Australian POS system)
# "Date", "Time", "Terminal", "Department", "Items Sold", "Gross Sales", "Voids/Refunds"
SWIFTPOS_COLUMN_MAP = {
    "date": ["Date", "date", "Transaction Date", "Sale Date"],
    "time": ["Time", "time", "Transaction Time", "Sale Time"],
    "covers": ["Covers", "covers", "Customers", "Pax", "Party Size"],  # May not be present
    "revenue": ["Gross Sales", "Net Sales", "Total Sale", "Amount", "Sales Total",
                "Total", "Turnover"],
    "items": ["Items Sold", "Items", "Item Count", "Qty", "Product Count"],
    "department": ["Department", "Category", "Dept", "Section", "Menu Category"],
    "voids": ["Voids", "Refunds", "Voids/Refunds", "Cancellations", "Void Amount"],
}

# Lightspeed Restaurant (K Series) exports — CSV export with receipt details
# "Sale Date", "Sale Time", "Receipt Number", "Customer", "Receipt Total", "Items", "Category"
LIGHTSPEED_RESTAURANT_COLUMN_MAP = {
    "date": ["Sale Date", "Date", "Receipt Date", "Completed", "Transaction Date"],
    "time": ["Sale Time", "Time", "Receipt Time", "Transaction Time", "Completed Time"],
    "covers": ["Covers", "Guests", "Pax", "Customer Count", "Customers"],
    "revenue": ["Receipt Total", "Total", "Amount", "Gross Sales", "Net Sales", "Revenue"],
    "items": ["Items", "Line Items", "Item Count", "Quantity", "Num Items"],
    "category": ["Category", "Department", "Menu Category", "Section"],
}

# Kounta (K Series) exports — typically via direct API but can export CSV
# Similar to Lightspeed but with payment method tracking
KOUNTA_COLUMN_MAP = {
    "date": ["Date", "Sale Date", "Order Date", "Created Date", "Transaction Date"],
    "time": ["Time", "Sale Time", "Order Time", "Created Time", "Transaction Time"],
    "covers": ["Covers", "Guests", "Pax", "Customer Count"],
    "revenue": ["Total", "Order Total", "Amount", "Gross Sales", "Revenue", "Sale Total"],
    "items": ["Items", "Item Count", "Line Items", "Quantity", "Order Items"],
    "payment": ["Payment Method", "Payment Type", "Tender Type", "Payment"],
}

POS_SYSTEMS = {
    "hl": {"name": "H&L", "columns": HL_COLUMN_MAP},
    "lightspeed": {"name": "Lightspeed", "columns": LIGHTSPEED_COLUMN_MAP},
    "lightspeed_restaurant": {"name": "Lightspeed Restaurant", "columns": LIGHTSPEED_RESTAURANT_COLUMN_MAP},
    "square": {"name": "Square", "columns": SQUARE_COLUMN_MAP},
    "swiftpos": {"name": "SwiftPOS", "columns": SWIFTPOS_COLUMN_MAP},
    "kounta": {"name": "Kounta", "columns": KOUNTA_COLUMN_MAP},
}

# Fingerprint columns unique to each system (for auto-detection)
_DETECTION_FINGERPRINTS = {
    "hl": ["Server", "Table", "Venue", "Register"],
    "lightspeed": ["Receipt Number", "Receipt ID", "Sale ID", "Register Name"],
    "lightspeed_restaurant": ["Receipt Number", "Customer", "Menu Category", "Sale Date"],
    "square": ["Transaction ID", "Payment ID", "Card Brand", "Fee",
               "Gross Sales", "Net Sales"],
    "swiftpos": ["Terminal", "Department", "Voids/Refunds", "Menu Category"],
    "kounta": ["Order Date", "Payment Method", "Created Date", "Order Total"],
}


# ============================================================================
# Auto-detection
# ============================================================================

def detect_pos_system(filepath: str | Path) -> Optional[str]:
    """
    Auto-detect which POS system generated a CSV file.

    Reads the header row and matches column names against known fingerprints.

    Args:
        filepath: Path to the CSV file.

    Returns:
        POS system key ('hl', 'lightspeed', 'lightspeed_restaurant', 'square',
        'swiftpos', 'kounta') or None if unrecognised.
    """
    filepath = Path(filepath)
    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return None

    header_set = set(h.strip() for h in header)

    scores = {}
    for system, fingerprints in _DETECTION_FINGERPRINTS.items():
        score = sum(1 for fp in fingerprints if fp in header_set)
        scores[system] = score

    best = max(scores, key=scores.get)
    if scores[best] > 0:
        return best

    # Fallback: try column mapping match
    for system, config in POS_SYSTEMS.items():
        col_map = config["columns"]
        matches = 0
        for field, candidates in col_map.items():
            if any(c in header_set for c in candidates):
                matches += 1
        if matches >= 3:
            return system

    return None


# ============================================================================
# Column resolution
# ============================================================================

def _resolve_column(header: list[str], candidates: list[str]) -> Optional[int]:
    """Find the index of the first matching column name."""
    header_stripped = [h.strip() for h in header]
    for candidate in candidates:
        if candidate in header_stripped:
            return header_stripped.index(candidate)
    return None


def _parse_date(value: str) -> Optional[date]:
    """Parse a date string, trying common AU/US formats."""
    value = value.strip()
    formats = [
        "%d/%m/%Y",    # AU: 07/04/2026
        "%d-%m-%Y",    # AU: 07-04-2026
        "%Y-%m-%d",    # ISO: 2026-04-07
        "%m/%d/%Y",    # US: 04/07/2026
        "%d/%m/%y",    # AU short: 07/04/26
        "%Y/%m/%d",    # ISO slash
        "%d %b %Y",    # 07 Apr 2026
        "%d %B %Y",    # 07 April 2026
    ]
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue

    # Try ISO datetime
    try:
        return datetime.fromisoformat(value).date()
    except (ValueError, TypeError):
        pass

    return None


def _parse_time(value: str) -> Optional[int]:
    """Parse a time string and return the hour (0-23)."""
    value = value.strip()

    # Handle AM/PM
    formats = [
        "%H:%M:%S",    # 14:30:00
        "%H:%M",       # 14:30
        "%I:%M %p",    # 2:30 PM
        "%I:%M:%S %p", # 2:30:00 PM
        "%I:%M%p",     # 2:30PM
        "%H%M",        # 1430
    ]
    for fmt in formats:
        try:
            t = datetime.strptime(value, fmt)
            return t.hour
        except ValueError:
            continue

    # Try ISO datetime
    try:
        return datetime.fromisoformat(value).hour
    except (ValueError, TypeError):
        pass

    # Try just extracting hour from HH:MM
    try:
        parts = value.replace(".", ":").split(":")
        hour = int(parts[0])
        if 0 <= hour <= 23:
            return hour
    except (ValueError, IndexError):
        pass

    return None


def _parse_float(value: str) -> float:
    """Parse a float from a potentially formatted string ($1,234.56)."""
    if not value:
        return 0.0
    cleaned = value.strip().replace("$", "").replace(",", "").replace(" ", "")
    # Handle parentheses for negatives: (123.45) -> -123.45
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = "-" + cleaned[1:-1]
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _parse_int(value: str) -> int:
    """Parse an integer from a string."""
    try:
        return int(_parse_float(value))
    except (ValueError, OverflowError):
        return 0


# ============================================================================
# Core import function
# ============================================================================

def import_pos_data(
    filepath: str | Path,
    system: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> list[dict]:
    """
    Import and normalise POS transaction data from a CSV file.

    Reads the CSV, maps columns to our internal format, aggregates
    transactions by date+hour, and returns records ready for the
    ensemble forecaster.

    Args:
        filepath: Path to the CSV export file.
        system: POS system key ('hl', 'lightspeed', 'square').
                Auto-detected if not specified.
        date_from: Optional start date filter (inclusive).
        date_to: Optional end date filter (inclusive).

    Returns:
        List of dicts, each with: date, hour, covers, revenue, items, source.
        Aggregated by date+hour.

    Raises:
        ValueError: If the POS system can't be detected or the file is invalid.
        FileNotFoundError: If the file doesn't exist.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"POS export file not found: {filepath}")

    # Detect system if not specified
    if system is None:
        system = detect_pos_system(filepath)
        if system is None:
            raise ValueError(
                f"Could not auto-detect POS system from {filepath.name}. "
                f"Please specify system='hl', 'lightspeed', 'lightspeed_restaurant', "
                f"'square', 'swiftpos', or 'kounta'."
            )

    if system not in POS_SYSTEMS:
        raise ValueError(
            f"Unknown POS system '{system}'. "
            f"Supported: {', '.join(POS_SYSTEMS.keys())}"
        )

    config = POS_SYSTEMS[system]
    col_map = config["columns"]
    source_name = config["name"]

    # Read CSV
    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError("CSV file is empty")

        # Resolve column indices
        date_col = _resolve_column(header, col_map["date"])
        time_col = _resolve_column(header, col_map["time"])
        covers_col = _resolve_column(header, col_map["covers"])
        revenue_col = _resolve_column(header, col_map["revenue"])
        items_col = _resolve_column(header, col_map["items"])

        if date_col is None:
            raise ValueError(
                f"Could not find a date column in {filepath.name}. "
                f"Expected one of: {col_map['date']}"
            )

        # Aggregate by date+hour
        aggregated: dict[tuple[date, int], dict] = {}
        skipped = 0

        for row_num, row in enumerate(reader, start=2):
            if not row or all(c.strip() == "" for c in row):
                continue

            try:
                row_date = _parse_date(row[date_col])
            except IndexError:
                skipped += 1
                continue

            if row_date is None:
                skipped += 1
                continue

            # Apply date filters
            if date_from and row_date < date_from:
                continue
            if date_to and row_date > date_to:
                continue

            # Parse hour (default to 12 if no time column)
            hour = 12
            if time_col is not None:
                try:
                    parsed_hour = _parse_time(row[time_col])
                    if parsed_hour is not None:
                        hour = parsed_hour
                except IndexError:
                    pass

            # Parse values
            covers = 1.0  # Default 1 cover per transaction if not available
            if covers_col is not None:
                try:
                    parsed = _parse_float(row[covers_col])
                    if parsed > 0:
                        covers = parsed
                except IndexError:
                    pass

            revenue = 0.0
            if revenue_col is not None:
                try:
                    revenue = _parse_float(row[revenue_col])
                except IndexError:
                    pass

            items = 0
            if items_col is not None:
                try:
                    items = _parse_int(row[items_col])
                except IndexError:
                    pass

            # Aggregate
            key = (row_date, hour)
            if key not in aggregated:
                aggregated[key] = {
                    "date": row_date,
                    "hour": hour,
                    "covers": 0.0,
                    "revenue": 0.0,
                    "items": 0,
                    "source": source_name,
                    "transactions": 0,
                }

            aggregated[key]["covers"] += covers
            aggregated[key]["revenue"] += revenue
            aggregated[key]["items"] += items
            aggregated[key]["transactions"] += 1

    # Convert to sorted list
    records = sorted(aggregated.values(), key=lambda r: (r["date"], r["hour"]))

    # Remove the internal transactions counter
    for r in records:
        r.pop("transactions", None)

    return records


# ============================================================================
# Convenience: import from string (for testing / API use)
# ============================================================================

def import_pos_string(
    csv_content: str,
    system: str,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> list[dict]:
    """
    Import POS data from a CSV string (useful for testing or API endpoints).

    Args:
        csv_content: CSV data as a string.
        system: POS system key ('hl', 'lightspeed', 'square').
        date_from: Optional start date filter.
        date_to: Optional end date filter.

    Returns:
        Same format as import_pos_data.
    """
    import tempfile
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        tmp_path = f.name

    try:
        return import_pos_data(tmp_path, system=system,
                               date_from=date_from, date_to=date_to)
    finally:
        Path(tmp_path).unlink(missing_ok=True)


# ============================================================================
# Summary / validation
# ============================================================================

def summarise_import(records: list[dict]) -> dict:
    """
    Produce a summary of imported POS data.

    Useful for validating the import before feeding to the forecaster.

    Args:
        records: Output from import_pos_data.

    Returns:
        Dict with: total_records, date_range, total_covers, total_revenue,
        total_items, avg_covers_per_hour, busiest_day, busiest_hour.
    """
    if not records:
        return {
            "total_records": 0,
            "date_range": None,
            "total_covers": 0,
            "total_revenue": 0,
            "total_items": 0,
        }

    dates = [r["date"] for r in records]
    total_covers = sum(r["covers"] for r in records)
    total_revenue = sum(r["revenue"] for r in records)
    total_items = sum(r["items"] for r in records)

    # Busiest day
    from collections import defaultdict
    daily_covers = defaultdict(float)
    hourly_covers = defaultdict(float)
    for r in records:
        daily_covers[r["date"]] += r["covers"]
        hourly_covers[r["hour"]] += r["covers"]

    busiest_day = max(daily_covers, key=daily_covers.get) if daily_covers else None
    busiest_hour = max(hourly_covers, key=hourly_covers.get) if hourly_covers else None

    unique_days = len(set(dates))

    return {
        "total_records": len(records),
        "date_range": {"from": min(dates), "to": max(dates)},
        "unique_days": unique_days,
        "total_covers": round(total_covers, 1),
        "total_revenue": round(total_revenue, 2),
        "total_items": total_items,
        "avg_covers_per_hour": round(total_covers / len(records), 1) if records else 0,
        "busiest_day": busiest_day,
        "busiest_hour": busiest_hour,
        "source": records[0]["source"] if records else None,
    }


# ============================================================================
# CLI entry point
# ============================================================================

def main():
    """CLI entry point for POS data import."""
    import argparse
    import json

    parser = argparse.ArgumentParser(
        description="Import and normalise POS data for RosterIQ"
    )
    parser.add_argument("file", help="Path to POS CSV export")
    parser.add_argument(
        "--system", choices=["hl", "lightspeed", "lightspeed_restaurant", "square", "swiftpos", "kounta"],
        help="POS system (auto-detected if not specified)",
    )
    parser.add_argument("--from-date", help="Start date filter (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="End date filter (YYYY-MM-DD)")
    parser.add_argument(
        "--output", help="Output JSON file (prints to stdout if not specified)"
    )
    parser.add_argument(
        "--summary", action="store_true",
        help="Print import summary instead of full data",
    )

    args = parser.parse_args()

    date_from = date.fromisoformat(args.from_date) if args.from_date else None
    date_to = date.fromisoformat(args.to_date) if args.to_date else None

    records = import_pos_data(
        args.file, system=args.system,
        date_from=date_from, date_to=date_to,
    )

    if args.summary:
        result = summarise_import(records)
    else:
        result = records

    # Serialise dates for JSON
    def json_serial(obj):
        if isinstance(obj, date):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    output = json.dumps(result, indent=2, default=json_serial)

    if args.output:
        Path(args.output).write_text(output)
        print(f"Wrote {len(records)} records to {args.output}")
    else:
        print(output)

    summary = summarise_import(records)
    print(f"\n--- Import Summary ---")
    print(f"System: {summary.get('source', 'Unknown')}")
    print(f"Records: {summary['total_records']}")
    if summary.get("date_range"):
        print(f"Date range: {summary['date_range']['from']} to {summary['date_range']['to']}")
    print(f"Total covers: {summary['total_covers']}")
    print(f"Total revenue: ${summary['total_revenue']:,.2f}")
    print(f"Avg covers/hour: {summary.get('avg_covers_per_hour', 0)}")


if __name__ == "__main__":
    main()
