"""
Bookings CSV Parser
===================

Parses CSV files with flexible column naming for booking data.

Column mapping (case-insensitive):
  - booking_date, date, Date → normalized to "date"
  - booking_time, time, Time → normalized to "time"
  - party_size, covers, pax, Party Size → normalized to "covers"
  - customer_name, name, Name → normalized to "name"
  - status → normalized to "status"

Default values:
  - time: 18:00 (if not provided)
  - status: confirmed (if not provided)
  - source: csv (always set)
"""

import csv
import io
import logging

logger = logging.getLogger("rosteriq.bookings_csv_parser")


def parse_bookings_csv(csv_text: str) -> list[dict]:
    """
    Parse a bookings CSV with flexible column naming.

    Args:
        csv_text: CSV content as string

    Returns:
        List of booking dicts with normalized keys:
        {
            "date": "YYYY-MM-DD",
            "time": "HH:MM",
            "covers": int,
            "name": str (optional),
            "status": str (confirmed|cancelled|no-show),
            "source": "csv",
        }

    Raises:
        ValueError: If CSV is invalid or missing headers
    """
    reader = csv.DictReader(io.StringIO(csv_text))
    if not reader or not reader.fieldnames:
        raise ValueError("CSV must have headers")

    # Normalize header names
    normalized_headers = {}
    for original in reader.fieldnames:
        lower = original.lower().strip()
        if "date" in lower:
            normalized_headers[original] = "date"
        elif "time" in lower:
            normalized_headers[original] = "time"
        elif any(x in lower for x in ["covers", "pax", "party"]):
            normalized_headers[original] = "covers"
        elif "name" in lower:
            normalized_headers[original] = "name"
        elif "status" in lower:
            normalized_headers[original] = "status"

    bookings = []
    for i, row in enumerate(reader, start=2):  # Row 2 onwards (headers are row 1)
        try:
            booking = {}
            for original, value in row.items():
                if not value or not value.strip():
                    continue
                normalized_key = normalized_headers.get(original)
                if normalized_key:
                    booking[normalized_key] = value.strip()

            # Validate required fields
            if "date" not in booking or "covers" not in booking:
                logger.warning(f"Row {i} missing required fields (date, covers); skipping")
                continue

            # Normalize covers to int
            try:
                booking["covers"] = int(booking["covers"])
            except ValueError:
                logger.warning(f"Row {i} invalid covers value; skipping")
                continue

            # Default time and status
            if "time" not in booking:
                booking["time"] = "18:00"
            if "status" not in booking:
                booking["status"] = "confirmed"

            booking["source"] = "csv"
            bookings.append(booking)

        except Exception as e:
            logger.warning(f"Error parsing row {i}: {e}")
            continue

    return bookings
