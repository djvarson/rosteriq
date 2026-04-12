"""RosterIQ data feed integrations (POS systems and reservations)."""

from rosteriq.data_feeds.nowbookit import (
    NowBookItAdapter,
    NowBookItClient,
    NowBookItCredentials,
    NowBookItError,
    NowBookItAuthError,
    create_nowbookit_adapter,
    Reservation,
    ReservationSnapshot,
    ReservationAnalyser,
    BookingPattern,
)

__all__ = [
    "NowBookItAdapter",
    "NowBookItClient",
    "NowBookItCredentials",
    "NowBookItError",
    "NowBookItAuthError",
    "create_nowbookit_adapter",
    "Reservation",
    "ReservationSnapshot",
    "ReservationAnalyser",
    "BookingPattern",
]
