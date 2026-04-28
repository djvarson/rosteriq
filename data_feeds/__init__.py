"""
RosterIQ External Data Feeds.

Adapters for ingesting external signals that improve demand forecasting:
- Weather (BOM / OpenWeatherMap)
- Events (sports, concerts, festivals, conferences)
- Foot traffic (Google Places Popular Times)
- Reservations (ResDiary, OpenTable, Now Book It)
- School holidays & calendar (AU state-specific terms, uni calendars)
- Sports schedules (AFL, NRL, Cricket, A-League, F1)
- Nearby venues & competition (Google Places, new openings, promos)
- Delivery platforms (UberEats, DoorDash order volumes)
- Tourism & transport (hotel occupancy, transit disruptions, cruise ships)
- Economic indicators (RBA consumer confidence, ABS unemployment)

All adapters produce normalised FeedSignal objects that plug directly
into the variance engine and ensemble forecaster.
"""

from rosteriq.data_feeds.base import (
    FeedSignal,
    DataFeedAdapter,
    FeedCategory,
    Location,
    SignalStrength,
)
from rosteriq.data_feeds.aggregator import SignalAggregator
from rosteriq.data_feeds.lightspeed import LightspeedAdapter
from rosteriq.data_feeds.kounta import KountaAdapter

__all__ = [
    "FeedSignal",
    "DataFeedAdapter",
    "FeedCategory",
    "Location",
    "SignalStrength",
    "SignalAggregator",
    "LightspeedAdapter",
    "KountaAdapter",
]
