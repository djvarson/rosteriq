"""
Strawberry GraphQL subscription definitions for RosterIQ.

Provides real-time event streaming for roster updates, alerts, and revenue.
"""

from datetime import datetime
from typing import AsyncGenerator, Optional
import strawberry
import asyncio

from rosteriq.graphql_schema.types import (
    RosterType, AlertType, RevenueUpdateType, OptimisationProgressType,
    ConflictEventType, RosterStateEventType, BidEventType, ForecastEventType,
)


# Global event queues for subscriptions
_roster_update_queues: dict[str, asyncio.Queue] = {}
_alert_queues: dict[str, asyncio.Queue] = {}
_revenue_queues: dict[str, asyncio.Queue] = {}
_optimisation_queues: dict[str, asyncio.Queue] = {}
_conflict_queues: dict[str, asyncio.Queue] = {}
_state_change_queues: dict[str, asyncio.Queue] = {}
_bid_queues: dict[str, asyncio.Queue] = {}
_forecast_queues: dict[str, asyncio.Queue] = {}


def _get_or_create_queue(venue_id: str, queue_dict: dict) -> asyncio.Queue:
    """Get or create an event queue for a venue."""
    if venue_id not in queue_dict:
        queue_dict[venue_id] = asyncio.Queue()
    return queue_dict[venue_id]


async def _publish_roster_update(venue_id: str, roster_data: RosterType) -> None:
    """Publish a roster update event."""
    if venue_id in _roster_update_queues:
        queue = _roster_update_queues[venue_id]
        await queue.put(roster_data)


async def _publish_alert(venue_id: str, alert_data: AlertType) -> None:
    """Publish an alert event."""
    if venue_id in _alert_queues:
        queue = _alert_queues[venue_id]
        await queue.put(alert_data)


async def _publish_revenue_update(venue_id: str, revenue_data: RevenueUpdateType) -> None:
    """Publish a revenue update event."""
    if venue_id in _revenue_queues:
        queue = _revenue_queues[venue_id]
        await queue.put(revenue_data)


async def _publish_optimisation_progress(roster_id: str, progress_data: OptimisationProgressType) -> None:
    """Publish an optimisation progress event."""
    if roster_id in _optimisation_queues:
        queue = _optimisation_queues[roster_id]
        await queue.put(progress_data)


async def _publish_conflict_event(venue_id: str, conflict_data: ConflictEventType) -> None:
    """Publish a conflict detection event."""
    if venue_id in _conflict_queues:
        queue = _conflict_queues[venue_id]
        await queue.put(conflict_data)


async def _publish_state_change(venue_id: str, state_data: RosterStateEventType) -> None:
    """Publish a roster state change event."""
    if venue_id in _state_change_queues:
        queue = _state_change_queues[venue_id]
        await queue.put(state_data)


async def _publish_bid_event(venue_id: str, bid_data: BidEventType) -> None:
    """Publish a bid activity event."""
    if venue_id in _bid_queues:
        queue = _bid_queues[venue_id]
        await queue.put(bid_data)


async def _publish_forecast_event(venue_id: str, forecast_data: ForecastEventType) -> None:
    """Publish a forecast update event."""
    if venue_id in _forecast_queues:
        queue = _forecast_queues[venue_id]
        await queue.put(forecast_data)


@strawberry.type
class Subscription:
    """GraphQL Subscription root type."""

    @strawberry.subscription
    async def roster_updated(
        self,
        venue_id: str,
    ) -> AsyncGenerator[RosterType, None]:
        """
        Subscribe to roster update events for a venue.

        Args:
            venue_id: Venue ID to watch

        Yields:
            RosterType events when roster is updated
        """
        queue = _get_or_create_queue(venue_id, _roster_update_queues)

        try:
            while True:
                # Wait for events with timeout
                try:
                    roster_data = await asyncio.wait_for(queue.get(), timeout=60.0)
                    yield roster_data
                except asyncio.TimeoutError:
                    # Keep connection alive with periodic check
                    continue

        finally:
            # Cleanup queue if empty
            if venue_id in _roster_update_queues and queue.empty():
                del _roster_update_queues[venue_id]

    @strawberry.subscription
    async def alert_triggered(
        self,
        venue_id: str,
    ) -> AsyncGenerator[AlertType, None]:
        """
        Subscribe to alert events for a venue.

        Args:
            venue_id: Venue ID to watch

        Yields:
            AlertType events when alerts are triggered
        """
        queue = _get_or_create_queue(venue_id, _alert_queues)

        try:
            while True:
                try:
                    alert_data = await asyncio.wait_for(queue.get(), timeout=60.0)
                    yield alert_data
                except asyncio.TimeoutError:
                    continue

        finally:
            if venue_id in _alert_queues and queue.empty():
                del _alert_queues[venue_id]

    @strawberry.subscription
    async def revenue_updated(
        self,
        venue_id: str,
    ) -> AsyncGenerator[RevenueUpdateType, None]:
        """
        Subscribe to revenue update events from POS for a venue.

        Args:
            venue_id: Venue ID to watch

        Yields:
            RevenueUpdateType events when POS data arrives
        """
        queue = _get_or_create_queue(venue_id, _revenue_queues)

        try:
            while True:
                try:
                    revenue_data = await asyncio.wait_for(queue.get(), timeout=60.0)
                    yield revenue_data
                except asyncio.TimeoutError:
                    continue

        finally:
            if venue_id in _revenue_queues and queue.empty():
                del _revenue_queues[venue_id]

    @strawberry.subscription
    async def optimisation_progress(
        self,
        roster_id: str,
    ) -> AsyncGenerator[OptimisationProgressType, None]:
        """
        Subscribe to roster optimisation progress updates.

        Yields progress events during long-running roster generation,
        including percentage complete and current phase name.

        Args:
            roster_id: Roster ID being optimised

        Yields:
            OptimisationProgressType events with progress information
        """
        queue = _get_or_create_queue(roster_id, _optimisation_queues)

        try:
            while True:
                try:
                    progress_data = await asyncio.wait_for(queue.get(), timeout=60.0)
                    yield progress_data
                except asyncio.TimeoutError:
                    continue

        finally:
            if roster_id in _optimisation_queues and queue.empty():
                del _optimisation_queues[roster_id]

    @strawberry.subscription
    async def conflict_detected(
        self,
        venue_id: str,
    ) -> AsyncGenerator[ConflictEventType, None]:
        """
        Subscribe to conflict detection events for a venue.

        Args:
            venue_id: Venue ID to watch

        Yields:
            ConflictEventType events when conflicts are detected
        """
        queue = _get_or_create_queue(venue_id, _conflict_queues)

        try:
            while True:
                try:
                    conflict_data = await asyncio.wait_for(queue.get(), timeout=60.0)
                    yield conflict_data
                except asyncio.TimeoutError:
                    continue

        finally:
            if venue_id in _conflict_queues and queue.empty():
                del _conflict_queues[venue_id]

    @strawberry.subscription
    async def roster_state_changed(
        self,
        venue_id: str,
    ) -> AsyncGenerator[RosterStateEventType, None]:
        """
        Subscribe to roster state change events for a venue.

        Args:
            venue_id: Venue ID to watch

        Yields:
            RosterStateEventType events when roster state changes
        """
        queue = _get_or_create_queue(venue_id, _state_change_queues)

        try:
            while True:
                try:
                    state_data = await asyncio.wait_for(queue.get(), timeout=60.0)
                    yield state_data
                except asyncio.TimeoutError:
                    continue

        finally:
            if venue_id in _state_change_queues and queue.empty():
                del _state_change_queues[venue_id]

    @strawberry.subscription
    async def bid_activity(
        self,
        venue_id: str,
    ) -> AsyncGenerator[BidEventType, None]:
        """
        Subscribe to shift bid activity events for a venue.

        Args:
            venue_id: Venue ID to watch

        Yields:
            BidEventType events when bids are placed, withdrawn, accepted, or rejected
        """
        queue = _get_or_create_queue(venue_id, _bid_queues)

        try:
            while True:
                try:
                    bid_data = await asyncio.wait_for(queue.get(), timeout=60.0)
                    yield bid_data
                except asyncio.TimeoutError:
                    continue

        finally:
            if venue_id in _bid_queues and queue.empty():
                del _bid_queues[venue_id]

    @strawberry.subscription
    async def forecast_updated(
        self,
        venue_id: str,
    ) -> AsyncGenerator[ForecastEventType, None]:
        """
        Subscribe to forecast update events for a venue.

        Args:
            venue_id: Venue ID to watch

        Yields:
            ForecastEventType events when forecasts are generated or updated
        """
        queue = _get_or_create_queue(venue_id, _forecast_queues)

        try:
            while True:
                try:
                    forecast_data = await asyncio.wait_for(queue.get(), timeout=60.0)
                    yield forecast_data
                except asyncio.TimeoutError:
                    continue

        finally:
            if venue_id in _forecast_queues and queue.empty():
                del _forecast_queues[venue_id]


# Public API for publishing events from other parts of the application

async def publish_roster_update(venue_id: str, roster_data: RosterType) -> None:
    """Publish a roster update event to all subscribers."""
    await _publish_roster_update(venue_id, roster_data)


async def publish_alert(venue_id: str, alert_data: AlertType) -> None:
    """Publish an alert event to all subscribers."""
    await _publish_alert(venue_id, alert_data)


async def publish_revenue_update(venue_id: str, revenue_data: RevenueUpdateType) -> None:
    """Publish a revenue update event to all subscribers."""
    await _publish_revenue_update(venue_id, revenue_data)


async def publish_optimisation_progress(roster_id: str, progress_data: OptimisationProgressType) -> None:
    """Publish an optimisation progress event to all subscribers."""
    await _publish_optimisation_progress(roster_id, progress_data)


async def publish_conflict_event(venue_id: str, conflict_data: ConflictEventType) -> None:
    """Publish a conflict detection event to all subscribers."""
    await _publish_conflict_event(venue_id, conflict_data)


async def publish_state_change(venue_id: str, state_data: RosterStateEventType) -> None:
    """Publish a roster state change event to all subscribers."""
    await _publish_state_change(venue_id, state_data)


async def publish_bid_event(venue_id: str, bid_data: BidEventType) -> None:
    """Publish a bid activity event to all subscribers."""
    await _publish_bid_event(venue_id, bid_data)


async def publish_forecast_event(venue_id: str, forecast_data: ForecastEventType) -> None:
    """Publish a forecast update event to all subscribers."""
    await _publish_forecast_event(venue_id, forecast_data)
