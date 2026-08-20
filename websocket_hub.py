"""
WebSocket support module for RosterIQ's real-time dashboard.

Enables live signal updates, variance changes, recommendations, and roster modifications
without requiring client-side polling. Manages persistent connections per venue and
broadcasts updates to all connected dashboards.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Callable, Optional

from fastapi import APIRouter, FastAPI, WebSocket, WebSocketDisconnect, status, Query
from pydantic import BaseModel
import jwt

logger = logging.getLogger(__name__)


# Message type definitions
class SignalUpdateMessage(BaseModel):
    """Broadcast when a new signal is received or an existing signal is updated."""

    type: str = "signal_update"
    category: str
    value: float
    description: str
    timestamp: str


class VarianceChangeMessage(BaseModel):
    """Broadcast when the overall variance score changes significantly."""

    type: str = "variance_change"
    old_variance: float
    new_variance: float
    change_percent: float
    timestamp: str


class RecommendationMessage(BaseModel):
    """Broadcast when the decision engine generates a new staffing recommendation."""

    type: str = "recommendation"
    recommendation_id: str
    venue_id: str
    description: str
    priority: str  # "high", "medium", "low"
    timestamp: str


class AlertMessage(BaseModel):
    """Broadcast when a threshold breach is detected."""

    type: str = "alert"
    alert_type: str
    severity: str  # "critical", "warning", "info"
    message: str
    timestamp: str


class RosterUpdateMessage(BaseModel):
    """Broadcast when the roster is modified."""

    type: str = "roster_update"
    roster_id: str
    change_type: str  # "created", "updated", "deleted"
    timestamp: str


class PulseMessage(BaseModel):
    """Periodic heartbeat with current state summary (sent every 60 seconds)."""

    type: str = "pulse"
    timestamp: str
    connected_dashboards: int
    active_signals: int
    current_variance: float
    pending_recommendations: int


class PingMessage(BaseModel):
    """Client keepalive ping message."""

    type: str = "ping"
    timestamp: str


class PongMessage(BaseModel):
    """Server keepalive pong response."""

    type: str = "pong"
    timestamp: str


class SubscribeMessage(BaseModel):
    """Client subscription request for specific signal categories."""

    type: str = "subscribe"
    categories: list[str]


# New event types for roster and shift changes
class RosterUpdatedMessage(BaseModel):
    """Broadcast when roster is modified."""

    type: str = "roster.updated"
    venue_id: str
    roster_id: str
    summary: str
    timestamp: str


class RosterPublishedMessage(BaseModel):
    """Broadcast when roster is published to staff."""

    type: str = "roster.published"
    venue_id: str
    roster_id: str
    week_start: str
    timestamp: str


class ShiftSwappedMessage(BaseModel):
    """Broadcast when shift swap is completed."""

    type: str = "shift.swapped"
    swap_id: str
    from_employee: str
    to_employee: str
    timestamp: str


class ShiftReminderMessage(BaseModel):
    """Broadcast shift reminder to employee."""

    type: str = "shift.reminder"
    employee_id: str
    shift_id: str
    shift_date: str
    shift_time: str
    starts_in_minutes: int
    timestamp: str


class AlertMessage(BaseModel):
    """Broadcast when compliance/variance alert fires."""

    type: str = "alert.new"
    venue_id: str
    alert_type: str
    severity: str  # "critical", "warning", "info"
    message: str
    timestamp: str


class HeadcountChangedMessage(BaseModel):
    """Broadcast live headcount update."""

    type: str = "headcount.changed"
    venue_id: str
    current: int
    required: int
    delta: int
    timestamp: str


class NotificationMessage(BaseModel):
    """Broadcast generic notification to user."""

    type: str = "notification.new"
    user_id: str
    title: str
    body: str
    action_url: Optional[str] = None
    timestamp: str


class ConnectionManager:
    """
    Manages active WebSocket connections per venue and user.

    Tracks all connected dashboards for each venue and provides methods to
    broadcast messages to specific venues, users, or all clients.
    Also manages heartbeat and stale connection cleanup.
    """

    def __init__(self):
        """Initialize the connection manager."""
        # venue_id -> set of WebSocket connections
        self.active_connections: dict[str, set[WebSocket]] = {}
        # user_id -> set of WebSocket connections
        self.user_connections: dict[str, set[WebSocket]] = {}
        # venue_id -> set of subscribed categories per connection
        self.subscriptions: dict[str, dict[WebSocket, set[str]]] = {}
        # websocket -> (venue_id, user_id, last_pong_time)
        self.connection_metadata: dict[WebSocket, dict[str, Any]] = {}
        logger.info("ConnectionManager initialized")

    async def connect(self, websocket: WebSocket, venue_id: str, user_id: Optional[str] = None) -> None:
        """
        Accept and register a new WebSocket connection for a venue and optional user.

        Args:
            websocket: The WebSocket connection to register.
            venue_id: The venue identifier for this connection.
            user_id: Optional user identifier (extracted from JWT).
        """
        await websocket.accept()
        if venue_id not in self.active_connections:
            self.active_connections[venue_id] = set()
            self.subscriptions[venue_id] = {}

        self.active_connections[venue_id].add(websocket)
        self.subscriptions[venue_id][websocket] = set()  # All categories by default

        # Track user connections if user_id is provided
        if user_id:
            if user_id not in self.user_connections:
                self.user_connections[user_id] = set()
            self.user_connections[user_id].add(websocket)

        # Store metadata
        self.connection_metadata[websocket] = {
            "venue_id": venue_id,
            "user_id": user_id,
            "last_pong_time": datetime.utcnow(),
        }

        logger.info(
            f"WebSocket connected for venue {venue_id} user {user_id or 'unknown'}. "
            f"Active connections: {self.get_venue_connection_count(venue_id)}"
        )

    async def disconnect(self, websocket: WebSocket) -> None:
        """
        Remove and clean up a WebSocket connection.

        Args:
            websocket: The WebSocket connection to remove.
        """
        metadata = self.connection_metadata.pop(websocket, {})
        venue_id = metadata.get("venue_id")
        user_id = metadata.get("user_id")

        if venue_id and venue_id in self.active_connections:
            self.active_connections[venue_id].discard(websocket)
            if venue_id in self.subscriptions:
                self.subscriptions[venue_id].pop(websocket, None)

            # Clean up empty venue entries
            if not self.active_connections[venue_id]:
                del self.active_connections[venue_id]
                del self.subscriptions[venue_id]
                logger.info(f"Removed all connections for venue {venue_id}")
            else:
                logger.info(
                    f"WebSocket disconnected for venue {venue_id}. "
                    f"Remaining connections: {self.get_venue_connection_count(venue_id)}"
                )

        # Remove from user connections
        if user_id and user_id in self.user_connections:
            self.user_connections[user_id].discard(websocket)
            if not self.user_connections[user_id]:
                del self.user_connections[user_id]

    def get_venue_connection_count(self, venue_id: str) -> int:
        """
        Get the number of active connections for a venue.

        Args:
            venue_id: The venue identifier.

        Returns:
            Number of active WebSocket connections for this venue.
        """
        return len(self.active_connections.get(venue_id, set()))

    def get_total_connection_count(self) -> int:
        """
        Get the total number of active connections across all venues.

        Returns:
            Total number of active WebSocket connections.
        """
        return sum(
            len(connections) for connections in self.active_connections.values()
        )

    async def broadcast_to_venue(self, venue_id: str, message: dict[str, Any]) -> None:
        """
        Broadcast a message to all connections for a specific venue.

        Args:
            venue_id: The venue identifier.
            message: The message dictionary to broadcast.
        """
        if venue_id not in self.active_connections:
            return

        disconnected = []
        message_json = json.dumps(message, default=str)

        for websocket in self.active_connections[venue_id]:
            try:
                await websocket.send_text(message_json)
            except Exception as e:
                logger.warning(f"Failed to send message to client: {e}")
                disconnected.append(websocket)

        # Clean up disconnected clients
        for websocket in disconnected:
            await self.disconnect(websocket)

    async def broadcast_to_user(self, user_id: str, message: dict[str, Any]) -> None:
        """
        Broadcast a message to all connections for a specific user.

        Args:
            user_id: The user identifier.
            message: The message dictionary to broadcast.
        """
        if user_id not in self.user_connections:
            return

        disconnected = []
        message_json = json.dumps(message, default=str)

        for websocket in self.user_connections[user_id]:
            try:
                await websocket.send_text(message_json)
            except Exception as e:
                logger.warning(f"Failed to send message to user {user_id}: {e}")
                disconnected.append(websocket)

        # Clean up disconnected clients
        for websocket in disconnected:
            await self.disconnect(websocket)

    async def broadcast_all(self, message: dict[str, Any]) -> None:
        """
        Broadcast a message to all connected clients across all venues.

        Args:
            message: The message dictionary to broadcast.
        """
        all_venues = list(self.active_connections.keys())
        for venue_id in all_venues:
            await self.broadcast_to_venue(venue_id, message)

    def record_pong(self, websocket: WebSocket) -> None:
        """
        Record that a client has responded to a ping (heartbeat).

        Args:
            websocket: The WebSocket connection.
        """
        if websocket in self.connection_metadata:
            self.connection_metadata[websocket]["last_pong_time"] = datetime.utcnow()

    def get_stale_connections(self, timeout_seconds: int = 90) -> list[WebSocket]:
        """
        Get list of connections that haven't ponged in timeout_seconds.

        Args:
            timeout_seconds: How long without pong before connection is stale (default 90s).

        Returns:
            List of stale WebSocket connections.
        """
        stale = []
        now = datetime.utcnow()
        for websocket, metadata in self.connection_metadata.items():
            last_pong = metadata.get("last_pong_time", datetime.utcnow())
            elapsed = (now - last_pong).total_seconds()
            if elapsed > timeout_seconds:
                stale.append(websocket)
        return stale

    async def set_subscription(
        self, websocket: WebSocket, venue_id: str, categories: list[str]
    ) -> None:
        """
        Update the subscription categories for a specific connection.

        Args:
            websocket: The WebSocket connection.
            venue_id: The venue identifier.
            categories: List of signal categories to subscribe to (empty = all).
        """
        if venue_id in self.subscriptions and websocket in self.subscriptions[venue_id]:
            if categories:
                self.subscriptions[venue_id][websocket] = set(categories)
            else:
                self.subscriptions[venue_id][websocket] = set()
            logger.info(
                f"Updated subscription for venue {venue_id}: {categories or 'all'}"
            )

    def should_deliver_to_connection(
        self, websocket: WebSocket, venue_id: str, message_category: Optional[str]
    ) -> bool:
        """
        Check if a message should be delivered to a specific connection.

        Args:
            websocket: The WebSocket connection.
            venue_id: The venue identifier.
            message_category: The signal category (None = send regardless).

        Returns:
            True if the message should be delivered, False otherwise.
        """
        if message_category is None:
            return True

        if venue_id not in self.subscriptions:
            return True

        subscribed = self.subscriptions[venue_id].get(websocket, set())
        if not subscribed:  # Empty set means subscribed to all
            return True

        return message_category in subscribed


class SignalWatcher:
    """
    Background task that periodically checks for signal changes.

    Monitors signal state and broadcasts updates when changes are detected.
    Runs as an asyncio background task with configurable check interval.
    """

    def __init__(
        self,
        manager: ConnectionManager,
        check_interval: int = 60,
        heartbeat_interval: int = 30,
    ):
        """
        Initialize the signal watcher.

        Args:
            manager: The ConnectionManager instance.
            check_interval: Seconds between signal state checks (default 60).
            heartbeat_interval: Seconds between heartbeat pulses (default 30).
        """
        self.manager = manager
        self.check_interval = check_interval
        self.heartbeat_interval = heartbeat_interval
        self.last_state: dict[str, Any] = {}
        self.is_running = False
        self.task: Optional[asyncio.Task] = None
        logger.info(
            f"SignalWatcher initialized: "
            f"check_interval={check_interval}s, heartbeat_interval={heartbeat_interval}s"
        )

    async def start(self) -> None:
        """Start the background watcher task."""
        if self.is_running:
            logger.warning("SignalWatcher is already running")
            return

        self.is_running = True
        self.task = asyncio.create_task(self._watch_loop())
        logger.info("SignalWatcher started")

    async def stop(self) -> None:
        """Stop the background watcher task."""
        if not self.is_running:
            return

        self.is_running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        logger.info("SignalWatcher stopped")

    async def _watch_loop(self) -> None:
        """Main watcher loop that checks for changes, sends heartbeats, and cleans up stale connections."""
        heartbeat_counter = 0

        try:
            while self.is_running:
                await asyncio.sleep(self.check_interval)

                if not self.is_running:
                    break

                heartbeat_counter += self.check_interval

                # Send heartbeat ping if interval elapsed
                if heartbeat_counter >= self.heartbeat_interval:
                    await self._send_ping()
                    await self._cleanup_stale_connections()
                    heartbeat_counter = 0

        except asyncio.CancelledError:
            logger.info("SignalWatcher loop cancelled")
        except Exception as e:
            logger.error(f"Error in SignalWatcher loop: {e}", exc_info=True)

    async def _send_ping(self) -> None:
        """Send a ping message to all connected venues (heartbeat every 30s)."""
        try:
            for venue_id in list(self.manager.active_connections.keys()):
                message = {
                    "type": "ping",
                    "timestamp": datetime.utcnow().isoformat(),
                }
                await self.manager.broadcast_to_venue(venue_id, message)
        except Exception as e:
            logger.error(f"Error sending ping: {e}")

    async def _cleanup_stale_connections(self) -> None:
        """Remove connections that haven't ponged in 90+ seconds."""
        try:
            stale = self.manager.get_stale_connections(timeout_seconds=90)
            for websocket in stale:
                logger.warning(f"Disconnecting stale connection (no pong in 90s)")
                try:
                    await websocket.close(code=status.WS_1000_NORMAL_CLOSURE)
                except Exception:
                    pass
                await self.manager.disconnect(websocket)
        except Exception as e:
            logger.error(f"Error cleaning up stale connections: {e}")

    async def _send_heartbeat(self) -> None:
        """Send a pulse message to all connected venues (deprecated, use _send_ping)."""
        try:
            for venue_id in list(self.manager.active_connections.keys()):
                message = {
                    "type": "pulse",
                    "timestamp": datetime.utcnow().isoformat(),
                    "connected_dashboards": self.manager.get_venue_connection_count(
                        venue_id
                    ),
                    "active_signals": 0,  # Would be populated from signal store
                    "current_variance": 0.0,  # Would be populated from metrics
                    "pending_recommendations": 0,  # Would be populated from DB
                }
                await self.manager.broadcast_to_venue(venue_id, message)
        except Exception as e:
            logger.error(f"Error sending heartbeat: {e}")

    async def check_and_broadcast_changes(
        self,
        venue_id: str,
        current_state: dict[str, Any],
        get_state_func: Callable[[], dict[str, Any]],
    ) -> None:
        """
        Compare current state to last-broadcast state and send updates.

        Args:
            venue_id: The venue identifier.
            current_state: The current state dictionary.
            get_state_func: Function to retrieve the latest state.
        """
        if venue_id not in self.last_state:
            self.last_state[venue_id] = {}

        latest_state = get_state_func()

        if latest_state != self.last_state[venue_id]:
            self.last_state[venue_id] = latest_state
            message = {
                "type": "state_update",
                "timestamp": datetime.utcnow().isoformat(),
                "state": latest_state,
            }
            await self.manager.broadcast_to_venue(venue_id, message)


# Global instances
_connection_manager = ConnectionManager()
_signal_watcher: Optional[SignalWatcher] = None


def get_connection_manager() -> ConnectionManager:
    """Get the global ConnectionManager instance."""
    return _connection_manager


def get_signal_watcher() -> SignalWatcher:
    """Get the global SignalWatcher instance."""
    global _signal_watcher
    if _signal_watcher is None:
        _signal_watcher = SignalWatcher(_connection_manager)
    return _signal_watcher


def _extract_user_from_token(token: Optional[str]) -> Optional[str]:
    """Extract user_id from JWT token."""
    if not token:
        return None
    try:
        from rosteriq.services.auth import JWT_SECRET, JWT_ALGORITHM
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload.get("sub")  # sub is user_id
    except Exception as e:
        logger.debug(f"Failed to decode JWT from WS connection: {e}")
        return None


def _may_watch_venue(token: Optional[str], venue_id: str):
    """(allowed, user_id) — may this token subscribe to this venue's live feed?

    A socket is a read stream of a venue's operations: roster changes, swaps,
    labour, alerts. HTTP routes go through TenantMiddleware, but a WebSocket
    upgrade does NOT — so the same check has to happen here explicitly, or
    every venue's live feed is readable by anyone holding any valid token.
    Owners pass; managers/staff must hold the venue. No token = no socket.
    """
    if not token:
        return False, None
    try:
        from rosteriq.services.auth import JWT_SECRET, JWT_ALGORITHM
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except Exception as e:
        logger.info(f"WS refused: undecodable token ({type(e).__name__})")
        return False, None

    user_id = payload.get("sub")
    role = payload.get("role")
    if role == "owner":
        return True, user_id

    # Read the venues off the user record, not the token: a token minted before
    # a join-code link would otherwise miss the venue the person now holds.
    venue_ids = []
    try:
        from rosteriq.database import get_db
        rec = get_db().get_user_by_id(user_id) if user_id else None
        if rec:
            if rec.get("role") == "owner" or rec.get("is_owner"):
                return True, user_id
            venue_ids = list(rec.get("venue_ids") or [])
    except Exception as e:  # noqa: BLE001 — fail CLOSED on a lookup failure
        logger.warning(f"WS venue lookup failed for {user_id}: {e}")
        return False, user_id

    if venue_id in venue_ids:
        return True, user_id

    logger.warning(f"WS refused: user {user_id} may not watch venue {venue_id}")
    try:
        from rosteriq.services.events import security
        security("access.denied", venue_id=venue_id, user_id=user_id,
                 transport="websocket", resource="live_feed")
    except Exception:
        pass
    return False, user_id


def create_websocket_router() -> APIRouter:
    """
    Create the WebSocket router with endpoints.

    Returns:
        FastAPI router configured with WebSocket endpoints.
    """
    router = APIRouter(prefix="/ws", tags=["websocket"])

    @router.websocket("/ws/{venue_id}")
    async def websocket_endpoint(websocket: WebSocket, venue_id: str, token: Optional[str] = Query(None)) -> None:
        """
        WebSocket endpoint for a venue's dashboard.

        On connect: sends full current state as initial payload.
        On message: supports "subscribe" (specific categories), "ping" (keepalive), "pong" (heartbeat response).
        On disconnect: cleans up.

        Args:
            websocket: The WebSocket connection.
            venue_id: The venue identifier.
            token: Optional JWT token passed as query parameter to identify user.
        """
        manager = get_connection_manager()
        allowed, user_id = _may_watch_venue(token, venue_id)
        if not allowed:
            # 4403: application-level "forbidden". Refuse BEFORE accepting, so
            # an unauthorised watcher never joins the venue's broadcast group.
            await websocket.close(code=4403, reason="Not authorised for this venue")
            return
        await manager.connect(websocket, venue_id, user_id)

        try:
            # Send initial state
            initial_message = {
                "type": "initial_state",
                "timestamp": datetime.utcnow().isoformat(),
                "venue_id": venue_id,
                "user_id": user_id,
                "connected_dashboards": manager.get_venue_connection_count(venue_id),
                "active_signals": 0,
                "current_variance": 0.0,
            }
            await websocket.send_json(initial_message)
            logger.info(f"Sent initial state to new connection for venue {venue_id} user {user_id or 'unknown'}")

            # Main message loop
            while True:
                data = await websocket.receive_text()

                try:
                    message = json.loads(data)
                    message_type = message.get("type", "")

                    if message_type == "ping":
                        # Respond to server ping with pong
                        pong = {
                            "type": "pong",
                            "timestamp": datetime.utcnow().isoformat(),
                        }
                        await websocket.send_json(pong)
                        logger.debug(f"Pong sent to venue {venue_id}")

                    elif message_type == "pong":
                        # Record that client responded to heartbeat
                        manager.record_pong(websocket)
                        logger.debug(f"Pong received from venue {venue_id}")

                    elif message_type == "subscribe":
                        # Update subscription categories
                        categories = message.get("categories", [])
                        await manager.set_subscription(websocket, venue_id, categories)
                        ack = {
                            "type": "subscribe_ack",
                            "timestamp": datetime.utcnow().isoformat(),
                            "categories": categories or "all",
                        }
                        await websocket.send_json(ack)
                        logger.info(
                            f"Subscription updated for venue {venue_id}: {categories}"
                        )

                    else:
                        logger.warning(f"Unknown message type: {message_type}")

                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON received: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        except WebSocketDisconnect:
            await manager.disconnect(websocket)
            logger.info(f"WebSocket disconnected for venue {venue_id}")

        except Exception as e:
            logger.error(f"WebSocket error for venue {venue_id}: {e}", exc_info=True)
            await manager.disconnect(websocket)

    return router


def setup_websocket(app: FastAPI) -> None:
    """
    Mount WebSocket routes onto an existing FastAPI app.

    Call this from api.py like: setup_websocket(app)

    Args:
        app: The FastAPI application instance.
    """
    router = create_websocket_router()
    app.include_router(router)
    logger.info("WebSocket routes mounted on FastAPI app")


async def initialize_websocket_background_tasks() -> None:
    """
    Initialize and start background tasks for the WebSocket system.

    Call this during FastAPI startup.
    """
    watcher = get_signal_watcher()
    await watcher.start()
    logger.info("WebSocket background tasks initialized")


async def shutdown_websocket_background_tasks() -> None:
    """
    Shut down background tasks for the WebSocket system.

    Call this during FastAPI shutdown.
    """
    watcher = get_signal_watcher()
    await watcher.stop()
    logger.info("WebSocket background tasks shut down")
