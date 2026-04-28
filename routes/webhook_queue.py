"""
REST API routes for webhook queue management, dead-letter handling, and circuit breaker control.

Endpoints:
- GET /api/webhooks/queue/status — queue depth, processing rate, oldest item
- GET /api/webhooks/queue/dead-letters — list dead letters with filters
- POST /api/webhooks/queue/replay/{delivery_id} — replay a dead letter
- POST /api/webhooks/queue/purge — purge old dead letters
- GET /api/webhooks/queue/circuits — circuit breaker status for all destinations
- POST /api/webhooks/queue/circuits/{url}/reset — manually reset a circuit breaker
"""

import logging
from typing import Optional
from urllib.parse import unquote

from fastapi import APIRouter, HTTPException, Query

from rosteriq.services.webhook_queue import get_webhook_queue
from rosteriq.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/webhooks/queue", tags=["webhook-queue"])


# ============================================================================
# Queue Status
# ============================================================================

@router.get("/status")
async def get_queue_status() -> dict:
    """
    Get webhook queue status.

    Returns queue depth, processing rate, and oldest pending item.
    """
    queue = get_webhook_queue()
    db = get_db()

    try:
        # Count pending deliveries
        now_iso = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
        pending = db.list_pending_retries(
            __import__("datetime").datetime.fromisoformat(now_iso.replace("Z", "+00:00"))
        )
        queue_depth = len(pending) if pending else 0

        # Get oldest pending
        oldest = None
        if pending:
            oldest = {
                "id": pending[0].get("id"),
                "url": pending[0].get("url"),
                "created_at": pending[0].get("created_at"),
                "next_retry_at": pending[0].get("next_retry_at"),
                "attempt": pending[0].get("attempt", 0),
            }

        return {
            "queue_depth": queue_depth,
            "processing": queue.processing,
            "poll_interval_seconds": queue.poll_interval,
            "oldest_pending": oldest,
        }

    except Exception as e:
        logger.error(f"Error getting queue status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get queue status")


# ============================================================================
# Dead Letter Queue
# ============================================================================

@router.get("/dead-letters")
async def list_dead_letters(
    venue_id: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
) -> dict:
    """
    List dead letter queue entries.

    Args:
        venue_id: Optional filter by venue ID
        limit: Max results (1-500, default 50)

    Returns:
        List of dead letter deliveries with metadata
    """
    queue = get_webhook_queue()

    try:
        dead_letters = await queue.list_dead_letters(venue_id=venue_id, limit=limit)

        return {
            "count": len(dead_letters),
            "limit": limit,
            "venue_id_filter": venue_id,
            "dead_letters": [
                {
                    "id": dl.get("id"),
                    "url": dl.get("url"),
                    "status": dl.get("status"),
                    "event_type": dl.get("event_type"),
                    "venue_id": dl.get("venue_id"),
                    "subscription_id": dl.get("subscription_id"),
                    "created_at": dl.get("created_at"),
                    "dead_lettered_at": dl.get("dead_lettered_at"),
                    "attempts": len(dl.get("attempts", [])),
                    "last_error": dl.get("error"),
                }
                for dl in dead_letters
            ],
        }

    except Exception as e:
        logger.error(f"Error listing dead letters: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to list dead letters")


@router.post("/replay/{delivery_id}")
async def replay_dead_letter(delivery_id: str) -> dict:
    """
    Replay a dead letter delivery for retry.

    Args:
        delivery_id: Dead letter delivery ID

    Returns:
        Replay status
    """
    queue = get_webhook_queue()

    try:
        success = await queue.replay_dead_letter(delivery_id)

        if not success:
            raise HTTPException(
                status_code=404,
                detail=f"Dead letter {delivery_id} not found or not in dead letter queue"
            )

        return {
            "message": f"Replayed dead letter {delivery_id}",
            "delivery_id": delivery_id,
            "status": "replay_scheduled",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error replaying dead letter: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to replay dead letter")


@router.post("/purge")
async def purge_old_dead_letters(
    older_than_days: int = Query(30, ge=1, le=365),
) -> dict:
    """
    Purge old dead letter entries.

    Args:
        older_than_days: Delete entries older than this many days (1-365, default 30)

    Returns:
        Count of entries deleted
    """
    queue = get_webhook_queue()

    try:
        count = await queue.purge_dead_letters(older_than_days=older_than_days)

        return {
            "message": f"Purged dead letter entries older than {older_than_days} days",
            "count_deleted": count,
        }

    except Exception as e:
        logger.error(f"Error purging dead letters: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to purge dead letters")


# ============================================================================
# Circuit Breaker Status & Control
# ============================================================================

@router.get("/circuits")
async def get_circuit_status(url: Optional[str] = Query(None)) -> dict:
    """
    Get circuit breaker status for all or specific URL.

    Args:
        url: Optional URL to filter for

    Returns:
        List of circuit breaker statuses
    """
    queue = get_webhook_queue()

    try:
        statuses = queue.get_circuit_status(url=url)

        return {
            "count": len(statuses),
            "url_filter": url,
            "circuits": statuses,
        }

    except Exception as e:
        logger.error(f"Error getting circuit status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get circuit status")


@router.post("/circuits/{url}/reset")
async def reset_circuit_breaker(url: str) -> dict:
    """
    Manually reset a circuit breaker for a URL.

    This closes the circuit and resets failure counts, allowing
    deliveries to resume immediately.

    Args:
        url: URL to reset circuit breaker for

    Returns:
        Reset status
    """
    queue = get_webhook_queue()

    # URL may come URL-encoded from the path parameter
    url = unquote(url)

    try:
        success = queue.reset_circuit(url)

        if not success:
            raise HTTPException(
                status_code=404,
                detail=f"No circuit breaker found for {url}"
            )

        status = queue.get_circuit_status(url=url)

        return {
            "message": f"Reset circuit breaker for {url}",
            "url": url,
            "circuit": status[0] if status else None,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resetting circuit: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to reset circuit breaker")
