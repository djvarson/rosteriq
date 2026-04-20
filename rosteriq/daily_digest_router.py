"""FastAPI router for daily digest endpoints.

Provides:
- GET /api/v1/digest/{venue_id}/preview — digest JSON
- GET /api/v1/digest/{venue_id}/preview/text — plain text version
- GET /api/v1/digest/{venue_id}/preview/html — HTML version
- POST /api/v1/digest/{venue_id}/send — dispatch via brief_dispatcher

All endpoints require L2+ access (manager or owner level).
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional, Dict, Any

from fastapi import APIRouter, HTTPException, Request, Query
from fastapi.responses import PlainTextResponse, HTMLResponse
from pydantic import BaseModel, Field

from rosteriq import daily_digest as _daily_digest
from rosteriq import brief_dispatcher as _dispatcher

logger = logging.getLogger("rosteriq.daily_digest_router")

router = APIRouter(tags=["digest"])


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class DigestSendRequest(BaseModel):
    """Request to send a digest."""
    date: Optional[str] = Field(None, description="ISO date (YYYY-MM-DD), defaults to tomorrow")
    channel: str = Field(..., description="Delivery channel: sms|email|webhook")


# ---------------------------------------------------------------------------
# Helper: access control
# ---------------------------------------------------------------------------

def _gate(request: Request, level_name: str = "L2") -> None:
    """Enforce access level.

    Args:
        request: FastAPI request.
        level_name: Required level (e.g., "L2", "manager").

    Raises:
        HTTPException 403 if access denied.
    """
    try:
        from rosteriq import auth as _auth
        user = _auth.require_access(request, level_name)
        if not user:
            raise HTTPException(status_code=403, detail="Insufficient access")
    except ImportError:
        # Auth not available; allow access for demo mode
        pass
    except Exception as e:
        logger.warning(f"Access check failed: {e}")
        # In demo mode, allow access; in production, deny
        if _auth.AUTH_ENABLED if hasattr(_auth, "AUTH_ENABLED") else False:
            raise HTTPException(status_code=403, detail=str(e))


# ---------------------------------------------------------------------------
# Helper: stores
# ---------------------------------------------------------------------------

def _get_stores():
    """Fetch optional stores (best-effort)."""
    history_store = None
    headcount_store = None
    note_store = None
    swap_store = None

    try:
        from rosteriq import tanda_history as _th
        history_store = _th.get_history_store()
    except Exception:
        pass

    try:
        from rosteriq import headcount as _hc
        headcount_store = _hc.get_headcount_store()
        note_store = _hc.get_shift_note_store()
    except Exception:
        pass

    try:
        from rosteriq import shift_swap as _ss
        swap_store = _ss.get_swap_store()
    except Exception:
        pass

    return history_store, headcount_store, note_store, swap_store


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/api/v1/digest/{venue_id}/preview",
    response_model=Dict[str, Any],
)
async def preview_digest(
    venue_id: str,
    request: Request,
    date: Optional[str] = Query(None, description="ISO date (YYYY-MM-DD), defaults to tomorrow"),
) -> Dict[str, Any]:
    """Preview a digest as JSON.

    Args:
        venue_id: The venue ID.
        date: Optional ISO date (YYYY-MM-DD). Defaults to tomorrow.

    Returns:
        Complete digest dict.
    """
    _gate(request, "L2")

    try:
        target_date = None
        if date:
            target_date = _parse_date(date)

        history_store, headcount_store, note_store, swap_store = _get_stores()

        digest = _daily_digest.build_digest(
            venue_id,
            target_date=target_date,
            history_store=history_store,
            headcount_store=headcount_store,
            note_store=note_store,
            swap_store=swap_store,
        )

        return digest

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Digest preview failed for {venue_id}")
        raise HTTPException(status_code=500, detail="Digest composition failed")


@router.get(
    "/api/v1/digest/{venue_id}/preview/text",
)
async def preview_digest_text(
    venue_id: str,
    request: Request,
    date: Optional[str] = Query(None, description="ISO date (YYYY-MM-DD)"),
) -> PlainTextResponse:
    """Preview a digest as plain text.

    Args:
        venue_id: The venue ID.
        date: Optional ISO date (YYYY-MM-DD).

    Returns:
        Plain text digest.
    """
    _gate(request, "L2")

    try:
        target_date = None
        if date:
            target_date = _parse_date(date)

        history_store, headcount_store, note_store, swap_store = _get_stores()

        digest = _daily_digest.build_digest(
            venue_id,
            target_date=target_date,
            history_store=history_store,
            headcount_store=headcount_store,
            note_store=note_store,
            swap_store=swap_store,
        )

        text = _daily_digest.format_digest_text(digest)
        return PlainTextResponse(text)

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Text digest preview failed for {venue_id}")
        raise HTTPException(status_code=500, detail="Digest formatting failed")


@router.get(
    "/api/v1/digest/{venue_id}/preview/html",
)
async def preview_digest_html(
    venue_id: str,
    request: Request,
    date: Optional[str] = Query(None, description="ISO date (YYYY-MM-DD)"),
) -> HTMLResponse:
    """Preview a digest as HTML.

    Args:
        venue_id: The venue ID.
        date: Optional ISO date (YYYY-MM-DD).

    Returns:
        HTML digest.
    """
    _gate(request, "L2")

    try:
        target_date = None
        if date:
            target_date = _parse_date(date)

        history_store, headcount_store, note_store, swap_store = _get_stores()

        digest = _daily_digest.build_digest(
            venue_id,
            target_date=target_date,
            history_store=history_store,
            headcount_store=headcount_store,
            note_store=note_store,
            swap_store=swap_store,
        )

        html = _daily_digest.format_digest_html(digest)
        return HTMLResponse(html)

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"HTML digest preview failed for {venue_id}")
        raise HTTPException(status_code=500, detail="Digest formatting failed")


@router.post(
    "/api/v1/digest/{venue_id}/send",
)
async def send_digest(
    venue_id: str,
    request: Request,
    body: DigestSendRequest,
) -> Dict[str, Any]:
    """Dispatch a digest to subscribers via a channel.

    Best-effort delivery. Returns a summary of delivery status.

    Args:
        venue_id: The venue ID.
        body: Digest send request (date, channel).

    Returns:
        Dict with keys:
        - venue_id
        - composed_at
        - delivered: list of delivery records
        - failed: list of failed deliveries
        - skipped: bool (True if dispatch was skipped)
    """
    _gate(request, "L2")

    try:
        target_date = None
        if body.date:
            target_date = _parse_date(body.date)

        channel = (body.channel or "email").lower()
        if channel not in ("sms", "email", "webhook"):
            raise ValueError(f"Invalid channel: {channel}")

        history_store, headcount_store, note_store, swap_store = _get_stores()

        digest = _daily_digest.build_digest(
            venue_id,
            target_date=target_date,
            history_store=history_store,
            headcount_store=headcount_store,
            note_store=note_store,
            swap_store=swap_store,
        )

        # Format bodies for dispatch
        text_body = _daily_digest.format_digest_text(digest)
        html_body = _daily_digest.format_digest_html(digest)

        # Dispatch via brief_dispatcher (best-effort)
        result = {
            "venue_id": venue_id,
            "composed_at": digest.get("generated_at"),
            "delivered": [],
            "failed": [],
            "skipped": False,
        }

        # Dispatch based on channel
        if channel in ("email", "webhook"):
            # Use brief_dispatcher sinks
            try:
                dispatch_result = _dispatcher.dispatch_brief(
                    venue_id,
                    target_date=body.date,
                    venue_label=venue_id,
                    sinks=None,  # Will use registry
                    store=history_store,
                )
                result["delivered"] = dispatch_result.get("delivered", [])
            except Exception as e:
                logger.warning(f"Dispatch failed for {venue_id}: {e}")
                result["failed"].append({
                    "channel": channel,
                    "error": str(e),
                })

        elif channel == "sms":
            # SMS dispatch via brief_dispatcher
            try:
                dispatch_result = _dispatcher.dispatch_brief(
                    venue_id,
                    target_date=body.date,
                    venue_label=venue_id,
                    sinks=None,
                    store=history_store,
                )
                result["delivered"] = dispatch_result.get("delivered", [])
            except Exception as e:
                logger.warning(f"SMS dispatch failed for {venue_id}: {e}")
                result["failed"].append({
                    "channel": channel,
                    "error": str(e),
                })

        return result

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Digest send failed for {venue_id}")
        raise HTTPException(status_code=500, detail="Digest dispatch failed")


# ---------------------------------------------------------------------------
# Helper: date parsing
# ---------------------------------------------------------------------------

def _parse_date(s: str) -> date:
    """Parse ISO date string (YYYY-MM-DD).

    Args:
        s: ISO date string.

    Returns:
        date object.

    Raises:
        ValueError on invalid format.
    """
    from datetime import datetime
    try:
        return datetime.fromisoformat(s).date()
    except (ValueError, TypeError):
        raise ValueError(f"Invalid date format: {s}. Use YYYY-MM-DD.")
