"""
Feedback — the human half of the self-improvement loop.

The event log records what the SYSTEM noticed (errors, slowness, failed
integrations). It cannot record what a person found confusing, wrong, or
missing. These two routes capture that, attached to the same correlation id
as the request they are complaining about, so a report lands next to the
stack trace instead of in a separate inbox:

    POST /api/feedback           "this is broken / this is confusing / idea"
    POST /api/ai/feedback        thumbs up|down on one AI answer
    GET  /api/feedback           what people said (manager: own venue,
                                 owner: platform-wide)

Both write through ``services.events`` — no new table, so everything shows up
in the Activity view and in /api/events/insights alongside the machine signal.
"""

import logging
from datetime import datetime, timedelta
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.middleware.tenant import enforce_venue_access
from rosteriq.services.events import record_event, fingerprint

logger = logging.getLogger(__name__)

router = APIRouter(tags=["feedback"])

MAX_TEXT = 2000


class FeedbackBody(BaseModel):
    """A person telling us something is wrong or missing."""
    kind: Literal["bug", "confusing", "idea", "praise"] = "bug"
    message: str = Field(..., min_length=3, max_length=MAX_TEXT)
    venue_id: Optional[str] = None
    page: Optional[str] = Field(None, max_length=200)
    # The reference shown on a 500 ("Reference: abc123"), if they are reporting one
    reference: Optional[str] = Field(None, max_length=64)


class AIFeedbackBody(BaseModel):
    """A verdict on one AI answer — the signal that says which replies to fix."""
    helpful: bool
    venue_id: Optional[str] = None
    question: Optional[str] = Field(None, max_length=MAX_TEXT)
    answer_preview: Optional[str] = Field(None, max_length=MAX_TEXT)
    reason: Optional[str] = Field(None, max_length=500)


def _venue_in_scope(user: UserContext, venue_id: Optional[str]) -> Optional[str]:
    """Feedback about a venue must come from someone who holds it. An unknown
    or foreign venue is dropped to None rather than refused — never lose a
    report over a metadata detail."""
    if not venue_id:
        return None
    try:
        enforce_venue_access(venue_id)
        return venue_id
    except HTTPException:
        return None


@router.post("/api/feedback")
async def submit_feedback(body: FeedbackBody, user: UserContext = Depends(get_current_user)):
    """Record a problem report. Always accepted (a report we refuse is a report
    we never see); the venue is attached only if the reporter holds it."""
    venue_id = _venue_in_scope(user, body.venue_id)
    text = body.message.strip()
    record_event(
        "audit", f"feedback.{body.kind}",
        venue_id=venue_id,
        resource_type="feedback",
        outcome="ok",
        details={
            "message": text[:MAX_TEXT],
            "page": body.page,
            # The reference the user was shown on a 500 ties this report to the
            # exact recorded error event.
            "reference": body.reference,
            "reporter_email": user.email,
            "reporter_role": user.role,
            # Group repeat reports of the same thing
            "fingerprint": fingerprint("feedback", body.kind, text[:200]),
        },
    )
    logger.info(f"feedback [{body.kind}] from {user.email}: {text[:120]}")
    return {
        "status": "received",
        "message": "Thanks — that's logged and we can see exactly what happened.",
    }


@router.post("/api/ai/feedback")
async def submit_ai_feedback(body: AIFeedbackBody, user: UserContext = Depends(get_current_user)):
    """Thumbs up/down on an AI answer. Recorded as an ``ai`` event so the
    satisfaction rate sits beside the model's latency and error rate in
    /api/events/insights."""
    venue_id = _venue_in_scope(user, body.venue_id)
    record_event(
        "ai", "ai.feedback_up" if body.helpful else "ai.feedback_down",
        venue_id=venue_id,
        resource_type="ai_answer",
        outcome="ok" if body.helpful else "failed",
        details={
            "helpful": body.helpful,
            "question": (body.question or "")[:MAX_TEXT] or None,
            "answer_preview": (body.answer_preview or "")[:500] or None,
            "reason": body.reason,
            "rater_role": user.role,
        },
    )
    return {"status": "recorded", "helpful": body.helpful}


@router.get("/api/feedback")
async def list_feedback(
    venue_id: Optional[str] = Query(None),
    days: int = Query(30, ge=1, le=365),
    limit: int = Query(50, ge=1, le=200),
    user: UserContext = Depends(get_current_user),
):
    """Read what people reported. Managers see their venue; owners may omit
    venue_id for everything (that is the product-feedback inbox)."""
    scope = (venue_id or "").strip() or None
    if scope is None and not user.is_owner:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="venue_id is required — only owners may read platform-wide feedback",
        )
    if scope:
        enforce_venue_access(scope)
    if user.role not in ("manager", "owner") and not user.is_owner:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Managers only")

    db = get_db()
    since = datetime.utcnow() - timedelta(days=days)
    rows = db.list_events(venue_id=scope, action_prefix="feedback.",
                          since=since, limit=limit) or []
    items = []
    for r in rows:
        d = r.get("details") or {}
        items.append({
            "kind": str(r.get("action") or "").replace("feedback.", "") or "bug",
            "message": d.get("message"),
            "page": d.get("page"),
            "reference": d.get("reference"),
            "from": d.get("reporter_email"),
            "role": d.get("reporter_role"),
            "venue_id": r.get("venue_id"),
            "at": r.get("created_at").isoformat() if hasattr(r.get("created_at"), "isoformat") else r.get("created_at"),
        })
    return {"venue_id": scope, "days": days, "count": len(items), "items": items}
