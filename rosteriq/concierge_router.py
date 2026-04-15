"""REST endpoints for the front-desk concierge AI (Round 9).

Endpoints:
  POST /api/v1/concierge/ask
    body: {venue_id, query}
    Public-ish (no auth in demo) so it can be embedded on a venue
    landing page or a guest-facing kiosk. In production we recommend
    rate-limiting upstream.

  POST /api/v1/concierge/{venue_id}/live-context
    L1+ — staff sets live trading context (open_time, specials, weather note).

  POST /api/v1/concierge/{venue_id}/faqs
    L2+ — managers add custom FAQs to the venue KB.

  GET  /api/v1/concierge/{venue_id}/kb
    L1+ — inspect the current KB.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Request

from rosteriq.concierge import (
    ConciergeAgent,
    FAQEntry,
    get_kb,
)

# Auth gating — fall back to no-op in demo/sandbox when auth stack unavailable
try:
    from rosteriq.auth import require_access, AccessLevel  # type: ignore
except Exception:  # pragma: no cover — demo/sandbox path
    require_access = None  # type: ignore
    AccessLevel = None  # type: ignore


async def _gate(request: Request, level_name: str) -> None:
    if require_access is None or AccessLevel is None:
        return
    level = getattr(AccessLevel, level_name)
    await require_access(level)(request=request)


logger = logging.getLogger("rosteriq.concierge_router")
router = APIRouter(tags=["concierge"])


@router.post("/api/v1/concierge/ask")
async def ask(request: Request) -> dict:
    """Answer a guest question. Public-ish — no auth in demo mode."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid JSON body")
    venue_id = body.get("venue_id")
    query = body.get("query") or ""
    if not venue_id:
        raise HTTPException(status_code=400, detail="venue_id is required")
    agent = ConciergeAgent()
    reply = agent.answer(venue_id, query)
    return reply.to_dict()


@router.post("/api/v1/concierge/{venue_id}/live-context")
async def set_live_context(request: Request, venue_id: str) -> dict:
    """Update the live context (open_time, specials, weather note, etc.)."""
    await _gate(request, "L1_SUPERVISOR")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid JSON body")
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="body must be an object")
    get_kb().upsert_live_context(venue_id, body)
    kb = get_kb().ensure(venue_id)
    return {"venue_id": venue_id, "live_context": dict(kb.live_context)}


@router.post("/api/v1/concierge/{venue_id}/faqs")
async def add_faqs(request: Request, venue_id: str) -> dict:
    """Add FAQs to a venue KB. body: {faqs: [{question, answer, keywords?, tags?}]}"""
    await _gate(request, "L2_ROSTER_MAKER")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid JSON body")
    raw_faqs = body.get("faqs") or []
    if not isinstance(raw_faqs, list):
        raise HTTPException(status_code=400, detail="faqs must be a list")
    parsed: List[FAQEntry] = []
    for i, raw in enumerate(raw_faqs):
        if not isinstance(raw, dict):
            raise HTTPException(status_code=400, detail=f"faq {i} must be an object")
        if not raw.get("question") or not raw.get("answer"):
            raise HTTPException(status_code=400, detail=f"faq {i} requires question + answer")
        parsed.append(FAQEntry(
            question=raw["question"],
            answer=raw["answer"],
            keywords=raw.get("keywords") or [],
            tags=raw.get("tags") or [],
        ))
    get_kb().add_faqs(venue_id, parsed)
    return {"venue_id": venue_id, "added": len(parsed)}


@router.get("/api/v1/concierge/{venue_id}/kb")
async def get_kb_info(request: Request, venue_id: str) -> dict:
    await _gate(request, "L1_SUPERVISOR")
    kb = get_kb().ensure(venue_id)
    return {
        "venue_id": kb.venue_id,
        "venue_name": kb.venue_name,
        "faq_count": len(kb.faqs),
        "live_context": dict(kb.live_context),
        "escalation_keywords": list(kb.escalation_keywords),
        "faqs": [
            {
                "question": e.question,
                "tags": list(e.tags),
                "keyword_count": len(e.keywords),
            } for e in kb.faqs
        ],
    }
