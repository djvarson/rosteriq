"""Front-desk concierge AI (Round 9).

A guest-facing knowledge agent that answers common front-desk questions
(opening hours, today's specials, parking, dietary options, kids-menu,
nearest pokies room, function room availability, public holiday hours)
and triages requests staff should escalate.

Architecture:
- ConciergeKnowledgeBase    — venue-keyed structured FAQ + dynamic
                              live-context (current bookings load,
                              today's specials, weather note).
- ConciergeAgent             — answers a guest question by combining
                              KB lookup with optional LLM rewriting,
                              and emits a confidence + escalation flag.
- Pure stdlib; LLM backend is optional (lazy import).

Why this matters: front desk is rostering's first multiplier — if a
single LLM-powered concierge can handle the 80% of repeat questions, the
roster maker can shave a head off the front-of-house plan during quieter
periods, and complex requests (group bookings, complaints) get routed
to a human immediately rather than queueing.
"""

from __future__ import annotations

import logging
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("rosteriq.concierge")
AU_TZ = timezone(timedelta(hours=10))


# ---------------------------------------------------------------------------
# Knowledge base
# ---------------------------------------------------------------------------


@dataclass
class FAQEntry:
    question: str
    answer: str
    keywords: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)

    def matches(self, query: str) -> int:
        """Score = number of keywords found in (lowercased) query."""
        q = query.lower()
        return sum(1 for kw in self.keywords if kw.lower() in q)


@dataclass
class VenueKB:
    venue_id: str
    venue_name: str
    faqs: List[FAQEntry] = field(default_factory=list)
    live_context: Dict[str, Any] = field(default_factory=dict)
    escalation_keywords: List[str] = field(default_factory=lambda: [
        "complaint", "manager", "refund", "lost", "stolen",
        "allergic reaction", "ill", "injury", "accident", "police",
    ])


_DEFAULT_FAQS = [
    FAQEntry(
        question="What time are you open today?",
        answer="We're open from {open_time} to {close_time} today.",
        keywords=["open", "opening", "hours", "close", "closing", "what time"],
        tags=["hours"],
    ),
    FAQEntry(
        question="Do you take walk-ins?",
        answer="Yes — we hold a portion of tables for walk-ins. Bookings still get priority.",
        keywords=["walk in", "walk-in", "without booking", "no booking"],
        tags=["bookings"],
    ),
    FAQEntry(
        question="Do you have a kids menu?",
        answer="Yes, we have a kids menu for under-12s with smaller mains and a free dessert.",
        keywords=["kids", "children", "child menu", "kid friendly"],
        tags=["menu"],
    ),
    FAQEntry(
        question="Are you dog friendly?",
        answer="Outdoor areas are dog-friendly; we ask that pets stay on leash.",
        keywords=["dog", "pet", "puppy"],
        tags=["amenities"],
    ),
    FAQEntry(
        question="Where can I park?",
        answer="On-site parking is free until 11pm; overflow is on the street out front.",
        keywords=["park", "parking", "carpark", "car park"],
        tags=["amenities"],
    ),
    FAQEntry(
        question="Do you have gluten-free options?",
        answer="Several mains and pizzas are gluten-free; ask the kitchen if anything's unclear.",
        keywords=["gluten", "gf", "celiac", "coeliac"],
        tags=["dietary"],
    ),
    FAQEntry(
        question="Can we book a function?",
        answer=(
            "Yes — function bookings of 15+ go through our events team. "
            "Please ask staff to take your details and they'll be in touch."
        ),
        keywords=["function", "private room", "events", "group booking", "party"],
        tags=["functions", "escalate"],
    ),
]


class ConciergeKnowledgeBase:
    """Thread-safe registry of per-venue concierge knowledge."""

    def __init__(self) -> None:
        self._kbs: Dict[str, VenueKB] = {}
        self._lock = threading.Lock()

    def register(self, kb: VenueKB) -> None:
        with self._lock:
            self._kbs[kb.venue_id] = kb

    def get(self, venue_id: str) -> Optional[VenueKB]:
        with self._lock:
            return self._kbs.get(venue_id)

    def upsert_live_context(self, venue_id: str, ctx: Dict[str, Any]) -> None:
        """Set/overlay live context for a venue (open_time, specials, etc.)."""
        with self._lock:
            kb = self._kbs.get(venue_id)
            if kb is None:
                kb = VenueKB(venue_id=venue_id, venue_name=venue_id)
                kb.faqs = list(_DEFAULT_FAQS)
                self._kbs[venue_id] = kb
            kb.live_context = {**kb.live_context, **ctx}

    def add_faqs(self, venue_id: str, faqs: List[FAQEntry]) -> None:
        with self._lock:
            kb = self._kbs.get(venue_id)
            if kb is None:
                kb = VenueKB(venue_id=venue_id, venue_name=venue_id)
                self._kbs[venue_id] = kb
            kb.faqs.extend(faqs)

    def venues(self) -> List[str]:
        with self._lock:
            return sorted(self._kbs.keys())

    def clear(self) -> None:
        with self._lock:
            self._kbs.clear()

    def ensure(self, venue_id: str, venue_name: Optional[str] = None) -> VenueKB:
        """Return a KB for venue_id, seeding defaults if missing."""
        with self._lock:
            kb = self._kbs.get(venue_id)
            if kb is None:
                kb = VenueKB(
                    venue_id=venue_id,
                    venue_name=venue_name or venue_id,
                    faqs=list(_DEFAULT_FAQS),
                )
                self._kbs[venue_id] = kb
            return kb


_kb: Optional[ConciergeKnowledgeBase] = None


def get_kb() -> ConciergeKnowledgeBase:
    global _kb
    if _kb is None:
        _kb = ConciergeKnowledgeBase()
    return _kb


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------


@dataclass
class ConciergeReply:
    answer: str
    confidence: float  # 0.0 – 1.0
    escalate: bool
    matched_question: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    source: str = "kb"  # "kb" | "live" | "fallback"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "answer": self.answer,
            "confidence": round(self.confidence, 4),
            "escalate": self.escalate,
            "matched_question": self.matched_question,
            "tags": list(self.tags),
            "source": self.source,
        }


_FALLBACK = (
    "I'm not 100% sure about that — let me grab a staff member who can help."
)


class ConciergeAgent:
    """Answer guest questions over the venue KB.

    The agent is deterministic by default (KB lookup with keyword scoring
    + simple template substitution from live_context). LLM rewriting is
    intentionally opt-in so this can serve guests in demo/sandbox without
    network access.
    """

    def __init__(self, kb: Optional[ConciergeKnowledgeBase] = None) -> None:
        self.kb = kb or get_kb()

    def _should_escalate(self, kb: VenueKB, query: str) -> bool:
        q = query.lower()
        return any(re.search(rf"\b{re.escape(k)}\b", q) for k in kb.escalation_keywords)

    def _format(self, answer: str, ctx: Dict[str, Any]) -> str:
        """Substitute {placeholder}s from live_context, leaving unknowns intact."""
        try:
            class _SafeDict(dict):
                def __missing__(self, key):
                    return "{" + key + "}"
            return answer.format_map(_SafeDict(ctx))
        except Exception:
            return answer

    def answer(self, venue_id: str, query: str) -> ConciergeReply:
        if not query or not query.strip():
            return ConciergeReply(
                answer="What can I help you with?",
                confidence=0.2, escalate=False, source="fallback",
            )

        kb = self.kb.ensure(venue_id)
        escalate = self._should_escalate(kb, query)

        # Score each FAQ
        scored: List[Tuple[int, FAQEntry]] = [
            (e.matches(query), e) for e in kb.faqs
        ]
        scored.sort(key=lambda t: t[0], reverse=True)

        if not scored or scored[0][0] == 0:
            return ConciergeReply(
                answer=_FALLBACK,
                confidence=0.1,
                escalate=True,
                source="fallback",
            )

        best_score, best = scored[0]
        # Confidence: 0.4 baseline + 0.15 per matched keyword, capped 0.95
        confidence = min(0.4 + (best_score * 0.15), 0.95)

        formatted = self._format(best.answer, kb.live_context)
        # If template still has unsubstituted placeholders, drop confidence
        if "{" in formatted and "}" in formatted:
            confidence = max(0.3, confidence - 0.2)

        # If FAQ itself is tagged for escalation (e.g. functions), set flag
        if "escalate" in best.tags:
            escalate = True

        return ConciergeReply(
            answer=formatted,
            confidence=confidence,
            escalate=escalate,
            matched_question=best.question,
            tags=list(best.tags),
            source="live" if best.tags and "hours" in best.tags else "kb",
        )

    def batch_answer(
        self, venue_id: str, queries: List[str],
    ) -> List[ConciergeReply]:
        return [self.answer(venue_id, q) for q in queries]
