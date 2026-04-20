"""Staff Performance Scoring for Australian hospitality venues.

Calculates comprehensive performance scores across five dimensions:
- RELIABILITY: no-shows, cancellations, swap-outs
- PUNCTUALITY: clock-in timing accuracy
- VERSATILITY: role coverage breadth
- ACCOUNTABILITY: task completion rates
- AVAILABILITY: weekly availability coverage

Scores are weighted and combined into an overall performance rating (0-100).
Persisted to SQLite for ranking and reporting.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger("rosteriq.staff_score")


# ---------------------------------------------------------------------------
# Enums & Data Classes
# ---------------------------------------------------------------------------


class ScoreDimension(str, Enum):
    """Performance scoring dimensions."""
    RELIABILITY = "reliability"
    PUNCTUALITY = "punctuality"
    VERSATILITY = "versatility"
    ACCOUNTABILITY = "accountability"
    AVAILABILITY = "availability"


@dataclass
class DimensionScore:
    """Score for a single performance dimension."""
    dimension: ScoreDimension
    score: float  # 0-100
    sample_size: int  # count of data points used
    details: str  # human-readable explanation

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dimension": self.dimension.value,
            "score": round(self.score, 2),
            "sample_size": self.sample_size,
            "details": self.details,
        }


@dataclass
class StaffScore:
    """Comprehensive performance score for an employee."""
    employee_id: str
    employee_name: str
    venue_id: str
    overall_score: float  # weighted average of dimensions, 0-100
    dimensions: List[DimensionScore] = field(default_factory=list)
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    period_days: int = 28  # lookback period

    def to_dict(self) -> Dict[str, Any]:
        return {
            "employee_id": self.employee_id,
            "employee_name": self.employee_name,
            "venue_id": self.venue_id,
            "overall_score": round(self.overall_score, 2),
            "dimensions": [d.to_dict() for d in self.dimensions],
            "computed_at": self.computed_at.isoformat(),
            "period_days": self.period_days,
        }


@dataclass
class ScoreWeight:
    """Weight for a performance dimension in overall calculation."""
    dimension: ScoreDimension
    weight: float  # default 0.2 for equal weighting across 5 dimensions

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dimension": self.dimension.value,
            "weight": round(self.weight, 3),
        }


# ---------------------------------------------------------------------------
# Persistence wiring
# ---------------------------------------------------------------------------


def _get_persistence():
    """Lazy import of persistence module."""
    try:
        from rosteriq import persistence as _p
        return _p
    except ImportError:
        return None


_STAFF_SCORES_SCHEMA = """
CREATE TABLE IF NOT EXISTS staff_scores (
    score_id TEXT PRIMARY KEY,
    employee_id TEXT NOT NULL,
    employee_name TEXT NOT NULL,
    venue_id TEXT NOT NULL,
    overall_score REAL NOT NULL,
    dimensions_json TEXT NOT NULL,
    computed_at TEXT NOT NULL,
    period_days INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_staff_score_venue ON staff_scores(venue_id);
CREATE INDEX IF NOT EXISTS ix_staff_score_employee ON staff_scores(employee_id);
CREATE INDEX IF NOT EXISTS ix_staff_score_computed ON staff_scores(computed_at);
"""


def _register_schema_and_callbacks():
    """Register schema and rehydration callback. Deferred until persistence is available."""
    try:
        _p = _get_persistence()
        if _p:
            _p.register_schema("staff_scores", _STAFF_SCORES_SCHEMA)
            # Register rehydration callback
            def _rehydrate_on_init():
                store = get_staff_score_store()
                store._rehydrate()
            _p.on_init(_rehydrate_on_init)
    except Exception:
        pass


_register_schema_and_callbacks()


# ---------------------------------------------------------------------------
# Scoring Functions
# ---------------------------------------------------------------------------


def score_reliability(
    employee_id: str,
    shift_events: Optional[List[Dict[str, Any]]] = None,
    swaps: Optional[List[Dict[str, Any]]] = None,
) -> DimensionScore:
    """Score employee reliability based on no-shows, cancellations, and swaps.

    Scoring:
    - Base score: 100
    - Each no-show: -20
    - Cancel rate >20%: -10
    - Swap-out frequency >30%: -10

    Args:
        employee_id: Employee ID
        shift_events: List of shift event dicts with 'event_type' key
        swaps: List of swap dicts with 'status' key

    Returns:
        DimensionScore for reliability
    """
    score = 100.0
    no_shows = 0
    total_shifts = 1  # avoid divide by zero

    if shift_events:
        for event in shift_events:
            total_shifts += 1
            if event.get("event_type") == "no_show":
                no_shows += 1
                score -= 20

        # Cancel rate penalty
        cancellations = sum(1 for e in shift_events if e.get("event_type") == "cancellation")
        cancel_rate = cancellations / total_shifts if total_shifts > 0 else 0
        if cancel_rate > 0.20:
            score -= 10

    if swaps:
        swap_outs = sum(1 for s in swaps if s.get("status") == "swap_out")
        swap_rate = swap_outs / max(total_shifts, 1)
        if swap_rate > 0.30:
            score -= 10

    score = max(0, min(100, score))  # clamp to 0-100
    sample_size = total_shifts + len(swaps) if swaps else total_shifts
    details = f"No-shows: {no_shows}, shifts analyzed: {total_shifts}"

    return DimensionScore(
        dimension=ScoreDimension.RELIABILITY,
        score=score,
        sample_size=sample_size,
        details=details,
    )


def score_punctuality(
    employee_id: str,
    shift_events: Optional[List[Dict[str, Any]]] = None,
) -> DimensionScore:
    """Score employee punctuality based on clock-in timing.

    Scoring:
    - On-time or early: 100
    - 1-5 min late: 95
    - 5-15 min late: 80
    - 15+ min late: 50
    Average across all shifts.

    Args:
        employee_id: Employee ID
        shift_events: List of shift event dicts with 'clock_in_minutes_late' key

    Returns:
        DimensionScore for punctuality
    """
    if not shift_events:
        return DimensionScore(
            dimension=ScoreDimension.PUNCTUALITY,
            score=100.0,
            sample_size=0,
            details="No clock-in data available",
        )

    scores = []
    for event in shift_events:
        minutes_late = event.get("clock_in_minutes_late", 0)
        if minutes_late <= 0:
            scores.append(100)
        elif minutes_late <= 5:
            scores.append(95)
        elif minutes_late <= 15:
            scores.append(80)
        else:
            scores.append(50)

    avg_score = sum(scores) / len(scores) if scores else 100
    late_count = sum(1 for s in shift_events if s.get("clock_in_minutes_late", 0) > 0)

    return DimensionScore(
        dimension=ScoreDimension.PUNCTUALITY,
        score=avg_score,
        sample_size=len(scores),
        details=f"Late arrivals: {late_count}/{len(scores)} shifts",
    )


def score_versatility(
    employee_id: str,
    roles_trained: Optional[List[str]] = None,
    total_roles: int = 4,
) -> DimensionScore:
    """Score employee versatility based on role coverage.

    Scoring:
    - 1 role: 20
    - 2 roles: 50
    - 3 roles: 75
    - 4+ roles: 100

    Args:
        employee_id: Employee ID
        roles_trained: List of roles employee is trained for
        total_roles: Total roles available at venue (default 4)

    Returns:
        DimensionScore for versatility
    """
    if not roles_trained:
        roles_trained = []

    num_roles = len(roles_trained)
    if num_roles == 0:
        score = 0
    elif num_roles == 1:
        score = 20
    elif num_roles == 2:
        score = 50
    elif num_roles == 3:
        score = 75
    else:  # 4+
        score = 100

    details = f"Trained in {num_roles}/{total_roles} roles: {', '.join(roles_trained) if roles_trained else 'none'}"

    return DimensionScore(
        dimension=ScoreDimension.VERSATILITY,
        score=score,
        sample_size=num_roles,
        details=details,
    )


def score_accountability(
    employee_id: str,
    accountability_records: Optional[List[Dict[str, Any]]] = None,
) -> DimensionScore:
    """Score employee accountability based on task completion.

    Scoring: (tasks_completed / tasks_assigned) * 100

    Args:
        employee_id: Employee ID
        accountability_records: List of accountability dicts with 'assigned' and 'completed' booleans

    Returns:
        DimensionScore for accountability
    """
    if not accountability_records:
        return DimensionScore(
            dimension=ScoreDimension.ACCOUNTABILITY,
            score=100.0,
            sample_size=0,
            details="No accountability records",
        )

    assigned = len(accountability_records)
    completed = sum(1 for r in accountability_records if r.get("completed", False))

    score = (completed / assigned * 100) if assigned > 0 else 100

    return DimensionScore(
        dimension=ScoreDimension.ACCOUNTABILITY,
        score=score,
        sample_size=assigned,
        details=f"Completed {completed}/{assigned} assigned tasks",
    )


def score_availability(
    employee_id: str,
    availability_slots: Optional[int] = None,
    total_slots: int = 21,
) -> DimensionScore:
    """Score employee availability based on weekly slot coverage.

    Scoring: (available_slots / total_slots) * 100
    Typical venue has 21 slots per week (3 shifts × 7 days).

    Args:
        employee_id: Employee ID
        availability_slots: Number of slots employee is available for
        total_slots: Total slots per week (default 21)

    Returns:
        DimensionScore for availability
    """
    if availability_slots is None:
        availability_slots = 0

    score = (availability_slots / total_slots * 100) if total_slots > 0 else 0
    score = min(100, max(0, score))

    details = f"Available for {availability_slots}/{total_slots} weekly slots"

    return DimensionScore(
        dimension=ScoreDimension.AVAILABILITY,
        score=score,
        sample_size=total_slots,
        details=details,
    )


# ---------------------------------------------------------------------------
# Weighted Scoring
# ---------------------------------------------------------------------------


def compute_staff_score(
    employee_id: str,
    employee_name: str,
    venue_id: str,
    shift_events: Optional[List[Dict[str, Any]]] = None,
    swaps: Optional[List[Dict[str, Any]]] = None,
    roles_trained: Optional[List[str]] = None,
    total_roles: int = 4,
    accountability_records: Optional[List[Dict[str, Any]]] = None,
    availability_slots: Optional[int] = None,
    total_slots: int = 21,
    weights: Optional[Dict[ScoreDimension, float]] = None,
    period_days: int = 28,
) -> StaffScore:
    """Compute comprehensive staff performance score.

    Combines all five dimensions with optional weights (default: equal 0.2 each).

    Args:
        employee_id: Employee ID
        employee_name: Employee name
        venue_id: Venue ID
        shift_events: Shift event records
        swaps: Shift swap records
        roles_trained: List of trained roles
        total_roles: Total roles at venue
        accountability_records: Task records
        availability_slots: Weekly available slots
        total_slots: Total weekly slots
        weights: Optional weight dict by dimension (must sum to 1.0)
        period_days: Lookback period in days

    Returns:
        StaffScore with overall score and dimension breakdown
    """
    # Default equal weights
    if weights is None:
        weights = {
            ScoreDimension.RELIABILITY: 0.2,
            ScoreDimension.PUNCTUALITY: 0.2,
            ScoreDimension.VERSATILITY: 0.2,
            ScoreDimension.ACCOUNTABILITY: 0.2,
            ScoreDimension.AVAILABILITY: 0.2,
        }

    # Compute dimension scores
    dim_scores = [
        score_reliability(employee_id, shift_events, swaps),
        score_punctuality(employee_id, shift_events),
        score_versatility(employee_id, roles_trained, total_roles),
        score_accountability(employee_id, accountability_records),
        score_availability(employee_id, availability_slots, total_slots),
    ]

    # Compute weighted average
    overall_score = sum(
        d.score * weights.get(d.dimension, 0.2) for d in dim_scores
    )
    overall_score = min(100, max(0, overall_score))

    return StaffScore(
        employee_id=employee_id,
        employee_name=employee_name,
        venue_id=venue_id,
        overall_score=overall_score,
        dimensions=dim_scores,
        computed_at=datetime.now(timezone.utc),
        period_days=period_days,
    )


def rank_staff(scores: List[StaffScore]) -> List[StaffScore]:
    """Rank staff by overall score descending.

    Args:
        scores: List of staff scores

    Returns:
        Sorted list, highest score first
    """
    return sorted(scores, key=lambda s: s.overall_score, reverse=True)


def get_top_performers(scores: List[StaffScore], n: int = 5) -> List[StaffScore]:
    """Get top N performers by score.

    Args:
        scores: List of staff scores
        n: Number of top performers to return

    Returns:
        Top N scores, sorted descending
    """
    ranked = rank_staff(scores)
    return ranked[:n]


def get_improvement_needed(scores: List[StaffScore], threshold: float = 60.0) -> List[StaffScore]:
    """Get staff scoring below threshold (needs improvement).

    Args:
        scores: List of staff scores
        threshold: Score threshold (default 60.0)

    Returns:
        Scores below threshold, sorted ascending (worst first)
    """
    below = [s for s in scores if s.overall_score < threshold]
    return sorted(below, key=lambda s: s.overall_score)


# ---------------------------------------------------------------------------
# Staff Score Store
# ---------------------------------------------------------------------------


class StaffScoreStore:
    """Thread-safe in-memory store for staff scores with SQLite persistence.

    Stores the latest score for each employee per venue.
    Persists on every computation when persistence is enabled.
    Rehydrates from SQLite on app startup.
    """

    def __init__(self):
        self._scores: Dict[str, StaffScore] = {}  # key: f"{venue_id}:{employee_id}"
        self._lock = threading.Lock()

    def _persist(self, score: StaffScore) -> None:
        """Persist a score to SQLite if enabled."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        import json
        row = {
            "score_id": f"score_{uuid.uuid4().hex[:12]}",
            "employee_id": score.employee_id,
            "employee_name": score.employee_name,
            "venue_id": score.venue_id,
            "overall_score": score.overall_score,
            "dimensions_json": json.dumps([d.to_dict() for d in score.dimensions]),
            "computed_at": score.computed_at.isoformat(),
            "period_days": score.period_days,
        }
        try:
            _p.upsert("staff_scores", row, pk="score_id")
        except Exception as e:
            logger.warning("Failed to persist score for %s: %s", score.employee_id, e)

    def _rehydrate(self) -> None:
        """Load all scores from SQLite. Called on startup by persistence.on_init."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        try:
            import json
            rows = _p.fetchall("SELECT * FROM staff_scores ORDER BY computed_at DESC")
            # Only keep latest score per employee per venue
            seen = set()
            for row in rows:
                key = f"{row['venue_id']}:{row['employee_id']}"
                if key in seen:
                    continue
                seen.add(key)
                score = self._row_to_score(dict(row))
                self._scores[key] = score
            logger.info("Rehydrated %d staff scores from persistence", len(self._scores))
        except Exception as e:
            logger.warning("Failed to rehydrate staff scores: %s", e)

    @staticmethod
    def _row_to_score(row: Dict[str, Any]) -> StaffScore:
        """Reconstruct a StaffScore from a DB row."""
        import json

        def parse_iso(s: Optional[str]) -> Optional[datetime]:
            if not s:
                return None
            try:
                return datetime.fromisoformat(s)
            except (ValueError, TypeError):
                return None

        dimensions_json = row.get("dimensions_json", "[]")
        try:
            dims_data = json.loads(dimensions_json)
            dimensions = [
                DimensionScore(
                    dimension=ScoreDimension(d["dimension"]),
                    score=d.get("score", 0),
                    sample_size=d.get("sample_size", 0),
                    details=d.get("details", ""),
                )
                for d in dims_data
            ]
        except Exception:
            dimensions = []

        return StaffScore(
            employee_id=row["employee_id"],
            employee_name=row["employee_name"],
            venue_id=row["venue_id"],
            overall_score=row.get("overall_score", 0),
            dimensions=dimensions,
            computed_at=parse_iso(row.get("computed_at")) or datetime.now(timezone.utc),
            period_days=row.get("period_days", 28),
        )

    def record_score(self, score: StaffScore) -> StaffScore:
        """Record or update a staff score.

        Args:
            score: StaffScore to record

        Returns:
            The recorded score
        """
        key = f"{score.venue_id}:{score.employee_id}"
        with self._lock:
            self._scores[key] = score
        self._persist(score)
        return score

    def get(self, venue_id: str, employee_id: str) -> Optional[StaffScore]:
        """Get latest score for employee at venue.

        Args:
            venue_id: Venue ID
            employee_id: Employee ID

        Returns:
            StaffScore or None if not found
        """
        key = f"{venue_id}:{employee_id}"
        with self._lock:
            return self._scores.get(key)

    def list_by_venue(
        self,
        venue_id: str,
        limit: int = 100,
    ) -> List[StaffScore]:
        """List all staff scores for a venue, sorted by overall_score descending.

        Args:
            venue_id: Venue ID
            limit: Max results

        Returns:
            List of StaffScores
        """
        with self._lock:
            venue_scores = [
                s for s in self._scores.values() if s.venue_id == venue_id
            ]
            venue_scores.sort(key=lambda s: s.overall_score, reverse=True)
            return venue_scores[:limit]

    def list_needing_improvement(
        self,
        venue_id: str,
        threshold: float = 60.0,
        limit: int = 50,
    ) -> List[StaffScore]:
        """List staff below threshold for a venue, sorted by score ascending (worst first).

        Args:
            venue_id: Venue ID
            threshold: Score threshold
            limit: Max results

        Returns:
            List of StaffScores below threshold
        """
        with self._lock:
            below = [
                s for s in self._scores.values()
                if s.venue_id == venue_id and s.overall_score < threshold
            ]
            below.sort(key=lambda s: s.overall_score)
            return below[:limit]


# ---------------------------------------------------------------------------
# Module singleton
# ---------------------------------------------------------------------------

_staff_score_store_singleton: Optional[StaffScoreStore] = None
_singleton_lock = threading.Lock()


def get_staff_score_store() -> StaffScoreStore:
    """Get the module-level staff score store singleton.

    Lazily initializes on first call. Thread-safe.
    """
    global _staff_score_store_singleton
    if _staff_score_store_singleton is None:
        with _singleton_lock:
            if _staff_score_store_singleton is None:
                _staff_score_store_singleton = StaffScoreStore()
    return _staff_score_store_singleton


# Test helper: reset singleton
def _reset_for_tests() -> None:
    """Reset the singleton. Used by tests."""
    global _staff_score_store_singleton
    _staff_score_store_singleton = None
