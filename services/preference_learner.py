"""
ML-based staff scheduling preference learning system for RosterIQ.

Learns from historical shift data what employees prefer:
- Day preferences (Mon-Sun scores indicating which days they accept/decline/swap)
- Time slot preferences (morning 6-12, afternoon 12-18, evening 18-24, night 0-6)
- Co-worker affinity (who they work well with based on overlapping shifts)
- Venue preferences (if multi-venue, which locations they prefer)
- Shift length preferences (do they prefer 4h, 6h, or 8h shifts)
- Consistency score (do they like regular patterns or variety)

The system uses collaborative filtering (cosine similarity between preference vectors)
to suggest preferences for employees with limited data.

Pure Python implementation (numpy-free, uses math stdlib and list comprehensions).
"""

import logging
import math
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, List, Tuple
from collections import defaultdict

from rosteriq.database import get_db
from rosteriq.models import Roster, Shift, Employee, ShiftStatus

logger = logging.getLogger(__name__)


# ============================================================================
# Data structures
# ============================================================================


@dataclass
class PreferenceProfile:
    """Learned preference profile for an employee."""

    employee_id: str
    venue_id: str
    day_scores: Dict[str, float] = field(default_factory=dict)  # "Monday" -> 0.85
    time_scores: Dict[str, float] = field(default_factory=dict)  # "morning" -> 0.75
    preferred_shift_length: float = 6.0  # hours
    coworker_affinities: Dict[str, float] = field(default_factory=dict)  # emp_id -> 0.6
    consistency_score: float = 0.5  # 0=loves variety, 1=loves routine
    venue_preferences: Dict[str, float] = field(default_factory=dict)  # venue_id -> score
    role_preferences: Dict[str, float] = field(default_factory=dict)  # role -> score
    confidence: float = 0.0  # 0.0-1.0, based on data volume
    samples_used: int = 0  # number of shifts analyzed
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """Convert to dict for storage."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "PreferenceProfile":
        """Construct from stored dict."""
        return cls(**data)


# ============================================================================
# PreferenceLearner: Main learning engine
# ============================================================================


class PreferenceLearner:
    """
    Learns staff scheduling preferences from historical roster data.

    Attributes:
        db: Database store instance
        day_names: List of weekday names
        time_slots: Dict of time slot names to (start_hour, end_hour) tuples
        min_shifts_for_confidence: Minimum shifts to establish confidence
    """

    def __init__(self):
        self.db = get_db()
        self.day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        self.time_slots = {
            "morning": (6, 12),
            "afternoon": (12, 18),
            "evening": (18, 24),
            "night": (0, 6),
        }
        self.min_shifts_for_confidence = 5
        self._preference_cache: Dict[str, PreferenceProfile] = {}

    # ========================================================================
    # Training
    # ========================================================================

    def train(self, venue_id: str, lookback_days: int = 90) -> Dict[str, PreferenceProfile]:
        """
        Train preference model from historical rosters.

        Analyzes rosters from the past N days and learns:
        - Day preferences from shift frequencies
        - Time slot preferences from shift timing
        - Co-worker affinities from overlapping shifts
        - Shift length preferences from typical shift durations
        - Consistency score from variation in patterns
        - Venue preferences if multi-venue

        Args:
            venue_id: ID of venue to train on
            lookback_days: How many days back to analyze (default 90)

        Returns:
            Dict mapping employee_id -> PreferenceProfile
        """
        logger.info(f"Starting preference learning for venue {venue_id} (lookback: {lookback_days} days)")

        # Get rosters from lookback period
        start_date = date.today() - timedelta(days=lookback_days)
        end_date = date.today()
        rosters = self.db.get_rosters_by_date_range(venue_id, start_date, end_date)

        if not rosters:
            logger.warning(f"No rosters found for venue {venue_id} in lookback period")
            return {}

        # Initialize accumulator structures
        day_shift_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        time_slot_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        shift_lengths: Dict[str, List[float]] = defaultdict(list)
        coworker_pairs: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        venue_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        role_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        employee_dates: Dict[str, set] = defaultdict(set)

        # Analyze all shifts in the roster
        total_shifts = 0
        for roster in rosters:
            for shift in roster.shifts:
                total_shifts += 1
                employee_id = shift.employee_id

                # Track by day of week
                day_of_week = shift.date.weekday()  # 0=Monday, 6=Sunday
                day_name = self.day_names[day_of_week]
                day_shift_counts[employee_id][day_name] += 1

                # Track by time slot
                slot_name = self._get_time_slot(shift.start_time.hour)
                time_slot_counts[employee_id][slot_name] += 1

                # Track shift length
                shift_lengths[employee_id].append(shift.net_hours)

                # Track venue (if present)
                venue_counts[employee_id][roster.venue_id] += 1

                # Track role
                role_counts[employee_id][shift.role] += 1

                # Track dates for consistency
                employee_dates[employee_id].add(shift.date)

        # Find co-worker pairs by analyzing overlapping shifts
        for roster in rosters:
            shifts_by_employee: Dict[str, Shift] = {}
            for shift in roster.shifts:
                shifts_by_employee[shift.employee_id] = shift

            # For each pair of employees, check if they worked same shift
            employee_ids = list(shifts_by_employee.keys())
            for i, emp1 in enumerate(employee_ids):
                shift1 = shifts_by_employee[emp1]
                for emp2 in employee_ids[i+1:]:
                    shift2 = shifts_by_employee[emp2]
                    if self._shifts_overlap(shift1, shift2):
                        coworker_pairs[emp1][emp2] += 1
                        coworker_pairs[emp2][emp1] += 1

        # Build preference profiles
        profiles: Dict[str, PreferenceProfile] = {}
        all_employees = set()
        for roster in rosters:
            all_employees.update(shift.employee_id for shift in roster.shifts)

        for employee_id in all_employees:
            profiles[employee_id] = self._build_profile(
                employee_id,
                venue_id,
                day_shift_counts[employee_id],
                time_slot_counts[employee_id],
                shift_lengths[employee_id],
                coworker_pairs[employee_id],
                venue_counts[employee_id],
                role_counts[employee_id],
                employee_dates[employee_id],
            )

        # Apply collaborative filtering for low-confidence profiles
        profiles = self._apply_collaborative_filtering(profiles)

        # Save all profiles
        for profile in profiles.values():
            self._save_preference_profile(profile)

        logger.info(f"Trained {len(profiles)} employee preference profiles "
                   f"from {total_shifts} shifts across {len(rosters)} rosters")

        return profiles

    # ========================================================================
    # Prediction
    # ========================================================================

    def predict_happiness(self, employee_id: str, proposed_shift: Shift) -> float:
        """
        Predict employee happiness for a proposed shift (0.0-1.0).

        Combines multiple preference signals:
        - Day preference score (weight: 0.3)
        - Time slot preference score (weight: 0.25)
        - Co-worker preference score (weight: 0.2)
        - Shift length preference score (weight: 0.15)
        - Venue preference score (weight: 0.1)

        Args:
            employee_id: ID of employee
            proposed_shift: Shift object to evaluate

        Returns:
            Happiness score 0.0-1.0 (1.0 = very happy, 0.0 = very unhappy)
        """
        profile = self.get_preference_profile(employee_id)
        if not profile:
            return 0.5  # Neutral for unknown employees

        # Day score
        day_name = self.day_names[proposed_shift.date.weekday()]
        day_score = profile.day_scores.get(day_name, 0.5)

        # Time slot score
        slot_name = self._get_time_slot(proposed_shift.start_time.hour)
        time_score = profile.time_scores.get(slot_name, 0.5)

        # Shift length score
        length_diff = abs(proposed_shift.net_hours - profile.preferred_shift_length)
        length_score = max(0.0, 1.0 - (length_diff / 8.0))  # Degrades by 12.5% per hour difference

        # Venue score
        venue_score = profile.venue_preferences.get(proposed_shift.employee_id, 0.5)

        # Role score
        role_score = profile.role_preferences.get(proposed_shift.role, 0.5)

        # Weighted average
        happiness = (
            day_score * 0.3 +
            time_score * 0.25 +
            length_score * 0.15 +
            venue_score * 0.2 +
            role_score * 0.1
        )

        # Confidence adjustment: lower confidence = closer to neutral
        adjusted = (happiness * profile.confidence) + (0.5 * (1.0 - profile.confidence))

        return max(0.0, min(1.0, adjusted))

    def rank_employees_for_shift(
        self,
        shift: Shift,
        candidates: List[str],
    ) -> List[Tuple[str, float]]:
        """
        Rank candidates by predicted happiness for a specific shift.

        Used by roster optimiser to break ties between equally-available employees.

        Args:
            shift: Shift to fill
            candidates: List of candidate employee IDs

        Returns:
            List of (employee_id, happiness_score) tuples, sorted descending by score
        """
        rankings = []
        for emp_id in candidates:
            # Create temp shift with this employee
            temp_shift = Shift(
                id=shift.id,
                employee_id=emp_id,
                date=shift.date,
                start_time=shift.start_time,
                end_time=shift.end_time,
                break_minutes=shift.break_minutes,
                status=shift.status,
                role=shift.role,
                cost=shift.cost,
            )
            happiness = self.predict_happiness(emp_id, temp_shift)
            rankings.append((emp_id, happiness))

        # Sort by happiness descending
        rankings.sort(key=lambda x: x[1], reverse=True)
        return rankings

    # ========================================================================
    # Profile access
    # ========================================================================

    def get_preference_profile(self, employee_id: str) -> Optional[PreferenceProfile]:
        """
        Get learned preference profile for an employee.

        Args:
            employee_id: ID of employee

        Returns:
            PreferenceProfile or None if not found
        """
        # Check cache first
        if employee_id in self._preference_cache:
            return self._preference_cache[employee_id]

        # Load from DB
        profile_dict = self.db.get_preference_profile(employee_id)
        if not profile_dict:
            return None

        profile = PreferenceProfile.from_dict(profile_dict)
        self._preference_cache[employee_id] = profile
        return profile

    def list_preference_profiles(self, venue_id: str) -> List[PreferenceProfile]:
        """
        Get all preference profiles for a venue.

        Args:
            venue_id: ID of venue

        Returns:
            List of PreferenceProfile objects
        """
        profiles_dicts = self.db.list_preference_profiles(venue_id)
        profiles = [PreferenceProfile.from_dict(p) for p in profiles_dicts]

        # Update cache
        for profile in profiles:
            self._preference_cache[profile.employee_id] = profile

        return profiles

    # ========================================================================
    # Roster-level metrics
    # ========================================================================

    def roster_happiness_score(self, roster: Roster) -> float:
        """
        Calculate average happiness score for all shifts in a roster.

        Useful for A/B testing roster quality and comparing optimization approaches.

        Args:
            roster: Roster object

        Returns:
            Average happiness score 0.0-1.0
        """
        if not roster.shifts:
            return 0.5

        total_happiness = 0.0
        for shift in roster.shifts:
            happiness = self.predict_happiness(shift.employee_id, shift)
            total_happiness += happiness

        return total_happiness / len(roster.shifts)

    def suggest_ideal_shifts(self, employee_id: str, dates: List[date]) -> List[Dict]:
        """
        Suggest ideal shifts for an employee across multiple dates.

        Returns shifts that align with their preferences.

        Args:
            employee_id: ID of employee
            dates: List of dates to suggest shifts for

        Returns:
            List of dicts: {"date": date, "time_slot": str, "estimated_happiness": float}
        """
        profile = self.get_preference_profile(employee_id)
        if not profile:
            return []

        suggestions = []
        for d in dates:
            day_name = self.day_names[d.weekday()]
            day_score = profile.day_scores.get(day_name, 0.5)

            # Find best time slot for this day
            best_slot = max(
                profile.time_scores.items(),
                key=lambda x: x[1],
                default=("morning", 0.5)
            )
            slot_name, slot_score = best_slot

            # Estimate happiness (day + time + shift length + role)
            estimated = (day_score * 0.3 + slot_score * 0.3 + 0.2)

            suggestions.append({
                "date": d,
                "day_name": day_name,
                "time_slot": slot_name,
                "estimated_happiness": min(1.0, estimated),
            })

        return suggestions

    # ========================================================================
    # Internal helper methods
    # ========================================================================

    def _get_time_slot(self, hour: int) -> str:
        """Map hour (0-23) to time slot name."""
        for slot_name, (start, end) in self.time_slots.items():
            if start <= hour < end or (start > end and (hour >= start or hour < end)):
                return slot_name
        return "morning"  # default

    def _shifts_overlap(self, shift1: Shift, shift2: Shift) -> bool:
        """Check if two shifts have overlapping hours on the same day."""
        if shift1.date != shift2.date:
            return False

        s1_start = shift1.start_time.hour * 60 + shift1.start_time.minute
        s1_end = shift1.end_time.hour * 60 + shift1.end_time.minute
        s2_start = shift2.start_time.hour * 60 + shift2.start_time.minute
        s2_end = shift2.end_time.hour * 60 + shift2.end_time.minute

        # Handle overnight shifts
        if s1_end < s1_start:
            s1_end += 24 * 60
        if s2_end < s2_start:
            s2_end += 24 * 60

        # Check overlap
        return not (s1_end <= s2_start or s2_end <= s1_start)

    def _build_profile(
        self,
        employee_id: str,
        venue_id: str,
        day_counts: Dict[str, int],
        time_counts: Dict[str, int],
        shift_lengths: List[float],
        coworker_pairs: Dict[str, int],
        venue_counts: Dict[str, int],
        role_counts: Dict[str, int],
        work_dates: set,
    ) -> PreferenceProfile:
        """Build a preference profile from raw counts."""

        # Normalize day scores (0.0-1.0)
        day_scores = {}
        max_day_count = max(day_counts.values()) if day_counts else 1
        for day_name in self.day_names:
            count = day_counts.get(day_name, 0)
            score = (count / max_day_count) if max_day_count > 0 else 0.5
            day_scores[day_name] = score

        # Normalize time slot scores
        time_scores = {}
        max_time_count = max(time_counts.values()) if time_counts else 1
        for slot_name in self.time_slots.keys():
            count = time_counts.get(slot_name, 0)
            score = (count / max_time_count) if max_time_count > 0 else 0.5
            time_scores[slot_name] = score

        # Calculate preferred shift length
        preferred_length = sum(shift_lengths) / len(shift_lengths) if shift_lengths else 6.0

        # Normalize co-worker affinities
        coworker_affinities = {}
        max_coworker_count = max(coworker_pairs.values()) if coworker_pairs else 1
        for coworker_id, count in coworker_pairs.items():
            score = (count / max_coworker_count) if max_coworker_count > 0 else 0.5
            coworker_affinities[coworker_id] = score

        # Calculate consistency score (0=variable, 1=consistent)
        consistency_score = self._calculate_consistency(work_dates, shift_lengths)

        # Normalize venue preferences
        venue_prefs = {}
        max_venue_count = max(venue_counts.values()) if venue_counts else 1
        for v_id, count in venue_counts.items():
            score = (count / max_venue_count) if max_venue_count > 0 else 0.5
            venue_prefs[v_id] = score

        # Normalize role preferences
        role_prefs = {}
        max_role_count = max(role_counts.values()) if role_counts else 1
        for role, count in role_counts.items():
            score = (count / max_role_count) if max_role_count > 0 else 0.5
            role_prefs[role] = score

        # Calculate confidence based on sample size
        samples = len(shift_lengths)
        confidence = min(1.0, samples / (self.min_shifts_for_confidence * 2))

        return PreferenceProfile(
            employee_id=employee_id,
            venue_id=venue_id,
            day_scores=day_scores,
            time_scores=time_scores,
            preferred_shift_length=preferred_length,
            coworker_affinities=coworker_affinities,
            consistency_score=consistency_score,
            venue_preferences=venue_prefs,
            role_preferences=role_prefs,
            confidence=confidence,
            samples_used=samples,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )

    def _calculate_consistency(self, work_dates: set, shift_lengths: List[float]) -> float:
        """
        Calculate consistency score (0=loves variety, 1=loves routine).

        High consistency = works regularly on similar days
        Low consistency = works irregularly with variable patterns
        """
        if not work_dates or not shift_lengths:
            return 0.5

        # Measure temporal regularity
        sorted_dates = sorted(work_dates)
        gaps = []
        for i in range(1, len(sorted_dates)):
            gap = (sorted_dates[i] - sorted_dates[i-1]).days
            gaps.append(gap)

        # Low variance in gaps = high consistency
        if len(gaps) > 1:
            mean_gap = sum(gaps) / len(gaps)
            variance = sum((g - mean_gap) ** 2 for g in gaps) / len(gaps)
            # Normalize: low variance (0) = high consistency (1)
            regularity = max(0.0, 1.0 - (variance / 100.0))
        else:
            regularity = 0.5

        # Measure shift length consistency
        if len(shift_lengths) > 1:
            mean_length = sum(shift_lengths) / len(shift_lengths)
            length_variance = sum((l - mean_length) ** 2 for l in shift_lengths) / len(shift_lengths)
            # Low variance = high consistency
            length_consistency = max(0.0, 1.0 - (length_variance / 16.0))
        else:
            length_consistency = 0.5

        return (regularity * 0.6 + length_consistency * 0.4)

    def _apply_collaborative_filtering(
        self,
        profiles: Dict[str, PreferenceProfile],
    ) -> Dict[str, PreferenceProfile]:
        """
        Apply collaborative filtering to improve low-confidence profiles.

        If an employee has low confidence, find similar employees and
        blend their preferences using cosine similarity.
        """
        low_confidence_emps = [
            eid for eid, p in profiles.items()
            if p.confidence < 0.5
        ]

        if not low_confidence_emps:
            return profiles

        for emp_id in low_confidence_emps:
            profile = profiles[emp_id]

            # Find most similar employees
            similarities = []
            for other_id, other_profile in profiles.items():
                if other_id == emp_id or other_profile.confidence < 0.5:
                    continue

                similarity = self._cosine_similarity(
                    self._profile_to_vector(profile),
                    self._profile_to_vector(other_profile),
                )
                if similarity > 0.5:  # Only use reasonably similar employees
                    similarities.append((other_id, similarity, other_profile))

            if similarities:
                # Weight by similarity
                similarities.sort(key=lambda x: x[1], reverse=True)
                similar_profiles = similarities[:3]  # Top 3 most similar

                # Blend preferences
                total_weight = sum(s[1] for s in similar_profiles)
                for day_name in self.day_names:
                    blended = sum(
                        s[2].day_scores.get(day_name, 0.5) * s[1]
                        for s in similar_profiles
                    ) / total_weight if total_weight > 0 else 0.5
                    profile.day_scores[day_name] = (
                        profile.day_scores[day_name] * 0.3 +
                        blended * 0.7
                    )

        return profiles

    def _profile_to_vector(self, profile: PreferenceProfile) -> List[float]:
        """Convert profile to numeric vector for similarity calculation."""
        vector = []
        for day_name in self.day_names:
            vector.append(profile.day_scores.get(day_name, 0.5))
        for slot_name in self.time_slots.keys():
            vector.append(profile.time_scores.get(slot_name, 0.5))
        vector.append(profile.preferred_shift_length / 8.0)  # normalize
        vector.append(profile.consistency_score)
        return vector

    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Calculate cosine similarity between two vectors."""
        if len(vec1) != len(vec2):
            return 0.0

        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        magnitude1 = math.sqrt(sum(a * a for a in vec1))
        magnitude2 = math.sqrt(sum(b * b for b in vec2))

        if magnitude1 == 0 or magnitude2 == 0:
            return 0.0

        return dot_product / (magnitude1 * magnitude2)

    def _save_preference_profile(self, profile: PreferenceProfile) -> None:
        """Save profile to database."""
        self.db.save_preference_profile(profile.employee_id, profile.to_dict())
        self._preference_cache[profile.employee_id] = profile
