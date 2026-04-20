"""Tests for rosteriq.staff_score module — pure stdlib, no pytest.

Runs with: PYTHONPATH=. python3 -m unittest tests.test_staff_score -v

Test coverage:
- Individual dimension scorers
- Weighted score combination
- Ranking and filtering
- Store persistence
- Edge cases (no data, perfect scores, all zeros)
"""

import sys
import os
import unittest
import tempfile
from datetime import datetime, timezone

# Add parent to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rosteriq.staff_score import (
    get_staff_score_store,
    _reset_for_tests,
    ScoreDimension,
    DimensionScore,
    StaffScore,
    ScoreWeight,
    score_reliability,
    score_punctuality,
    score_versatility,
    score_accountability,
    score_availability,
    compute_staff_score,
    rank_staff,
    get_top_performers,
    get_improvement_needed,
)
from rosteriq import persistence as _p


class TestDimensionScorers(unittest.TestCase):
    """Test individual dimension scorer functions."""

    def test_reliability_no_data(self):
        """Test reliability scoring with no data."""
        score = score_reliability("emp_001")
        self.assertEqual(score.dimension, ScoreDimension.RELIABILITY)
        self.assertEqual(score.score, 100.0)
        self.assertEqual(score.sample_size, 1)

    def test_reliability_with_no_shows(self):
        """Test reliability scoring penalizes no-shows heavily."""
        events = [
            {"event_type": "no_show"},
            {"event_type": "no_show"},
            {"event_type": "attendance"},
        ]
        score = score_reliability("emp_001", shift_events=events)
        # Base 100 - 20 - 20 = 60
        self.assertEqual(score.score, 60.0)
        self.assertGreater(score.sample_size, 0)

    def test_reliability_with_high_cancellation_rate(self):
        """Test reliability scoring penalizes high cancellation rate."""
        events = [
            {"event_type": "cancellation"},
            {"event_type": "cancellation"},
            {"event_type": "cancellation"},
            {"event_type": "cancellation"},
            {"event_type": "cancellation"},
            {"event_type": "attendance"},
        ]
        score = score_reliability("emp_001", shift_events=events)
        # Cancel rate = 5/6 = 83% > 20%, so -10 penalty
        self.assertLessEqual(score.score, 90.0)

    def test_reliability_with_high_swap_rate(self):
        """Test reliability scoring penalizes high swap-out rate."""
        swaps = [
            {"status": "swap_out"},
            {"status": "swap_out"},
            {"status": "swap_out"},
            {"status": "swap_in"},
        ]
        score = score_reliability("emp_001", swaps=swaps)
        # Swap-out rate = 3/4 = 75% > 30%, so -10 penalty
        self.assertLessEqual(score.score, 90.0)

    def test_reliability_clamped_to_zero(self):
        """Test reliability score doesn't go below 0."""
        events = [
            {"event_type": "no_show"},
            {"event_type": "no_show"},
            {"event_type": "no_show"},
            {"event_type": "no_show"},
            {"event_type": "no_show"},
            {"event_type": "no_show"},
        ]
        score = score_reliability("emp_001", shift_events=events)
        self.assertGreaterEqual(score.score, 0)
        self.assertLessEqual(score.score, 100)

    def test_punctuality_no_data(self):
        """Test punctuality scoring with no data."""
        score = score_punctuality("emp_001")
        self.assertEqual(score.dimension, ScoreDimension.PUNCTUALITY)
        self.assertEqual(score.score, 100.0)
        self.assertEqual(score.sample_size, 0)

    def test_punctuality_on_time(self):
        """Test punctuality scoring for on-time arrivals."""
        events = [
            {"clock_in_minutes_late": 0},
            {"clock_in_minutes_late": -2},
            {"clock_in_minutes_late": 0},
        ]
        score = score_punctuality("emp_001", shift_events=events)
        self.assertEqual(score.score, 100.0)

    def test_punctuality_slightly_late(self):
        """Test punctuality scoring for 1-5 min late."""
        events = [
            {"clock_in_minutes_late": 2},
            {"clock_in_minutes_late": 3},
        ]
        score = score_punctuality("emp_001", shift_events=events)
        self.assertEqual(score.score, 95.0)

    def test_punctuality_moderately_late(self):
        """Test punctuality scoring for 5-15 min late."""
        events = [
            {"clock_in_minutes_late": 10},
            {"clock_in_minutes_late": 8},
        ]
        score = score_punctuality("emp_001", shift_events=events)
        self.assertEqual(score.score, 80.0)

    def test_punctuality_very_late(self):
        """Test punctuality scoring for 15+ min late."""
        events = [
            {"clock_in_minutes_late": 20},
            {"clock_in_minutes_late": 30},
        ]
        score = score_punctuality("emp_001", shift_events=events)
        self.assertEqual(score.score, 50.0)

    def test_punctuality_mixed_times(self):
        """Test punctuality scoring with mixed arrival times."""
        events = [
            {"clock_in_minutes_late": 0},    # 100
            {"clock_in_minutes_late": 3},    # 95
            {"clock_in_minutes_late": 10},   # 80
            {"clock_in_minutes_late": 25},   # 50
        ]
        score = score_punctuality("emp_001", shift_events=events)
        # Average: (100 + 95 + 80 + 50) / 4 = 81.25
        self.assertAlmostEqual(score.score, 81.25, places=1)

    def test_versatility_no_roles(self):
        """Test versatility scoring with no roles trained."""
        score = score_versatility("emp_001")
        self.assertEqual(score.score, 0)

    def test_versatility_one_role(self):
        """Test versatility scoring with 1 role."""
        score = score_versatility("emp_001", roles_trained=["bar"])
        self.assertEqual(score.score, 20)

    def test_versatility_two_roles(self):
        """Test versatility scoring with 2 roles."""
        score = score_versatility("emp_001", roles_trained=["bar", "floor"])
        self.assertEqual(score.score, 50)

    def test_versatility_three_roles(self):
        """Test versatility scoring with 3 roles."""
        score = score_versatility("emp_001", roles_trained=["bar", "floor", "kitchen"])
        self.assertEqual(score.score, 75)

    def test_versatility_four_plus_roles(self):
        """Test versatility scoring with 4+ roles."""
        score = score_versatility(
            "emp_001",
            roles_trained=["bar", "floor", "kitchen", "manager"],
            total_roles=4,
        )
        self.assertEqual(score.score, 100)

    def test_accountability_no_data(self):
        """Test accountability scoring with no data."""
        score = score_accountability("emp_001")
        self.assertEqual(score.score, 100.0)

    def test_accountability_all_completed(self):
        """Test accountability scoring when all tasks completed."""
        records = [
            {"completed": True},
            {"completed": True},
            {"completed": True},
        ]
        score = score_accountability("emp_001", accountability_records=records)
        self.assertEqual(score.score, 100.0)

    def test_accountability_partial_completion(self):
        """Test accountability scoring with partial completion."""
        records = [
            {"completed": True},
            {"completed": True},
            {"completed": False},
            {"completed": False},
        ]
        score = score_accountability("emp_001", accountability_records=records)
        # 2/4 = 50%
        self.assertEqual(score.score, 50.0)

    def test_accountability_none_completed(self):
        """Test accountability scoring when no tasks completed."""
        records = [
            {"completed": False},
            {"completed": False},
        ]
        score = score_accountability("emp_001", accountability_records=records)
        self.assertEqual(score.score, 0)

    def test_availability_no_slots(self):
        """Test availability scoring with no available slots."""
        score = score_availability("emp_001", availability_slots=0)
        self.assertEqual(score.score, 0)

    def test_availability_half_available(self):
        """Test availability scoring at 50% slots available."""
        score = score_availability("emp_001", availability_slots=10, total_slots=21)
        self.assertAlmostEqual(score.score, (10/21) * 100, places=1)

    def test_availability_all_slots(self):
        """Test availability scoring when all slots available."""
        score = score_availability("emp_001", availability_slots=21, total_slots=21)
        self.assertEqual(score.score, 100.0)

    def test_availability_clamped_above_100(self):
        """Test availability score doesn't exceed 100."""
        score = score_availability("emp_001", availability_slots=25, total_slots=21)
        self.assertEqual(score.score, 100.0)


class TestWeightedScoring(unittest.TestCase):
    """Test weighted score combination."""

    def test_compute_staff_score_default_weights(self):
        """Test staff score computation with default equal weights."""
        score = compute_staff_score(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            roles_trained=["bar", "floor"],
            total_roles=4,
            availability_slots=18,
            total_slots=21,
        )

        self.assertEqual(score.employee_id, "emp_001")
        self.assertEqual(score.employee_name, "Alice")
        self.assertEqual(score.venue_id, "venue_001")
        self.assertEqual(len(score.dimensions), 5)
        self.assertGreater(score.overall_score, 0)
        self.assertLessEqual(score.overall_score, 100)

    def test_compute_staff_score_all_perfect(self):
        """Test staff score when all dimensions are perfect."""
        score = compute_staff_score(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            roles_trained=["bar", "floor", "kitchen", "manager"],
            total_roles=4,
            availability_slots=21,
            total_slots=21,
            shift_events=[],  # No delays
            accountability_records=[{"completed": True}],  # All done
        )

        self.assertEqual(score.overall_score, 100.0)

    def test_compute_staff_score_all_poor(self):
        """Test staff score when all dimensions are poor."""
        score = compute_staff_score(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            shift_events=[
                {"event_type": "no_show"},
                {"event_type": "no_show"},
                {"clock_in_minutes_late": 30},
            ],
            roles_trained=["bar"],
            total_roles=4,
            accountability_records=[
                {"completed": False},
                {"completed": False},
            ],
            availability_slots=0,
            total_slots=21,
        )

        self.assertLess(score.overall_score, 50)

    def test_compute_staff_score_custom_weights(self):
        """Test staff score with custom weights."""
        custom_weights = {
            ScoreDimension.RELIABILITY: 0.5,
            ScoreDimension.PUNCTUALITY: 0.5,
            ScoreDimension.VERSATILITY: 0,
            ScoreDimension.ACCOUNTABILITY: 0,
            ScoreDimension.AVAILABILITY: 0,
        }

        score = compute_staff_score(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            roles_trained=["bar"],
            availability_slots=0,  # Would be poor
            shift_events=[],  # Perfect reliability
            accountability_records=[],  # Perfect accountability
            weights=custom_weights,
        )

        # Overall should be heavily weighted toward reliability + punctuality
        self.assertGreater(score.overall_score, 50)

    def test_compute_staff_score_period_days(self):
        """Test staff score period_days field."""
        score = compute_staff_score(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            period_days=60,
        )

        self.assertEqual(score.period_days, 60)


class TestRankingAndFiltering(unittest.TestCase):
    """Test ranking and filtering helper functions."""

    def test_rank_staff(self):
        """Test ranking staff by score descending."""
        scores = [
            StaffScore("emp_001", "Alice", "venue_001", overall_score=85),
            StaffScore("emp_002", "Bob", "venue_001", overall_score=92),
            StaffScore("emp_003", "Charlie", "venue_001", overall_score=78),
        ]

        ranked = rank_staff(scores)

        self.assertEqual(ranked[0].overall_score, 92)
        self.assertEqual(ranked[1].overall_score, 85)
        self.assertEqual(ranked[2].overall_score, 78)

    def test_get_top_performers(self):
        """Test getting top N performers."""
        scores = [
            StaffScore("emp_001", "Alice", "venue_001", overall_score=85),
            StaffScore("emp_002", "Bob", "venue_001", overall_score=92),
            StaffScore("emp_003", "Charlie", "venue_001", overall_score=78),
            StaffScore("emp_004", "Diana", "venue_001", overall_score=88),
        ]

        top_3 = get_top_performers(scores, n=3)

        self.assertEqual(len(top_3), 3)
        self.assertEqual(top_3[0].overall_score, 92)
        self.assertEqual(top_3[1].overall_score, 88)
        self.assertEqual(top_3[2].overall_score, 85)

    def test_get_top_performers_fewer_than_n(self):
        """Test getting top N when fewer than N exist."""
        scores = [
            StaffScore("emp_001", "Alice", "venue_001", overall_score=85),
            StaffScore("emp_002", "Bob", "venue_001", overall_score=92),
        ]

        top_5 = get_top_performers(scores, n=5)

        self.assertEqual(len(top_5), 2)

    def test_get_improvement_needed(self):
        """Test getting staff below threshold."""
        scores = [
            StaffScore("emp_001", "Alice", "venue_001", overall_score=85),
            StaffScore("emp_002", "Bob", "venue_001", overall_score=45),
            StaffScore("emp_003", "Charlie", "venue_001", overall_score=55),
            StaffScore("emp_004", "Diana", "venue_001", overall_score=72),
        ]

        below_60 = get_improvement_needed(scores, threshold=60.0)

        self.assertEqual(len(below_60), 2)
        # Should be sorted ascending (worst first)
        self.assertEqual(below_60[0].overall_score, 45)
        self.assertEqual(below_60[1].overall_score, 55)

    def test_get_improvement_needed_none_below_threshold(self):
        """Test get_improvement_needed when none below threshold."""
        scores = [
            StaffScore("emp_001", "Alice", "venue_001", overall_score=85),
            StaffScore("emp_002", "Bob", "venue_001", overall_score=92),
        ]

        below_60 = get_improvement_needed(scores, threshold=60.0)

        self.assertEqual(len(below_60), 0)


class TestStaffScoreStore(unittest.TestCase):
    """Test the StaffScoreStore with persistence."""

    @classmethod
    def setUpClass(cls):
        """Set up a temp DB file for the entire test class."""
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        """Clean up temp DB file."""
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        """Reset store and persistence before each test."""
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        # Clear the DB file between tests
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        """Clean up after each test."""
        _p.reset_for_tests()

    def test_record_and_retrieve_score(self):
        """Test recording and retrieving a staff score."""
        store = get_staff_score_store()

        score = compute_staff_score(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            roles_trained=["bar", "floor"],
        )

        recorded = store.record_score(score)
        retrieved = store.get("venue_001", "emp_001")

        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.employee_id, "emp_001")
        self.assertEqual(retrieved.employee_name, "Alice")
        self.assertEqual(retrieved.venue_id, "venue_001")

    def test_list_by_venue(self):
        """Test listing scores by venue."""
        store = get_staff_score_store()

        # Record scores for multiple employees
        for i in range(3):
            score = compute_staff_score(
                employee_id=f"emp_{i:03d}",
                employee_name=f"Employee {i}",
                venue_id="venue_001",
            )
            store.record_score(score)

        # List scores for venue
        scores = store.list_by_venue("venue_001")
        self.assertEqual(len(scores), 3)

    def test_list_by_venue_sorted_descending(self):
        """Test that list_by_venue returns scores sorted descending."""
        store = get_staff_score_store()

        # Record scores in random order
        for score_val in [45, 92, 78]:
            score = StaffScore(
                employee_id=f"emp_{score_val}",
                employee_name=f"Employee {score_val}",
                venue_id="venue_001",
                overall_score=score_val,
            )
            store.record_score(score)

        scores = store.list_by_venue("venue_001")
        # Should be sorted descending
        self.assertEqual(scores[0].overall_score, 92)
        self.assertEqual(scores[1].overall_score, 78)
        self.assertEqual(scores[2].overall_score, 45)

    def test_list_needing_improvement(self):
        """Test listing staff below threshold."""
        store = get_staff_score_store()

        for score_val in [85, 45, 55, 72]:
            score = StaffScore(
                employee_id=f"emp_{score_val}",
                employee_name=f"Employee {score_val}",
                venue_id="venue_001",
                overall_score=score_val,
            )
            store.record_score(score)

        below_60 = store.list_needing_improvement("venue_001", threshold=60.0)
        self.assertEqual(len(below_60), 2)
        # Should be sorted ascending (worst first)
        self.assertEqual(below_60[0].overall_score, 45)
        self.assertEqual(below_60[1].overall_score, 55)

    def test_multiple_venues(self):
        """Test store correctly separates venues."""
        store = get_staff_score_store()

        # Record for venue_001
        for i in range(2):
            score = StaffScore(
                employee_id=f"emp_{i:03d}",
                employee_name=f"Employee {i}",
                venue_id="venue_001",
                overall_score=80 + i,
            )
            store.record_score(score)

        # Record for venue_002
        for i in range(3):
            score = StaffScore(
                employee_id=f"emp_{i:03d}",
                employee_name=f"Employee {i}",
                venue_id="venue_002",
                overall_score=70 + i,
            )
            store.record_score(score)

        scores_1 = store.list_by_venue("venue_001")
        scores_2 = store.list_by_venue("venue_002")

        self.assertEqual(len(scores_1), 2)
        self.assertEqual(len(scores_2), 3)
        self.assertTrue(all(s.venue_id == "venue_001" for s in scores_1))
        self.assertTrue(all(s.venue_id == "venue_002" for s in scores_2))


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions."""

    def test_score_with_all_none_values(self):
        """Test score computation with all None/empty inputs."""
        score = compute_staff_score(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
        )

        self.assertGreaterEqual(score.overall_score, 0)
        self.assertLessEqual(score.overall_score, 100)

    def test_dimension_score_to_dict(self):
        """Test DimensionScore serialization."""
        dim = DimensionScore(
            dimension=ScoreDimension.RELIABILITY,
            score=85.5,
            sample_size=10,
            details="Test details",
        )

        d = dim.to_dict()
        self.assertEqual(d["dimension"], "reliability")
        self.assertEqual(d["score"], 85.5)
        self.assertEqual(d["sample_size"], 10)
        self.assertEqual(d["details"], "Test details")

    def test_staff_score_to_dict(self):
        """Test StaffScore serialization."""
        score = StaffScore(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            overall_score=85.5,
        )

        d = score.to_dict()
        self.assertEqual(d["employee_id"], "emp_001")
        self.assertEqual(d["employee_name"], "Alice")
        self.assertEqual(d["venue_id"], "venue_001")
        self.assertEqual(d["overall_score"], 85.5)
        self.assertIsInstance(d["computed_at"], str)
        self.assertIsInstance(d["dimensions"], list)

    def test_score_weight_to_dict(self):
        """Test ScoreWeight serialization."""
        weight = ScoreWeight(
            dimension=ScoreDimension.RELIABILITY,
            weight=0.25,
        )

        d = weight.to_dict()
        self.assertEqual(d["dimension"], "reliability")
        self.assertEqual(d["weight"], 0.25)


def run_tests():
    """Run all tests."""
    unittest.main(verbosity=2, exit=True)


if __name__ == "__main__":
    run_tests()
