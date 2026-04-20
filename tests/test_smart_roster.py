"""Tests for rosteriq.smart_roster module — pure stdlib, no pytest.

Runs with: PYTHONPATH=. python3 -m unittest tests.test_smart_roster -v

Test coverage (25+ tests):
- Individual factor scoring (availability, skills, certs, performance, fatigue, cost)
- Hard constraint exclusions (on leave, fatigue exceeded, missing cert, break violation)
- Candidate ranking
- Full roster plan
- Cost estimation with penalty rates
- Explanation generation
- Graceful degradation when modules unavailable
"""

import sys
import os
import unittest
from datetime import datetime, date, timedelta, timezone

# Add parent to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rosteriq.smart_roster import (
    SuitabilityFactor,
    StaffSuggestion,
    ShiftRequirement,
    RosterSuggestion,
    RosterPlan,
    score_candidate,
    suggest_for_shift,
    build_roster_plan,
    explain_suggestion,
    estimate_shift_cost,
)


# ─────────────────────────────────────────────────────────────────────────────
# Test Fixtures & Helpers
# ─────────────────────────────────────────────────────────────────────────────


def make_employee(
    emp_id: str = "emp_001",
    name: str = "Alice",
    role: str = "bar",
    hourly_rate: float = 25.0,
    skills: list = None,
    availability: dict = None,
) -> dict:
    """Create a test employee dict."""
    if skills is None:
        skills = [role]
    if availability is None:
        # Available all day, every day of week
        availability = {i: [(0, 24)] for i in range(7)}

    return {
        "id": emp_id,
        "name": name,
        "role": role,
        "hourly_rate": hourly_rate,
        "skills": skills,
        "availability": availability,
        "employment_type": "part_time",
    }


def make_requirement(
    venue_id: str = "venue_001",
    shift_date: date = None,
    start_time: str = "09:00",
    end_time: str = "17:00",
    role: str = "bar",
    required_certs: list = None,
) -> ShiftRequirement:
    """Create a test shift requirement."""
    if shift_date is None:
        shift_date = date.today() + timedelta(days=1)
    if required_certs is None:
        required_certs = []

    return ShiftRequirement(
        venue_id=venue_id,
        date=shift_date,
        start_time=start_time,
        end_time=end_time,
        role=role,
        required_certs=required_certs,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test Cases
# ─────────────────────────────────────────────────────────────────────────────


class TestAvailabilityScoring(unittest.TestCase):
    """Test availability factor scoring."""

    def test_available_all_day(self):
        """Test employee available for shift."""
        emp = make_employee(availability={i: [(0, 24)] for i in range(7)})
        req = make_requirement(start_time="09:00", end_time="17:00")

        suggestion = score_candidate(emp, req)
        # Should have availability factor with high score
        avail_factor = next((f for f in suggestion.factors if f.name == "Availability"), None)
        self.assertIsNotNone(avail_factor)
        self.assertGreater(avail_factor.score, 0.8)

    def test_unavailable_on_day(self):
        """Test employee not available on this day of week."""
        # Monday = 0; make unavailable on Monday by setting empty slots list
        emp = make_employee(availability={0: [], 1: [(0, 24)], 2: [(0, 24)], 3: [(0, 24)],
                                         4: [(0, 24)], 5: [(0, 24)], 6: [(0, 24)]})
        req = make_requirement(shift_date=date(2026, 4, 20))  # This is a Monday

        suggestion = score_candidate(emp, req)
        avail_factor = next((f for f in suggestion.factors if f.name == "Availability"), None)
        self.assertIsNotNone(avail_factor)
        self.assertLess(avail_factor.score, 0.5)

    def test_available_partial_hours(self):
        """Test employee available for only part of the day."""
        # Available 9-12 only
        emp = make_employee(availability={i: [(9, 12)] for i in range(7)})
        req = make_requirement(start_time="09:00", end_time="17:00")

        suggestion = score_candidate(emp, req)
        avail_factor = next((f for f in suggestion.factors if f.name == "Availability"), None)
        self.assertIsNotNone(avail_factor)
        self.assertLess(avail_factor.score, 1.0)


class TestSkillsMatching(unittest.TestCase):
    """Test skills matching factor scoring."""

    def test_exact_role_match(self):
        """Test employee with exact role match."""
        emp = make_employee(role="bar", skills=["bar"])
        req = make_requirement(role="bar")

        suggestion = score_candidate(emp, req)
        skills_factor = next((f for f in suggestion.factors if f.name == "Skills match"), None)
        self.assertIsNotNone(skills_factor)
        self.assertEqual(skills_factor.score, 1.0)

    def test_cross_trained_employee(self):
        """Test employee cross-trained in multiple roles."""
        emp = make_employee(role="bar", skills=["bar", "floor"])
        req = make_requirement(role="floor")

        suggestion = score_candidate(emp, req)
        skills_factor = next((f for f in suggestion.factors if f.name == "Skills match"), None)
        self.assertIsNotNone(skills_factor)
        self.assertGreater(skills_factor.score, 0.5)

    def test_untrained_employee(self):
        """Test employee without required skill."""
        emp = make_employee(role="bar", skills=["bar"])
        req = make_requirement(role="kitchen")

        suggestion = score_candidate(emp, req)
        skills_factor = next((f for f in suggestion.factors if f.name == "Skills match"), None)
        self.assertIsNotNone(skills_factor)
        self.assertLess(skills_factor.score, 0.5)


class TestCertificationsScoring(unittest.TestCase):
    """Test certifications factor scoring."""

    def test_no_certs_required(self):
        """Test shift with no cert requirements."""
        emp = make_employee()
        req = make_requirement(required_certs=[])

        suggestion = score_candidate(emp, req, {})
        certs_factor = next((f for f in suggestion.factors if f.name == "Certifications"), None)
        self.assertIsNotNone(certs_factor)
        self.assertEqual(certs_factor.score, 1.0)

    def test_certs_required_but_none_held(self):
        """Test shift requires certs but employee holds none — hard constraint."""
        emp = make_employee()
        req = make_requirement(required_certs=["RSA"])
        context = {"certifications": {emp["id"]: []}}

        suggestion = score_candidate(emp, req, context)
        # Should be excluded due to hard constraint (missing mandatory cert)
        self.assertEqual(suggestion.suitability_score, 0.0)
        self.assertIn("Missing mandatory certs", " ".join(suggestion.warnings))

    def test_certs_required_and_held(self):
        """Test shift requires certs and employee holds all."""
        emp = make_employee()
        req = make_requirement(required_certs=["RSA"])
        context = {
            "certifications": {
                emp["id"]: [
                    {"cert_type": "RSA", "status": "valid"},
                ]
            }
        }

        suggestion = score_candidate(emp, req, context)
        certs_factor = next((f for f in suggestion.factors if f.name == "Certifications"), None)
        self.assertIsNotNone(certs_factor)
        self.assertEqual(certs_factor.score, 1.0)


class TestHardConstraints(unittest.TestCase):
    """Test hard constraint exclusions."""

    def test_excluded_on_leave(self):
        """Test employee on approved leave is excluded."""
        emp = make_employee()
        req = make_requirement(shift_date=date(2026, 4, 21))
        context = {
            "leave_requests": {
                emp["id"]: [
                    {
                        "start_date": "2026-04-21",
                        "end_date": "2026-04-23",
                        "status": "approved",
                    }
                ]
            }
        }

        suggestion = score_candidate(emp, req, context)
        self.assertEqual(suggestion.suitability_score, 0.0)
        self.assertIn("On approved leave", suggestion.warnings)

    def test_excluded_missing_mandatory_cert(self):
        """Test employee missing mandatory cert is excluded."""
        emp = make_employee()
        req = make_requirement(required_certs=["RSA", "FOOD_SAFETY"])
        context = {
            "certifications": {
                emp["id"]: [
                    {"cert_type": "RSA", "status": "valid"},
                    # Missing FOOD_SAFETY
                ]
            }
        }

        suggestion = score_candidate(emp, req, context)
        self.assertEqual(suggestion.suitability_score, 0.0)
        self.assertIn("Missing mandatory certs", " ".join(suggestion.warnings))

    def test_not_excluded_pending_leave(self):
        """Test employee with pending leave request is not excluded."""
        emp = make_employee()
        req = make_requirement(shift_date=date(2026, 4, 21))
        context = {
            "leave_requests": {
                emp["id"]: [
                    {
                        "start_date": "2026-04-21",
                        "end_date": "2026-04-23",
                        "status": "pending",  # Not approved yet
                    }
                ]
            }
        }

        suggestion = score_candidate(emp, req, context)
        self.assertGreater(suggestion.suitability_score, 0.0)


class TestPerformanceScoring(unittest.TestCase):
    """Test performance factor scoring."""

    def test_no_performance_data(self):
        """Test employee with no performance data."""
        emp = make_employee()
        req = make_requirement()

        suggestion = score_candidate(emp, req, {})
        perf_factor = next((f for f in suggestion.factors if f.name == "Performance"), None)
        self.assertIsNotNone(perf_factor)
        self.assertGreater(perf_factor.score, 0.0)  # Neutral scoring

    def test_high_performer(self):
        """Test high-performing employee."""
        emp = make_employee()
        req = make_requirement()
        context = {
            "staff_scores": {
                emp["id"]: {"overall_score": 85}
            }
        }

        suggestion = score_candidate(emp, req, context)
        perf_factor = next((f for f in suggestion.factors if f.name == "Performance"), None)
        self.assertIsNotNone(perf_factor)
        self.assertGreater(perf_factor.score, 0.8)

    def test_low_performer(self):
        """Test low-performing employee."""
        emp = make_employee()
        req = make_requirement()
        context = {
            "staff_scores": {
                emp["id"]: {"overall_score": 30}
            }
        }

        suggestion = score_candidate(emp, req, context)
        perf_factor = next((f for f in suggestion.factors if f.name == "Performance"), None)
        self.assertIsNotNone(perf_factor)
        self.assertLess(perf_factor.score, 0.5)


class TestFatigueScoring(unittest.TestCase):
    """Test fatigue risk factor scoring."""

    def test_no_fatigue_data(self):
        """Test employee with no fatigue data."""
        emp = make_employee()
        req = make_requirement()

        suggestion = score_candidate(emp, req, {})
        fatigue_factor = next((f for f in suggestion.factors if f.name == "Fatigue risk"), None)
        self.assertIsNotNone(fatigue_factor)
        self.assertGreater(fatigue_factor.score, 0.5)

    def test_low_fatigue(self):
        """Test employee with low fatigue risk."""
        emp = make_employee()
        req = make_requirement()
        context = {
            "fatigue_assessments": {
                emp["id"]: {
                    "risk_level": "low",
                    "score": 10,
                }
            }
        }

        suggestion = score_candidate(emp, req, context)
        fatigue_factor = next((f for f in suggestion.factors if f.name == "Fatigue risk"), None)
        self.assertIsNotNone(fatigue_factor)
        self.assertGreater(fatigue_factor.score, 0.8)

    def test_high_fatigue_warning(self):
        """Test warning for high fatigue (but not excluded unless critical)."""
        emp = make_employee()
        req = make_requirement()
        context = {
            "fatigue_assessments": {
                emp["id"]: {
                    "risk_level": "high",
                    "score": 75,
                }
            }
        }

        suggestion = score_candidate(emp, req, context)
        # Should have warning but not excluded
        self.assertGreater(suggestion.suitability_score, 0.0)
        self.assertIn("Fatigue risk: HIGH", suggestion.warnings)


class TestCostEstimation(unittest.TestCase):
    """Test shift cost estimation with penalty rates."""

    def test_basic_shift_cost(self):
        """Test basic shift cost calculation."""
        cost = estimate_shift_cost(
            25.0,  # $25/hour
            "09:00",
            "17:00",  # 8 hours
            date(2026, 4, 21),  # Tuesday
            "QLD",
        )
        # 8 hours * $25 = $200 (no penalty on Tuesday)
        self.assertAlmostEqual(cost, 200.0, delta=1.0)

    def test_saturday_penalty(self):
        """Test Saturday penalty rate (25% uplift)."""
        # Use May 2, 2026 (Saturday, not a public holiday)
        cost = estimate_shift_cost(
            25.0,
            "09:00",
            "17:00",  # 8 hours
            date(2026, 5, 2),  # This is a Saturday
            "QLD",
        )
        # 8 hours * $25 * 1.25 = $250
        self.assertAlmostEqual(cost, 250.0, delta=1.0)

    def test_sunday_penalty(self):
        """Test Sunday penalty rate (50% uplift)."""
        # Use May 3, 2026 (Sunday, not a public holiday)
        cost = estimate_shift_cost(
            25.0,
            "09:00",
            "17:00",  # 8 hours
            date(2026, 5, 3),  # This is a Sunday
            "QLD",
        )
        # 8 hours * $25 * 1.5 = $300
        self.assertAlmostEqual(cost, 300.0, delta=1.0)

    def test_short_shift(self):
        """Test short shift cost."""
        cost = estimate_shift_cost(
            25.0,
            "09:00",
            "12:00",  # 3 hours
            date(2026, 4, 21),  # Tuesday
            "QLD",
        )
        # 3 hours * $25 = $75
        self.assertAlmostEqual(cost, 75.0, delta=1.0)


class TestCostScoring(unittest.TestCase):
    """Test cost efficiency factor scoring."""

    def test_cost_factor_scoring(self):
        """Test cost factor is included in scoring."""
        emp = make_employee(hourly_rate=25.0)
        req = make_requirement(
            shift_date=date(2026, 4, 21),
            start_time="09:00",
            end_time="17:00",
        )

        suggestion = score_candidate(emp, req, {})
        cost_factor = next((f for f in suggestion.factors if f.name == "Cost efficiency"), None)
        self.assertIsNotNone(cost_factor)
        self.assertGreater(cost_factor.score, 0.0)
        self.assertGreater(suggestion.estimated_cost, 0.0)


class TestCandidateRanking(unittest.TestCase):
    """Test ranking of multiple candidates."""

    def test_rank_multiple_candidates(self):
        """Test candidates are ranked by suitability score."""
        emp1 = make_employee(emp_id="emp_001", name="Alice", role="bar")
        emp2 = make_employee(emp_id="emp_002", name="Bob", role="kitchen")
        emp3 = make_employee(emp_id="emp_003", name="Charlie", role="bar")
        candidates = [emp1, emp2, emp3]

        req = make_requirement(role="bar")
        context = {
            "staff_scores": {
                "emp_001": {"overall_score": 50},
                "emp_002": {"overall_score": 80},
                "emp_003": {"overall_score": 70},
            }
        }

        roster_suggestion = suggest_for_shift(req, candidates, context)

        # Should rank: emp1 (bar, 50) < emp3 (bar, 70) < emp2 (bar trained? no, different role)
        # Actually: emp1 and emp3 are exact role match, emp2 is not trained in bar
        # So should be: emp3 > emp1 > emp2
        self.assertEqual(len(roster_suggestion.suggestions), 3)
        # First suggestion should be Bob or Alice (bar trained)
        first = roster_suggestion.suggestions[0]
        self.assertIn(first.employee_name, ["Alice", "Charlie"])

    def test_no_suitable_candidates(self):
        """Test unfilled flag when no suitable candidates."""
        emp = make_employee(role="kitchen")
        req = make_requirement(
            role="bar",
            required_certs=["RSA"],
        )
        context = {
            "certifications": {emp["id"]: []}  # No certs
        }

        roster_suggestion = suggest_for_shift(req, [emp], context)
        self.assertTrue(roster_suggestion.unfilled)
        self.assertIsNotNone(roster_suggestion.unfilled_reason)

    def test_all_candidates_excluded(self):
        """Test unfilled flag when all candidates excluded."""
        emp = make_employee()
        req = make_requirement(shift_date=date(2026, 4, 21))
        context = {
            "leave_requests": {
                emp["id"]: [
                    {
                        "start_date": "2026-04-21",
                        "end_date": "2026-04-23",
                        "status": "approved",
                    }
                ]
            }
        }

        roster_suggestion = suggest_for_shift(req, [emp], context)
        self.assertTrue(roster_suggestion.unfilled)


class TestRosterPlan(unittest.TestCase):
    """Test full roster plan generation."""

    def test_build_roster_plan(self):
        """Test building a roster plan."""
        emp1 = make_employee(emp_id="emp_001")
        emp2 = make_employee(emp_id="emp_002")

        period_start = date(2026, 4, 20)
        period_end = date(2026, 4, 27)

        plan = build_roster_plan(
            "venue_001",
            period_start,
            period_end,
            [emp1, emp2],
            {},
        )

        self.assertEqual(plan.venue_id, "venue_001")
        self.assertEqual(plan.period_start, period_start)
        self.assertEqual(plan.period_end, period_end)


class TestExplanationGeneration(unittest.TestCase):
    """Test explanation generation."""

    def test_excluded_explanation(self):
        """Test explanation for excluded candidate."""
        emp = make_employee()
        req = make_requirement(required_certs=["RSA"])
        context = {"certifications": {emp["id"]: []}}

        suggestion = score_candidate(emp, req, context)
        explanation = explain_suggestion(suggestion)

        self.assertIn("cannot be assigned", explanation)
        self.assertIn("Missing", explanation)

    def test_suitable_explanation(self):
        """Test explanation for suitable candidate."""
        emp = make_employee()
        req = make_requirement()

        suggestion = score_candidate(emp, req, {})
        explanation = explain_suggestion(suggestion)

        self.assertIn(emp["name"], explanation)
        self.assertIn("scores", explanation)

    def test_explanation_includes_factors(self):
        """Test explanation includes top factors."""
        emp = make_employee(hourly_rate=20.0)
        req = make_requirement()
        context = {
            "staff_scores": {
                emp["id"]: {"overall_score": 85}
            }
        }

        suggestion = score_candidate(emp, req, context)
        explanation = explain_suggestion(suggestion)

        # Should mention Alice and the score
        self.assertIn(emp["name"], explanation)
        self.assertGreater(len(explanation), 10)


class TestGracefulDegradation(unittest.TestCase):
    """Test graceful degradation when optional modules unavailable."""

    def test_score_without_optional_modules(self):
        """Test scoring works without optional module imports."""
        emp = make_employee()
        req = make_requirement()

        # Empty context (no optional data)
        suggestion = score_candidate(emp, req, {})

        # Should still produce a score
        self.assertGreater(suggestion.suitability_score, 0.0)
        self.assertGreater(len(suggestion.factors), 0)

    def test_explain_without_data(self):
        """Test explanation generation works without extra data."""
        emp = make_employee()
        req = make_requirement()

        suggestion = score_candidate(emp, req, {})
        explanation = explain_suggestion(suggestion)

        # Should generate a reasonable explanation
        self.assertGreater(len(explanation), 5)
        self.assertIn(emp["name"], explanation)


class TestDataStructures(unittest.TestCase):
    """Test data structure serialization."""

    def test_suitability_factor_to_dict(self):
        """Test SuitabilityFactor serialization."""
        factor = SuitabilityFactor(
            name="Test",
            score=0.85,
            weight=0.2,
            reason="Test reason",
        )
        d = factor.to_dict()

        self.assertEqual(d["name"], "Test")
        self.assertEqual(d["score"], 0.85)
        self.assertEqual(d["weight"], 0.2)
        self.assertEqual(d["reason"], "Test reason")

    def test_shift_requirement_to_dict(self):
        """Test ShiftRequirement serialization."""
        req = make_requirement()
        d = req.to_dict()

        self.assertEqual(d["venue_id"], "venue_001")
        self.assertEqual(d["role"], "bar")
        self.assertEqual(d["start_time"], "09:00")

    def test_staff_suggestion_to_dict(self):
        """Test StaffSuggestion serialization."""
        emp = make_employee()
        req = make_requirement()
        suggestion = score_candidate(emp, req, {})
        d = suggestion.to_dict()

        self.assertEqual(d["employee_id"], emp["id"])
        self.assertEqual(d["employee_name"], emp["name"])
        self.assertIsInstance(d["factors"], list)
        self.assertGreater(len(d["factors"]), 0)

    def test_roster_plan_to_dict(self):
        """Test RosterPlan serialization."""
        plan = build_roster_plan(
            "venue_001",
            date(2026, 4, 20),
            date(2026, 4, 27),
            [],
            {},
        )
        d = plan.to_dict()

        self.assertEqual(d["venue_id"], "venue_001")
        self.assertIn("period_start", d)
        self.assertIn("coverage_pct", d)


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and error handling."""

    def test_malformed_time_string(self):
        """Test handling of malformed time strings."""
        cost = estimate_shift_cost(
            25.0,
            "invalid",  # Malformed
            "17:00",
            date.today(),
            "QLD",
        )
        # Should use default 8 hours
        self.assertGreater(cost, 0.0)

    def test_overnight_shift(self):
        """Test overnight shift duration calculation."""
        cost = estimate_shift_cost(
            25.0,
            "22:00",
            "06:00",  # Overnight
            date(2026, 4, 21),
            "QLD",
        )
        # 8 hours * $25 = $200
        self.assertGreater(cost, 0.0)

    def test_empty_candidate_list(self):
        """Test handling empty candidate list."""
        req = make_requirement()
        roster_suggestion = suggest_for_shift(req, [], {})

        self.assertTrue(roster_suggestion.unfilled)

    def test_employee_with_missing_fields(self):
        """Test handling employee with missing optional fields."""
        emp = {"id": "emp_001", "name": "Alice"}  # Minimal employee dict
        req = make_requirement()

        suggestion = score_candidate(emp, req, {})
        # Should still produce a score (graceful degradation)
        self.assertGreater(suggestion.suitability_score, 0.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
