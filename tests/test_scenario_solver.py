"""Tests for rosteriq.scenario_solver — bidirectional wage-cost calculator.

Pure stdlib, no pytest. Tests three main modes:
  1. solve_required_sales — given wage cost and target %, what sales do I need?
  2. solve_wage_budget — given forecast sales and target %, what's the wage budget?
  3. diagnose — given both wage cost and sales, what's the current wage %?
"""
from __future__ import annotations

import sys
import unittest
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq.scenario_solver import (  # noqa: E402
    ScenarioMode,
    ScenarioResult,
    diagnose,
    normalise_pct,
    solve_required_sales,
    solve_wage_budget,
    DEFAULT_ON_COST_MULTIPLIER,
)


# ---------------------------------------------------------------------------
# Test Cases
# ---------------------------------------------------------------------------

class TestNormalisePct(unittest.TestCase):
    """Test percentage normalization: 0.18, 18, and "18%" should all normalize to Decimal('0.18')."""

    def test_normalize_fraction(self):
        """0.18 should stay as Decimal('0.18')."""
        result = normalise_pct(0.18)
        self.assertEqual(result, Decimal("0.18"))

    def test_normalize_whole_number(self):
        """18 should convert to Decimal('0.18')."""
        result = normalise_pct(18)
        self.assertEqual(result, Decimal("0.18"))

    def test_normalize_string_with_percent(self):
        """'18%' should convert to Decimal('0.18')."""
        result = normalise_pct("18%")
        self.assertEqual(result, Decimal("0.18"))

    def test_normalize_string_bare(self):
        """'18' should convert to Decimal('0.18')."""
        result = normalise_pct("18")
        self.assertEqual(result, Decimal("0.18"))

    def test_normalize_decimal_input(self):
        """Decimal('0.25') should stay as Decimal('0.25')."""
        result = normalise_pct(Decimal("0.25"))
        self.assertEqual(result, Decimal("0.25"))

    def test_reject_zero_pct(self):
        """Zero or negative pct should raise ValueError."""
        with self.assertRaises(ValueError):
            normalise_pct(0)
        with self.assertRaises(ValueError):
            normalise_pct(-5)

    def test_reject_none(self):
        """None should raise ValueError."""
        with self.assertRaises(ValueError):
            normalise_pct(None)


class TestSolveRequiredSales(unittest.TestCase):
    """Test solve_required_sales: 'I want 18% wage cost, what sales do I need?'"""

    def test_basic_solve_no_forecast(self):
        """Given wage_cost=$1000 and target=20%, required_sales=$5000."""
        result = solve_required_sales(
            wage_cost=Decimal("1000"),
            target_wage_cost_pct=0.20,
        )
        self.assertEqual(result.mode, ScenarioMode.SOLVE_SALES)
        self.assertEqual(result.target_wage_cost_pct, Decimal("0.20"))
        self.assertEqual(result.outputs["required_sales"], Decimal("5000.00"))
        self.assertIn("forecast_sales", result.inputs)
        self.assertIsNone(result.inputs["forecast_sales"])

    def test_solve_with_forecast_upside(self):
        """When forecast exceeds required sales, we have headroom."""
        result = solve_required_sales(
            wage_cost=Decimal("1000"),
            target_wage_cost_pct=0.20,
            forecast_sales=Decimal("5500"),
        )
        self.assertEqual(result.outputs["required_sales"], Decimal("5000.00"))
        self.assertEqual(result.outputs["forecast_sales"], Decimal("5500.00"))
        gap = result.outputs["gap_to_forecast"]
        self.assertLess(gap, 0)  # negative gap = upside
        self.assertGreater(len(result.suggestions), 0)

    def test_solve_with_forecast_shortfall(self):
        """When forecast is less than required sales, we have a gap."""
        result = solve_required_sales(
            wage_cost=Decimal("1000"),
            target_wage_cost_pct=0.20,
            forecast_sales=Decimal("4500"),
        )
        gap = result.outputs["gap_to_forecast"]
        self.assertGreater(gap, 0)  # positive gap = shortfall
        self.assertGreater(len(result.suggestions), 0)

    def test_solve_with_matching_forecast(self):
        """When forecast exactly matches required sales."""
        result = solve_required_sales(
            wage_cost=Decimal("1000"),
            target_wage_cost_pct=0.20,
            forecast_sales=Decimal("5000"),
        )
        gap = result.outputs["gap_to_forecast"]
        self.assertEqual(gap, Decimal("0.00"))

    def test_normalizes_target_pct_from_whole_number(self):
        """Target 18 (not 0.18) should still work."""
        result = solve_required_sales(
            wage_cost=Decimal("1000"),
            target_wage_cost_pct=18,
        )
        # 18% of sales should equal $1000
        # sales = 1000 / 0.18 ≈ 5555.56
        expected_sales = Decimal("1000") / Decimal("0.18")
        self.assertAlmostEqual(
            float(result.outputs["required_sales"]),
            float(expected_sales),
            places=1
        )

    def test_reject_zero_wage_cost(self):
        """Wage cost <= 0 should raise ValueError."""
        with self.assertRaises(ValueError):
            solve_required_sales(
                wage_cost=Decimal("0"),
                target_wage_cost_pct=0.20,
            )

    def test_reject_none_wage_cost(self):
        """None wage_cost should raise ValueError."""
        with self.assertRaises(ValueError):
            solve_required_sales(
                wage_cost=None,
                target_wage_cost_pct=0.20,
            )

    def test_wildly_high_target_triggers_warning(self):
        """Target > 80% should add a warning."""
        result = solve_required_sales(
            wage_cost=Decimal("1000"),
            target_wage_cost_pct=0.90,
        )
        self.assertGreater(len(result.warnings), 0)
        self.assertIn("outside", result.warnings[0].lower())

    def test_wildly_low_target_triggers_warning(self):
        """Target < 5% should add a warning."""
        result = solve_required_sales(
            wage_cost=Decimal("1000"),
            target_wage_cost_pct=0.02,
        )
        self.assertGreater(len(result.warnings), 0)


class TestSolveWageBudget(unittest.TestCase):
    """Test solve_wage_budget: 'Given $28k sales and 18% target, what's my budget?'"""

    def test_basic_solve_no_rate(self):
        """forecast_sales=$28000, target=18%, => wage_budget=$5040."""
        result = solve_wage_budget(
            forecast_sales=Decimal("28000"),
            target_wage_cost_pct=0.18,
        )
        self.assertEqual(result.mode, ScenarioMode.SOLVE_WAGE_BUDGET)
        self.assertEqual(result.target_wage_cost_pct, Decimal("0.18"))
        self.assertEqual(result.outputs["target_wage_cost"], Decimal("5040.00"))

    def test_solve_with_blended_rate(self):
        """With blended_hourly_rate, should compute affordable_hours."""
        result = solve_wage_budget(
            forecast_sales=Decimal("28000"),
            target_wage_cost_pct=0.18,
            blended_hourly_rate=Decimal("35"),
        )
        wage_budget = result.outputs["target_wage_cost"]  # $5040
        loaded_rate = result.outputs["fully_loaded_hourly_rate"]
        affordable_hours = result.outputs["affordable_hours"]

        # loaded_rate = 35 * 1.165 ≈ 40.775
        expected_loaded = Decimal("35") * DEFAULT_ON_COST_MULTIPLIER
        self.assertAlmostEqual(
            float(loaded_rate),
            float(expected_loaded),
            places=1
        )
        # affordable_hours = 5040 / loaded_rate ≈ 123.5
        expected_hours = wage_budget / loaded_rate
        self.assertAlmostEqual(
            float(affordable_hours),
            float(expected_hours),
            places=1
        )

    def test_solve_with_custom_on_cost(self):
        """Custom on_cost_multiplier should override default."""
        result = solve_wage_budget(
            forecast_sales=Decimal("28000"),
            target_wage_cost_pct=0.18,
            blended_hourly_rate=Decimal("35"),
            on_cost_multiplier=Decimal("1.25"),  # higher than default 1.165
        )
        loaded_rate = result.outputs["fully_loaded_hourly_rate"]
        expected = Decimal("35") * Decimal("1.25")
        self.assertEqual(loaded_rate, expected)

    def test_solve_with_planned_wage_cost_underbudget(self):
        """When planned_wage_cost < budget, we have headroom."""
        result = solve_wage_budget(
            forecast_sales=Decimal("28000"),
            target_wage_cost_pct=0.18,
            planned_wage_cost=Decimal("4800"),  # less than $5040 budget
        )
        headroom = result.outputs["headroom_vs_planned"]
        self.assertEqual(headroom, Decimal("240.00"))
        self.assertGreater(len(result.suggestions), 0)

    def test_solve_with_planned_wage_cost_overbudget(self):
        """When planned_wage_cost > budget, we have overspend."""
        result = solve_wage_budget(
            forecast_sales=Decimal("28000"),
            target_wage_cost_pct=0.18,
            planned_wage_cost=Decimal("5500"),  # more than $5040 budget
        )
        headroom = result.outputs["headroom_vs_planned"]
        self.assertLess(headroom, 0)  # negative = overspend
        overspend = abs(headroom)
        self.assertEqual(overspend, Decimal("460.00"))

    def test_solve_with_planned_wage_cost_on_target(self):
        """When planned_wage_cost == budget, headroom is 0."""
        result = solve_wage_budget(
            forecast_sales=Decimal("28000"),
            target_wage_cost_pct=0.18,
            planned_wage_cost=Decimal("5040"),
        )
        headroom = result.outputs["headroom_vs_planned"]
        self.assertEqual(headroom, Decimal("0.00"))

    def test_normalizes_target_pct_from_whole_number(self):
        """Target 18 (not 0.18) should work."""
        result = solve_wage_budget(
            forecast_sales=Decimal("28000"),
            target_wage_cost_pct=18,
        )
        expected = Decimal("28000") * Decimal("0.18")
        self.assertEqual(result.outputs["target_wage_cost"], expected)

    def test_reject_zero_sales(self):
        """forecast_sales <= 0 should raise ValueError."""
        with self.assertRaises(ValueError):
            solve_wage_budget(
                forecast_sales=Decimal("0"),
                target_wage_cost_pct=0.18,
            )

    def test_reject_none_sales(self):
        """None forecast_sales should raise ValueError."""
        with self.assertRaises(ValueError):
            solve_wage_budget(
                forecast_sales=None,
                target_wage_cost_pct=0.18,
            )


class TestDiagnose(unittest.TestCase):
    """Test diagnose: 'What's my current wage % given wage_cost and sales?'"""

    def test_basic_diagnose(self):
        """wage_cost=$1000, sales=$5000 => 20% wage cost."""
        result = diagnose(
            wage_cost=Decimal("1000"),
            forecast_sales=Decimal("5000"),
        )
        self.assertEqual(result.mode, ScenarioMode.DIAGNOSE)
        current_pct = result.outputs["current_wage_pct"]
        self.assertEqual(current_pct, Decimal("20.00"))

    def test_diagnose_with_target(self):
        """When a target is supplied, compute gap."""
        result = diagnose(
            wage_cost=Decimal("1000"),
            forecast_sales=Decimal("5000"),
            target_wage_cost_pct=0.18,
        )
        current_pct = result.outputs["current_wage_pct"]
        self.assertEqual(current_pct, Decimal("20.00"))
        target_wage = result.outputs["target_wage_cost"]
        self.assertEqual(target_wage, Decimal("900.00"))  # 5000 * 0.18
        gap = result.outputs["gap_vs_target"]
        self.assertEqual(gap, Decimal("100.00"))  # 1000 - 900

    def test_diagnose_red_zone(self):
        """When gap > 2 pts above target, suggest red."""
        result = diagnose(
            wage_cost=Decimal("1500"),
            forecast_sales=Decimal("5000"),
            target_wage_cost_pct=0.18,
        )
        current_pct = Decimal("30.00")
        self.assertEqual(result.outputs["current_wage_pct"], current_pct)
        # 30% is 12 points above 18%, triggers red suggestion
        suggestions_text = " ".join(result.suggestions).lower()
        self.assertIn("red", suggestions_text)

    def test_diagnose_amber_zone(self):
        """When 0 < gap <= 2 pts above target, suggest amber."""
        result = diagnose(
            wage_cost=Decimal("980"),
            forecast_sales=Decimal("5000"),
            target_wage_cost_pct=0.18,
        )
        current_pct = Decimal("19.60")
        self.assertEqual(result.outputs["current_wage_pct"], current_pct)
        # 19.6% is 1.6 points above 18%, triggers amber (not red)
        suggestions_text = " ".join(result.suggestions).lower()
        self.assertIn("amber", suggestions_text)

    def test_diagnose_green_zone(self):
        """When gap <= 0, suggest green."""
        result = diagnose(
            wage_cost=Decimal("850"),
            forecast_sales=Decimal("5000"),
            target_wage_cost_pct=0.18,
        )
        current_pct = Decimal("17.00")
        self.assertEqual(result.outputs["current_wage_pct"], current_pct)
        suggestions_text = " ".join(result.suggestions).lower()
        self.assertIn("green", suggestions_text)

    def test_diagnose_without_target(self):
        """target_wage_cost_pct not supplied => no target_wage_cost in outputs."""
        result = diagnose(
            wage_cost=Decimal("1000"),
            forecast_sales=Decimal("5000"),
        )
        self.assertNotIn("target_wage_cost", result.outputs)
        self.assertNotIn("gap_vs_target", result.outputs)
        self.assertEqual(result.target_wage_cost_pct, Decimal("0"))

    def test_reject_zero_wage_cost(self):
        """wage_cost <= 0 should raise ValueError."""
        with self.assertRaises(ValueError):
            diagnose(
                wage_cost=Decimal("0"),
                forecast_sales=Decimal("5000"),
            )

    def test_reject_zero_sales(self):
        """forecast_sales <= 0 should raise ValueError."""
        with self.assertRaises(ValueError):
            diagnose(
                wage_cost=Decimal("1000"),
                forecast_sales=Decimal("0"),
            )

    def test_small_decimals(self):
        """Test with small wage cost and large sales."""
        result = diagnose(
            wage_cost=Decimal("100"),
            forecast_sales=Decimal("10000"),
        )
        current_pct = result.outputs["current_wage_pct"]
        self.assertEqual(current_pct, Decimal("1.00"))


class TestScenarioResultToDict(unittest.TestCase):
    """Test ScenarioResult.to_dict() serialization."""

    def test_to_dict_serializes_decimals_to_floats(self):
        """Decimal values should convert to float for JSON."""
        result = solve_required_sales(
            wage_cost=Decimal("1000"),
            target_wage_cost_pct=0.20,
        )
        d = result.to_dict()
        self.assertIsInstance(d["target_wage_cost_pct"], float)
        self.assertIsInstance(d["outputs"]["required_sales"], float)

    def test_to_dict_includes_all_fields(self):
        """to_dict() should have mode, inputs, outputs, assumptions, warnings, suggestions."""
        result = solve_wage_budget(
            forecast_sales=Decimal("28000"),
            target_wage_cost_pct=0.18,
            blended_hourly_rate=Decimal("35"),
        )
        d = result.to_dict()
        self.assertIn("mode", d)
        self.assertIn("target_wage_cost_pct", d)
        self.assertIn("inputs", d)
        self.assertIn("outputs", d)
        self.assertIn("assumptions", d)
        self.assertIn("warnings", d)
        self.assertIn("suggestions", d)


class TestEdgeCases(unittest.TestCase):
    """Edge cases and robustness."""

    def test_very_high_wage_cost(self):
        """High wage cost should still compute correctly."""
        result = solve_required_sales(
            wage_cost=Decimal("50000"),
            target_wage_cost_pct=0.20,
        )
        expected_sales = Decimal("50000") / Decimal("0.20")
        self.assertEqual(result.outputs["required_sales"], expected_sales)

    def test_very_small_wage_cost(self):
        """Small wage cost should still compute."""
        result = solve_required_sales(
            wage_cost=Decimal("10"),
            target_wage_cost_pct=0.20,
        )
        expected_sales = Decimal("10") / Decimal("0.20")
        self.assertEqual(result.outputs["required_sales"], expected_sales)

    def test_very_low_target_pct(self):
        """Very low target (e.g. 1%) should still compute (with warning)."""
        result = solve_wage_budget(
            forecast_sales=Decimal("10000"),
            target_wage_cost_pct=0.01,
        )
        expected_budget = Decimal("10000") * Decimal("0.01")
        self.assertEqual(result.outputs["target_wage_cost"], expected_budget)
        # Should have a warning about this being unrealistic
        self.assertGreater(len(result.warnings), 0)

    def test_decimal_precision_maintained(self):
        """Decimal arithmetic should maintain precision."""
        result = solve_wage_budget(
            forecast_sales=Decimal("33333.33"),
            target_wage_cost_pct=0.175,  # unusual percentage
        )
        # Verify rounding to 2 decimal places happens (cents)
        wage_cost = result.outputs["target_wage_cost"]
        # The result should be rounded to cents
        self.assertEqual(wage_cost, Decimal("5833.33"))
        # Verify it's quantized to 2 decimal places
        self.assertEqual(wage_cost.as_tuple().exponent, -2)

    def test_assumes_wage_cost_already_loaded(self):
        """The solver assumes wage_cost includes super, penalties, etc."""
        result = solve_required_sales(
            wage_cost=Decimal("1000"),
            target_wage_cost_pct=0.20,
        )
        # Should have an assumption about this
        assumptions_text = " ".join(result.assumptions).lower()
        self.assertIn("loaded", assumptions_text)


if __name__ == "__main__":
    unittest.main()
