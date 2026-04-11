"""
Scenario solver — bidirectional wage-cost calculator.

Answers the two questions a duty manager actually asks:

  A. "I want an 18% wage cost. What sales do I need to hit?"
     -> solve_required_sales(wage_cost, target_pct)

  B. "Forecast is $28k tonight. What's my wage budget and how many hours
      can I afford at our blended rate?"
     -> solve_wage_budget(forecast_sales, target_pct,
                          blended_hourly_rate=..., on_cost_multiplier=...)

Also exposes a diagnostic mode:

  C. diagnose(wage_cost, forecast_sales) -> current wage%.

All monetary inputs/outputs are Decimal for precision. Percentages are
accepted as either a fraction (0.18) or a whole number (18). Normalisation
is explicit so the caller always knows what shape they fed in.

This module is deliberately free of DB / HTTP / FastAPI imports so it is
trivially unit-testable and reusable from the chatbot agent, the live
dashboard, and a future CLI.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import List, Optional


# Default Aussie hospitality on-cost multiplier:
#   1.00 base wage
# + 0.115 superannuation (FY 2025-26)
# + 0.05  workers comp / payroll tax blend (rule-of-thumb, editable)
#   = 1.165
# The solver accepts an override so venues with a different cost-to-company
# factor (e.g. heavier leave loading, penalty-heavy sites) can replace it.
DEFAULT_ON_COST_MULTIPLIER = Decimal("1.165")

# Realistic wage% guardrails for AU hospitality. Outside this range we flag
# the input as suspicious rather than silently producing nonsense.
_MIN_SANE_PCT = Decimal("0.05")  # 5%
_MAX_SANE_PCT = Decimal("0.80")  # 80%

_CENT = Decimal("0.01")
_PCT = Decimal("0.0001")  # 4 dp internally, we surface 2 dp


class ScenarioMode(str, Enum):
    SOLVE_SALES = "solve_sales"            # given wage cost + target %  -> required sales
    SOLVE_WAGE_BUDGET = "solve_wage_budget"  # given sales + target %    -> wage budget
    DIAGNOSE = "diagnose"                  # given both                  -> current wage %


# ---------------------------------------------------------------------------
# Result objects
# ---------------------------------------------------------------------------

@dataclass
class ScenarioResult:
    mode: ScenarioMode
    target_wage_cost_pct: Decimal           # normalised to a fraction (0.18)
    inputs: dict
    outputs: dict
    assumptions: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    suggestions: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "mode": self.mode.value,
            "target_wage_cost_pct": _pct_to_float(self.target_wage_cost_pct),
            "inputs": {k: _jsonable(v) for k, v in self.inputs.items()},
            "outputs": {k: _jsonable(v) for k, v in self.outputs.items()},
            "assumptions": list(self.assumptions),
            "warnings": list(self.warnings),
            "suggestions": list(self.suggestions),
        }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def normalise_pct(value) -> Decimal:
    """Accept 0.18 or 18 or "18%" and return Decimal("0.18")."""
    if value is None:
        raise ValueError("target_wage_cost_pct is required")
    if isinstance(value, str):
        value = value.strip().rstrip("%").strip()
    pct = Decimal(str(value))
    if pct > 1:
        pct = pct / Decimal("100")
    if pct <= 0:
        raise ValueError("target_wage_cost_pct must be > 0")
    return pct


def solve_required_sales(
    wage_cost: Decimal,
    target_wage_cost_pct,
    forecast_sales: Optional[Decimal] = None,
) -> ScenarioResult:
    """
    Inverse mode: given a wage cost (from the planned roster) and a target
    wage%, compute the sales required to hit that target and compare to the
    forecast (if provided).

    required_sales = wage_cost / target_pct
    """
    wage_cost = _require_positive(wage_cost, "wage_cost")
    target = normalise_pct(target_wage_cost_pct)
    _warn_if_wild_target(target, warnings := [])

    required_sales = (wage_cost / target).quantize(_CENT, rounding=ROUND_HALF_UP)

    outputs: dict = {
        "required_sales": required_sales,
    }
    suggestions: List[str] = []

    if forecast_sales is not None:
        forecast_sales = _require_positive(forecast_sales, "forecast_sales")
        gap = (required_sales - forecast_sales).quantize(_CENT, rounding=ROUND_HALF_UP)
        gap_pct = ((gap / forecast_sales) * Decimal("100")).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        current_pct = ((wage_cost / forecast_sales) * Decimal("100")).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        outputs.update(
            {
                "forecast_sales": forecast_sales,
                "gap_to_forecast": gap,
                "gap_pct_of_forecast": gap_pct,
                "current_wage_pct": current_pct,
            }
        )
        target_pct_display = _pct_to_display(target)
        if gap > 0:
            suggestions.append(
                f"Forecast is ${forecast_sales:,.0f} but you need "
                f"${required_sales:,.0f} to land at {target_pct_display}% wage cost — "
                f"${gap:,.0f} short. Options: cut ~${gap * target:,.0f} of "
                f"labour, push a promo, or accept a higher wage% tonight."
            )
        elif gap < 0:
            upside = (-gap).quantize(_CENT, rounding=ROUND_HALF_UP)
            suggestions.append(
                f"Forecast ${forecast_sales:,.0f} is already "
                f"${upside:,.0f} above the sales needed for {target_pct_display}% — "
                f"you have headroom to add one more on-shift if the service "
                f"signals are strong."
            )
        else:
            suggestions.append(
                f"Forecast exactly hits the {target_pct_display}% target. No action."
            )
    else:
        suggestions.append(
            f"Forecast sales not supplied — required sales is the raw break-even "
            f"against the {_pct_to_display(target)}% target. Pair with a forecast "
            f"to get a gap and action."
        )

    return ScenarioResult(
        mode=ScenarioMode.SOLVE_SALES,
        target_wage_cost_pct=target,
        inputs={"wage_cost": wage_cost, "forecast_sales": forecast_sales},
        outputs=outputs,
        assumptions=[
            "wage_cost is already fully loaded (penalties, casual loading, super). "
            "If you passed in a raw base figure, the result will understate the "
            "sales required to hit the target."
        ],
        warnings=warnings,
        suggestions=suggestions,
    )


def solve_wage_budget(
    forecast_sales: Decimal,
    target_wage_cost_pct,
    blended_hourly_rate: Optional[Decimal] = None,
    on_cost_multiplier: Decimal = DEFAULT_ON_COST_MULTIPLIER,
    planned_wage_cost: Optional[Decimal] = None,
) -> ScenarioResult:
    """
    Forward mode: given forecast sales and a target wage%, compute the wage
    budget and optionally the affordable hours at a blended rate.

    wage_budget = forecast_sales * target_pct
    affordable_hours = wage_budget / (blended_hourly_rate * on_cost_multiplier)
    """
    forecast_sales = _require_positive(forecast_sales, "forecast_sales")
    target = normalise_pct(target_wage_cost_pct)
    _warn_if_wild_target(target, warnings := [])

    wage_budget = (forecast_sales * target).quantize(_CENT, rounding=ROUND_HALF_UP)

    outputs: dict = {
        "target_wage_cost": wage_budget,
    }
    assumptions: List[str] = []
    suggestions: List[str] = []

    if blended_hourly_rate is not None:
        blended_hourly_rate = _require_positive(
            blended_hourly_rate, "blended_hourly_rate"
        )
        on_cost_multiplier = _require_positive(
            on_cost_multiplier, "on_cost_multiplier"
        )
        fully_loaded_rate = (blended_hourly_rate * on_cost_multiplier).quantize(
            _CENT, rounding=ROUND_HALF_UP
        )
        affordable_hours = (wage_budget / fully_loaded_rate).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        outputs.update(
            {
                "affordable_hours": affordable_hours,
                "fully_loaded_hourly_rate": fully_loaded_rate,
            }
        )
        assumptions.append(
            f"blended_hourly_rate=${blended_hourly_rate} "
            f"× on_cost_multiplier={on_cost_multiplier} "
            f"⇒ fully-loaded rate ${fully_loaded_rate}/hr "
            f"(super, payroll tax, workers comp)."
        )

    if planned_wage_cost is not None:
        planned_wage_cost = _require_positive(
            planned_wage_cost, "planned_wage_cost"
        )
        headroom = (wage_budget - planned_wage_cost).quantize(
            _CENT, rounding=ROUND_HALF_UP
        )
        current_pct = ((planned_wage_cost / forecast_sales) * Decimal("100")).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        outputs.update(
            {
                "planned_wage_cost": planned_wage_cost,
                "headroom_vs_planned": headroom,
                "current_wage_pct": current_pct,
            }
        )
        target_pct_display = _pct_to_display(target)
        if headroom < 0:
            overspend = (-headroom).quantize(_CENT, rounding=ROUND_HALF_UP)
            suggestions.append(
                f"Planned roster is ${overspend:,.0f} over budget at "
                f"{current_pct}% vs target {target_pct_display}%. Pull "
                f"~{(overspend / ((blended_hourly_rate or Decimal('35')) * on_cost_multiplier)).quantize(Decimal('0.1'))} "
                f"hours from the lowest-urgency shifts to close the gap."
            )
        elif headroom > 0:
            suggestions.append(
                f"Planned roster sits at {current_pct}% — "
                f"${headroom:,.0f} of headroom under the {target_pct_display}% "
                f"target. Safe to add coverage if demand signals spike."
            )
        else:
            suggestions.append(
                f"Planned roster is exactly on the {target_pct_display}% target. "
                f"Hold the line."
            )
    else:
        target_pct_display = _pct_to_display(target)
        suggestions.append(
            f"Tonight's wage budget at {target_pct_display}% on ${forecast_sales:,.0f} "
            f"forecast is ${wage_budget:,.0f}. "
            + (
                f"That buys you about {outputs['affordable_hours']} hours "
                f"at the blended rate."
                if "affordable_hours" in outputs
                else "Supply a blended_hourly_rate to convert that into hours."
            )
        )

    return ScenarioResult(
        mode=ScenarioMode.SOLVE_WAGE_BUDGET,
        target_wage_cost_pct=target,
        inputs={
            "forecast_sales": forecast_sales,
            "blended_hourly_rate": blended_hourly_rate,
            "on_cost_multiplier": on_cost_multiplier,
            "planned_wage_cost": planned_wage_cost,
        },
        outputs=outputs,
        assumptions=assumptions,
        warnings=warnings,
        suggestions=suggestions,
    )


def diagnose(
    wage_cost: Decimal,
    forecast_sales: Decimal,
    target_wage_cost_pct=None,
) -> ScenarioResult:
    """
    Diagnostic mode: caller has both numbers and just wants the live wage%.
    If a target is supplied, we also compute gap-to-target so the dashboard
    widget can colour itself green/amber/red.
    """
    wage_cost = _require_positive(wage_cost, "wage_cost")
    forecast_sales = _require_positive(forecast_sales, "forecast_sales")

    current_pct_fraction = wage_cost / forecast_sales
    current_pct = (current_pct_fraction * Decimal("100")).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )

    outputs: dict = {
        "current_wage_pct": current_pct,
        "wage_cost": wage_cost,
        "forecast_sales": forecast_sales,
    }
    warnings: List[str] = []
    suggestions: List[str] = []
    target = None

    if target_wage_cost_pct is not None:
        target = normalise_pct(target_wage_cost_pct)
        _warn_if_wild_target(target, warnings)
        target_wage_cost = (forecast_sales * target).quantize(
            _CENT, rounding=ROUND_HALF_UP
        )
        gap = (wage_cost - target_wage_cost).quantize(_CENT, rounding=ROUND_HALF_UP)
        outputs["target_wage_cost"] = target_wage_cost
        outputs["gap_vs_target"] = gap
        target_pct_display = _pct_to_display(target)
        if current_pct_fraction > target + Decimal("0.02"):
            suggestions.append(
                f"Wage% is {current_pct}% — more than 2 pts above target "
                f"{target_pct_display}%. Red zone."
            )
        elif current_pct_fraction > target:
            suggestions.append(
                f"Wage% is {current_pct}%, {((current_pct_fraction - target) * 100).quantize(Decimal('0.01'))} "
                f"pts above target {target_pct_display}%. Amber — watch the next signal."
            )
        else:
            suggestions.append(
                f"Wage% {current_pct}% is at or under target "
                f"{target_pct_display}%. Green."
            )

    return ScenarioResult(
        mode=ScenarioMode.DIAGNOSE,
        target_wage_cost_pct=target if target is not None else Decimal("0"),
        inputs={
            "wage_cost": wage_cost,
            "forecast_sales": forecast_sales,
            "target_wage_cost_pct_supplied": target_wage_cost_pct is not None,
        },
        outputs=outputs,
        warnings=warnings,
        suggestions=suggestions,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_positive(value, name: str) -> Decimal:
    if value is None:
        raise ValueError(f"{name} is required")
    d = Decimal(str(value))
    if d <= 0:
        raise ValueError(f"{name} must be > 0")
    return d


def _warn_if_wild_target(target: Decimal, warnings: List[str]) -> None:
    if target < _MIN_SANE_PCT or target > _MAX_SANE_PCT:
        warnings.append(
            f"target_wage_cost_pct={_pct_to_display(target)}% is outside the "
            f"typical AU hospitality range "
            f"({_pct_to_display(_MIN_SANE_PCT)}%-{_pct_to_display(_MAX_SANE_PCT)}%). "
            f"Double-check before acting on this."
        )


def _pct_to_display(fraction: Decimal) -> Decimal:
    return (fraction * Decimal("100")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _pct_to_float(fraction: Decimal) -> float:
    return float(fraction.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))


def _jsonable(value):
    if isinstance(value, Decimal):
        return float(value)
    return value
