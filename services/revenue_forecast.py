"""
Revenue Forecasting engine for RosterIQ.

Trains on historical POS data to predict venue revenue and labour budgets.
Adjusts for weather, events, day-of-week patterns, and trends.

All monetary values in AUD as Decimal.
"""

from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, List, Dict, Tuple
import statistics
import logging

from rosteriq.models import Roster, Shift
from rosteriq.database import get_db

logger = logging.getLogger(__name__)


@dataclass
class RevenueEstimate:
    """Revenue forecast for a single day."""
    venue_id: str
    target_date: date
    daily_total: Decimal
    hourly_breakdown: Dict[int, Decimal]  # hour (0-23) -> revenue
    confidence: float  # 0.0-1.0
    factors: Dict[str, float] = field(default_factory=dict)  # day_of_week, trend, etc.


@dataclass
class BudgetCheckResult:
    """Result of labour budget vs revenue check."""
    venue_id: str
    target_date: date
    predicted_revenue: Decimal
    roster_labour_cost: Decimal
    labour_pct: float  # labour_cost / revenue * 100
    target_labour_pct: float
    within_budget: bool  # labour_pct < target
    savings_opportunity: Optional[Decimal] = None  # if overstaffed
    risk_flag: Optional[str] = None  # if understaffed


class RevenueForecaster:
    """Trains on historical POS data and forecasts future revenue."""

    def __init__(self):
        self.db = get_db()
        self.models: Dict[str, dict] = {}  # venue_id -> model dict
        self.actuals: Dict[str, list] = {}  # venue_id -> list of actual records

    def train(
        self,
        venue_id: str,
        lookback_days: int = 90,
        start_date: Optional[date] = None,
    ) -> Dict:
        """
        Train revenue model on historical POS data.

        Learns:
        - Day-of-week patterns (Mon-Sun revenue multipliers)
        - Hourly distribution within day
        - Trend (linear regression)
        - Base revenue level

        Args:
            venue_id: Venue to train for
            lookback_days: How many days of history to use (default 90)
            start_date: Optional override for training period end

        Returns:
            Model dict with learned parameters
        """
        if start_date is None:
            start_date = date.today()

        start_of_period = start_date - timedelta(days=lookback_days)

        # Retrieve historical actuals from database
        actuals = self.db.list_revenue_actuals(
            venue_id,
            start=start_of_period.isoformat(),
            end=start_date.isoformat(),
        )

        if not actuals or len(actuals) < 7:
            logger.warning(
                f"Insufficient data to train model for {venue_id} "
                f"(need >= 7 days, got {len(actuals)})"
            )
            return self._empty_model(venue_id)

        # Parse actuals
        daily_revenues: Dict[date, Decimal] = {}
        hourly_distributions: Dict[int, List[Decimal]] = {i: [] for i in range(24)}

        for actual_record in actuals:
            day_str = actual_record["date"]
            day = datetime.fromisoformat(day_str).date()
            daily_total = Decimal(str(actual_record["daily_total"]))
            daily_revenues[day] = daily_total

            # Extract hourly breakdown if available
            hourly = actual_record.get("hourly_breakdown", {})
            for hour_str, revenue_str in hourly.items():
                try:
                    hour = int(hour_str)
                    revenue = Decimal(str(revenue_str))
                    if 0 <= hour < 24:
                        hourly_distributions[hour].append(revenue)
                except (ValueError, TypeError):
                    pass

        # Calculate day-of-week multipliers
        dow_revenues: Dict[int, List[Decimal]] = {i: [] for i in range(7)}
        for day, revenue in daily_revenues.items():
            dow = day.weekday()  # 0=Mon, 6=Sun
            dow_revenues[dow].append(revenue)

        dow_multipliers = {}
        base_revenue = Decimal("0")
        for dow in range(7):
            if dow_revenues[dow]:
                avg = sum(dow_revenues[dow]) / len(dow_revenues[dow])
                dow_multipliers[dow] = float(avg)
                base_revenue = max(base_revenue, avg)

        if base_revenue == 0:
            base_revenue = Decimal("1000")  # Fallback

        # Normalise multipliers
        for dow in dow_multipliers:
            dow_multipliers[dow] = dow_multipliers[dow] / float(base_revenue)

        # Calculate hourly distribution (average % of daily revenue per hour)
        hourly_pcts = {}
        total_hourly_sum = Decimal("0")
        for hour in range(24):
            if hourly_distributions[hour]:
                avg_hourly = (
                    sum(hourly_distributions[hour]) / len(hourly_distributions[hour])
                )
                hourly_pcts[hour] = float(avg_hourly)
                total_hourly_sum += avg_hourly

        if total_hourly_sum > 0:
            for hour in hourly_pcts:
                hourly_pcts[hour] = hourly_pcts[hour] / float(total_hourly_sum)
        else:
            # Fallback: uniform distribution
            for hour in range(24):
                hourly_pcts[hour] = 1.0 / 24.0

        # Simple trend: linear regression on daily revenues
        if len(daily_revenues) >= 2:
            days_sorted = sorted(daily_revenues.keys())
            x_vals = list(range(len(days_sorted)))
            y_vals = [float(daily_revenues[d]) for d in days_sorted]

            # Linear regression (y = mx + b)
            n = len(x_vals)
            mean_x = sum(x_vals) / n
            mean_y = sum(y_vals) / n

            numerator = sum(
                (x_vals[i] - mean_x) * (y_vals[i] - mean_y) for i in range(n)
            )
            denominator = sum((x_vals[i] - mean_x) ** 2 for i in range(n))

            slope = numerator / denominator if denominator > 0 else 0.0
            intercept = mean_y - slope * mean_x

            trend = {"slope": slope, "intercept": intercept}
        else:
            trend = {"slope": 0.0, "intercept": float(base_revenue)}

        model = {
            "venue_id": venue_id,
            "trained_at": datetime.utcnow().isoformat(),
            "base_revenue": float(base_revenue),
            "dow_multipliers": dow_multipliers,
            "hourly_pcts": hourly_pcts,
            "trend": trend,
            "sample_size": len(daily_revenues),
            "confidence": min(1.0, len(daily_revenues) / 90.0),
        }

        self.models[venue_id] = model
        self.db.save_revenue_model(venue_id, model)

        logger.info(
            f"Trained revenue model for {venue_id}: "
            f"base=${base_revenue}, trend slope={trend['slope']:.2f}"
        )

        return model

    def predict_revenue(
        self,
        venue_id: str,
        target_date: date,
        weather_modifier: float = 1.0,
        event_modifier: float = 1.0,
        target_labour_pct: float = 0.30,
    ) -> RevenueEstimate:
        """
        Predict revenue for a specific date.

        Uses trained model with adjustments for:
        - Day-of-week pattern
        - Weather (e.g., rainy day might reduce footfall)
        - Events (e.g., local festival increases demand)
        - Trend (gradual increase/decrease over time)

        Args:
            venue_id: Venue to predict for
            target_date: Target date
            weather_modifier: 0.5-1.5 (0.8 = 20% reduction)
            event_modifier: 0.5-2.0 (1.2 = 20% increase)
            target_labour_pct: Target labour cost as % of revenue

        Returns:
            RevenueEstimate with daily_total and hourly breakdown
        """
        # Load or train model
        if venue_id not in self.models:
            self.models[venue_id] = (
                self.db.get_revenue_model(venue_id) or self.train(venue_id)
            )

        model = self.models[venue_id]

        if not model or model.get("base_revenue") is None:
            # Fallback: neutral estimate
            return RevenueEstimate(
                venue_id=venue_id,
                target_date=target_date,
                daily_total=Decimal("1000"),
                hourly_breakdown={i: Decimal("42") for i in range(24)},
                confidence=0.0,
            )

        # Base revenue adjusted for day-of-week
        dow = target_date.weekday()  # 0=Mon, 6=Sun
        dow_mult = model["dow_multipliers"].get(dow, 1.0)
        base = Decimal(str(model["base_revenue"])) * Decimal(str(dow_mult))

        # Apply trend
        trend = model["trend"]
        trend_adjustment = 1.0 + (trend["slope"] / float(base)) if base > 0 else 1.0
        base = base * Decimal(str(max(0.5, trend_adjustment)))

        # Apply modifiers
        daily_total = base * Decimal(str(weather_modifier)) * Decimal(str(event_modifier))
        daily_total = daily_total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        # Hourly breakdown
        hourly_breakdown = {}
        hourly_pcts = model["hourly_pcts"]
        for hour in range(24):
            pct = hourly_pcts.get(hour, 1.0 / 24.0)
            hourly_revenue = daily_total * Decimal(str(pct))
            hourly_breakdown[hour] = hourly_revenue.quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )

        estimate = RevenueEstimate(
            venue_id=venue_id,
            target_date=target_date,
            daily_total=daily_total,
            hourly_breakdown=hourly_breakdown,
            confidence=model.get("confidence", 0.5),
            factors={
                "day_of_week": dow_mult,
                "weather": weather_modifier,
                "event": event_modifier,
                "trend": trend_adjustment,
            },
        )

        return estimate

    def predict_week(
        self,
        venue_id: str,
        start_date: date,
        weather_modifiers: Optional[Dict[date, float]] = None,
        event_modifiers: Optional[Dict[date, float]] = None,
        target_labour_pct: float = 0.30,
    ) -> List[RevenueEstimate]:
        """
        Predict revenue for a full week.

        Args:
            venue_id: Venue to predict for
            start_date: Monday of the week
            weather_modifiers: Optional dict of date -> modifier
            event_modifiers: Optional dict of date -> modifier
            target_labour_pct: Target labour cost %

        Returns:
            List of 7 RevenueEstimate objects
        """
        estimates = []

        for i in range(7):
            target_date = start_date + timedelta(days=i)
            weather_mod = (weather_modifiers or {}).get(target_date, 1.0)
            event_mod = (event_modifiers or {}).get(target_date, 1.0)

            estimate = self.predict_revenue(
                venue_id,
                target_date,
                weather_modifier=weather_mod,
                event_modifier=event_mod,
                target_labour_pct=target_labour_pct,
            )
            estimates.append(estimate)

        return estimates

    def labour_budget_check(
        self,
        venue_id: str,
        roster: Roster,
        target_labour_pct: float = 0.30,
    ) -> BudgetCheckResult:
        """
        Check roster labour cost against predicted revenue.

        Args:
            venue_id: Venue to check
            roster: The roster to validate
            target_labour_pct: Target labour cost % (usually 0.30 = 30%)

        Returns:
            BudgetCheckResult with budget status and opportunities
        """
        # Get first date from roster
        if not roster.shifts:
            raise ValueError("Roster has no shifts")

        target_date = roster.shifts[0].start_time.date()

        # Predict revenue
        estimate = self.predict_revenue(venue_id, target_date)
        predicted_revenue = estimate.daily_total

        # Calculate roster labour cost
        roster_labour_cost = Decimal("0")
        for shift in roster.shifts:
            if shift.cost:
                roster_labour_cost += shift.cost

        # Calculate labour percentage
        if predicted_revenue > 0:
            labour_pct = (roster_labour_cost / predicted_revenue) * 100
        else:
            labour_pct = 0.0

        within_budget = labour_pct < (target_labour_pct * 100)

        savings_opportunity = None
        risk_flag = None

        if within_budget:
            # Calculate potential savings if we reduce staff
            target_cost = predicted_revenue * Decimal(str(target_labour_pct))
            if roster_labour_cost > target_cost:
                savings_opportunity = roster_labour_cost - target_cost
        else:
            # Understaffed risk
            budget_cost = predicted_revenue * Decimal(str(target_labour_pct))
            if roster_labour_cost > budget_cost:
                risk_flag = (
                    "understaffed_risk: roster cost exceeds budget despite "
                    f"high labour %; consider reducing hours or increasing revenue"
                )

        return BudgetCheckResult(
            venue_id=venue_id,
            target_date=target_date,
            predicted_revenue=predicted_revenue,
            roster_labour_cost=roster_labour_cost,
            labour_pct=float(labour_pct),
            target_labour_pct=target_labour_pct * 100,
            within_budget=within_budget,
            savings_opportunity=savings_opportunity,
            risk_flag=risk_flag,
        )

    def track_accuracy(
        self,
        venue_id: str,
        date_val: date,
        actual_revenue: Dict,
    ) -> Dict:
        """
        Record actual revenue for accuracy tracking.

        Args:
            venue_id: Venue ID
            date_val: Date of actual revenue
            actual_revenue: Dict with 'daily_total' and optionally 'hourly_breakdown'

        Returns:
            Accuracy metrics
        """
        self.db.save_revenue_actual(venue_id, date_val.isoformat(), actual_revenue)

        # Calculate accuracy vs previous predictions if available
        # (Simple placeholder for now)
        return {
            "venue_id": venue_id,
            "date": date_val.isoformat(),
            "actual_revenue": str(actual_revenue.get("daily_total")),
            "recorded": True,
        }

    def _empty_model(self, venue_id: str) -> Dict:
        """Return a neutral/empty model when no training data."""
        return {
            "venue_id": venue_id,
            "trained_at": datetime.utcnow().isoformat(),
            "base_revenue": 1000.0,
            "dow_multipliers": {i: 1.0 for i in range(7)},
            "hourly_pcts": {i: 1.0 / 24.0 for i in range(24)},
            "trend": {"slope": 0.0, "intercept": 1000.0},
            "sample_size": 0,
            "confidence": 0.0,
        }
