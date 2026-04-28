"""
Forecast accuracy tracking and analysis service for RosterIQ.

Provides comprehensive accuracy metrics, breakdowns by day/hour/signal type,
accuracy trending, model comparison, pattern identification, and actionable
improvement suggestions for demand forecasting.

All calculations use pure Python (math module only, no numpy/scipy).
"""

import math
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from collections import defaultdict

from rosteriq.database import BaseStore
from rosteriq.models import DemandForecast, SignalType
from rosteriq.award_rules import get_day_type

logger = logging.getLogger(__name__)


# ============================================================================
# Data Contracts
# ============================================================================


@dataclass
class DayAccuracy:
    """Accuracy metrics for a specific day of week."""
    day_of_week: str
    mape: Optional[float]
    mae: float
    rmse: float
    samples: int


@dataclass
class HourAccuracy:
    """Accuracy metrics for a specific hour of day."""
    hour: int
    mape: Optional[float]
    mae: float
    rmse: float
    samples: int
    avg_predicted: float
    avg_actual: float


@dataclass
class WeeklyAccuracy:
    """Weekly accuracy snapshot."""
    week_ending: date
    mape: Optional[float]
    mae: float
    rmse: float
    samples: int
    directional_accuracy: float


@dataclass
class PredictionDetail:
    """Details of a single prediction for best/worst lists."""
    date: date
    hour: int
    predicted_covers: float
    actual_covers: float
    error: float
    absolute_error: float
    percentage_error: Optional[float]
    confidence: float
    signals_used: List[str]


@dataclass
class AccuracyReport:
    """Complete forecast accuracy analysis report."""
    venue_id: str
    period_start: date
    period_end: date

    # Overall metrics
    overall_mape: Optional[float]
    overall_rmse: float
    overall_mae: float
    overall_bias: float
    overall_r_squared: float
    directional_accuracy: float

    # Breakdowns
    by_day_of_week: Dict[str, DayAccuracy]
    by_hour: Dict[int, HourAccuracy]
    by_signal: Dict[str, float]
    weekly_trend: List[WeeklyAccuracy]

    # Best and worst
    worst_predictions: List[PredictionDetail]
    best_predictions: List[PredictionDetail]

    # Recommendations
    recommendations: List[str]


class ForecastAccuracyService:
    """Service for tracking and analyzing forecast accuracy."""

    def __init__(self, db: BaseStore):
        """Initialize with database connection."""
        self.db = db

    # ========================================================================
    # Core Accuracy Calculation
    # ========================================================================

    def calculate_accuracy(
        self,
        venue_id: str,
        start_date: date,
        end_date: date,
    ) -> AccuracyReport:
        """
        Calculate comprehensive accuracy metrics comparing predictions to actuals.

        Args:
            venue_id: Target venue
            start_date: Period start
            end_date: Period end

        Returns:
            Complete accuracy report with metrics and breakdowns
        """
        forecasts = self.db.get_forecasts(venue_id, start_date, end_date)
        rosters = self.db.get_rosters_by_date_range(venue_id, start_date, end_date)

        if not forecasts or not rosters:
            return self._empty_report(venue_id, start_date, end_date)

        # Build actual hourly covers from rosters
        actual_by_hour = self._build_actual_hourly_data(rosters)

        # Collect all errors for calculations
        errors = []
        absolute_errors = []
        squared_errors = []
        biases = []
        predictions_details = []

        by_day_of_week = defaultdict(list)
        by_hour_data = defaultdict(list)
        by_signal_data = defaultdict(list)

        # Compare each forecast to actual
        for forecast in forecasts:
            key = (forecast.date, forecast.hour)
            if key not in actual_by_hour:
                continue

            actual = Decimal(str(actual_by_hour[key]["covers"]))
            predicted = Decimal(str(forecast.predicted_covers))

            # Calculate errors
            error = actual - predicted
            abs_error = abs(error)
            sq_error = error ** 2

            errors.append(float(error))
            absolute_errors.append(float(abs_error))
            squared_errors.append(float(sq_error))
            biases.append(float(predicted - actual))  # Positive = over-prediction

            # MAPE component
            if forecast.predicted_covers > 0:
                pct_error = float(abs_error / predicted) * 100
            else:
                pct_error = None

            # Store prediction detail
            detail = PredictionDetail(
                date=forecast.date,
                hour=forecast.hour,
                predicted_covers=forecast.predicted_covers,
                actual_covers=float(actual),
                error=float(error),
                absolute_error=float(abs_error),
                percentage_error=pct_error,
                confidence=forecast.confidence,
                signals_used=[s.value for s in forecast.signals_used],
            )
            predictions_details.append(detail)

            # Track by day of week
            day_of_week = forecast.date.strftime("%A")
            by_day_of_week[day_of_week].append(float(abs_error))

            # Track by hour
            by_hour_data[forecast.hour].append({
                "abs_error": float(abs_error),
                "sq_error": float(sq_error),
                "predicted": float(predicted),
                "actual": float(actual),
            })

            # Track by signal
            for signal in forecast.signals_used:
                by_signal_data[signal.value].append(float(abs_error))

        if not errors:
            return self._empty_report(venue_id, start_date, end_date)

        # Calculate overall metrics
        overall_mae = sum(absolute_errors) / len(absolute_errors)
        overall_rmse = math.sqrt(sum(squared_errors) / len(squared_errors))
        overall_bias = sum(biases) / len(biases)

        # MAPE: mean absolute percentage error
        mape_values = [
            float(abs(detail.error / detail.predicted_covers)) * 100
            for detail in predictions_details
            if detail.predicted_covers > 0
        ]
        overall_mape = sum(mape_values) / len(mape_values) if mape_values else None

        # R-squared: coefficient of determination
        mean_actual = sum(d.actual_covers for d in predictions_details) / len(predictions_details)
        ss_tot = sum((d.actual_covers - mean_actual) ** 2 for d in predictions_details)
        ss_res = sum(d.error ** 2 for d in predictions_details)
        overall_r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0

        # Directional accuracy
        directional_correct = 0
        directional_total = 0
        for i in range(len(predictions_details) - 1):
            curr = predictions_details[i]
            next_actual = predictions_details[i + 1].actual_covers
            next_pred = predictions_details[i + 1].predicted_covers

            curr_direction = 1 if next_actual > curr.actual_covers else -1 if next_actual < curr.actual_covers else 0
            pred_direction = 1 if next_pred > curr.predicted_covers else -1 if next_pred < curr.predicted_covers else 0

            if curr_direction == pred_direction and curr_direction != 0:
                directional_correct += 1

            if curr_direction != 0:
                directional_total += 1

        directional_accuracy = (
            (directional_correct / directional_total * 100)
            if directional_total > 0
            else 0
        )

        # Build breakdowns
        by_day_of_week_stats = self._calculate_day_accuracy(by_day_of_week)
        by_hour_stats = self._calculate_hour_accuracy(by_hour_data)
        by_signal_stats = self._calculate_signal_accuracy(by_signal_data)

        # Weekly trend
        weekly_trend = self._calculate_weekly_trend(venue_id, start_date, end_date)

        # Best and worst predictions
        sorted_by_error = sorted(predictions_details, key=lambda x: x.absolute_error, reverse=True)
        worst_predictions = sorted_by_error[:10]
        best_predictions = sorted(sorted_by_error, key=lambda x: x.absolute_error)[:10]

        # Recommendations
        recommendations = self._generate_recommendations(
            overall_mape, overall_mae, by_day_of_week_stats, by_signal_stats
        )

        return AccuracyReport(
            venue_id=venue_id,
            period_start=start_date,
            period_end=end_date,
            overall_mape=overall_mape,
            overall_rmse=overall_rmse,
            overall_mae=overall_mae,
            overall_bias=overall_bias,
            overall_r_squared=overall_r_squared,
            directional_accuracy=directional_accuracy,
            by_day_of_week=by_day_of_week_stats,
            by_hour=by_hour_stats,
            by_signal=by_signal_stats,
            weekly_trend=weekly_trend,
            worst_predictions=worst_predictions,
            best_predictions=best_predictions,
            recommendations=recommendations,
        )

    # ========================================================================
    # Accuracy Trending
    # ========================================================================

    def get_accuracy_trend(
        self,
        venue_id: str,
        weeks: int = 12,
    ) -> List[WeeklyAccuracy]:
        """
        Get weekly accuracy trend showing improvement/degradation over time.

        Args:
            venue_id: Target venue
            weeks: Number of weeks to look back

        Returns:
            List of weekly accuracy snapshots
        """
        trend = []
        end_date = date.today()

        for week_offset in range(weeks):
            week_end = end_date - timedelta(days=week_offset * 7)
            week_start = week_end - timedelta(days=6)

            report = self.calculate_accuracy(venue_id, week_start, week_end)

            if report.overall_mae > 0:  # Only include weeks with data
                trend.append(
                    WeeklyAccuracy(
                        week_ending=week_end,
                        mape=report.overall_mape,
                        mae=report.overall_mae,
                        rmse=report.overall_rmse,
                        samples=len(self.db.get_forecasts(venue_id, week_start, week_end)),
                        directional_accuracy=report.directional_accuracy,
                    )
                )

        return sorted(trend, key=lambda x: x.week_ending)

    # ========================================================================
    # Model Comparison
    # ========================================================================

    def compare_models(
        self,
        venue_id: str,
        start_date: date,
        end_date: date,
    ) -> Dict[str, AccuracyReport]:
        """
        Compare accuracy across different model versions.

        Args:
            venue_id: Target venue
            start_date: Period start
            end_date: Period end

        Returns:
            Dict mapping model_version -> AccuracyReport
        """
        forecasts = self.db.get_forecasts(venue_id, start_date, end_date)

        if not forecasts:
            return {}

        # Group forecasts by model version
        by_model = defaultdict(list)
        for forecast in forecasts:
            by_model[forecast.model_version].append(forecast)

        # For each model, create a pseudo-report
        comparison = {}
        rosters = self.db.get_rosters_by_date_range(venue_id, start_date, end_date)
        actual_by_hour = self._build_actual_hourly_data(rosters)

        for model_version, model_forecasts in by_model.items():
            errors = []
            absolute_errors = []
            squared_errors = []

            for forecast in model_forecasts:
                key = (forecast.date, forecast.hour)
                if key in actual_by_hour:
                    actual = Decimal(str(actual_by_hour[key]["covers"]))
                    predicted = Decimal(str(forecast.predicted_covers))

                    error = actual - predicted
                    errors.append(float(error))
                    absolute_errors.append(float(abs(error)))
                    squared_errors.append(float(error ** 2))

            if errors:
                mae = sum(absolute_errors) / len(absolute_errors)
                rmse = math.sqrt(sum(squared_errors) / len(squared_errors))

                # Build minimal report for this model
                comparison[model_version] = AccuracyReport(
                    venue_id=venue_id,
                    period_start=start_date,
                    period_end=end_date,
                    overall_mape=None,  # Simplified
                    overall_rmse=rmse,
                    overall_mae=mae,
                    overall_bias=0,
                    overall_r_squared=0,
                    directional_accuracy=0,
                    by_day_of_week={},
                    by_hour={},
                    by_signal={},
                    weekly_trend=[],
                    worst_predictions=[],
                    best_predictions=[],
                    recommendations=[],
                )

        return comparison

    # ========================================================================
    # Pattern Identification
    # ========================================================================

    def identify_patterns(self, venue_id: str) -> List[str]:
        """
        Identify systematic prediction patterns in recent data.

        Args:
            venue_id: Target venue

        Returns:
            List of actionable pattern descriptions
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=90)

        report = self.calculate_accuracy(venue_id, start_date, end_date)
        patterns = []

        # Check day-of-week bias
        worst_dow = min(
            report.by_day_of_week.items(),
            key=lambda x: x[1].mae if x[1] else float('inf'),
            default=None,
        )
        if worst_dow and worst_dow[1].mae > report.overall_mae * 1.3:
            patterns.append(
                f"Systematically poor accuracy on {worst_dow[0]}s "
                f"(MAE: {worst_dow[1].mae:.1f} vs avg {report.overall_mae:.1f}). "
                f"Consider venue-specific model for this day."
            )

        # Check time-of-day bias
        worst_hour = min(
            report.by_hour.items(),
            key=lambda x: x[1].mae,
            default=None,
        )
        if worst_hour and worst_hour[1].mae > report.overall_mae * 1.4:
            patterns.append(
                f"Consistently inaccurate during hour {worst_hour[0]:02d}:00. "
                f"Check for recurring events or staff breaks at this time."
            )

        # Check signal effectiveness
        signal_impact = {k: v for k, v in report.by_signal.items()}
        if signal_impact:
            best_signal = min(signal_impact.items(), key=lambda x: x[1])
            worst_signal = max(signal_impact.items(), key=lambda x: x[1])

            if worst_signal[1] > best_signal[1] * 1.5:
                patterns.append(
                    f"Signal '{worst_signal[0]}' degrades accuracy (MAE: {worst_signal[1]:.1f}). "
                    f"Consider reducing its weight in the ensemble."
                )

        # Check bias direction
        if report.overall_bias > report.overall_mae * 0.3:
            patterns.append(
                f"Systematic over-prediction (bias: +{report.overall_bias:.1f}). "
                f"Model is consistently overestimating demand. Check for seasonal trends."
            )
        elif report.overall_bias < -report.overall_mae * 0.3:
            patterns.append(
                f"Systematic under-prediction (bias: {report.overall_bias:.1f}). "
                f"Model is consistently underestimating demand. Check for growth trends."
            )

        # Check R-squared
        if report.overall_r_squared < 0.5:
            patterns.append(
                f"Low correlation with actuals (R²: {report.overall_r_squared:.2f}). "
                f"Model may be missing key driving factors. Review data sources."
            )

        return patterns

    # ========================================================================
    # Improvement Suggestions
    # ========================================================================

    def suggest_improvements(self, report: AccuracyReport) -> List[str]:
        """
        Generate ML-informed suggestions to improve accuracy.

        Args:
            report: Accuracy report from calculate_accuracy()

        Returns:
            List of actionable improvement suggestions
        """
        suggestions = []

        # MAPE-based suggestions
        if report.overall_mape and report.overall_mape > 30:
            suggestions.append(
                "High MAPE (>30%) suggests model is missing key demand drivers. "
                "Consider adding weather, events, or foot traffic signals."
            )
        elif report.overall_mape and report.overall_mape < 10:
            suggestions.append(
                "Excellent MAPE (<10%). Model is performing well; focus on edge cases."
            )

        # RMSE vs MAE indicates outliers
        if report.overall_rmse > report.overall_mae * 1.5:
            suggestions.append(
                "RMSE significantly higher than MAE suggests presence of outliers. "
                "Consider robust regression or outlier detection."
            )

        # Directional accuracy
        if report.directional_accuracy < 55:
            suggestions.append(
                f"Low directional accuracy ({report.directional_accuracy:.0f}%) means "
                "model fails to capture trend reversals. Add momentum or trend features."
            )
        elif report.directional_accuracy > 75:
            suggestions.append(
                "Excellent directional accuracy. Focus on magnitude estimation."
            )

        # R-squared recommendations
        if report.overall_r_squared < 0.3:
            suggestions.append(
                "Poor R² indicates weak model fit. Review feature selection and "
                "ensure sufficient historical training data."
            )
        elif report.overall_r_squared > 0.8:
            suggestions.append("Model fit is excellent. Current approach is sound.")

        # Signal-based suggestions
        if report.by_signal:
            high_mae_signals = [
                (sig, mae) for sig, mae in report.by_signal.items()
                if mae > report.overall_mae * 1.2
            ]
            if high_mae_signals:
                sig_names = ", ".join([s[0] for s in high_mae_signals])
                suggestions.append(
                    f"Signals with degraded accuracy: {sig_names}. "
                    "Consider reweighting or retraining signal components."
                )

        # Day-of-week patterns
        worst_days = [
            (dow, acc.mae) for dow, acc in report.by_day_of_week.items()
            if acc.mae > report.overall_mae * 1.25
        ]
        if worst_days:
            dow_names = ", ".join([d[0] for d in worst_days])
            suggestions.append(
                f"Accuracy drops significantly on {dow_names}. "
                "Consider day-of-week specific features or model variants."
            )

        # Recommendations based on worst predictions
        if report.worst_predictions:
            worst_error = report.worst_predictions[0].percentage_error
            if worst_error and worst_error > 100:
                suggestions.append(
                    "Presence of predictions off by >100%. Review data quality "
                    "and consider outlier handling."
                )

        return suggestions

    # ========================================================================
    # Helper Methods
    # ========================================================================

    def _build_actual_hourly_data(self, rosters) -> Dict[tuple, Dict]:
        """Build actual covers by (date, hour) from rosters."""
        actual_by_hour = defaultdict(lambda: {"covers": Decimal("0.00"), "count": 0})

        for roster in rosters:
            for shift in roster.shifts:
                # Estimate covers served = hours / staff_per_cover ratio
                staff_per_cover = Decimal("0.04")  # 1 staff per 25 covers
                shift_covers = Decimal(str(shift.net_hours)) / staff_per_cover
                hour = shift.start_time.hour

                actual_by_hour[(shift.date, hour)]["covers"] += shift_covers
                actual_by_hour[(shift.date, hour)]["count"] += 1

        return actual_by_hour

    def _calculate_day_accuracy(self, by_day_of_week: Dict[str, List[float]]) -> Dict[str, DayAccuracy]:
        """Calculate accuracy metrics by day of week."""
        result = {}

        for dow, errors in by_day_of_week.items():
            if not errors:
                continue

            mae = sum(errors) / len(errors)
            rmse = math.sqrt(sum(e ** 2 for e in errors) / len(errors))
            result[dow] = DayAccuracy(
                day_of_week=dow,
                mape=None,  # Would need actual values
                mae=mae,
                rmse=rmse,
                samples=len(errors),
            )

        return result

    def _calculate_hour_accuracy(self, by_hour_data: Dict[int, List[Dict]]) -> Dict[int, HourAccuracy]:
        """Calculate accuracy metrics by hour of day."""
        result = {}

        for hour, data_points in by_hour_data.items():
            if not data_points:
                continue

            errors = [d["abs_error"] for d in data_points]
            sq_errors = [d["sq_error"] for d in data_points]
            preds = [d["predicted"] for d in data_points]
            actuals = [d["actual"] for d in data_points]

            mae = sum(errors) / len(errors)
            rmse = math.sqrt(sum(sq_errors) / len(sq_errors))
            avg_pred = sum(preds) / len(preds)
            avg_actual = sum(actuals) / len(actuals)

            result[hour] = HourAccuracy(
                hour=hour,
                mape=None,  # Would need predicted covers
                mae=mae,
                rmse=rmse,
                samples=len(data_points),
                avg_predicted=avg_pred,
                avg_actual=avg_actual,
            )

        return result

    def _calculate_signal_accuracy(self, by_signal_data: Dict[str, List[float]]) -> Dict[str, float]:
        """Calculate MAE for each signal type."""
        result = {}

        for signal, errors in by_signal_data.items():
            if errors:
                result[signal] = sum(errors) / len(errors)

        return result

    def _calculate_weekly_trend(
        self,
        venue_id: str,
        start_date: date,
        end_date: date,
    ) -> List[WeeklyAccuracy]:
        """Calculate weekly accuracy trend."""
        trend = []
        current_date = start_date

        while current_date <= end_date:
            week_start = current_date
            week_end = min(current_date + timedelta(days=6), end_date)

            report = self.calculate_accuracy(venue_id, week_start, week_end)
            if report.overall_mae > 0:
                trend.append(
                    WeeklyAccuracy(
                        week_ending=week_end,
                        mape=report.overall_mape,
                        mae=report.overall_mae,
                        rmse=report.overall_rmse,
                        samples=len(self.db.get_forecasts(venue_id, week_start, week_end)),
                        directional_accuracy=report.directional_accuracy,
                    )
                )

            current_date = week_end + timedelta(days=1)

        return trend

    def _generate_recommendations(
        self,
        mape: Optional[float],
        mae: float,
        by_day_of_week: Dict[str, DayAccuracy],
        by_signal: Dict[str, float],
    ) -> List[str]:
        """Generate actionable recommendations based on metrics."""
        recommendations = []

        if mape is None or mape == 0:
            return []

        # MAPE-based
        if mape > 40:
            recommendations.append(
                "MAPE >40%: Model needs significant retraining with more recent data. "
                "Check data quality and consider ensemble approach."
            )
        elif mape > 25:
            recommendations.append(
                "MAPE 25-40%: Acceptable but can improve. Review top error sources "
                "and add missing signals."
            )

        # Day-of-week analysis
        if by_day_of_week:
            dowmae_values = [a.mae for a in by_day_of_week.values() if a]
            if dowmae_values:
                avg_dow_mae = sum(dowmae_values) / len(dowmae_values)
                for dow, acc in by_day_of_week.items():
                    if acc.mae > avg_dow_mae * 1.3:
                        recommendations.append(
                            f"Focus: {dow} accuracy is {acc.mae/avg_dow_mae:.1f}x worse than average. "
                            f"Create day-specific features or ensemble variant."
                        )

        # Signal analysis
        if by_signal:
            signal_values = [v for v in by_signal.values() if v]
            if signal_values:
                avg_signal = sum(signal_values) / len(signal_values)
                for sig, sig_mae in by_signal.items():
                    if sig_mae > avg_signal * 1.4:
                        recommendations.append(
                            f"Signal '{sig}' contributes excess error. "
                            f"Reduce weight or retrain that component."
                        )

        return recommendations[:5]  # Limit to top 5

    def _empty_report(self, venue_id: str, start_date: date, end_date: date) -> AccuracyReport:
        """Return empty report when no data available."""
        return AccuracyReport(
            venue_id=venue_id,
            period_start=start_date,
            period_end=end_date,
            overall_mape=None,
            overall_rmse=0,
            overall_mae=0,
            overall_bias=0,
            overall_r_squared=0,
            directional_accuracy=0,
            by_day_of_week={},
            by_hour={},
            by_signal={},
            weekly_trend=[],
            worst_predictions=[],
            best_predictions=[],
            recommendations=[],
        )
