"""
A/B testing framework for comparing roster generation strategies.

Provides statistical analysis of different rostering approaches:
- Cost optimisation (minimise labour cost)
- Coverage optimisation (maximise demand coverage)
- Balanced (default behaviour)
- Staff preference (prioritise employee preferences)
- Compliance-first (strict award compliance)

Tracks outcomes per roster and performs t-tests to determine statistical significance.
"""

import uuid
import random
import logging
from datetime import datetime, date
from decimal import Decimal
from typing import Optional, List, Dict, Any
from enum import Enum
import math
import statistics

logger = logging.getLogger(__name__)


class StrategyType(str, Enum):
    """Available roster generation strategies."""
    cost_optimised = "cost_optimised"
    coverage_optimised = "coverage_optimised"
    balanced = "balanced"
    staff_preference = "staff_preference"
    compliance_first = "compliance_first"


class ExperimentStatus(str, Enum):
    """Status of an A/B experiment."""
    draft = "draft"
    active = "active"
    paused = "paused"
    ended = "ended"


# ============================================================================
# Data classes for experiment results
# ============================================================================

class MetricComparison:
    """Comparison of a single metric between control and variant."""

    def __init__(self, metric_name: str):
        self.metric_name = metric_name
        self.control_mean: float = 0.0
        self.control_stddev: float = 0.0
        self.control_n: int = 0
        self.variant_mean: float = 0.0
        self.variant_stddev: float = 0.0
        self.variant_n: int = 0
        self.t_statistic: Optional[float] = None
        self.p_value: Optional[float] = None
        self.cohens_d: Optional[float] = None
        self.is_significant: bool = False
        self.effect_direction: str = ""  # "improved", "degraded", or "no_change"

    def to_dict(self) -> dict:
        return {
            "metric_name": self.metric_name,
            "control_mean": round(float(self.control_mean), 4),
            "control_stddev": round(float(self.control_stddev), 4),
            "control_n": self.control_n,
            "variant_mean": round(float(self.variant_mean), 4),
            "variant_stddev": round(float(self.variant_stddev), 4),
            "variant_n": self.variant_n,
            "t_statistic": round(float(self.t_statistic), 4) if self.t_statistic else None,
            "p_value": round(float(self.p_value), 6) if self.p_value else None,
            "cohens_d": round(float(self.cohens_d), 4) if self.cohens_d else None,
            "is_significant": self.is_significant,
            "effect_direction": self.effect_direction,
        }


class ExperimentResult:
    """Summary of an A/B test experiment."""

    def __init__(self, experiment_id: str):
        self.experiment_id = experiment_id
        self.metric_comparisons: Dict[str, MetricComparison] = {}
        self.is_ready: bool = False  # True if both groups have >= 30 rosters
        self.readiness_message: str = ""
        self.analysis_time: Optional[datetime] = None

    def add_metric(self, metric_name: str, comparison: MetricComparison):
        self.metric_comparisons[metric_name] = comparison

    def to_dict(self) -> dict:
        return {
            "experiment_id": self.experiment_id,
            "is_ready": self.is_ready,
            "readiness_message": self.readiness_message,
            "analysis_time": self.analysis_time.isoformat() if self.analysis_time else None,
            "metrics": {
                name: comp.to_dict()
                for name, comp in self.metric_comparisons.items()
            },
        }


# ============================================================================
# Main experiment engine
# ============================================================================

class ExperimentEngine:
    """
    Manages A/B experiments for roster generation strategies.

    Handles experiment definition, venue assignment, outcome tracking,
    and statistical analysis.
    """

    def __init__(self, db_store):
        """
        Args:
            db_store: Database store (BaseStore) for persistence
        """
        self.db_store = db_store

    # ========================================================================
    # Experiment lifecycle
    # ========================================================================

    def create_experiment(
        self,
        name: str,
        description: str,
        control_strategy: StrategyType,
        variant_strategy: StrategyType,
        start_date: date,
        end_date: date,
    ) -> dict:
        """
        Create a new A/B experiment.

        Args:
            name: Human-readable experiment name
            description: Detailed description of the experiment
            control_strategy: Control group strategy
            variant_strategy: Treatment group strategy
            start_date: When experiment starts
            end_date: When experiment ends

        Returns:
            Created experiment dict
        """
        experiment_id = str(uuid.uuid4())

        experiment = {
            "id": experiment_id,
            "name": name,
            "description": description,
            "control_strategy": control_strategy.value,
            "variant_strategy": variant_strategy.value,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "status": ExperimentStatus.draft.value,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "control_venues": [],  # venue IDs in control group
            "variant_venues": [],  # venue IDs in variant group
            "minimum_sample_size": 30,
        }

        self.db_store.save_experiment(experiment)
        logger.info(f"Created experiment {experiment_id}: {name}")
        return experiment

    def get_experiment(self, experiment_id: str) -> Optional[dict]:
        """Get experiment by ID."""
        return self.db_store.get_experiment(experiment_id)

    def list_experiments(self, active_only: bool = False) -> List[dict]:
        """
        List all experiments.

        Args:
            active_only: If True, only return experiments with status='active'

        Returns:
            List of experiment dicts
        """
        experiments = self.db_store.list_experiments(active_only=active_only)
        return experiments

    def end_experiment(self, experiment_id: str) -> dict:
        """
        End an active experiment (mark as 'ended').

        Args:
            experiment_id: The experiment to end

        Returns:
            Updated experiment dict
        """
        experiment = self.db_store.get_experiment(experiment_id)
        if not experiment:
            raise ValueError(f"Experiment {experiment_id} not found")

        experiment["status"] = ExperimentStatus.ended.value
        experiment["updated_at"] = datetime.utcnow().isoformat()
        self.db_store.save_experiment(experiment)

        logger.info(f"Ended experiment {experiment_id}")
        return experiment

    # ========================================================================
    # Venue assignment
    # ========================================================================

    def assign_venues_random(
        self,
        experiment_id: str,
        venue_ids: List[str],
        control_ratio: float = 0.5,
        seed: Optional[int] = None,
    ) -> dict:
        """
        Randomly assign venues to control/variant groups.

        Args:
            experiment_id: The experiment ID
            venue_ids: List of venue IDs to assign
            control_ratio: Proportion assigned to control (0.5 = 50/50 split)
            seed: Random seed for reproducibility

        Returns:
            Assignment result dict with control_venues and variant_venues
        """
        experiment = self.db_store.get_experiment(experiment_id)
        if not experiment:
            raise ValueError(f"Experiment {experiment_id} not found")

        if seed is not None:
            random.seed(seed)

        shuffled = venue_ids.copy()
        random.shuffle(shuffled)

        split_idx = max(1, int(len(shuffled) * control_ratio))
        control_venues = shuffled[:split_idx]
        variant_venues = shuffled[split_idx:]

        experiment["control_venues"] = control_venues
        experiment["variant_venues"] = variant_venues
        experiment["updated_at"] = datetime.utcnow().isoformat()
        self.db_store.save_experiment(experiment)

        logger.info(
            f"Assigned {len(control_venues)} venues to control, "
            f"{len(variant_venues)} to variant for experiment {experiment_id}"
        )

        return {
            "experiment_id": experiment_id,
            "control_venues": control_venues,
            "variant_venues": variant_venues,
            "seed": seed,
        }

    def assign_venues_manual(
        self,
        experiment_id: str,
        control_venues: List[str],
        variant_venues: List[str],
    ) -> dict:
        """
        Manually assign venues to control/variant groups.

        Args:
            experiment_id: The experiment ID
            control_venues: Venue IDs for control group
            variant_venues: Venue IDs for variant group

        Returns:
            Assignment result dict
        """
        experiment = self.db_store.get_experiment(experiment_id)
        if not experiment:
            raise ValueError(f"Experiment {experiment_id} not found")

        experiment["control_venues"] = control_venues
        experiment["variant_venues"] = variant_venues
        experiment["updated_at"] = datetime.utcnow().isoformat()
        self.db_store.save_experiment(experiment)

        logger.info(
            f"Manually assigned {len(control_venues)} venues to control, "
            f"{len(variant_venues)} to variant for experiment {experiment_id}"
        )

        return {
            "experiment_id": experiment_id,
            "control_venues": control_venues,
            "variant_venues": variant_venues,
        }

    # ========================================================================
    # Strategy assignment
    # ========================================================================

    def get_strategy_for_venue(self, experiment_id: str, venue_id: str) -> StrategyType:
        """
        Get the strategy a venue should use in this experiment.

        Returns:
            StrategyType (control or variant strategy)
        """
        experiment = self.db_store.get_experiment(experiment_id)
        if not experiment:
            return StrategyType.balanced

        # Check if experiment is active
        status = experiment.get("status", ExperimentStatus.draft.value)
        if status != ExperimentStatus.active.value:
            return StrategyType.balanced

        # Check venue assignment
        if venue_id in experiment.get("control_venues", []):
            return StrategyType(experiment["control_strategy"])
        elif venue_id in experiment.get("variant_venues", []):
            return StrategyType(experiment["variant_strategy"])
        else:
            return StrategyType.balanced

    # ========================================================================
    # Outcome tracking
    # ========================================================================

    def record_outcome(
        self,
        experiment_id: str,
        venue_id: str,
        roster_id: str,
        metrics: dict,
    ) -> None:
        """
        Record the outcome of a roster generation for an experiment.

        Args:
            experiment_id: The experiment ID
            venue_id: The venue that generated the roster
            roster_id: The roster ID
            metrics: Dict of metric values:
                - total_labour_cost (Decimal)
                - labour_percentage (float, 0-100)
                - demand_coverage_pct (float, 0-100)
                - compliance_score (float, 0-100)
                - staff_satisfaction_proxy (float, 0-100)
                - overtime_hours (float)
                - penalty_hours (float)
        """
        experiment = self.db_store.get_experiment(experiment_id)
        if not experiment:
            raise ValueError(f"Experiment {experiment_id} not found")

        # Determine which group venue is in
        is_control = venue_id in experiment.get("control_venues", [])
        is_variant = venue_id in experiment.get("variant_venues", [])

        if not (is_control or is_variant):
            logger.warning(
                f"Venue {venue_id} not assigned to experiment {experiment_id}"
            )
            return

        outcome = {
            "id": str(uuid.uuid4()),
            "experiment_id": experiment_id,
            "venue_id": venue_id,
            "roster_id": roster_id,
            "group": "control" if is_control else "variant",
            "total_labour_cost": float(metrics.get("total_labour_cost", 0)),
            "labour_percentage": float(metrics.get("labour_percentage", 0)),
            "demand_coverage_pct": float(metrics.get("demand_coverage_pct", 0)),
            "compliance_score": float(metrics.get("compliance_score", 0)),
            "staff_satisfaction_proxy": float(metrics.get("staff_satisfaction_proxy", 0)),
            "overtime_hours": float(metrics.get("overtime_hours", 0)),
            "penalty_hours": float(metrics.get("penalty_hours", 0)),
            "recorded_at": datetime.utcnow().isoformat(),
        }

        self.db_store.save_experiment_outcome(outcome)
        logger.debug(
            f"Recorded outcome for {experiment_id}/{venue_id}/{roster_id}"
        )

    # ========================================================================
    # Statistical analysis
    # ========================================================================

    def analyse_experiment(self, experiment_id: str) -> ExperimentResult:
        """
        Analyse an experiment by comparing metrics between control and variant.

        Returns:
            ExperimentResult with t-tests, effect sizes, and significance
        """
        result = ExperimentResult(experiment_id)

        outcomes = self.db_store.list_experiment_outcomes(experiment_id)
        if not outcomes:
            result.readiness_message = "No outcomes recorded yet"
            return result

        # Split outcomes by group
        control_outcomes = [o for o in outcomes if o["group"] == "control"]
        variant_outcomes = [o for o in outcomes if o["group"] == "variant"]

        min_n = 30
        result.control_n = len(control_outcomes)
        result.variant_n = len(variant_outcomes)

        # Check if we have sufficient sample size
        if len(control_outcomes) < min_n or len(variant_outcomes) < min_n:
            result.is_ready = False
            result.readiness_message = (
                f"Insufficient sample size. Control: {len(control_outcomes)}/{min_n}, "
                f"Variant: {len(variant_outcomes)}/{min_n}"
            )
            return result

        result.is_ready = True
        result.analysis_time = datetime.utcnow()

        # Define metrics to compare
        metric_keys = [
            "total_labour_cost",
            "labour_percentage",
            "demand_coverage_pct",
            "compliance_score",
            "staff_satisfaction_proxy",
            "overtime_hours",
            "penalty_hours",
        ]

        for metric_key in metric_keys:
            comparison = self._compare_metric(
                metric_key,
                control_outcomes,
                variant_outcomes,
            )
            result.add_metric(metric_key, comparison)

        return result

    def _compare_metric(
        self,
        metric_key: str,
        control_outcomes: List[dict],
        variant_outcomes: List[dict],
    ) -> MetricComparison:
        """
        Compare a single metric between control and variant groups.

        Uses independent samples t-test and Cohen's d effect size.
        """
        comparison = MetricComparison(metric_key)

        # Extract values
        control_values = [o.get(metric_key, 0) for o in control_outcomes]
        variant_values = [o.get(metric_key, 0) for o in variant_outcomes]

        # Remove None values
        control_values = [v for v in control_values if v is not None]
        variant_values = [v for v in variant_values if v is not None]

        if not control_values or not variant_values:
            return comparison

        # Calculate descriptive statistics
        comparison.control_mean = statistics.mean(control_values)
        comparison.control_n = len(control_values)
        comparison.variant_mean = statistics.mean(variant_values)
        comparison.variant_n = len(variant_values)

        if len(control_values) > 1:
            comparison.control_stddev = statistics.stdev(control_values)
        if len(variant_values) > 1:
            comparison.variant_stddev = statistics.stdev(variant_values)

        # Perform independent samples t-test
        t_stat, p_val = self._ttest_ind(
            control_values,
            variant_values,
        )
        comparison.t_statistic = t_stat
        comparison.p_value = p_val

        # Calculate Cohen's d
        cohens_d = self._cohens_d(
            comparison.control_mean,
            comparison.control_stddev,
            comparison.control_n,
            comparison.variant_mean,
            comparison.variant_stddev,
            comparison.variant_n,
        )
        comparison.cohens_d = cohens_d

        # Determine significance (p < 0.05)
        comparison.is_significant = p_val < 0.05 if p_val is not None else False

        # Determine direction of effect
        if comparison.is_significant:
            if comparison.variant_mean > comparison.control_mean:
                # Check if higher is better for this metric
                if metric_key in [
                    "demand_coverage_pct",
                    "compliance_score",
                    "staff_satisfaction_proxy",
                ]:
                    comparison.effect_direction = "improved"
                elif metric_key in [
                    "total_labour_cost",
                    "labour_percentage",
                    "overtime_hours",
                    "penalty_hours",
                ]:
                    comparison.effect_direction = "degraded"
            else:
                if metric_key in [
                    "demand_coverage_pct",
                    "compliance_score",
                    "staff_satisfaction_proxy",
                ]:
                    comparison.effect_direction = "degraded"
                elif metric_key in [
                    "total_labour_cost",
                    "labour_percentage",
                    "overtime_hours",
                    "penalty_hours",
                ]:
                    comparison.effect_direction = "improved"
        else:
            comparison.effect_direction = "no_change"

        return comparison

    @staticmethod
    def _ttest_ind(
        group1: List[float],
        group2: List[float],
    ) -> tuple[Optional[float], Optional[float]]:
        """
        Perform independent samples t-test.

        Returns:
            (t_statistic, p_value) or (None, None) if insufficient data
        """
        if len(group1) < 2 or len(group2) < 2:
            return None, None

        mean1 = statistics.mean(group1)
        mean2 = statistics.mean(group2)
        var1 = statistics.variance(group1)
        var2 = statistics.variance(group2)
        n1 = len(group1)
        n2 = len(group2)

        # Welch's t-test (doesn't assume equal variances)
        se = math.sqrt((var1 / n1) + (var2 / n2))
        if se == 0:
            return None, None

        t_stat = (mean1 - mean2) / se

        # Approximate degrees of freedom (Welch-Satterthwaite equation)
        numerator = ((var1 / n1) + (var2 / n2)) ** 2
        denominator = (
            ((var1 / n1) ** 2 / (n1 - 1)) +
            ((var2 / n2) ** 2 / (n2 - 1))
        )
        if denominator == 0:
            return None, None

        df = numerator / denominator

        # Two-tailed p-value from t-distribution (approximation)
        # For simplicity, use normal approximation for df > 30
        p_value = ExperimentEngine._t_to_p_value(t_stat, df)

        return t_stat, p_value

    @staticmethod
    def _t_to_p_value(t_stat: float, df: float) -> float:
        """
        Convert t-statistic and degrees of freedom to two-tailed p-value.

        Uses normal approximation for df > 30.
        """
        from math import erf

        # For large df, t-distribution approximates normal
        if df > 30:
            # Normal approximation: p = 2 * (1 - Phi(|t|))
            # where Phi is the CDF of standard normal
            abs_t = abs(t_stat)
            # Approximation of normal CDF
            z = abs_t
            p = math.exp(-0.5 * z * z) / (1 + 0.2316 * z + 0.3989 * z * z)
            return 2 * p

        # For smaller df, use Student's t approximation
        # This is a simplified approximation
        abs_t = abs(t_stat)
        return 2 * (1 - ExperimentEngine._student_t_cdf(abs_t, df))

    @staticmethod
    def _student_t_cdf(t: float, df: float) -> float:
        """
        Approximate CDF of Student's t-distribution.

        Uses a simplified beta function approximation.
        """
        from math import atan, pi

        # For t > 0, use symmetry
        if t < 0:
            return 1 - ExperimentEngine._student_t_cdf(-t, df)

        # Beta function approximation
        x = df / (df + t * t)
        return 0.5 + 0.5 * ExperimentEngine._beta_cdf(x, df / 2, 0.5)

    @staticmethod
    def _beta_cdf(x: float, alpha: float, beta: float) -> float:
        """
        Approximate CDF of beta distribution.

        Very simplified approximation for beta(alpha, beta).
        """
        if x <= 0:
            return 0
        if x >= 1:
            return 1

        # Use a simple polynomial approximation for small alpha, beta
        if alpha < 1 or beta < 1:
            return x ** alpha * (1 - x) ** beta / (alpha + beta)

        return x

    @staticmethod
    def _cohens_d(
        mean1: float,
        stddev1: float,
        n1: int,
        mean2: float,
        stddev2: float,
        n2: int,
    ) -> Optional[float]:
        """
        Calculate Cohen's d effect size.

        Uses pooled standard deviation.
        """
        if n1 < 2 or n2 < 2:
            return None

        if stddev1 == 0 and stddev2 == 0:
            return 0

        # Pooled standard deviation
        pooled_var = (
            ((n1 - 1) * stddev1 ** 2 + (n2 - 1) * stddev2 ** 2) /
            (n1 + n2 - 2)
        )

        if pooled_var == 0:
            return 0

        pooled_sd = math.sqrt(pooled_var)
        cohens_d = (mean1 - mean2) / pooled_sd
        return cohens_d
