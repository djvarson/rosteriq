"""
RosterIQ Integration & Orchestration Pipeline

Coordinates the full roster generation and operational workflow:
- Connects demand forecasts (via signal feeds) with demand conversion
- Integrates employee data from Tanda WFM
- Runs roster engine with award cost calculations
- Powers on-shift dashboards and roster planning views
- Handles operational events like call-ins

This is the main orchestration layer that external APIs consume.
Handles graceful degradation: partial failures fall back to demo data
rather than crashing the entire pipeline.

Full async, type-hinted, comprehensive error handling and logging.
"""

import logging
from datetime import datetime, date, timedelta, time
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import asdict

try:
    import asyncio
except ImportError:
    asyncio = None

# Internal imports
from rosteriq.roster_engine import (
    RosterEngine,
    RosterConstraints,
    Employee as RosterEmployee,
    Role,
    DemandForecast,
    Shift,
)
from rosteriq.award_engine import AwardEngine, RosterCostSummary
from rosteriq.signal_feeds import SignalAggregator, SignalImpactType
from rosteriq.forecast_engine import get_forecast_engine, ForecastEngine
from rosteriq.tanda_adapter import (
    get_tanda_adapter,
    SchedulingPlatformAdapter,
    Employee as TandaEmployee,
)
from rosteriq.pos_adapter import get_pos_adapter, POSAdapter

logger = logging.getLogger("rosteriq.pipeline")


# ============================================================================
# Data Models for Pipeline I/O
# ============================================================================


class RosterIQPipeline:
    """
    Main orchestration pipeline for RosterIQ operations.

    Coordinates between:
    - Tanda WFM adapter (employee & availability data)
    - Signal feeds (demand signals: weather, events, bookings, traffic)
    - Roster engine (AI rostering with constraints)
    - Award engine (AU compliance & cost calculations)
    - POS adapter (actual vs forecasted revenue)

    All methods are async and gracefully handle component failures.
    """

    def __init__(
        self,
        venue_id: str,
        constraints: Optional[RosterConstraints] = None,
        award_year: int = 2025,
    ):
        """
        Initialize the pipeline.

        Args:
            venue_id: Unique venue identifier
            constraints: Optional roster constraints (uses defaults if not provided)
            award_year: Award year for pay calculations (default 2025)
        """
        self.venue_id = venue_id
        self.constraints = constraints or RosterConstraints()
        self.award_year = award_year

        # Component instances
        self.roster_engine = RosterEngine(self.constraints)
        self.award_engine = AwardEngine(award_year=self.award_year)
        self.signal_aggregator = SignalAggregator()

        # Adapters (lazy loaded)
        self._tanda_adapter: Optional[SchedulingPlatformAdapter] = None
        self._pos_adapter: Optional[POSAdapter] = None

        logger.info(f"RosterIQ pipeline initialized for venue {venue_id}")

    async def _get_tanda_adapter(self) -> SchedulingPlatformAdapter:
        """Lazy load Tanda adapter."""
        if self._tanda_adapter is None:
            self._tanda_adapter = get_tanda_adapter()
            logger.debug(f"Loaded Tanda adapter: {type(self._tanda_adapter).__name__}")
        return self._tanda_adapter

    async def _get_pos_adapter(self) -> POSAdapter:
        """Lazy load POS adapter."""
        if self._pos_adapter is None:
            self._pos_adapter = get_pos_adapter()
            logger.debug(f"Loaded POS adapter: {type(self._pos_adapter).__name__}")
        return self._pos_adapter

    # ========================================================================
    # Core Roster Generation Pipeline
    # ========================================================================

    async def generate_roster(
        self,
        week_start_date: str,
    ) -> Dict[str, Any]:
        """
        Generate an optimal weekly roster with full cost and quality metrics.

        Pipeline:
        1. Fetch employees from Tanda
        2. Collect demand signals and forecasts
        3. Convert forecasts to demand (handled by roster_engine)
        4. Run roster generation with constraints
        5. Calculate award costs via AwardEngine
        6. Compile warnings and quality metrics

        Args:
            week_start_date: Start of roster week (YYYY-MM-DD)

        Returns:
            Dict with keys:
                - roster_id: Unique roster identifier
                - venue_id: Venue identifier
                - week_start_date: Week start date
                - shifts: List of assigned shifts with employee details
                - total_labour_cost: Sum of all labour costs (AUD)
                - total_hours: Sum of all hours assigned
                - coverage_score: 0-1, how well demand was met
                - fairness_score: 0-1, hour distribution fairness
                - cost_efficiency_score: 0-1, cost vs budget
                - award_cost_summary: Detailed cost breakdown by employee/day/role
                - warnings: List of compliance/operational warnings
                - quality_metrics: Additional quality indicators
                - generated_at: ISO timestamp when roster was created
        """
        try:
            logger.info(
                f"Generating roster for {self.venue_id} week starting {week_start_date}"
            )

            # Step 1: Get employees from Tanda
            employees = await self._fetch_employees()
            logger.debug(f"Fetched {len(employees)} employees from Tanda")

            # Convert Tanda employee format to roster_engine format
            roster_employees = self._convert_tanda_employees_to_roster(employees)

            # Step 2: Collect demand signals
            signals = await self._collect_demand_signals(week_start_date)
            logger.debug(f"Collected {len(signals)} demand signals")

            # Step 3: Generate demand forecasts (simplified: base + signal adjustments)
            # In a real system, you'd have a separate ForecastEngine
            demand_forecasts = await self._generate_demand_forecasts(
                week_start_date, signals
            )
            logger.debug(f"Generated {len(demand_forecasts)} day forecasts")

            # Step 4: Run roster generation
            roster = self.roster_engine.generate_roster(
                roster_employees, demand_forecasts, week_start_date
            )
            logger.debug(f"Roster generated with {len(roster.shifts)} shifts")

            # Step 5: Calculate award costs
            roster_for_costing = [
                {
                    "employee_id": s.employee_id,
                    "award_level": self._get_award_level_for_employee(
                        s.employee_id, roster_employees
                    ),
                    "employment_type": self._get_employment_type_for_employee(
                        s.employee_id, roster_employees
                    ),
                    "shifts": [
                        (
                            s.date,
                            time(s.start_hour, 0),
                            time(s.end_hour, 0),
                        )
                    ],
                }
                for s in roster.shifts
                if s.is_filled
            ]
            cost_summary = self.award_engine.calculate_roster_cost(
                roster_for_costing, budget=self.constraints.budget_limit_weekly
            )

            # Step 6: Compile output
            result = {
                "roster_id": f"roster_{self.venue_id}_{week_start_date}",
                "venue_id": self.venue_id,
                "week_start_date": week_start_date,
                "shifts": [
                    {
                        "id": s.id,
                        "date": s.date,
                        "start_hour": s.start_hour,
                        "end_hour": s.end_hour,
                        "role_required": s.role_required.value if hasattr(s.role_required, 'value') else s.role_required,
                        "employee_id": s.employee_id,
                        "duration_hours": s.duration_hours,
                        "is_filled": s.is_filled,
                    }
                    for s in roster.shifts
                ],
                "total_labour_cost": roster.total_labour_cost,
                "total_hours": roster.total_hours,
                "coverage_score": roster.coverage_score,
                "fairness_score": roster.fairness_score,
                "cost_efficiency_score": roster.cost_efficiency_score,
                "award_cost_summary": {
                    "total_gross_pay": str(cost_summary.total_gross_pay),
                    "total_super": str(cost_summary.total_super),
                    "total_employer_cost": str(cost_summary.total_employer_cost),
                    "overtime_hours": str(cost_summary.overtime_hours),
                    "overtime_cost": str(cost_summary.overtime_cost),
                },
                "warnings": roster.warnings,
                "quality_metrics": {
                    "unfilled_shifts": sum(1 for s in roster.shifts if not s.is_filled),
                    "total_shifts": len(roster.shifts),
                    "shift_fill_rate": (
                        sum(1 for s in roster.shifts if s.is_filled) / len(roster.shifts)
                        if roster.shifts
                        else 0.0
                    ),
                },
                "generated_at": datetime.utcnow().isoformat() + "Z",
            }

            logger.info(f"Roster generation complete: {len(result['shifts'])} shifts")
            return result

        except Exception as e:
            logger.error(f"Error generating roster: {e}", exc_info=True)
            raise

    # ========================================================================
    # On-Shift Dashboard (Real-time Operations)
    # ========================================================================

    async def get_on_shift_dashboard(self, target_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Real-time on-shift dashboard for current venue operations.

        Shows:
        - Who's on shift now / coming up
        - Revenue vs forecast
        - Demand signals affecting the venue
        - Staffing levels vs recommended
        - Real-time recommendations (e.g., call in someone?)

        Args:
            target_date: Date to show data for (default: today). Format YYYY-MM-DD.

        Returns:
            Dict with keys:
                - venue_id: Venue identifier
                - date: Date shown
                - current_time: Current time (HH:MM)
                - staff_on_deck: List of employees currently scheduled
                - current_demand_multiplier: Demand strength (1.0 = normal)
                - signals_active: List of active signals affecting demand
                - revenue_metrics: Actual vs forecast revenue
                - staffing_recommendations: List of recommended actions
                - hourly_curve: Forecast staffing curve for the day
                - staff_availability: Who could cover a call-in
        """
        try:
            if target_date is None:
                target_date = date.today().isoformat()

            logger.info(f"Fetching on-shift dashboard for {self.venue_id} on {target_date}")

            # Collect current signals
            signals = await self._collect_demand_signals(target_date)

            # Get demand multiplier
            demand_multiplier = await self.signal_aggregator.get_demand_multiplier(
                self.venue_id, date.fromisoformat(target_date)
            )

            # Get employees for availability data
            employees = await self._fetch_employees()

            # Get POS data for revenue comparison
            revenue_actual, revenue_forecast = await self._get_revenue_metrics(
                target_date
            )

            # Compile recommendations
            recommendations = self._generate_on_shift_recommendations(
                demand_multiplier, signals, revenue_actual, revenue_forecast
            )

            result = {
                "venue_id": self.venue_id,
                "date": target_date,
                "current_time": datetime.now().strftime("%H:%M"),
                "staff_on_deck": [
                    {
                        "employee_id": e.id,
                        "name": e.name,
                        "role": e.role,
                    }
                    for e in employees[:5]  # Top 5
                ],
                "current_demand_multiplier": demand_multiplier,
                "signals_active": [
                    {
                        "source": s.source.value if hasattr(s.source, 'value') else s.source,
                        "description": s.description,
                        "impact_score": s.impact_score,
                    }
                    for s in signals
                ],
                "revenue_metrics": {
                    "actual": revenue_actual,
                    "forecast": revenue_forecast,
                    "variance_pct": (
                        ((revenue_actual - revenue_forecast) / revenue_forecast * 100)
                        if revenue_forecast > 0
                        else 0.0
                    ),
                },
                "staffing_recommendations": recommendations,
                "hourly_curve": self._generate_hourly_curve(),
                "generated_at": datetime.utcnow().isoformat() + "Z",
            }

            logger.info("On-shift dashboard ready")
            return result

        except Exception as e:
            logger.warning(f"Error fetching on-shift dashboard: {e}")
            # Return minimal safe default
            return {
                "venue_id": self.venue_id,
                "date": target_date or date.today().isoformat(),
                "error": str(e),
                "staff_on_deck": [],
                "signals_active": [],
                "revenue_metrics": {"actual": 0, "forecast": 0},
                "staffing_recommendations": [],
            }

    # ========================================================================
    # Roster Maker Dashboard (Planning)
    # ========================================================================

    async def get_roster_maker_dashboard(self, week_start_date: str) -> Dict[str, Any]:
        """
        Planning dashboard for roster manager operations.

        Shows:
        - Staff summary (availability, hours used, preferences)
        - Weekly demand forecast
        - Signal outlook for the week
        - Cost budget status
        - Compliance checks

        Args:
            week_start_date: Start of week. Format YYYY-MM-DD.

        Returns:
            Dict with keys:
                - venue_id: Venue identifier
                - week_start_date: Week start date
                - staff_summary: List of employees with availability/hours
                - weekly_forecast: Demand forecast by day
                - signal_outlook: Summary of signals affecting the week
                - budget_status: Current vs limit
                - compliance_checks: List of potential compliance issues
                - planning_recommendations: Suggested roster actions
        """
        try:
            logger.info(
                f"Fetching roster maker dashboard for {self.venue_id} week {week_start_date}"
            )

            # Get employees
            employees = await self._fetch_employees()

            # Get week's signals and forecasts
            signals_by_day = {}
            for day_offset in range(7):
                day = (
                    date.fromisoformat(week_start_date) + timedelta(days=day_offset)
                ).isoformat()
                signals_by_day[day] = await self._collect_demand_signals(day)

            # Compile staff summary
            staff_summary = [
                {
                    "employee_id": e.id,
                    "name": e.name,
                    "role": e.role,
                    "employment_type": e.employment_type,
                    "hourly_rate": e.hourly_rate,
                    "max_hours_week": 38,
                    "hours_scheduled": 0,  # Would fetch from existing roster
                    "availability_notes": "Full week available",
                }
                for e in employees
            ]

            # Forecast summary (simplified)
            weekly_forecast = []
            for day_offset in range(7):
                forecast_date = (
                    date.fromisoformat(week_start_date) + timedelta(days=day_offset)
                ).isoformat()
                multiplier = await self.signal_aggregator.get_demand_multiplier(
                    self.venue_id, date.fromisoformat(forecast_date)
                )
                weekly_forecast.append(
                    {
                        "date": forecast_date,
                        "day_of_week": date.fromisoformat(forecast_date).strftime("%A"),
                        "demand_multiplier": multiplier,
                        "expected_covers": int(200 * multiplier),  # Demo
                    }
                )

            # Signal outlook
            signal_outlook = []
            for day, sigs in signals_by_day.items():
                if sigs:
                    signal_outlook.append(
                        {
                            "date": day,
                            "signal_count": len(sigs),
                            "top_signals": [s.description for s in sigs[:2]],
                        }
                    )

            result = {
                "venue_id": self.venue_id,
                "week_start_date": week_start_date,
                "staff_summary": staff_summary,
                "staff_count": len(staff_summary),
                "weekly_forecast": weekly_forecast,
                "signal_outlook": signal_outlook,
                "budget_status": {
                    "limit": self.constraints.budget_limit_weekly,
                    "used": 0,
                    "remaining": self.constraints.budget_limit_weekly,
                    "variance_pct": 0.0,
                },
                "compliance_checks": [],
                "planning_recommendations": [
                    "Ensure coverage for peak hours (Thu-Sat)",
                    "Check availability for staff nearing max hours",
                ],
                "generated_at": datetime.utcnow().isoformat() + "Z",
            }

            logger.info("Roster maker dashboard ready")
            return result

        except Exception as e:
            logger.warning(f"Error fetching roster maker dashboard: {e}")
            return {
                "venue_id": self.venue_id,
                "week_start_date": week_start_date,
                "error": str(e),
                "staff_summary": [],
                "weekly_forecast": [],
            }

    # ========================================================================
    # Operational: Call-In Handling
    # ========================================================================

    async def handle_call_in(
        self,
        shift_id: str,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Handle a staff member calling in sick/unavailable.

        Finds the best replacement from available staff and provides
        contact info and SMS template.

        Args:
            shift_id: Shift ID that needs coverage (format: date_role_time)
            reason: Optional reason for absence (sick, personal, etc.)

        Returns:
            Dict with keys:
                - shift_id: Original shift ID
                - replacement_candidates: List of staff who can cover
                  Each with: employee_id, name, phone, seniority, contact_template
                - sms_template: Ready-to-send SMS for top candidate
                - escalation_needed: Boolean whether manager intervention needed
        """
        try:
            logger.info(f"Handling call-in for shift {shift_id}")

            # Parse shift_id (simplified format: YYYY-MM-DD_role_hour)
            # In production, look up actual shift from DB
            parts = shift_id.split("_")
            if len(parts) >= 2:
                shift_date = parts[0]
                required_role = parts[1]
            else:
                required_role = "bar"  # fallback
                shift_date = date.today().isoformat()

            # Get available employees
            employees = await self._fetch_employees()

            # Filter for those with matching skills and availability
            candidates = [
                {
                    "employee_id": e.id,
                    "name": e.name,
                    "phone": e.phone or "N/A",
                    "role": e.role,
                    "seniority_score": getattr(e, "seniority_score", 0.5),
                    "contact_template": f"Hi {e.name.split()[0]}, we need cover for {shift_date}. Can you help? Reply YES/NO.",
                }
                for e in employees
                if hasattr(e, "role")
                and required_role in getattr(e, "role", "")
            ]

            # Sort by seniority (prefer experienced staff)
            candidates.sort(
                key=lambda c: c["seniority_score"], reverse=True
            )

            result = {
                "shift_id": shift_id,
                "shift_date": shift_date,
                "required_role": required_role,
                "reason": reason or "Not specified",
                "replacement_candidates": candidates[:5],  # Top 5
                "sms_template": (
                    f"Hi {candidates[0]['name'].split()[0] if candidates else 'Staff'}, "
                    f"we need cover for {shift_date}. Can you work? Reply YES/NO."
                )
                if candidates
                else "No available candidates",
                "escalation_needed": len(candidates) < 2,
                "generated_at": datetime.utcnow().isoformat() + "Z",
            }

            logger.info(
                f"Call-in handled: {len(candidates)} candidates found"
            )
            return result

        except Exception as e:
            logger.error(f"Error handling call-in: {e}", exc_info=True)
            raise

    # ========================================================================
    # Helper Methods: Data Fetching
    # ========================================================================

    async def _fetch_employees(self) -> List[TandaEmployee]:
        """
        Fetch all active employees from Tanda adapter.

        Handles failures gracefully by returning empty list.
        """
        try:
            adapter = await self._get_tanda_adapter()
            employees = await adapter.get_employees(self.venue_id)
            logger.debug(f"Fetched {len(employees)} employees")
            return employees
        except Exception as e:
            logger.warning(f"Failed to fetch employees from Tanda: {e}")
            return []

    async def _collect_demand_signals(self, target_date: str) -> List[Any]:
        """
        Collect demand signals from all sources for a single day.

        Falls back to empty list on error (safe degradation).
        """
        try:
            date_obj = date.fromisoformat(target_date)
            signals = await self.signal_aggregator.collect_all_signals(
                self.venue_id, date_obj
            )
            logger.debug(f"Collected {len(signals)} signals for {target_date}")
            return signals
        except Exception as e:
            logger.warning(f"Failed to collect signals: {e}")
            return []

    async def _generate_demand_forecasts(
        self,
        week_start_date: str,
        signals: List[Any],
    ) -> List[DemandForecast]:
        """
        Generate demand forecasts for a week using the ForecastEngine.

        Delegates to forecast_engine.py which combines:
        - Historical POS baseline (revenue patterns by day-of-week)
        - Signal adjustments (weather, events, bookings, foot traffic)
        - Role-specific allocation (covers → staff per role)

        Falls back to simple inline forecast if ForecastEngine unavailable.

        Returns:
            List of DemandForecast objects for each day of the week
        """
        try:
            engine = get_forecast_engine()
            forecasts = await engine.forecast_week(self.venue_id, week_start_date)
            logger.info(f"ForecastEngine produced {len(forecasts)} day forecasts")
            return forecasts
        except Exception as e:
            logger.warning(f"ForecastEngine unavailable ({e}), using inline fallback")

        # Fallback: simple inline forecast if ForecastEngine fails
        forecasts = []
        base_date = date.fromisoformat(week_start_date)

        for day_offset in range(7):
            forecast_date = (base_date + timedelta(days=day_offset)).isoformat()
            hourly_demand = {}
            for hour in range(11, 22):
                is_weekend = (base_date + timedelta(days=day_offset)).weekday() >= 4
                base_count = 4 if is_weekend else 3
                hourly_demand[hour] = {
                    Role.BAR: base_count,
                    Role.FLOOR: base_count,
                    Role.KITCHEN: base_count - 1,
                }

            multiplier = await self.signal_aggregator.get_demand_multiplier(
                self.venue_id, date.fromisoformat(forecast_date)
            )
            for hour in hourly_demand:
                for role in hourly_demand[hour]:
                    hourly_demand[hour][role] = max(
                        1, int(hourly_demand[hour][role] * multiplier)
                    )

            forecast = DemandForecast(
                date=forecast_date,
                hourly_demand=hourly_demand,
                total_covers_expected=int(200 * multiplier),
                signals=[s.description for s in signals] if signals else [],
                confidence=0.75 if signals else 0.6,
            )
            forecasts.append(forecast)

        return forecasts

    async def _get_revenue_metrics(
        self, target_date: str
    ) -> Tuple[float, float]:
        """
        Get actual vs forecasted revenue for a date from POS.

        Returns:
            (actual_revenue, forecast_revenue) tuple in AUD
        """
        try:
            pos = await self._get_pos_adapter()
            # Simplified: assumes pos adapter has methods to get revenue
            # In production, implement proper POS integration
            actual = 1200.0  # Demo
            forecast = 1000.0  # Demo
            logger.debug(f"Revenue for {target_date}: ${actual} actual vs ${forecast} forecast")
            return actual, forecast
        except Exception as e:
            logger.warning(f"Failed to fetch revenue metrics: {e}")
            return 0.0, 0.0

    # ========================================================================
    # Helper Methods: Data Conversion & Mapping
    # ========================================================================

    def _convert_tanda_employees_to_roster(
        self, tanda_employees: List[TandaEmployee]
    ) -> List[RosterEmployee]:
        """
        Convert Tanda employee format to roster_engine Employee format.

        Handles mapping between different schemas:
        - tanda_adapter.Employee → roster_engine.Employee
        - String roles → Role enum
        """
        roster_employees = []

        for tanda_emp in tanda_employees:
            try:
                # Map string role to Role enum
                role_str = getattr(tanda_emp, "role", "bar").lower()
                try:
                    role = Role[role_str.upper()]
                except (KeyError, AttributeError):
                    role = Role.BAR  # Default

                roster_emp = RosterEmployee(
                    id=tanda_emp.id,
                    name=tanda_emp.name,
                    role=role,
                    skills=[
                        Role(skill) if isinstance(skill, str) else skill
                        for skill in getattr(tanda_emp, "skills", [])
                    ],
                    hourly_rate=float(
                        getattr(tanda_emp, "hourly_rate", 25.0)
                    ),
                    max_hours_per_week=38,
                    min_hours_per_week=0,
                    availability={},  # Would populate from adapter if available
                    employment_type=getattr(tanda_emp, "employment_type", "part_time"),
                    is_manager=(
                        "manager" in role_str or
                        getattr(tanda_emp, "is_manager", False)
                    ),
                )
                roster_employees.append(roster_emp)
            except Exception as e:
                logger.warning(f"Failed to convert employee {tanda_emp.id}: {e}")
                continue

        return roster_employees

    def _get_award_level_for_employee(
        self, employee_id: str, employees: List[RosterEmployee]
    ) -> int:
        """Estimate award level (1-6) based on employment attributes."""
        emp = next((e for e in employees if e.id == employee_id), None)
        if not emp:
            return 2

        # Simplified: based on role and employment type
        if "manager" in emp.role.value.lower():
            return 6
        elif "kitchen" in emp.role.value.lower():
            return 3
        else:
            return 2

    def _get_employment_type_for_employee(
        self, employee_id: str, employees: List[RosterEmployee]
    ) -> str:
        """Get employment type for employee."""
        emp = next((e for e in employees if e.id == employee_id), None)
        if not emp:
            return "casual"
        return str(emp.employment_type)

    # ========================================================================
    # Helper Methods: Recommendations & Analysis
    # ========================================================================

    def _generate_on_shift_recommendations(
        self,
        demand_multiplier: float,
        signals: List[Any],
        revenue_actual: float,
        revenue_forecast: float,
    ) -> List[Dict[str, str]]:
        """
        Generate actionable recommendations for on-shift management.

        Args:
            demand_multiplier: Current demand strength
            signals: Active signals
            revenue_actual: Actual revenue
            revenue_forecast: Forecasted revenue

        Returns:
            List of recommendation dicts with action, reason, priority
        """
        recommendations = []

        # Demand-based recommendations
        if demand_multiplier > 1.2:
            recommendations.append(
                {
                    "action": "Consider calling in extra staff",
                    "reason": "Demand 20%+ above normal",
                    "priority": "high",
                }
            )
        elif demand_multiplier < 0.8:
            recommendations.append(
                {
                    "action": "Adjust to minimal crew, offer voluntary breaks",
                    "reason": "Demand 20%+ below normal",
                    "priority": "medium",
                }
            )

        # Revenue variance
        if revenue_actual > revenue_forecast * 1.1:
            recommendations.append(
                {
                    "action": "Increase service levels, check for bottlenecks",
                    "reason": "Revenue tracking 10%+ above forecast",
                    "priority": "medium",
                }
            )

        return recommendations

    def _generate_hourly_curve(self) -> List[Dict[str, Any]]:
        """Generate hourly staffing curve for visualization."""
        return [
            {"hour": h, "staff_needed": 3 if 11 <= h <= 21 else 1}
            for h in range(24)
        ]


# ============================================================================
# Factory Function
# ============================================================================


def get_pipeline(
    venue_id: str,
    constraints: Optional[RosterConstraints] = None,
    award_year: int = 2025,
) -> RosterIQPipeline:
    """
    Factory function to create and configure a RosterIQ pipeline instance.

    Args:
        venue_id: Unique venue identifier
        constraints: Optional custom roster constraints
        award_year: Award year for cost calculations (default 2025)

    Returns:
        Configured RosterIQPipeline instance ready for operations
    """
    logger.info(f"Creating pipeline for venue {venue_id}")
    return RosterIQPipeline(
        venue_id=venue_id,
        constraints=constraints,
        award_year=award_year,
    )
