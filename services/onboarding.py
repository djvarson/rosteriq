"""
Onboarding service for managing first-run setup when a new venue connects via Tanda.

Handles multi-step process:
1. CONNECT_TANDA: validate token, fetch venue info
2. IMPORT_EMPLOYEES: pull all employees, map to models, save to DB
3. IMPORT_ROSTERS: pull current + next week rosters, save shifts
4. IMPORT_DEPARTMENTS: fetch department list, store in venue config
5. CONFIGURE_VENUE: set award rules, timezone, create VenueConfig
6. COMPLETE: mark venue as onboarded, log completion

Uses the custom BaseStore database layer and TandaAdapter for API calls.
"""

import logging
import asyncio
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Optional, List
from uuid import uuid4

from rosteriq.database import get_db
from rosteriq.tanda_adapter import TandaAdapter, TandaAPIError
from rosteriq.models import (
    TandaCredentials, VenueConfig, Employee, Roster, Shift,
    State, EmploymentType, ShiftStatus, AwardLevel,
)

logger = logging.getLogger(__name__)


class OnboardingStep(str, Enum):
    """Steps in the onboarding process."""
    CONNECT_TANDA = "connect_tanda"
    IMPORT_EMPLOYEES = "import_employees"
    IMPORT_ROSTERS = "import_rosters"
    IMPORT_DEPARTMENTS = "import_departments"
    CONFIGURE_VENUE = "configure_venue"
    COMPLETE = "complete"


@dataclass
class OnboardingState:
    """State tracking for a venue's onboarding process."""
    venue_id: str
    current_step: OnboardingStep
    started_at: datetime
    completed_steps: List[str] = field(default_factory=list)
    errors: List[dict] = field(default_factory=list)
    imported_counts: dict = field(default_factory=lambda: {
        "employees": 0,
        "rosters": 0,
        "shifts": 0,
        "departments": 0,
    })
    venue_name: Optional[str] = None
    venue_config_id: Optional[str] = None
    last_error: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        data = asdict(self)
        data["current_step"] = self.current_step.value
        data["started_at"] = self.started_at.isoformat() if isinstance(self.started_at, datetime) else self.started_at
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "OnboardingState":
        """Create from stored dictionary."""
        if "started_at" in data and isinstance(data["started_at"], str):
            data["started_at"] = datetime.fromisoformat(data["started_at"])
        if "current_step" in data and isinstance(data["current_step"], str):
            data["current_step"] = OnboardingStep(data["current_step"])
        return cls(**data)


class OnboardingService:
    """
    Service managing venue onboarding workflow.

    Coordinates multi-step setup process with error handling and progress tracking.
    """

    # Define step order
    STEP_ORDER = [
        OnboardingStep.CONNECT_TANDA,
        OnboardingStep.IMPORT_EMPLOYEES,
        OnboardingStep.IMPORT_ROSTERS,
        OnboardingStep.IMPORT_DEPARTMENTS,
        OnboardingStep.CONFIGURE_VENUE,
        OnboardingStep.COMPLETE,
    ]

    def __init__(self, db=None):
        self.db = db or get_db()

    async def start_onboarding(self, venue_id: str, tanda_token: str) -> OnboardingState:
        """
        Start onboarding for a venue.

        Args:
            venue_id: Unique venue identifier
            tanda_token: OAuth token from Tanda (access token)

        Returns:
            OnboardingState tracking the process
        """
        logger.info(f"Starting onboarding for venue {venue_id}")

        state = OnboardingState(
            venue_id=venue_id,
            current_step=OnboardingStep.CONNECT_TANDA,
            started_at=datetime.utcnow(),
        )

        # Store initial state
        self.db.save_onboarding_state(state.to_dict())

        return state

    def get_status(self, venue_id: str) -> Optional[dict]:
        """
        Get current onboarding status for a venue.

        Returns:
            Dict with step, progress %, errors, or None if not found
        """
        state_data = self.db.get_onboarding_state(venue_id)
        if not state_data:
            return None

        state = OnboardingState.from_dict(state_data)
        current_index = self._get_step_index(state.current_step)
        progress_pct = int((len(state.completed_steps) / len(self.STEP_ORDER)) * 100)

        return {
            "venue_id": venue_id,
            "current_step": state.current_step.value,
            "progress_pct": progress_pct,
            "completed_steps": state.completed_steps,
            "errors": state.errors,
            "last_error": state.last_error,
            "imported_counts": state.imported_counts,
            "venue_name": state.venue_name,
            "started_at": state.started_at.isoformat(),
        }

    async def run_step(
        self,
        venue_id: str,
        step: OnboardingStep,
        tanda_credentials: Optional[TandaCredentials] = None,
    ) -> bool:
        """
        Execute a specific onboarding step.

        Args:
            venue_id: Venue identifier
            step: Step to execute
            tanda_credentials: Tanda credentials (required for some steps)

        Returns:
            True if successful, False on error
        """
        state_data = self.db.get_onboarding_state(venue_id)
        if not state_data:
            logger.warning(f"No onboarding state for venue {venue_id}")
            return False

        state = OnboardingState.from_dict(state_data)

        try:
            logger.info(f"Running step {step.value} for venue {venue_id}")

            if step == OnboardingStep.CONNECT_TANDA:
                await self._step_connect_tanda(venue_id, state, tanda_credentials)
            elif step == OnboardingStep.IMPORT_EMPLOYEES:
                await self._step_import_employees(venue_id, state, tanda_credentials)
            elif step == OnboardingStep.IMPORT_ROSTERS:
                await self._step_import_rosters(venue_id, state, tanda_credentials)
            elif step == OnboardingStep.IMPORT_DEPARTMENTS:
                await self._step_import_departments(venue_id, state, tanda_credentials)
            elif step == OnboardingStep.CONFIGURE_VENUE:
                await self._step_configure_venue(venue_id, state)
            elif step == OnboardingStep.COMPLETE:
                await self._step_complete(venue_id, state)
            else:
                raise ValueError(f"Unknown step: {step}")

            # Mark as completed
            if step.value not in state.completed_steps:
                state.completed_steps.append(step.value)
            state.current_step = self._get_next_step(step)
            state.last_error = None

            self.db.save_onboarding_state(state.to_dict())
            logger.info(f"Step {step.value} completed for venue {venue_id}")
            return True

        except Exception as e:
            logger.error(f"Step {step.value} failed for venue {venue_id}: {str(e)}")
            state.errors.append({
                "step": step.value,
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
            })
            state.last_error = str(e)
            self.db.save_onboarding_state(state.to_dict())
            return False

    async def run_all(self, venue_id: str, tanda_credentials: Optional[TandaCredentials] = None) -> bool:
        """
        Execute all steps sequentially.

        Returns:
            True if all steps completed, False if any failed
        """
        logger.info(f"Running all onboarding steps for venue {venue_id}")

        for step in self.STEP_ORDER:
            success = await self.run_step(venue_id, step, tanda_credentials)
            if not success:
                logger.warning(f"Onboarding halted at step {step.value} for venue {venue_id}")
                return False

        return True

    async def retry_step(self, venue_id: str, tanda_credentials: Optional[TandaCredentials] = None) -> bool:
        """
        Retry the current failed step.

        Returns:
            True if successful, False on error
        """
        state_data = self.db.get_onboarding_state(venue_id)
        if not state_data:
            return False

        state = OnboardingState.from_dict(state_data)
        return await self.run_step(venue_id, state.current_step, tanda_credentials)

    # ========================================================================
    # Step implementations
    # ========================================================================

    async def _step_connect_tanda(
        self,
        venue_id: str,
        state: OnboardingState,
        credentials: Optional[TandaCredentials],
    ) -> None:
        """Validate token, fetch venue info from Tanda."""
        if not credentials:
            raise ValueError("Tanda credentials required for CONNECT_TANDA")

        async with TandaAdapter(credentials) as tanda:
            # Health check
            if not await tanda.health_check():
                raise RuntimeError("Tanda API health check failed")

            # Fetch venue info (org_id from credentials)
            org_id = credentials.org_id
            state.venue_name = org_id  # Store org_id as venue identifier

        logger.info(f"Connected to Tanda organization {org_id}")

    async def _step_import_employees(
        self,
        venue_id: str,
        state: OnboardingState,
        credentials: Optional[TandaCredentials],
    ) -> None:
        """Pull all employees, map to Employee model, save to DB."""
        if not credentials:
            raise ValueError("Tanda credentials required for IMPORT_EMPLOYEES")

        async with TandaAdapter(credentials) as tanda:
            employees = await tanda.get_employees()
            logger.info(f"Fetched {len(employees)} employees from Tanda")

            # Add venue_id to each employee for DB storage
            for emp in employees:
                emp.venue_id = venue_id

            self.db.save_employees(employees)
            state.imported_counts["employees"] = len(employees)

    async def _step_import_rosters(
        self,
        venue_id: str,
        state: OnboardingState,
        credentials: Optional[TandaCredentials],
    ) -> None:
        """Pull current + next week rosters, map to Roster/Shift models, save."""
        if not credentials:
            raise ValueError("Tanda credentials required for IMPORT_ROSTERS")

        async with TandaAdapter(credentials) as tanda:
            rosters = []
            shifts = []

            # Get current week + next week
            today = date.today()
            week_start = today - timedelta(days=today.weekday())

            for week_offset in [0, 1]:
                week_date = week_start + timedelta(weeks=week_offset)
                roster = await tanda.get_roster(week_date)

                if roster:
                    roster.venue_id = venue_id
                    rosters.append(roster)
                    shifts.extend(roster.shifts)
                    self.db.save_roster(roster)

            state.imported_counts["rosters"] = len(rosters)
            state.imported_counts["shifts"] = len(shifts)
            logger.info(f"Imported {len(rosters)} rosters with {len(shifts)} shifts")

    async def _step_import_departments(
        self,
        venue_id: str,
        state: OnboardingState,
        credentials: Optional[TandaCredentials],
    ) -> None:
        """Pull department list, store in venue config."""
        if not credentials:
            raise ValueError("Tanda credentials required for IMPORT_DEPARTMENTS")

        # Note: Tanda doesn't have a dedicated departments endpoint in this adapter
        # This is a placeholder for future expansion
        # For now, we'll track 0 departments
        state.imported_counts["departments"] = 0
        logger.info("Department import: no dedicated endpoint available")

    async def _step_configure_venue(self, venue_id: str, state: OnboardingState) -> None:
        """Set default award rules, timezone, create VenueConfig."""
        # Create venue config with defaults
        venue_id_internal = str(uuid4())

        config = VenueConfig(
            id=venue_id_internal,
            name=state.venue_name or f"Venue {venue_id}",
            tanda_org_id=venue_id,
            state=State.vic,  # Default to Victoria
            timezone="Australia/Melbourne",  # Default timezone
            min_staff={},  # Will be configured per role later
            max_labour_pct=30.0,  # Default 30% labour percentage
            pos_system=None,  # Will be set via integrations
            created_at=datetime.utcnow(),
        )

        self.db.save_venue(config)
        state.venue_config_id = venue_id_internal
        logger.info(f"Created venue config {venue_id_internal} for {venue_id}")

    async def _step_complete(self, venue_id: str, state: OnboardingState) -> None:
        """Mark venue as onboarded, log completion."""
        elapsed = datetime.utcnow() - state.started_at
        logger.info(
            f"Onboarding completed for venue {venue_id} "
            f"(took {elapsed.total_seconds():.1f}s). "
            f"Imported: {state.imported_counts['employees']} employees, "
            f"{state.imported_counts['rosters']} rosters, "
            f"{state.imported_counts['shifts']} shifts"
        )

    # ========================================================================
    # Helper methods
    # ========================================================================

    def _get_step_index(self, step: OnboardingStep) -> int:
        """Get the index of a step in the sequence."""
        try:
            return self.STEP_ORDER.index(step)
        except ValueError:
            return -1

    def _get_next_step(self, current: OnboardingStep) -> OnboardingStep:
        """Get the next step in the sequence."""
        current_index = self._get_step_index(current)
        next_index = current_index + 1

        if next_index < len(self.STEP_ORDER):
            return self.STEP_ORDER[next_index]

        return OnboardingStep.COMPLETE

    def get_summary(self, venue_id: str) -> Optional[dict]:
        """
        Get full import summary with counts.

        Returns:
            Dict with import statistics or None if not found
        """
        state_data = self.db.get_onboarding_state(venue_id)
        if not state_data:
            return None

        state = OnboardingState.from_dict(state_data)

        return {
            "venue_id": venue_id,
            "venue_name": state.venue_name,
            "status": "completed" if OnboardingStep.COMPLETE.value in state.completed_steps else "in_progress",
            "imported_counts": state.imported_counts,
            "total_errors": len(state.errors),
            "started_at": state.started_at.isoformat(),
            "completed_steps": state.completed_steps,
        }
