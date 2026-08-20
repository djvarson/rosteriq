"""
Tanda roster push service — pushes RosterIQ-optimised rosters back to Tanda via their API.

This module handles:
- Mapping RosterIQ Shift objects to Tanda schedule format
- Pushing rosters to Tanda (with optional dry-run mode)
- Diffing rosters to identify additions, removals, and changes
- Individual shift push/delete operations
- Rate limiting (max 10 requests/second)
- Validation of employee mappings

Usage:
    tanda = TandaAdapter(credentials)
    pusher = TandaRosterPush(tanda)

    # Diff to see what would change
    diff = await pusher.diff_roster(roster, venue_id)
    print(f"Would add {len(diff.new_shifts)} shifts")

    # Push with dry-run enabled (default)
    result = await pusher.push_roster(roster, venue_id, dry_run=True)

    # Push for real
    result = await pusher.push_roster(roster, venue_id, dry_run=False)
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Dict, List

from rosteriq.models import Roster, Shift, Employee
from rosteriq.tanda_adapter import TandaAdapter, TandaAPIError


logger = logging.getLogger(__name__)


# ============================================================================
# Data Classes (not Pydantic)
# ============================================================================


@dataclass
class PushResult:
    """Result of pushing a roster to Tanda."""

    success_count: int = 0
    failed_count: int = 0
    errors: List[str] = field(default_factory=list)
    dry_run: bool = True
    pushed_shift_ids: List[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.now)

    def add_error(self, error: str) -> None:
        """Log an error."""
        self.errors.append(error)
        self.failed_count += 1

    def add_success(self, shift_id: str) -> None:
        """Log a successful push."""
        self.pushed_shift_ids.append(shift_id)
        self.success_count += 1

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dict."""
        return {
            "success_count": self.success_count,
            "failed_count": self.failed_count,
            "errors": self.errors,
            "dry_run": self.dry_run,
            "pushed_shift_ids": self.pushed_shift_ids,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class ShiftMapping:
    """Maps a RosterIQ shift ID to a Tanda schedule ID."""

    rosteriq_shift_id: str
    tanda_shift_id: Optional[str] = None
    tanda_user_id: Optional[str] = None


@dataclass
class RosterDiff:
    """Comparison between RosterIQ roster and current Tanda roster."""

    new_shifts: List[Shift] = field(default_factory=list)
    removed_shifts: List[Dict] = field(default_factory=list)  # Tanda shift dicts
    changed_shifts: List[Dict] = field(default_factory=list)  # List of {rosteriq, tanda}
    unchanged_count: int = 0

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dict."""
        return {
            "new_shifts_count": len(self.new_shifts),
            "removed_shifts_count": len(self.removed_shifts),
            "changed_shifts_count": len(self.changed_shifts),
            "unchanged_count": self.unchanged_count,
            "summary": {
                "new": [
                    {
                        "shift_id": s.id,
                        "employee_id": s.employee_id,
                        "date": s.date.isoformat(),
                        "start": s.start_time.isoformat(),
                        "end": s.end_time.isoformat(),
                    }
                    for s in self.new_shifts
                ],
                "removed": [
                    {
                        "tanda_shift_id": s.get("id"),
                        "user_id": s.get("user_id"),
                        "date": s.get("start", "")[:10],
                    }
                    for s in self.removed_shifts
                ],
            },
        }


# ============================================================================
# Rate Limiter
# ============================================================================


class PushRateLimiter:
    """Rate limiter for push operations (max 10 requests/second)."""

    def __init__(self, max_requests: int = 10, window_seconds: float = 1.0):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests: List[float] = []

    async def acquire(self) -> None:
        """Wait if necessary to stay within rate limits."""
        self._clean_old_requests()

        if len(self.requests) >= self.max_requests:
            oldest = self.requests[0]
            wait_time = self.window_seconds - (time.time() - oldest)
            if wait_time > 0:
                logger.debug(f"Rate limit: waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)
            self._clean_old_requests()

        self.requests.append(time.time())

    def _clean_old_requests(self) -> None:
        """Remove request timestamps outside the current window."""
        cutoff = time.time() - self.window_seconds
        self.requests = [t for t in self.requests if t > cutoff]


# ============================================================================
# TandaRosterPush Service
# ============================================================================


class TandaRosterPush:
    """
    Pushes RosterIQ-optimised rosters back to Tanda via their API.

    Handles mapping, diffing, validation, and safe push operations.
    """

    def __init__(self, tanda: TandaAdapter):
        """
        Initialize the roster push service.

        Args:
            tanda: An initialized TandaAdapter instance (provides auth & HTTP).
        """
        self.tanda = tanda
        self.rate_limiter = PushRateLimiter()
        self._employee_id_map: Dict[str, str] = {}  # RosterIQ ID -> Tanda user ID
        # RosterIQ-shift-id -> Tanda-schedule-id, so re-publishing a week is
        # idempotent: a shift already pushed maps to its Tanda id and is
        # updated in place instead of being POSTed again (which would create a
        # duplicate schedule in Tanda).
        self._shift_id_map: Dict[str, str] = {}

    async def push_roster(
        self,
        roster: Roster,
        venue_id: str,
        dry_run: bool = True,
    ) -> PushResult:
        """
        Push a roster to Tanda, idempotently.

        Drives off diff_roster() rather than blindly POSTing every shift:
        - NEW shifts (present in RosterIQ, absent in Tanda) are created (POST).
        - CHANGED shifts (present in both, timing/assignment differs) are
          updated in place (PUT) against their existing Tanda schedule id.
        - REMOVED shifts (present in Tanda, absent in RosterIQ) are deleted.
        - UNCHANGED shifts are left untouched.

        This means re-publishing the same week does NOT create duplicate
        shifts in Tanda — the previous implementation POSTed every shift on
        every call, duplicating the entire roster on each re-publish.

        Args:
            roster: The RosterIQ Roster to push.
            venue_id: Tanda venue/org ID.
            dry_run: If True, validate + diff without mutating Tanda.

        Returns:
            PushResult with success/failure counts and details.
        """
        result = PushResult(dry_run=dry_run)

        logger.info(
            f"Starting roster push: {len(roster.shifts)} shifts, dry_run={dry_run}"
        )

        # Validate all shifts have valid employee mappings up front.
        for shift in roster.shifts:
            if not await self._validate_employee_mapping(shift.employee_id):
                error = f"No Tanda user ID mapping for employee {shift.employee_id}"
                logger.warning(error)
                result.add_error(error)

        if result.failed_count > 0:
            logger.error(
                f"Validation failed: {result.failed_count} shifts have missing employee mappings"
            )
            return result

        # Diff against what is currently in Tanda so we only apply deltas.
        diff = await self.diff_roster(roster, venue_id)

        tanda_roster_id = venue_id

        # 1) CREATE new shifts (POST). Record the resulting Tanda schedule id
        #    so a subsequent re-publish updates rather than re-creates.
        for shift in diff.new_shifts:
            try:
                if not dry_run:
                    response = await self.push_shift(shift, tanda_roster_id)
                    tanda_id = self._extract_tanda_shift_id(response)
                    if tanda_id is not None:
                        self._shift_id_map[shift.id] = tanda_id
                result.add_success(shift.id)
                logger.debug(f"Created shift {shift.id}")
            except TandaAPIError as e:
                error = f"Failed to create shift {shift.id}: {e.api_error.message}"
                logger.error(error)
                result.add_error(error)
            except Exception as e:
                error = f"Unexpected error creating shift {shift.id}: {str(e)}"
                logger.error(error)
                result.add_error(error)

        # 2) UPDATE changed shifts in place (PUT) against the existing Tanda id.
        for change in diff.changed_shifts:
            rosteriq_shift: Shift = change["rosteriq"]
            tanda_shift = change["tanda"]
            tanda_id = getattr(tanda_shift, "id", None) or self._shift_id_map.get(
                rosteriq_shift.id
            )
            try:
                if not dry_run:
                    if tanda_id is None:
                        # No existing id to target — fall back to a create so
                        # the shift still lands, but never blind-POST a shift
                        # we believe already exists.
                        response = await self.push_shift(rosteriq_shift, tanda_roster_id)
                        new_id = self._extract_tanda_shift_id(response)
                        if new_id is not None:
                            self._shift_id_map[rosteriq_shift.id] = new_id
                    else:
                        await self.update_shift(
                            rosteriq_shift, str(tanda_id), tanda_roster_id
                        )
                        self._shift_id_map[rosteriq_shift.id] = str(tanda_id)
                result.add_success(rosteriq_shift.id)
                logger.debug(f"Updated shift {rosteriq_shift.id} -> Tanda {tanda_id}")
            except TandaAPIError as e:
                error = f"Failed to update shift {rosteriq_shift.id}: {e.api_error.message}"
                logger.error(error)
                result.add_error(error)
            except Exception as e:
                error = f"Unexpected error updating shift {rosteriq_shift.id}: {str(e)}"
                logger.error(error)
                result.add_error(error)

        # 3) DELETE shifts that are in Tanda but no longer in RosterIQ.
        for removed in diff.removed_shifts:
            tanda_id = removed.get("id")
            if tanda_id is None:
                continue
            try:
                if not dry_run:
                    ok = await self.delete_shift(str(tanda_id))
                    if not ok:
                        result.add_error(f"Failed to delete Tanda shift {tanda_id}")
                        continue
                result.add_success(f"deleted:{tanda_id}")
                logger.debug(f"Deleted Tanda shift {tanda_id}")
            except Exception as e:
                error = f"Unexpected error deleting Tanda shift {tanda_id}: {str(e)}"
                logger.error(error)
                result.add_error(error)

        logger.info(
            f"Roster push complete: {result.success_count} succeeded, "
            f"{result.failed_count} failed "
            f"(created={len(diff.new_shifts)}, changed={len(diff.changed_shifts)}, "
            f"removed={len(diff.removed_shifts)}, unchanged={diff.unchanged_count})"
        )
        return result

    @staticmethod
    def _extract_tanda_shift_id(response: dict) -> Optional[str]:
        """
        Pull the created schedule id out of a Tanda POST /schedules response.

        Tanda may return the record directly ({"id": ...}) or wrapped in a
        "schedule"/"data" envelope. Returns None if no id is present.
        """
        if not isinstance(response, dict):
            return None
        for container in (response, response.get("schedule"), response.get("data")):
            if isinstance(container, dict) and container.get("id") is not None:
                return str(container["id"])
        return None

    async def diff_roster(
        self,
        roster: Roster,
        venue_id: str,
    ) -> RosterDiff:
        """
        Compare RosterIQ roster against current Tanda roster.

        Args:
            roster: The RosterIQ Roster to compare.
            venue_id: Tanda venue/org ID.

        Returns:
            RosterDiff with new, removed, and changed shifts.
        """
        logger.info(f"Diffing roster for venue {venue_id}")

        diff = RosterDiff()

        # Fetch current shifts from Tanda
        try:
            tanda_shifts = await self.tanda.get_shifts(roster.week_start, roster.week_end)
        except TandaAPIError as e:
            logger.error(f"Failed to fetch current Tanda shifts: {e}")
            return diff

        # Build mapping of (employee_id, date, time) to Tanda shift
        tanda_shift_map = {}
        for ts in tanda_shifts:
            key = (ts.employee_id, ts.date, ts.start_time, ts.end_time)
            tanda_shift_map[key] = ts

        # Compare
        rosteriq_shifts_seen = set()

        for rosteriq_shift in roster.shifts:
            key = (
                rosteriq_shift.employee_id,
                rosteriq_shift.date,
                rosteriq_shift.start_time,
                rosteriq_shift.end_time,
            )
            rosteriq_shifts_seen.add(key)

            if key in tanda_shift_map:
                # Shift exists in both — check for changes
                tanda_shift = tanda_shift_map[key]
                if self._shifts_differ(rosteriq_shift, tanda_shift):
                    diff.changed_shifts.append({
                        "rosteriq": rosteriq_shift,
                        "tanda": tanda_shift,
                    })
                else:
                    diff.unchanged_count += 1
            else:
                # New shift in RosterIQ
                diff.new_shifts.append(rosteriq_shift)

        # Find removed shifts (in Tanda but not in RosterIQ)
        for tanda_shift in tanda_shifts:
            key = (
                tanda_shift.employee_id,
                tanda_shift.date,
                tanda_shift.start_time,
                tanda_shift.end_time,
            )
            if key not in rosteriq_shifts_seen:
                diff.removed_shifts.append({
                    "id": tanda_shift.id,
                    "user_id": tanda_shift.employee_id,
                    "start": tanda_shift.date.isoformat(),
                })

        logger.info(
            f"Diff complete: {len(diff.new_shifts)} new, "
            f"{len(diff.removed_shifts)} removed, "
            f"{len(diff.changed_shifts)} changed, "
            f"{diff.unchanged_count} unchanged"
        )
        return diff

    async def push_shift(self, shift: Shift, tanda_roster_id: str) -> dict:
        """
        Push a single shift to Tanda.

        Args:
            shift: The Shift to push.
            tanda_roster_id: Tanda roster ID (or venue ID).

        Returns:
            Tanda API response dict.

        Raises:
            TandaAPIError: If the push fails.
        """
        # Get Tanda user ID for this employee
        tanda_user_id = await self._get_tanda_user_id(shift.employee_id)
        if not tanda_user_id:
            raise ValueError(f"No Tanda user ID for employee {shift.employee_id}")

        # Build Tanda shift payload
        payload = self._build_tanda_shift_payload(shift, tanda_user_id, tanda_roster_id)

        await self.rate_limiter.acquire()

        logger.info(
            f"Pushing shift {shift.id} (employee {shift.employee_id}, "
            f"{shift.date} {shift.start_time}-{shift.end_time})"
        )

        # POST to Tanda
        response = await self.tanda._request("POST", "/schedules", json=payload)
        return response

    async def update_shift(
        self,
        shift: Shift,
        tanda_shift_id: str,
        tanda_roster_id: str,
    ) -> dict:
        """
        Update an existing Tanda schedule in place (PUT /schedules/{id}).

        Used for shifts that already exist in Tanda but whose timing or
        assignment changed in RosterIQ — avoids creating a duplicate.

        Args:
            shift: The RosterIQ Shift with the new values.
            tanda_shift_id: The existing Tanda schedule id to update.
            tanda_roster_id: Tanda roster ID (or venue ID).

        Returns:
            Tanda API response dict.

        Raises:
            TandaAPIError: If the update fails.
        """
        tanda_user_id = await self._get_tanda_user_id(shift.employee_id)
        if not tanda_user_id:
            raise ValueError(f"No Tanda user ID for employee {shift.employee_id}")

        payload = self._build_tanda_shift_payload(shift, tanda_user_id, tanda_roster_id)

        await self.rate_limiter.acquire()

        logger.info(
            f"Updating Tanda shift {tanda_shift_id} from RosterIQ shift {shift.id}"
        )

        response = await self.tanda._request(
            "PUT", f"/schedules/{tanda_shift_id}", json=payload
        )
        return response

    async def delete_shift(self, tanda_shift_id: str) -> bool:
        """
        Delete a shift from Tanda.

        Args:
            tanda_shift_id: The Tanda schedule ID to delete.

        Returns:
            True if successful, False otherwise.
        """
        await self.rate_limiter.acquire()

        logger.info(f"Deleting Tanda shift {tanda_shift_id}")

        try:
            await self.tanda._request("DELETE", f"/schedules/{tanda_shift_id}")
            return True
        except TandaAPIError as e:
            logger.error(f"Failed to delete shift {tanda_shift_id}: {e}")
            return False

    # ========================================================================
    # Private helpers
    # ========================================================================

    async def _validate_employee_mapping(self, rosteriq_employee_id: str) -> bool:
        """Check if employee has a valid Tanda user ID mapping."""
        tanda_id = await self._get_tanda_user_id(rosteriq_employee_id)
        return tanda_id is not None

    async def _get_tanda_user_id(self, rosteriq_employee_id: str) -> Optional[str]:
        """
        Get Tanda user ID for a RosterIQ employee.

        Checks cache first, then falls back to fetching from Tanda API.
        """
        # Check cache
        if rosteriq_employee_id in self._employee_id_map:
            return self._employee_id_map[rosteriq_employee_id]

        # For now, assume RosterIQ employee ID is the same as Tanda user ID
        # In production, you'd look this up from a proper mapping table
        self._employee_id_map[rosteriq_employee_id] = rosteriq_employee_id
        return rosteriq_employee_id

    def _build_tanda_shift_payload(
        self,
        shift: Shift,
        tanda_user_id: str,
        tanda_roster_id: str,
    ) -> dict:
        """
        Convert RosterIQ Shift to Tanda schedule API format.

        Tanda expects:
        {
            "user_id": 123,
            "start": "2026-04-15T09:00:00+10:00",
            "finish": "2026-04-15T17:00:00+10:00",
            "department_id": 456,
            "roster_id": 789
        }
        """
        # Convert times to ISO datetime (assumes Melbourne/Australian timezone)
        # In production, use the venue's actual timezone
        from datetime import datetime as dt
        from zoneinfo import ZoneInfo

        # Assume Australia/Melbourne for now
        tz = ZoneInfo("Australia/Melbourne")
        start_dt = dt.combine(shift.date, shift.start_time, tzinfo=tz)
        end_dt = dt.combine(shift.date, shift.end_time, tzinfo=tz)

        # Handle overnight shifts
        if end_dt < start_dt:
            # timedelta, not replace(day=+1): on the 31st that becomes day=32
            # and raises ValueError, so every month-end overnight shift failed
            # to reach Tanda.
            end_dt = end_dt + timedelta(days=1)

        return {
            "user_id": int(tanda_user_id),
            "start": start_dt.isoformat(),
            "finish": end_dt.isoformat(),
            "department_id": 1,  # Default department — customize as needed
            "roster_id": int(tanda_roster_id),
            "role": shift.role,
            "break_minutes": shift.break_minutes,
        }

    def _shifts_differ(self, rosteriq_shift: Shift, tanda_shift: Shift) -> bool:
        """
        Check if two shifts differ materially.

        Ignores ID, status, and timestamps — focuses on timing and assignment.
        """
        return (
            rosteriq_shift.employee_id != tanda_shift.employee_id
            or rosteriq_shift.date != tanda_shift.date
            or rosteriq_shift.start_time != tanda_shift.start_time
            or rosteriq_shift.end_time != tanda_shift.end_time
            or rosteriq_shift.break_minutes != tanda_shift.break_minutes
        )

    def set_employee_id_mapping(self, mapping: Dict[str, str]) -> None:
        """
        Set a custom employee ID mapping (RosterIQ ID -> Tanda user ID).

        Args:
            mapping: Dict mapping RosterIQ employee IDs to Tanda user IDs.
        """
        self._employee_id_map.update(mapping)
        logger.info(f"Updated employee ID mapping with {len(mapping)} entries")
