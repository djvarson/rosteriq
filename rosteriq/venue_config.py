"""Venue Configuration & Settings Manager for RosterIQ (Round 39).

Centralises all venue-specific configuration (operating hours, roles, penalty
rates, staffing levels, integrations) into a single source of truth.

Thread-safe singleton store with SQLite persistence and versioning support.

Data model:
- DayOfWeek: enum for day-of-week (0-6)
- VenueArea: physical areas within a venue with capacity and cert requirements
- OperatingHours: venue open/close times by day of week
- RoleConfig: staff role definitions with staffing targets
- StaffingLevel: hourly/area-specific staffing minimums and ideals
- PenaltyOverride: award multipliers for special periods (Saturday, public holidays, etc.)
- IntegrationConfig: third-party integration setup (Tanda, Deputy, etc.)
- VenueConfig: complete venue configuration with versioning

Functions:
- create_default_config() → default AU pub configuration
- get_config() → get current config (or create default if missing)
- update_config() → partial update with automatic version save
- get_config_history() → retrieve version history (last N versions)
- rollback_config() → restore a previous version
- validate_config() → consistency checking (hours, staffing, capacity)
- get_staffing_requirement() → min/ideal staffing for time slot
- is_open() → venue open at this time?
"""

from __future__ import annotations

import json
import logging
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from rosteriq import persistence as _p

logger = logging.getLogger("rosteriq.venue_config")

# Register schema at module load
_p.register_schema(
    "venue_config",
    """
    CREATE TABLE IF NOT EXISTS venue_configs (
        id TEXT PRIMARY KEY,
        config_id TEXT NOT NULL,
        venue_id TEXT NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        is_current BOOLEAN NOT NULL DEFAULT 1,
        config_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_venue_id ON venue_configs(venue_id);
    CREATE INDEX IF NOT EXISTS idx_venue_version ON venue_configs(venue_id, version DESC);
    """,
)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class DayOfWeek(int, Enum):
    """Day of week (0 = Monday, 6 = Sunday)."""
    MON = 0
    TUE = 1
    WED = 2
    THU = 3
    FRI = 4
    SAT = 5
    SUN = 6

    @classmethod
    def from_name(cls, name: str) -> DayOfWeek:
        """Get DayOfWeek from short name (MON, TUE, etc.)."""
        return cls[name.upper()]


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class VenueArea:
    """Physical area within a venue."""
    area_id: str  # Unique identifier (e.g. "area_001", "bar", "kitchen")
    name: str  # Display name (e.g. "Main Bar", "Kitchen", "Beer Garden")
    capacity: int  # Maximum staff capacity for this area
    requires_certs: List[str] = field(default_factory=list)  # e.g. ["RSA", "RSG"]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> VenueArea:
        return cls(**d)


@dataclass
class OperatingHours:
    """Venue operating hours for a specific day."""
    day: DayOfWeek
    open_time: str  # "HH:MM" format, e.g. "10:00"
    close_time: str  # "HH:MM" format, e.g. "23:59" or "00:00" (midnight)
    is_closed: bool = False  # Override: venue closed this day

    def to_dict(self) -> Dict[str, Any]:
        return {
            "day": self.day.value,
            "open_time": self.open_time,
            "close_time": self.close_time,
            "is_closed": self.is_closed,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> OperatingHours:
        d = dict(d)
        d["day"] = DayOfWeek(d["day"])
        return cls(**d)


@dataclass
class RoleConfig:
    """Staff role definition."""
    role_name: str  # e.g. "Bar", "Floor", "Kitchen", "Manager"
    min_staff_per_shift: int  # Minimum staff required for any shift
    ideal_staff_per_shift: int  # Ideal/target staff per shift
    hourly_rate_override: Optional[float] = None  # Override base rate if set
    requires_certs: List[str] = field(default_factory=list)  # e.g. ["RSA", "RSG"]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RoleConfig:
        return cls(**d)


@dataclass
class StaffingLevel:
    """Hour-specific, area-specific staffing requirement."""
    day: DayOfWeek
    hour: int  # 0-23
    area_id: str
    min_staff: int  # Minimum required
    ideal_staff: int  # Target ideal

    def to_dict(self) -> Dict[str, Any]:
        return {
            "day": self.day.value,
            "hour": self.hour,
            "area_id": self.area_id,
            "min_staff": self.min_staff,
            "ideal_staff": self.ideal_staff,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> StaffingLevel:
        d = dict(d)
        d["day"] = DayOfWeek(d["day"])
        return cls(**d)


@dataclass
class PenaltyOverride:
    """Award multiplier override for special periods."""
    name: str  # e.g. "Saturday Penalty", "Public Holiday", "Evening Shift"
    multiplier: float  # e.g. 1.5 for 50% penalty
    applies_to: str  # e.g. "saturday", "sunday", "public_holiday", "evening"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PenaltyOverride:
        return cls(**d)


@dataclass
class IntegrationConfig:
    """Third-party integration configuration."""
    provider: str  # "tanda", "deputy", "humanforce"
    api_key_ref: str  # Reference key (NOT the actual secret)
    org_id: str  # Organisation/account ID with provider
    enabled: bool = True
    last_sync: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "provider": self.provider,
            "api_key_ref": self.api_key_ref,
            "org_id": self.org_id,
            "enabled": self.enabled,
            "last_sync": self.last_sync.isoformat() if self.last_sync else None,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> IntegrationConfig:
        d = dict(d)
        d["last_sync"] = (
            datetime.fromisoformat(d["last_sync"]) if d.get("last_sync") else None
        )
        return cls(**d)


@dataclass
class VenueConfig:
    """Complete venue configuration with versioning."""
    config_id: str  # Unique config ID (for versioning)
    venue_id: str  # Venue identifier
    venue_name: str  # Display name
    timezone: str = "Australia/Brisbane"  # IANA timezone
    currency: str = "AUD"
    areas: List[VenueArea] = field(default_factory=list)
    operating_hours: List[OperatingHours] = field(default_factory=list)  # 7 entries, one per day
    roles: List[RoleConfig] = field(default_factory=list)
    staffing_levels: List[StaffingLevel] = field(default_factory=list)
    penalty_overrides: List[PenaltyOverride] = field(default_factory=list)
    integrations: List[IntegrationConfig] = field(default_factory=list)
    budget_target_labour_pct: float = 30.0
    break_compliance_enabled: bool = True
    fatigue_management_enabled: bool = True
    max_shift_hours: float = 10.0
    min_gap_hours: float = 11.0
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "config_id": self.config_id,
            "venue_id": self.venue_id,
            "venue_name": self.venue_name,
            "timezone": self.timezone,
            "currency": self.currency,
            "areas": [a.to_dict() for a in self.areas],
            "operating_hours": [oh.to_dict() for oh in self.operating_hours],
            "roles": [r.to_dict() for r in self.roles],
            "staffing_levels": [sl.to_dict() for sl in self.staffing_levels],
            "penalty_overrides": [po.to_dict() for po in self.penalty_overrides],
            "integrations": [i.to_dict() for i in self.integrations],
            "budget_target_labour_pct": self.budget_target_labour_pct,
            "break_compliance_enabled": self.break_compliance_enabled,
            "fatigue_management_enabled": self.fatigue_management_enabled,
            "max_shift_hours": self.max_shift_hours,
            "min_gap_hours": self.min_gap_hours,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> VenueConfig:
        d = dict(d)
        d["areas"] = [VenueArea.from_dict(a) for a in d.get("areas", [])]
        d["operating_hours"] = [
            OperatingHours.from_dict(oh) for oh in d.get("operating_hours", [])
        ]
        d["roles"] = [RoleConfig.from_dict(r) for r in d.get("roles", [])]
        d["staffing_levels"] = [
            StaffingLevel.from_dict(sl) for sl in d.get("staffing_levels", [])
        ]
        d["penalty_overrides"] = [
            PenaltyOverride.from_dict(po) for po in d.get("penalty_overrides", [])
        ]
        d["integrations"] = [
            IntegrationConfig.from_dict(i) for i in d.get("integrations", [])
        ]
        # Handle both string and datetime objects
        if isinstance(d["created_at"], str):
            d["created_at"] = datetime.fromisoformat(d["created_at"])
        if isinstance(d["updated_at"], str):
            d["updated_at"] = datetime.fromisoformat(d["updated_at"])
        return cls(**d)


# ---------------------------------------------------------------------------
# Store (thread-safe singleton with versioning)
# ---------------------------------------------------------------------------


class VenueConfigStore:
    """Thread-safe store for venue configs with version history."""

    def __init__(self):
        self._configs: Dict[str, VenueConfig] = {}  # venue_id -> current config
        self._history: Dict[str, List[VenueConfig]] = {}  # venue_id -> [configs by version]
        self._lock = threading.RLock()
        self._rehydrate()

    def _rehydrate(self) -> None:
        """Load all configs from persistence."""
        if not _p.is_persistence_enabled():
            return
        try:
            rows = _p.fetchall(
                "SELECT config_json FROM venue_configs WHERE is_current = 1",
            )
            for row in rows:
                config = VenueConfig.from_dict(json.loads(row["config_json"]))
                self._configs[config.venue_id] = config
        except Exception as e:
            logger.warning("rehydrate failed: %s", e)

    def get_config(self, venue_id: str) -> Optional[VenueConfig]:
        """Get current config for venue."""
        with self._lock:
            return self._configs.get(venue_id)

    def get_config_history(
        self, venue_id: str, limit: int = 5
    ) -> List[VenueConfig]:
        """Get version history for venue (most recent first)."""
        if not _p.is_persistence_enabled():
            config = self._configs.get(venue_id)
            return [config] if config else []
        try:
            rows = _p.fetchall(
                "SELECT config_json FROM venue_configs "
                "WHERE venue_id = ? ORDER BY ROWID DESC LIMIT ?",
                [venue_id, limit],
            )
            return [VenueConfig.from_dict(json.loads(row["config_json"])) for row in rows]
        except Exception as e:
            logger.warning("get_config_history failed: %s", e)
            config = self._configs.get(venue_id)
            return [config] if config else []

    def create_config(self, config: VenueConfig) -> None:
        """Create a new config, marking previous as historical."""
        with self._lock:
            # Mark previous as non-current
            if config.venue_id in self._configs:
                if _p.is_persistence_enabled():
                    try:
                        _p.connection().execute(
                            "UPDATE venue_configs SET is_current = 0 "
                            "WHERE venue_id = ? AND is_current = 1",
                            [config.venue_id],
                        )
                    except Exception as e:
                        logger.warning("update previous to historical failed: %s", e)

            # Store new config
            self._configs[config.venue_id] = config
            if _p.is_persistence_enabled():
                _p.upsert(
                    "venue_configs",
                    {
                        "id": config.config_id,
                        "config_id": config.config_id,
                        "venue_id": config.venue_id,
                        "version": 1,  # Will be incremented in caller
                        "is_current": 1,
                        "config_json": json.dumps(config.to_dict()),
                        "created_at": config.created_at.isoformat(),
                        "updated_at": config.updated_at.isoformat(),
                    },
                    pk="id",
                )

    def update_config(self, config: VenueConfig) -> None:
        """Update config and save previous version."""
        with self._lock:
            self.create_config(config)

    def rollback_to_version(
        self, venue_id: str, version_index: int
    ) -> Optional[VenueConfig]:
        """Rollback to a specific historical version."""
        history = self.get_config_history(venue_id)
        if version_index >= len(history):
            return None
        old_config = history[version_index]
        new_config = VenueConfig(
            config_id=f"config_{uuid.uuid4().hex[:12]}",
            venue_id=old_config.venue_id,
            venue_name=old_config.venue_name,
            timezone=old_config.timezone,
            currency=old_config.currency,
            areas=old_config.areas,
            operating_hours=old_config.operating_hours,
            roles=old_config.roles,
            staffing_levels=old_config.staffing_levels,
            penalty_overrides=old_config.penalty_overrides,
            integrations=old_config.integrations,
            budget_target_labour_pct=old_config.budget_target_labour_pct,
            break_compliance_enabled=old_config.break_compliance_enabled,
            fatigue_management_enabled=old_config.fatigue_management_enabled,
            max_shift_hours=old_config.max_shift_hours,
            min_gap_hours=old_config.min_gap_hours,
            created_at=old_config.created_at,
            updated_at=datetime.now(timezone.utc),
        )
        self.update_config(new_config)
        return new_config


_store: Optional[VenueConfigStore] = None
_store_lock = threading.Lock()


def get_store() -> VenueConfigStore:
    """Get or create the global venue config store."""
    global _store
    with _store_lock:
        if _store is None:
            _store = VenueConfigStore()
    return _store


def _reset_for_tests() -> None:
    """Test helper — reset the store."""
    global _store
    with _store_lock:
        _store = None


# ---------------------------------------------------------------------------
# Default Configuration
# ---------------------------------------------------------------------------


def create_default_config(
    venue_id: str, venue_name: str, venue_type: str = "pub"
) -> VenueConfig:
    """Create a sensible default AU venue configuration.

    Args:
        venue_id: Unique venue identifier
        venue_name: Display name
        venue_type: "pub", "bar", "restaurant" (affects defaults)

    Returns:
        VenueConfig with sensible AU defaults
    """
    # Venue areas
    areas = [
        VenueArea(area_id="main_bar", name="Main Bar", capacity=4, requires_certs=["RSA"]),
        VenueArea(
            area_id="kitchen", name="Kitchen", capacity=3, requires_certs=["Food Handler"]
        ),
        VenueArea(area_id="floor", name="Floor", capacity=3, requires_certs=[]),
    ]

    if venue_type == "pub":
        areas.append(
            VenueArea(
                area_id="gaming",
                name="Gaming Room",
                capacity=2,
                requires_certs=["RSG"],
            )
        )

    # Operating hours (AU standard pub: 10am-midnight weekdays, 10am-2am Fri/Sat)
    operating_hours = [
        OperatingHours(DayOfWeek.MON, "10:00", "23:59"),
        OperatingHours(DayOfWeek.TUE, "10:00", "23:59"),
        OperatingHours(DayOfWeek.WED, "10:00", "23:59"),
        OperatingHours(DayOfWeek.THU, "10:00", "23:59"),
        OperatingHours(DayOfWeek.FRI, "10:00", "00:00"),  # till 2am next day
        OperatingHours(DayOfWeek.SAT, "10:00", "00:00"),  # till 2am next day
        OperatingHours(DayOfWeek.SUN, "10:00", "22:00"),
    ]

    # Roles
    roles = [
        RoleConfig(
            role_name="Bar",
            min_staff_per_shift=1,
            ideal_staff_per_shift=2,
            requires_certs=["RSA"],
        ),
        RoleConfig(
            role_name="Floor",
            min_staff_per_shift=1,
            ideal_staff_per_shift=1,
            requires_certs=[],
        ),
        RoleConfig(
            role_name="Kitchen",
            min_staff_per_shift=1,
            ideal_staff_per_shift=2,
            requires_certs=["Food Handler"],
        ),
        RoleConfig(
            role_name="Manager",
            min_staff_per_shift=1,
            ideal_staff_per_shift=1,
            requires_certs=[],
        ),
    ]

    # Standard AU penalty rates
    penalty_overrides = [
        PenaltyOverride(
            name="Saturday Penalty",
            multiplier=1.5,
            applies_to="saturday",
        ),
        PenaltyOverride(
            name="Sunday Penalty",
            multiplier=2.0,
            applies_to="sunday",
        ),
        PenaltyOverride(
            name="Public Holiday",
            multiplier=2.5,
            applies_to="public_holiday",
        ),
        PenaltyOverride(
            name="Evening Shift",
            multiplier=1.25,
            applies_to="evening",
        ),
    ]

    return VenueConfig(
        config_id=f"config_{uuid.uuid4().hex[:12]}",
        venue_id=venue_id,
        venue_name=venue_name,
        timezone="Australia/Brisbane",
        currency="AUD",
        areas=areas,
        operating_hours=operating_hours,
        roles=roles,
        penalty_overrides=penalty_overrides,
        budget_target_labour_pct=30.0,
        break_compliance_enabled=True,
        fatigue_management_enabled=True,
        max_shift_hours=10.0,
        min_gap_hours=11.0,
    )


# ---------------------------------------------------------------------------
# High-Level API
# ---------------------------------------------------------------------------


def get_config(venue_id: str) -> VenueConfig:
    """Get current config for venue, or create default if none exists."""
    store = get_store()
    config = store.get_config(venue_id)
    if config is not None:
        return config
    # Create default
    config = create_default_config(venue_id, f"Venue {venue_id}")
    store.create_config(config)
    return config


def update_config(venue_id: str, **updates) -> VenueConfig:
    """Partial update to venue config.

    Accepts any VenueConfig field. Returns updated config.
    Previous version is automatically saved.
    """
    store = get_store()
    current = get_config(venue_id)  # Ensures it exists
    config_dict = current.to_dict()
    config_dict.update(updates)
    config_dict["config_id"] = f"config_{uuid.uuid4().hex[:12]}"
    config_dict["updated_at"] = datetime.now(timezone.utc)
    new_config = VenueConfig.from_dict(config_dict)
    store.update_config(new_config)
    return new_config


def get_config_history(venue_id: str, limit: int = 5) -> List[VenueConfig]:
    """Get version history for venue."""
    store = get_store()
    return store.get_config_history(venue_id, limit)


def rollback_config(venue_id: str, version_index: int = 1) -> VenueConfig:
    """Rollback to a previous version by index (0 = current, 1 = previous, etc.)."""
    store = get_store()
    rolled_back = store.rollback_to_version(venue_id, version_index)
    if rolled_back is None:
        raise ValueError(f"No version at index {version_index}")
    return rolled_back


def validate_config(config: VenueConfig) -> Tuple[bool, List[str]]:
    """Validate config consistency.

    Returns:
        (is_valid, list_of_errors)

    Checks:
    - Operating hours format (HH:MM)
    - Staffing levels don't exceed area capacity
    - At least one role defined
    - Staffing times are valid hours (0-23)
    """
    errors = []

    # Check operating hours format
    for oh in config.operating_hours:
        try:
            datetime.strptime(oh.open_time, "%H:%M")
            datetime.strptime(oh.close_time, "%H:%M")
        except ValueError:
            errors.append(
                f"Invalid time format for {oh.day.name}: {oh.open_time}-{oh.close_time}"
            )

    # Check we have 7 days
    if len(config.operating_hours) != 7:
        errors.append(
            f"Operating hours must have 7 entries (one per day), got {len(config.operating_hours)}"
        )

    # Check roles
    if not config.roles:
        errors.append("At least one role must be defined")

    # Check staffing levels
    area_capacities = {a.area_id: a.capacity for a in config.areas}
    for sl in config.staffing_levels:
        if not (0 <= sl.hour <= 23):
            errors.append(f"Invalid hour {sl.hour} in staffing level")
        if sl.area_id not in area_capacities:
            errors.append(f"Staffing level references undefined area {sl.area_id}")
        elif sl.ideal_staff > area_capacities[sl.area_id]:
            errors.append(
                f"Staffing level {sl.ideal_staff} exceeds area {sl.area_id} "
                f"capacity {area_capacities[sl.area_id]}"
            )

    return (len(errors) == 0, errors)


def get_staffing_requirement(
    venue_id: str, day: DayOfWeek, hour: int
) -> Dict[str, Any]:
    """Get staffing requirement for a specific time slot.

    Returns dict with min/ideal staffing by area.
    """
    config = get_config(venue_id)
    requirement = {
        "day": day.name,
        "hour": hour,
        "by_area": {},
    }
    for sl in config.staffing_levels:
        if sl.day == day and sl.hour == hour:
            requirement["by_area"][sl.area_id] = {
                "min": sl.min_staff,
                "ideal": sl.ideal_staff,
            }
    return requirement


def is_open(venue_id: str, day: DayOfWeek, hour: int) -> bool:
    """Check if venue is open at this time.

    Args:
        venue_id: Venue identifier
        day: DayOfWeek enum
        hour: 0-23

    Returns:
        True if venue is open during this hour
    """
    config = get_config(venue_id)
    oh = next((o for o in config.operating_hours if o.day == day), None)
    if oh is None or oh.is_closed:
        return False

    open_h, open_m = map(int, oh.open_time.split(":"))
    close_h, close_m = map(int, oh.close_time.split(":"))

    open_minutes = open_h * 60 + open_m
    close_minutes = close_h * 60 + close_m
    # Check if hour is open: from start of hour to end of hour (not including next hour's start)
    hour_start_minutes = hour * 60
    hour_end_minutes = (hour + 1) * 60

    # Handle midnight crossing (close_time < open_time)
    if close_minutes <= open_minutes:
        # Venue stays open past midnight
        return hour_start_minutes >= open_minutes or hour_end_minutes <= close_minutes
    else:
        return hour_start_minutes < close_minutes and hour_end_minutes > open_minutes
