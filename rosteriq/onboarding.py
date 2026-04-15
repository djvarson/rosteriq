"""Onboarding wizard state machine for new tenants (Round 8 Track D).

A multi-step wizard that walks new tenants through:
  1. venue_basics      — name, timezone, business hours
  2. tanda_connect     — OAuth/API key for Tanda (or "skip — manual rosters")
  3. data_feeds        — POS provider, bookings provider (optional)
  4. team_size         — rough headcount, key roles
  5. billing_tier      — startup / pro / enterprise (or trial)
  6. confirm           — review + commit

State is persisted in-memory keyed by (tenant_id, wizard_id) and is
thread-safe. Production should swap _StateStore for a real DB-backed
implementation; the API surface stays the same.

Why this matters: trials drop off when setup is opaque. The wizard
captures only the bare minimum needed to render a useful demo dashboard
and lights up the rest progressively.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

from rosteriq import persistence as _p

logger = logging.getLogger("rosteriq.onboarding")
AU_TZ = timezone(timedelta(hours=10))


# Round 11 — SQLite persistence schema.
_SCHEMA = """
CREATE TABLE IF NOT EXISTS onboarding_wizards (
    wizard_id        TEXT PRIMARY KEY,
    tenant_id        TEXT NOT NULL,
    started_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    current_step     TEXT NOT NULL,
    completed_steps  TEXT NOT NULL,  -- JSON array
    data             TEXT NOT NULL,  -- JSON object
    completed        INTEGER NOT NULL DEFAULT 0,
    completed_at     TEXT
);
CREATE INDEX IF NOT EXISTS ix_onboarding_tenant
    ON onboarding_wizards(tenant_id);
"""
_p.register_schema("onboarding", _SCHEMA)


# ---------------------------------------------------------------------------
# Step definitions
# ---------------------------------------------------------------------------


@dataclass
class StepSpec:
    key: str
    title: str
    description: str
    required_fields: List[str]
    optional_fields: List[str] = field(default_factory=list)


STEPS: List[StepSpec] = [
    StepSpec(
        key="venue_basics",
        title="Tell us about your venue",
        description="Name, timezone and standard trading hours.",
        required_fields=["venue_name", "timezone"],
        optional_fields=["open_time", "close_time", "country"],
    ),
    StepSpec(
        key="tanda_connect",
        title="Connect Tanda",
        description="Paste your Tanda API key, or skip and use manual rosters.",
        required_fields=[],
        optional_fields=["tanda_api_key", "tanda_org_id", "skip"],
    ),
    StepSpec(
        key="data_feeds",
        title="Data feeds",
        description="Pick a POS and bookings system. Both are optional.",
        required_fields=[],
        optional_fields=["pos_provider", "bookings_provider"],
    ),
    StepSpec(
        key="team_size",
        title="Team size",
        description="Roughly how many people work here?",
        required_fields=["headcount"],
        optional_fields=["roles"],
    ),
    StepSpec(
        key="billing_tier",
        title="Pick a plan",
        description="Choose startup, pro, enterprise — or stay on the 14-day trial.",
        required_fields=["tier"],
        optional_fields=[],
    ),
    StepSpec(
        key="confirm",
        title="Review & finish",
        description="Confirm the details and we'll seed your dashboard.",
        required_fields=[],
        optional_fields=[],
    ),
]

STEP_INDEX = {s.key: i for i, s in enumerate(STEPS)}
ALLOWED_TIERS = {"trial", "startup", "pro", "enterprise"}


# ---------------------------------------------------------------------------
# Wizard state
# ---------------------------------------------------------------------------


@dataclass
class WizardState:
    wizard_id: str
    tenant_id: str
    started_at: datetime
    updated_at: datetime
    current_step: str
    completed_steps: List[str] = field(default_factory=list)
    data: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    completed: bool = False
    completed_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "wizard_id": self.wizard_id,
            "tenant_id": self.tenant_id,
            "started_at": self.started_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "current_step": self.current_step,
            "current_step_index": STEP_INDEX.get(self.current_step, 0),
            "total_steps": len(STEPS),
            "completed_steps": list(self.completed_steps),
            "data": dict(self.data),
            "completed": self.completed,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "step_specs": [
                {
                    "key": s.key,
                    "title": s.title,
                    "description": s.description,
                    "required_fields": s.required_fields,
                    "optional_fields": s.optional_fields,
                }
                for s in STEPS
            ],
        }


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_step(step_key: str, payload: Dict[str, Any]) -> List[str]:
    """Return a list of validation errors (empty list = OK)."""
    if step_key not in STEP_INDEX:
        return [f"unknown step {step_key!r}"]

    spec = STEPS[STEP_INDEX[step_key]]
    errors: List[str] = []

    if step_key == "tanda_connect":
        # Either skip=True, or provide api_key (org_id optional but recommended)
        if not payload.get("skip"):
            if not payload.get("tanda_api_key"):
                errors.append("either provide tanda_api_key or set skip=true")

    elif step_key == "team_size":
        try:
            hc = int(payload.get("headcount", 0))
            if hc <= 0:
                errors.append("headcount must be a positive integer")
        except (TypeError, ValueError):
            errors.append("headcount must be an integer")

    elif step_key == "billing_tier":
        tier = (payload.get("tier") or "").lower()
        if tier not in ALLOWED_TIERS:
            errors.append(
                f"tier must be one of {sorted(ALLOWED_TIERS)}, got {tier!r}"
            )

    elif step_key == "venue_basics":
        for f in ("venue_name", "timezone"):
            if not payload.get(f):
                errors.append(f"{f} is required")

    # Generic required_fields enforcement
    for f in spec.required_fields:
        if f not in payload or payload.get(f) in (None, ""):
            msg = f"{f} is required"
            if msg not in errors:
                errors.append(msg)

    return errors


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------


def _state_to_row(state: "WizardState") -> Dict[str, Any]:
    return {
        "wizard_id": state.wizard_id,
        "tenant_id": state.tenant_id,
        "started_at": state.started_at.isoformat(),
        "updated_at": state.updated_at.isoformat(),
        "current_step": state.current_step,
        "completed_steps": _p.json_dumps(list(state.completed_steps)),
        "data": _p.json_dumps(dict(state.data)),
        "completed": 1 if state.completed else 0,
        "completed_at": state.completed_at.isoformat() if state.completed_at else None,
    }


def _row_to_state(row) -> "WizardState":
    return WizardState(
        wizard_id=row["wizard_id"],
        tenant_id=row["tenant_id"],
        started_at=datetime.fromisoformat(row["started_at"]),
        updated_at=datetime.fromisoformat(row["updated_at"]),
        current_step=row["current_step"],
        completed_steps=_p.json_loads(row["completed_steps"], default=[]) or [],
        data=_p.json_loads(row["data"], default={}) or {},
        completed=bool(row["completed"]),
        completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
    )


class OnboardingStore:
    """Thread-safe in-memory store of WizardState keyed by wizard_id.

    When `ROSTERIQ_DB_PATH` is set, mutations are mirrored to SQLite
    (table `onboarding_wizards`) and `rehydrate()` rebuilds the in-mem
    map from disk on startup. Persistence is best-effort — write
    failures are logged but never raise; the in-memory copy is the
    source of truth for live reads.
    """

    def __init__(self) -> None:
        self._states: Dict[str, WizardState] = {}
        self._lock = threading.Lock()

    def rehydrate(self) -> None:
        """Load all wizards from SQLite into the in-memory map."""
        if not _p.is_persistence_enabled():
            return
        rows = _p.fetchall("SELECT * FROM onboarding_wizards")
        with self._lock:
            for r in rows:
                try:
                    state = _row_to_state(r)
                    self._states[state.wizard_id] = state
                except Exception as e:
                    logger.warning("rehydrate row failed: %s", e)
        logger.info("Onboarding rehydrated %d wizards", len(rows))

    def _persist(self, state: "WizardState") -> None:
        _p.upsert("onboarding_wizards", _state_to_row(state), pk="wizard_id")

    def start(self, tenant_id: str) -> WizardState:
        wid = f"wiz_{uuid.uuid4().hex[:12]}"
        now = datetime.now(AU_TZ)
        state = WizardState(
            wizard_id=wid,
            tenant_id=tenant_id,
            started_at=now,
            updated_at=now,
            current_step=STEPS[0].key,
        )
        with self._lock:
            self._states[wid] = state
        self._persist(state)
        logger.info("Onboarding wizard started: %s for tenant %s", wid, tenant_id)
        return state

    def get(self, wizard_id: str) -> Optional[WizardState]:
        with self._lock:
            return self._states.get(wizard_id)

    def list_for_tenant(self, tenant_id: str) -> List[WizardState]:
        with self._lock:
            return [s for s in self._states.values() if s.tenant_id == tenant_id]

    def submit_step(
        self,
        wizard_id: str,
        step_key: str,
        payload: Dict[str, Any],
    ) -> WizardState:
        """Validate + persist a step; advance current_step if successful."""
        errors = validate_step(step_key, payload)
        if errors:
            raise ValueError("; ".join(errors))

        with self._lock:
            state = self._states.get(wizard_id)
            if state is None:
                raise KeyError(f"wizard {wizard_id} not found")
            if state.completed:
                raise ValueError("wizard already completed")

            state.data[step_key] = dict(payload)
            if step_key not in state.completed_steps:
                state.completed_steps.append(step_key)

            # Advance
            idx = STEP_INDEX[step_key]
            if idx + 1 < len(STEPS):
                state.current_step = STEPS[idx + 1].key
            else:
                state.current_step = STEPS[-1].key
            state.updated_at = datetime.now(AU_TZ)
        self._persist(state)
        return state

    def complete(
        self,
        wizard_id: str,
        finalize: Optional[Callable[[WizardState], None]] = None,
    ) -> WizardState:
        """Mark the wizard complete. Calls finalize(state) under the lock."""
        with self._lock:
            state = self._states.get(wizard_id)
            if state is None:
                raise KeyError(f"wizard {wizard_id} not found")
            if state.completed:
                return state

            # Required steps must all be present (everything except the
            # confirm step itself, which has no fields).
            missing = [
                s.key for s in STEPS
                if s.key != "confirm"
                and s.required_fields
                and s.key not in state.completed_steps
            ]
            if missing:
                raise ValueError(
                    f"cannot complete — missing steps: {', '.join(missing)}"
                )

            if finalize is not None:
                try:
                    finalize(state)
                except Exception as e:  # pragma: no cover - infra
                    logger.warning("finalize() raised: %s", e)

            state.completed = True
            state.completed_at = datetime.now(AU_TZ)
            state.updated_at = state.completed_at
            state.current_step = "confirm"
        self._persist(state)
        return state

    def clear(self) -> None:
        """Test helper."""
        with self._lock:
            self._states.clear()


_store: Optional[OnboardingStore] = None


def get_onboarding_store() -> OnboardingStore:
    global _store
    if _store is None:
        _store = OnboardingStore()
    return _store


# Rehydrate at init_db() time
@_p.on_init
def _rehydrate_on_init() -> None:
    get_onboarding_store().rehydrate()
