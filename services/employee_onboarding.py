"""
Employee Onboarding Checklist Service

Manages employee onboarding checklists with document tracking for Australian hospitality venues.

Provides:
- Default checklist templates for AU hospitality with mandatory/optional items
- Role-based auto-selection of checklist items (kitchen staff get food safety, bar staff get RSA, etc)
- Document upload tracking (proofs of certificates, declarations, etc)
- Completion status tracking and reminders
- Rostering readiness verification (all mandatories complete?)
- Venue-level template customization
- Bulk reminders for incomplete items

Default items (13 total):
- TAX_FILE_DECLARATION: mandatory for all
- SUPER_CHOICE: mandatory for all
- BANK_DETAILS: mandatory for all
- RSA_CERTIFICATE: mandatory for bar/floor staff
- FOOD_SAFETY: mandatory for kitchen staff
- WORKING_RIGHTS: mandatory for all
- EMERGENCY_CONTACT: mandatory for all
- UNIFORM_ISSUED: optional
- VENUE_TOUR: optional
- POS_TRAINING: optional
- FIRE_SAFETY: mandatory for all
- WHS_INDUCTION: mandatory for all
- POLICY_ACKNOWLEDGEMENT: mandatory for all

Usage:
    from rosteriq.services.employee_onboarding import get_onboarding_service
    service = get_onboarding_service()
    checklist = await service.create_onboarding("emp-123", "venue-456", "kitchen")
    await service.update_item("emp-123", "item-001", "completed", document_url="https://...")
    is_ready, missing = service.is_rostering_ready("emp-123")
"""

import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, List, Dict, Tuple
from uuid import uuid4

from rosteriq.database import get_db
from rosteriq.services.notification_hub import get_notification_hub

logger = logging.getLogger(__name__)


# ============================================================================
# Enums and Constants
# ============================================================================

class OnboardingItemStatus(str, Enum):
    """Status of a checklist item."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    WAIVED = "waived"


class OnboardingItemCategory(str, Enum):
    """Category of onboarding item."""
    DOCUMENT = "document"
    TRAINING = "training"
    EQUIPMENT = "equipment"


class OnboardingItemType(str, Enum):
    """Predefined onboarding item types."""
    TAX_FILE_DECLARATION = "tax_file_declaration"
    SUPER_CHOICE = "super_choice"
    BANK_DETAILS = "bank_details"
    RSA_CERTIFICATE = "rsa_certificate"
    FOOD_SAFETY = "food_safety"
    WORKING_RIGHTS = "working_rights"
    EMERGENCY_CONTACT = "emergency_contact"
    UNIFORM_ISSUED = "uniform_issued"
    VENUE_TOUR = "venue_tour"
    POS_TRAINING = "pos_training"
    FIRE_SAFETY = "fire_safety"
    WHS_INDUCTION = "whs_induction"
    POLICY_ACKNOWLEDGEMENT = "policy_acknowledgement"


# Default templates for each item type
DEFAULT_ITEMS = {
    OnboardingItemType.TAX_FILE_DECLARATION: {
        "name": "Tax File Number Declaration",
        "description": "Complete and sign the Tax File Number declaration form",
        "category": OnboardingItemCategory.DOCUMENT,
        "is_mandatory": True,
        "required_for_roles": ["all"],
    },
    OnboardingItemType.SUPER_CHOICE: {
        "name": "Superannuation Choice Form",
        "description": "Select your superannuation fund and complete the choice form",
        "category": OnboardingItemCategory.DOCUMENT,
        "is_mandatory": True,
        "required_for_roles": ["all"],
    },
    OnboardingItemType.BANK_DETAILS: {
        "name": "Bank Account Details for Payroll",
        "description": "Provide bank account details for salary deposit",
        "category": OnboardingItemCategory.DOCUMENT,
        "is_mandatory": True,
        "required_for_roles": ["all"],
    },
    OnboardingItemType.RSA_CERTIFICATE: {
        "name": "Responsible Service of Alcohol Certificate",
        "description": "Provide proof of current RSA certificate",
        "category": OnboardingItemCategory.DOCUMENT,
        "is_mandatory": True,
        "required_for_roles": ["bar", "floor", "manager"],
    },
    OnboardingItemType.FOOD_SAFETY: {
        "name": "Food Safety Certificate",
        "description": "Provide proof of food safety/handling certificate",
        "category": OnboardingItemCategory.DOCUMENT,
        "is_mandatory": True,
        "required_for_roles": ["kitchen", "chef", "cook"],
    },
    OnboardingItemType.WORKING_RIGHTS: {
        "name": "Proof of Working Rights in Australia",
        "description": "Provide passport, visa, or other proof of working rights",
        "category": OnboardingItemCategory.DOCUMENT,
        "is_mandatory": True,
        "required_for_roles": ["all"],
    },
    OnboardingItemType.EMERGENCY_CONTACT: {
        "name": "Emergency Contact Details",
        "description": "Provide emergency contact information",
        "category": OnboardingItemCategory.DOCUMENT,
        "is_mandatory": True,
        "required_for_roles": ["all"],
    },
    OnboardingItemType.UNIFORM_ISSUED: {
        "name": "Uniform Issued",
        "description": "Uniform received and signed off by manager",
        "category": OnboardingItemCategory.EQUIPMENT,
        "is_mandatory": False,
        "required_for_roles": ["all"],
    },
    OnboardingItemType.VENUE_TOUR: {
        "name": "Venue Tour Completed",
        "description": "Complete tour of venue, facilities, and emergency exits",
        "category": OnboardingItemCategory.TRAINING,
        "is_mandatory": False,
        "required_for_roles": ["all"],
    },
    OnboardingItemType.POS_TRAINING: {
        "name": "POS System Training",
        "description": "Training on the venue's POS system",
        "category": OnboardingItemCategory.TRAINING,
        "is_mandatory": False,
        "required_for_roles": ["bar", "floor", "kitchen"],
    },
    OnboardingItemType.FIRE_SAFETY: {
        "name": "Fire Safety & Emergency Procedures",
        "description": "Training on fire safety and emergency procedures",
        "category": OnboardingItemCategory.TRAINING,
        "is_mandatory": True,
        "required_for_roles": ["all"],
    },
    OnboardingItemType.WHS_INDUCTION: {
        "name": "Workplace Health & Safety Induction",
        "description": "Complete WHS induction training",
        "category": OnboardingItemCategory.TRAINING,
        "is_mandatory": True,
        "required_for_roles": ["all"],
    },
    OnboardingItemType.POLICY_ACKNOWLEDGEMENT: {
        "name": "Employee Handbook & Policies Signed",
        "description": "Sign off on employee handbook and venue policies",
        "category": OnboardingItemCategory.DOCUMENT,
        "is_mandatory": True,
        "required_for_roles": ["all"],
    },
}


# ============================================================================
# Data Models
# ============================================================================

@dataclass
class OnboardingItem:
    """Single checklist item for an employee's onboarding."""
    id: str
    name: str
    description: str
    category: str  # document, training, equipment
    is_mandatory: bool
    status: str = OnboardingItemStatus.PENDING.value
    completed_at: Optional[str] = None
    document_url: Optional[str] = None
    notes: Optional[str] = None
    reminded_at: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "OnboardingItem":
        """Create from stored dictionary."""
        return cls(**data)


@dataclass
class OnboardingChecklist:
    """Complete onboarding checklist for an employee."""
    employee_id: str
    venue_id: str
    role: str
    started_at: str
    completed_at: Optional[str] = None
    items: List[OnboardingItem] = field(default_factory=list)
    completion_pct: float = 0.0
    is_rostering_ready: bool = False

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        data = asdict(self)
        data["items"] = [item.to_dict() if isinstance(item, OnboardingItem) else item for item in self.items]
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "OnboardingChecklist":
        """Create from stored dictionary."""
        items = data.pop("items", [])
        checklist = cls(**data)
        checklist.items = [
            OnboardingItem.from_dict(item) if isinstance(item, dict) else item
            for item in items
        ]
        return checklist


@dataclass
class OnboardingItemTemplate:
    """Template for an onboarding item at venue level."""
    id: str
    name: str
    description: str
    category: str
    is_mandatory: bool
    required_for_roles: List[str]

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "OnboardingItemTemplate":
        """Create from stored dictionary."""
        return cls(**data)


@dataclass
class VenueOnboardingStatus:
    """Overview of onboarding status for all employees at a venue."""
    venue_id: str
    total_employees_onboarding: int
    completed: int
    in_progress: int
    pending: int
    completion_rate: float  # percentage
    average_days_to_complete: float
    most_common_pending_items: List[Dict[str, any]]  # [{item_name, count}, ...]

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        return asdict(self)


# ============================================================================
# Onboarding Service
# ============================================================================

class OnboardingService:
    """
    Service managing employee onboarding checklists.

    Tracks mandatory documents and training, supports document uploads,
    and verifies completion for rostering eligibility.
    """

    def __init__(self, db=None, notification_hub=None):
        self.db = db or get_db()
        self.hub = notification_hub or get_notification_hub()
        # In-memory storage for checklists (venue_id + employee_id -> checklist)
        self._checklists: Dict[str, OnboardingChecklist] = {}
        # Venue-level templates (venue_id -> list of templates)
        self._venue_templates: Dict[str, List[OnboardingItemTemplate]] = {}

    # ========================================================================
    # Create & Retrieve
    # ========================================================================

    def create_onboarding(self, employee_id: str, venue_id: str, role: str) -> OnboardingChecklist:
        """
        Create a new onboarding checklist for an employee.

        Auto-selects mandatory items based on role:
        - All employees: TAX_FILE_DECLARATION, SUPER_CHOICE, BANK_DETAILS,
          WORKING_RIGHTS, EMERGENCY_CONTACT, FIRE_SAFETY, WHS_INDUCTION,
          POLICY_ACKNOWLEDGEMENT
        - Bar/Floor staff: RSA_CERTIFICATE
        - Kitchen staff: FOOD_SAFETY
        - Optional for all: UNIFORM_ISSUED, VENUE_TOUR, POS_TRAINING

        Args:
            employee_id: Employee ID
            venue_id: Venue ID
            role: Employee role (e.g., "kitchen", "bar", "floor", "manager")

        Returns:
            OnboardingChecklist with pre-selected items
        """
        now = datetime.utcnow().isoformat()
        key = f"{venue_id}:{employee_id}"

        # Get venue template or use defaults
        templates = self._venue_templates.get(venue_id, [])
        if not templates:
            templates = self._build_default_templates()

        # Select items based on role and mandatory status
        items = []
        for template in templates:
            if self._item_applies_to_role(template, role):
                item = OnboardingItem(
                    id=str(uuid4()),
                    name=template.name,
                    description=template.description,
                    category=template.category,
                    is_mandatory=template.is_mandatory,
                    status=OnboardingItemStatus.PENDING.value,
                )
                items.append(item)

        checklist = OnboardingChecklist(
            employee_id=employee_id,
            venue_id=venue_id,
            role=role,
            started_at=now,
            items=items,
        )

        self._checklists[key] = checklist
        logger.info(f"Created onboarding checklist for {employee_id} at {venue_id} with {len(items)} items")

        return checklist

    def get_onboarding(self, employee_id: str, venue_id: Optional[str] = None) -> Optional[OnboardingChecklist]:
        """
        Retrieve an existing onboarding checklist.

        Args:
            employee_id: Employee ID
            venue_id: Venue ID (required if employee has multiple onboardings)

        Returns:
            OnboardingChecklist or None if not found
        """
        if venue_id:
            key = f"{venue_id}:{employee_id}"
            return self._checklists.get(key)

        # Search across all venues
        for key, checklist in self._checklists.items():
            if key.endswith(f":{employee_id}"):
                return checklist

        return None

    # ========================================================================
    # Update Items
    # ========================================================================

    def update_item(
        self,
        employee_id: str,
        item_id: str,
        status: str,
        venue_id: Optional[str] = None,
        notes: Optional[str] = None,
        document_url: Optional[str] = None,
    ) -> Optional[OnboardingItem]:
        """
        Update the status of a checklist item.

        Args:
            employee_id: Employee ID
            item_id: Item ID within the checklist
            status: New status (pending, in_progress, completed, waived)
            venue_id: Venue ID (optional, searched if not provided)
            notes: Optional notes about the item
            document_url: Optional URL to uploaded document

        Returns:
            Updated OnboardingItem or None if not found
        """
        checklist = self.get_onboarding(employee_id, venue_id)
        if not checklist:
            logger.warning(f"Checklist not found for {employee_id}")
            return None

        item = next((item for item in checklist.items if item.id == item_id), None)
        if not item:
            logger.warning(f"Item {item_id} not found in checklist for {employee_id}")
            return None

        # Update item
        item.status = status
        if notes:
            item.notes = notes
        if document_url:
            item.document_url = document_url

        # Set completion timestamp if completed
        if status == OnboardingItemStatus.COMPLETED.value:
            item.completed_at = datetime.utcnow().isoformat()
            logger.info(f"Completed item {item_id} for {employee_id}")

        # Recalculate completion percentage
        self._update_completion_status(checklist)

        return item

    def _update_completion_status(self, checklist: OnboardingChecklist) -> None:
        """Recalculate completion percentage and rostering readiness."""
        if not checklist.items:
            checklist.completion_pct = 0.0
            checklist.is_rostering_ready = False
            return

        total = len(checklist.items)
        completed = sum(
            1 for item in checklist.items
            if item.status in (OnboardingItemStatus.COMPLETED.value, OnboardingItemStatus.WAIVED.value)
        )
        checklist.completion_pct = round((completed / total) * 100, 1) if total > 0 else 0.0

        # Check if rostering ready
        checklist.is_rostering_ready, _ = self._is_rostering_ready_internal(checklist)

        # Mark complete if all items done
        if checklist.completion_pct == 100.0 and not checklist.completed_at:
            checklist.completed_at = datetime.utcnow().isoformat()
            logger.info(f"Onboarding complete for {checklist.employee_id}")

    # ========================================================================
    # Rostering Readiness
    # ========================================================================

    def is_rostering_ready(self, employee_id: str, venue_id: Optional[str] = None) -> Tuple[bool, List[str]]:
        """
        Check if an employee's onboarding is complete enough for rostering.

        "Rostering ready" means all mandatory items are completed or waived.

        Args:
            employee_id: Employee ID
            venue_id: Venue ID (optional)

        Returns:
            Tuple of (is_ready: bool, missing_items: list[str])
        """
        checklist = self.get_onboarding(employee_id, venue_id)
        if not checklist:
            return False, ["Onboarding checklist not found"]

        return self._is_rostering_ready_internal(checklist)

    def _is_rostering_ready_internal(self, checklist: OnboardingChecklist) -> Tuple[bool, List[str]]:
        """Internal helper to check rostering readiness."""
        missing = []

        for item in checklist.items:
            if item.is_mandatory:
                if item.status not in (OnboardingItemStatus.COMPLETED.value, OnboardingItemStatus.WAIVED.value):
                    missing.append(item.name)

        return len(missing) == 0, missing

    # ========================================================================
    # Reminders
    # ========================================================================

    async def send_reminder(
        self,
        employee_id: str,
        item_ids: Optional[List[str]] = None,
        venue_id: Optional[str] = None,
    ) -> int:
        """
        Send notification reminders for incomplete items.

        Args:
            employee_id: Employee ID
            item_ids: Specific item IDs to remind about (None = all incomplete)
            venue_id: Venue ID (optional)

        Returns:
            Number of reminders sent
        """
        checklist = self.get_onboarding(employee_id, venue_id)
        if not checklist:
            logger.warning(f"Checklist not found for {employee_id}")
            return 0

        # Select items to remind about
        if item_ids:
            items_to_remind = [item for item in checklist.items if item.id in item_ids]
        else:
            items_to_remind = [
                item for item in checklist.items
                if item.status == OnboardingItemStatus.PENDING.value
            ]

        if not items_to_remind:
            logger.info(f"No incomplete items to remind for {employee_id}")
            return 0

        # Send notification via hub
        try:
            item_names = [item.name for item in items_to_remind]
            await self.hub.dispatch(
                event_type="ONBOARDING_REMINDER",
                venue_id=checklist.venue_id,
                target_employee_id=employee_id,
                payload={
                    "checklist_id": f"{checklist.venue_id}:{employee_id}",
                    "pending_items": item_names,
                    "completion_pct": checklist.completion_pct,
                },
            )

            # Update reminded_at timestamp
            now = datetime.utcnow().isoformat()
            for item in items_to_remind:
                item.reminded_at = now

            logger.info(f"Sent {len(items_to_remind)} reminders to {employee_id}")
            return len(items_to_remind)

        except Exception as e:
            logger.error(f"Failed to send reminder to {employee_id}: {e}")
            return 0

    # ========================================================================
    # Venue Templates
    # ========================================================================

    def get_venue_template(self, venue_id: str) -> List[OnboardingItemTemplate]:
        """
        Get the customized onboarding template for a venue.

        Returns default template if venue has not customized.

        Args:
            venue_id: Venue ID

        Returns:
            List of OnboardingItemTemplate
        """
        if venue_id in self._venue_templates:
            return self._venue_templates[venue_id]
        return self._build_default_templates()

    def update_venue_template(
        self,
        venue_id: str,
        items: List[Dict[str, any]],
    ) -> List[OnboardingItemTemplate]:
        """
        Customize the onboarding template for a venue.

        Allows venues to add, remove, or modify which items apply to which roles.

        Args:
            venue_id: Venue ID
            items: List of dicts with:
                {name, description, category, is_mandatory, required_for_roles}

        Returns:
            Updated list of templates
        """
        templates = []
        for item_data in items:
            template = OnboardingItemTemplate(
                id=item_data.get("id", str(uuid4())),
                name=item_data["name"],
                description=item_data.get("description", ""),
                category=item_data.get("category", "document"),
                is_mandatory=item_data.get("is_mandatory", True),
                required_for_roles=item_data.get("required_for_roles", ["all"]),
            )
            templates.append(template)

        self._venue_templates[venue_id] = templates
        logger.info(f"Updated onboarding template for venue {venue_id} with {len(templates)} items")

        return templates

    # ========================================================================
    # Venue-level Status
    # ========================================================================

    def get_venue_onboarding_status(self, venue_id: str) -> VenueOnboardingStatus:
        """
        Get overview of onboarding status for all employees at a venue.

        Args:
            venue_id: Venue ID

        Returns:
            VenueOnboardingStatus with aggregated metrics
        """
        venue_checklists = [
            checklist for checklist in self._checklists.values()
            if checklist.venue_id == venue_id
        ]

        if not venue_checklists:
            return VenueOnboardingStatus(
                venue_id=venue_id,
                total_employees_onboarding=0,
                completed=0,
                in_progress=0,
                pending=0,
                completion_rate=0.0,
                average_days_to_complete=0.0,
                most_common_pending_items=[],
            )

        completed_count = sum(1 for c in venue_checklists if c.completion_pct == 100.0)
        in_progress_count = sum(
            1 for c in venue_checklists
            if 0 < c.completion_pct < 100.0
        )
        pending_count = sum(1 for c in venue_checklists if c.completion_pct == 0.0)

        # Calculate average days to complete (for those marked complete)
        days_to_complete = []
        for checklist in venue_checklists:
            if checklist.completed_at:
                start = datetime.fromisoformat(checklist.started_at)
                end = datetime.fromisoformat(checklist.completed_at)
                days = (end - start).days
                days_to_complete.append(days)

        avg_days = sum(days_to_complete) / len(days_to_complete) if days_to_complete else 0.0

        # Most common pending items
        pending_items_count: Dict[str, int] = {}
        for checklist in venue_checklists:
            for item in checklist.items:
                if item.status == OnboardingItemStatus.PENDING.value:
                    pending_items_count[item.name] = pending_items_count.get(item.name, 0) + 1

        most_common = sorted(
            [{"name": name, "count": count} for name, count in pending_items_count.items()],
            key=lambda x: x["count"],
            reverse=True,
        )[:5]

        completion_rate = (completed_count / len(venue_checklists) * 100) if venue_checklists else 0.0

        return VenueOnboardingStatus(
            venue_id=venue_id,
            total_employees_onboarding=len(venue_checklists),
            completed=completed_count,
            in_progress=in_progress_count,
            pending=pending_count,
            completion_rate=round(completion_rate, 1),
            average_days_to_complete=round(avg_days, 1),
            most_common_pending_items=most_common,
        )

    # ========================================================================
    # Bulk Operations
    # ========================================================================

    async def auto_remind_incomplete(self, venue_id: str, days_since_start: int = 7) -> int:
        """
        Bulk remind employees with incomplete onboarding after N days.

        Useful for automated reminders triggered periodically (e.g., daily cron).

        Args:
            venue_id: Venue ID
            days_since_start: Only remind if onboarding started N+ days ago

        Returns:
            Total number of reminders sent
        """
        venue_checklists = [
            checklist for checklist in self._checklists.values()
            if checklist.venue_id == venue_id and checklist.completion_pct < 100.0
        ]

        if not venue_checklists:
            logger.info(f"No incomplete onboardings to remind for venue {venue_id}")
            return 0

        now = datetime.utcnow()
        total_reminded = 0

        for checklist in venue_checklists:
            # Check if started long enough ago
            started = datetime.fromisoformat(checklist.started_at)
            days_elapsed = (now - started).days

            if days_elapsed >= days_since_start:
                # Remind about incomplete mandatory items only
                incomplete_mandatory = [
                    item for item in checklist.items
                    if item.is_mandatory and item.status == OnboardingItemStatus.PENDING.value
                ]

                if incomplete_mandatory:
                    reminded = await self.send_reminder(
                        checklist.employee_id,
                        venue_id=venue_id,
                    )
                    total_reminded += reminded

        logger.info(f"Auto-reminded {total_reminded} items across {len(venue_checklists)} employees at {venue_id}")
        return total_reminded

    # ========================================================================
    # Helpers
    # ========================================================================

    def _build_default_templates(self) -> List[OnboardingItemTemplate]:
        """Build default template from DEFAULT_ITEMS constant."""
        templates = []
        for item_type, item_spec in DEFAULT_ITEMS.items():
            template = OnboardingItemTemplate(
                id=item_type.value,
                name=item_spec["name"],
                description=item_spec["description"],
                category=item_spec["category"].value,
                is_mandatory=item_spec["is_mandatory"],
                required_for_roles=item_spec["required_for_roles"],
            )
            templates.append(template)
        return templates

    def _item_applies_to_role(self, template: OnboardingItemTemplate, role: str) -> bool:
        """Check if a template applies to the given role."""
        return "all" in template.required_for_roles or role.lower() in template.required_for_roles


# ============================================================================
# Singleton getter
# ============================================================================

_onboarding_service: Optional[OnboardingService] = None


def get_onboarding_service(db=None, notification_hub=None) -> OnboardingService:
    """Get or create the singleton OnboardingService instance."""
    global _onboarding_service
    if _onboarding_service is None:
        _onboarding_service = OnboardingService(db=db, notification_hub=notification_hub)
    return _onboarding_service
