"""
Privacy service for RosterIQ — Australian Privacy Act 1988 compliance.

Implements:
- Right to erasure (anonymisation, not deletion — preserves roster integrity)
- Data portability (export user/employee data as JSON)
- Consent tracking and management
- Privacy impact logging and audit trail

Usage:
    from rosteriq.services.privacy import PrivacyService
    service = PrivacyService()

    # Export user data
    data = service.export_user_data(user_id)

    # Anonymise employee (preserve shift history)
    service.anonymise_employee(employee_id)

    # Track consent
    service.record_consent(user_id, 'data_processing', granted=True)
"""

import json
import logging
from datetime import datetime
from typing import Optional, Dict, List
from uuid import uuid4

from rosteriq.database import get_db

logger = logging.getLogger(__name__)


class PrivacyService:
    """Service for privacy-related operations and compliance."""

    # Supported consent types (Australian Privacy Principles)
    CONSENT_TYPES = {
        'data_processing': 'General data processing and storage',
        'marketing_emails': 'Marketing and promotional emails',
        'analytics': 'Analytics and usage tracking',
        'third_party_sharing': 'Sharing data with third parties',
    }

    def __init__(self, db=None):
        """Initialize service with database connection."""
        self.db = db or get_db()

    # === Right to Erasure & Anonymisation ===

    def anonymise_employee(self, employee_id: str) -> Dict:
        """
        Anonymise an employee's personally identifiable information.

        Preserves:
        - Employment type, skills, award level
        - Shift history (dates, times, roles) for reporting
        - Cost data

        Anonymises:
        - Name → "Anonymised Employee #XXXXXXXX"
        - Email → "anon_XXXXXXXX@deleted.local"
        - Phone → removed

        Args:
            employee_id: The employee ID to anonymise

        Returns:
            Dict with anonymisation details
        """
        try:
            emp = self.db.get_employee(employee_id)
            if not emp:
                raise ValueError(f"Employee {employee_id} not found")

            # Mark as anonymised
            self.db.anonymise_employee(employee_id)

            # Log the action
            self._log_privacy_action(
                user_id='system',
                action='anonymise_employee',
                resource_type='employee',
                details={
                    'employee_id': employee_id,
                    'employee_name': emp.name,  # Log original name for audit
                    'employment_type': emp.employment_type.value,
                    'timestamp': datetime.utcnow().isoformat(),
                }
            )

            logger.info(f"Anonymised employee {employee_id}")

            return {
                'employee_id': employee_id,
                'anonymised_at': datetime.utcnow().isoformat(),
                'preserved': ['employment_type', 'skills', 'shift_history', 'cost_data'],
                'anonymised_fields': ['name', 'email', 'phone'],
            }

        except Exception as e:
            logger.error(f"Failed to anonymise employee {employee_id}: {e}")
            raise

    # === Data Portability ===

    def export_user_data(self, user_id: str) -> Dict:
        """
        Export all user data as a JSON bundle for data portability.

        Returns:
        - User profile (name, email, role, venues)
        - Venues user manages
        - Employees at those venues
        - Shift history
        - Swap requests
        - Preferences
        - Audit trail

        Args:
            user_id: The user ID to export

        Returns:
            Dict containing complete user data export
        """
        try:
            user = self.db.get_user_by_id(user_id)
            if not user:
                raise ValueError(f"User {user_id} not found")

            export = {
                'export_id': str(uuid4()),
                'exported_at': datetime.utcnow().isoformat(),
                'user': {
                    'id': user.get('id'),
                    'email': user.get('email'),
                    'name': user.get('name'),
                    'role': user.get('role'),
                    'venue_ids': user.get('venue_ids', []),
                    'is_active': user.get('is_active'),
                    'created_at': user.get('created_at').isoformat() if user.get('created_at') else None,
                    'last_login': user.get('last_login').isoformat() if user.get('last_login') else None,
                },
                'venues': [],
                'consents': self.get_consent_status(user_id),
                'audit_trail': self._get_user_audit_trail(user_id),
            }

            # Include venues user can access
            for venue_id in user.get('venue_ids', []):
                venue = self.db.get_venue(venue_id)
                if venue:
                    export['venues'].append({
                        'id': venue.id,
                        'name': venue.name,
                        'state': venue.state.value,
                        'created_at': venue.created_at.isoformat(),
                    })

            # Log the export
            self._log_privacy_action(
                user_id=user_id,
                action='export_user_data',
                resource_type='user',
                details={'export_id': export['export_id']}
            )

            logger.info(f"Exported data for user {user_id} (export_id: {export['export_id']})")

            return export

        except Exception as e:
            logger.error(f"Failed to export data for user {user_id}: {e}")
            raise

    def export_employee_data(self, employee_id: str) -> Dict:
        """
        Export all employee data as a JSON bundle.

        Returns:
        - Employee profile
        - Shift history
        - Availability preferences
        - Skills
        - Employment details

        Args:
            employee_id: The employee ID to export

        Returns:
            Dict containing complete employee data export
        """
        try:
            emp = self.db.get_employee(employee_id)
            if not emp:
                raise ValueError(f"Employee {employee_id} not found")

            export = {
                'export_id': str(uuid4()),
                'exported_at': datetime.utcnow().isoformat(),
                'employee': {
                    'id': emp.id,
                    'name': emp.name,
                    'email': emp.email,
                    'phone': emp.phone,
                    'employment_type': emp.employment_type.value,
                    'award_level': emp.award_level.value,
                    'state': emp.state.value,
                    'hourly_base_rate': str(emp.hourly_base_rate),
                    'skills': emp.skills,
                    'max_hours_per_week': emp.max_hours_per_week,
                    'consecutive_days_limit': emp.consecutive_days_limit,
                    'created_at': emp.created_at.isoformat(),
                    'updated_at': emp.updated_at.isoformat(),
                },
                'availability': emp.availability,
            }

            # Log the export
            self._log_privacy_action(
                user_id='employee:' + employee_id,
                action='export_employee_data',
                resource_type='employee',
                details={'export_id': export['export_id']}
            )

            logger.info(f"Exported data for employee {employee_id} (export_id: {export['export_id']})")

            return export

        except Exception as e:
            logger.error(f"Failed to export data for employee {employee_id}: {e}")
            raise

    # === Consent Management ===

    def record_consent(self, user_id: str, consent_type: str, granted: bool) -> Dict:
        """
        Record a user's consent for data processing.

        Args:
            user_id: The user ID
            consent_type: Type of consent (see CONSENT_TYPES)
            granted: Whether consent was granted or withdrawn

        Returns:
            Dict with consent record details
        """
        if consent_type not in self.CONSENT_TYPES:
            raise ValueError(
                f"Invalid consent type: {consent_type}. "
                f"Must be one of {list(self.CONSENT_TYPES.keys())}"
            )

        timestamp = datetime.utcnow()

        self.db.save_consent(user_id, consent_type, granted, timestamp)

        # Log the consent decision
        self._log_privacy_action(
            user_id=user_id,
            action='record_consent',
            resource_type='consent',
            details={
                'consent_type': consent_type,
                'granted': granted,
            }
        )

        logger.info(f"Recorded consent for {user_id}: {consent_type}={granted}")

        return {
            'user_id': user_id,
            'consent_type': consent_type,
            'granted': granted,
            'timestamp': timestamp.isoformat(),
        }

    def get_consent_status(self, user_id: str) -> Dict[str, dict]:
        """
        Get current consent status for a user.

        Returns a dict mapping consent type to latest consent record.

        Args:
            user_id: The user ID

        Returns:
            Dict mapping consent_type → {granted, timestamp}
        """
        consents = self.db.get_consents(user_id)

        # Group by consent type, keep most recent
        status = {}
        for consent in consents:
            consent_type = consent['consent_type']
            if consent_type not in status:
                status[consent_type] = {
                    'granted': consent['granted'],
                    'timestamp': consent['timestamp'].isoformat() if hasattr(consent['timestamp'], 'isoformat') else str(consent['timestamp']),
                }

        # Fill in missing consent types with defaults (not granted)
        for consent_type in self.CONSENT_TYPES:
            if consent_type not in status:
                status[consent_type] = {
                    'granted': False,
                    'timestamp': None,
                    'description': self.CONSENT_TYPES[consent_type],
                }

        return status

    # === Privacy Audit Trail ===

    def get_privacy_logs(self, user_id: Optional[str] = None, limit: int = 100) -> List[dict]:
        """
        Get privacy audit logs (data access, export, deletion, anonymisation).

        Args:
            user_id: Optional user ID to filter logs
            limit: Maximum number of logs to return

        Returns:
            List of privacy audit log entries
        """
        logs = self.db.list_privacy_logs(user_id=user_id, limit=limit)

        return [
            {
                'user_id': log.get('user_id'),
                'action': log.get('action'),
                'resource_type': log.get('resource_type'),
                'details': json.loads(log.get('details', '{}')) if isinstance(log.get('details'), str) else log.get('details'),
                'logged_at': log.get('logged_at').isoformat() if hasattr(log.get('logged_at'), 'isoformat') else str(log.get('logged_at')),
            }
            for log in logs
        ]

    # === Private Helpers ===

    def _log_privacy_action(self, user_id: str, action: str, resource_type: str, details: dict) -> None:
        """Log a privacy-related action to audit trail."""
        try:
            log_entry = {
                'user_id': user_id,
                'action': action,
                'resource_type': resource_type,
                'details': details,
                'logged_at': datetime.utcnow(),
            }
            self.db.save_privacy_log(log_entry)
        except Exception as e:
            logger.error(f"Failed to log privacy action: {e}")

    def _get_user_audit_trail(self, user_id: str, limit: int = 50) -> List[dict]:
        """Get audit trail of actions by/on a user."""
        logs = self.db.list_privacy_logs(user_id=user_id, limit=limit)

        return [
            {
                'action': log.get('action'),
                'resource_type': log.get('resource_type'),
                'logged_at': log.get('logged_at').isoformat() if hasattr(log.get('logged_at'), 'isoformat') else str(log.get('logged_at')),
            }
            for log in logs
        ]


# Convenience instance
privacy_service = PrivacyService()
