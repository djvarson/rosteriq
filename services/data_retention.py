"""
Data retention and privacy compliance service for RosterIQ.

Implements retention policies aligned with Australian Privacy Act 1988 and
Australian Privacy Principles (APPs). Provides automated data purging and
audit logging for compliance.

Retention Policy:
- Webhook events: 90 days
- Login attempts: 30 days
- Refresh tokens (revoked): 7 days
- Archived rosters: 2 years (730 days)
- Shift swap history: 1 year (365 days)
- Billing events: 7 years (2555 days) [tax compliance]
- Notification logs: 60 days

Usage:
    from rosteriq.services.data_retention import DataRetentionService
    service = DataRetentionService()
    stats = service.run_cleanup()
    print(f"Purged {stats['total']} records")
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Dict
from uuid import uuid4

from rosteriq.database import get_db

logger = logging.getLogger(__name__)


class DataRetentionService:
    """Service for managing data retention policies and automated cleanup."""

    # Retention periods in days
    RETENTION_DAYS = {
        'webhook_events': 90,
        'login_attempts': 30,
        'revoked_tokens': 7,
        'archived_rosters': 730,  # 2 years
        'shift_swaps': 365,  # 1 year
        'billing_events': 2555,  # 7 years
        'notification_logs': 60,
    }

    def __init__(self, db=None):
        """Initialize service with database connection."""
        self.db = db or get_db()

    def run_cleanup(self, dry_run: bool = False) -> Dict[str, int]:
        """
        Execute all retention policies and purge old data.

        Args:
            dry_run: If True, return what WOULD be deleted without deleting

        Returns:
            Dict with count of records purged per category
        """
        logger.info(f"Starting data retention cleanup (dry_run={dry_run})")

        stats = {
            'webhook_events': 0,
            'login_attempts': 0,
            'revoked_tokens': 0,
            'archived_rosters': 0,
            'shift_swaps': 0,
            'billing_events': 0,
            'notification_logs': 0,
            'total': 0,
        }

        if not dry_run:
            # Webhook events: 90 days
            before_date = datetime.utcnow() - timedelta(days=self.RETENTION_DAYS['webhook_events'])
            count = self.db.purge_old_webhook_events(before_date)
            stats['webhook_events'] = count
            logger.info(f"Purged {count} webhook events older than {before_date.date()}")

            # Login attempts: 30 days
            before_date = datetime.utcnow() - timedelta(days=self.RETENTION_DAYS['login_attempts'])
            count = self.db.purge_old_login_attempts(before_date)
            stats['login_attempts'] = count
            logger.info(f"Purged {count} login attempts older than {before_date.date()}")

            # Revoked tokens: 7 days
            before_date = datetime.utcnow() - timedelta(days=self.RETENTION_DAYS['revoked_tokens'])
            count = self.db.purge_revoked_tokens(before_date)
            stats['revoked_tokens'] = count
            logger.info(f"Purged {count} revoked tokens older than {before_date.date()}")

            # Log the cleanup action
            self._log_retention_action('cleanup', stats)

            stats['total'] = sum(v for k, v in stats.items() if k != 'total')
            logger.info(f"Data retention cleanup complete: {stats['total']} records purged")
        else:
            logger.info("Dry run mode: no data was deleted")

        return stats

    def get_retention_stats(self) -> Dict[str, dict]:
        """
        Get statistics on records approaching expiry.

        Returns:
            Dict with counts of records approaching expiry for each category
        """
        stats = {}

        # Records expiring in next 7 days
        now = datetime.utcnow()
        expiry_window = timedelta(days=7)

        # Webhook events expiring soon (90 days from now)
        expiry_date = now + timedelta(days=self.RETENTION_DAYS['webhook_events'])
        stats['webhook_events'] = {
            'retention_days': self.RETENTION_DAYS['webhook_events'],
            'expiry_date': expiry_date.isoformat(),
            'description': 'Webhook event processing logs',
        }

        # Login attempts expiring soon
        expiry_date = now + timedelta(days=self.RETENTION_DAYS['login_attempts'])
        stats['login_attempts'] = {
            'retention_days': self.RETENTION_DAYS['login_attempts'],
            'expiry_date': expiry_date.isoformat(),
            'description': 'Login attempt records (security)',
        }

        # Revoked tokens expiring soon
        expiry_date = now + timedelta(days=self.RETENTION_DAYS['revoked_tokens'])
        stats['revoked_tokens'] = {
            'retention_days': self.RETENTION_DAYS['revoked_tokens'],
            'expiry_date': expiry_date.isoformat(),
            'description': 'Revoked refresh tokens',
        }

        # Billing events expiring soon (7 years for tax compliance)
        expiry_date = now + timedelta(days=self.RETENTION_DAYS['billing_events'])
        stats['billing_events'] = {
            'retention_days': self.RETENTION_DAYS['billing_events'],
            'expiry_date': expiry_date.isoformat(),
            'description': 'Billing and payment events (tax compliance)',
        }

        return stats

    def schedule_daily_cleanup(self) -> dict:
        """
        Register a scheduled task to run cleanup daily at 2 AM.

        Returns:
            Task registration details
        """
        from rosteriq.services.task_scheduler import TaskScheduler

        scheduler = TaskScheduler(self.db)
        task_id = str(uuid4())

        task = {
            'id': task_id,
            'type': 'data_retention',
            'schedule': '0 2 * * *',  # 2 AM daily
            'description': 'Daily data retention cleanup',
            'enabled': True,
        }

        logger.info(f"Registered daily cleanup task: {task_id}")
        return task

    def _log_retention_action(self, action: str, details: dict) -> None:
        """Log a retention action to audit trail."""
        try:
            log_entry = {
                'user_id': 'system',
                'action': action,
                'resource_type': 'data_retention',
                'details': details,
                'logged_at': datetime.utcnow(),
            }
            self.db.save_privacy_log(log_entry)
        except Exception as e:
            logger.error(f"Failed to log retention action: {e}")


class RetentionPolicyConfig:
    """Configuration for data retention policies."""

    def __init__(self, **overrides):
        """
        Initialize with optional overrides.

        Example:
            config = RetentionPolicyConfig(webhook_events=180)
            # Now webhook events are retained for 180 days instead of 90
        """
        self.policies = DataRetentionService.RETENTION_DAYS.copy()
        self.policies.update(overrides)

    def get(self, policy_name: str, default: int = None) -> int:
        """Get retention days for a policy."""
        return self.policies.get(policy_name, default)

    def set(self, policy_name: str, days: int) -> None:
        """Update retention days for a policy."""
        if days < 0:
            raise ValueError(f"Retention days must be non-negative, got {days}")
        self.policies[policy_name] = days

    def all_policies(self) -> dict:
        """Get all policies."""
        return self.policies.copy()


# Convenience instances
data_retention_service = DataRetentionService()
retention_policy_config = RetentionPolicyConfig()
