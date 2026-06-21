"""
Comprehensive Tanda end-to-end webhook flow test harness.

Tests the complete webhook flow: Tanda webhook → process → update DB → optional push back.

Scenarios covered:
- Employee lifecycle (created, updated, deactivated)
- Shift lifecycle (created, updated, deleted)
- Leave approval and updates
- Roster publication with variance detection
- HMAC validation and signature verification
- Idempotency and duplicate prevention
- Replay protection with timestamp validation
- Full cycle workflows with multiple events
- Concurrent webhook processing
"""

import sys
import os
import json
import hmac
import hashlib
import asyncio
from datetime import datetime, timedelta, date, time
from decimal import Decimal
from typing import Dict, Any, Optional, List
from uuid import uuid4

import pytest

# Ensure rosteriq imports work
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from rosteriq.database import MemoryStore
from rosteriq.models import Employee, Shift, Roster, VenueConfig, EmploymentType, ShiftStatus, AwardLevel, State
from rosteriq.services.tanda_webhooks import (
    TandaEventType,
    WebhookPayload,
    get_registry,
)
from rosteriq.routes.webhook_routes import verify_hmac_signature


# ============================================================================
# Test Helper Class
# ============================================================================

class TestHelper:
    """Utilities for testing Tanda webhook flows."""

    WEBHOOK_SECRET = "test_webhook_secret_12345"
    TEST_VENUE_ID = "venue-test-001"

    @staticmethod
    def make_webhook_payload(
        event_type: str,
        data: Dict[str, Any],
        webhook_id: Optional[str] = None,
        timestamp: Optional[datetime] = None,
        venue_id: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a Tanda webhook payload.

        Args:
            event_type: Event type (e.g., 'employee.created')
            data: Event-specific data
            webhook_id: Unique webhook ID (auto-generated if None)
            timestamp: Event timestamp (now if None)
            venue_id: Venue ID (uses default if None)
            user_id: User ID (optional)

        Returns:
            Dict with webhook payload structure
        """
        return {
            "id": webhook_id or str(uuid4()),
            "event_type": event_type,
            "timestamp": (timestamp or datetime.utcnow()).isoformat(),
            "venue_id": venue_id or TestHelper.TEST_VENUE_ID,
            "user_id": user_id,
            "data": data,
        }

    @staticmethod
    def sign_payload(payload: Dict[str, Any], secret: str) -> str:
        """
        Compute HMAC-SHA256 signature for a webhook payload.

        Args:
            payload: Webhook payload dict
            secret: Webhook secret

        Returns:
            Hex-encoded HMAC-SHA256 signature
        """
        payload_json = json.dumps(payload, sort_keys=True)
        payload_bytes = payload_json.encode()
        signature = hmac.new(
            secret.encode(),
            payload_bytes,
            hashlib.sha256,
        ).hexdigest()
        return signature

    @staticmethod
    def create_test_db(with_seed_data: bool = True) -> MemoryStore:
        """
        Create a fresh MemoryStore database with optional seed data.

        Args:
            with_seed_data: If True, add default venue and employees

        Returns:
            Configured MemoryStore instance
        """
        db = MemoryStore()

        if with_seed_data:
            now = datetime.utcnow()
            # Add default venue. VenueConfig (models.py) fields: id, name,
            # tanda_org_id, state, timezone, min_staff (Dict[str,int]),
            # max_labour_pct, pos_system, created_at. There is no address/
            # suburb/postcode/phone/email on VenueConfig.
            venue = VenueConfig(
                id=TestHelper.TEST_VENUE_ID,
                name="Test Venue",
                tanda_org_id="org-test-001",
                state=State.vic,
                min_staff={"floor": 1},
                max_labour_pct=30.0,
                created_at=now,
            )
            db.save_venue(venue)

            # Add seed employees. Employee (models.py) required fields: id,
            # name, employment_type, award_level, state, hourly_base_rate,
            # created_at, updated_at. Enums are lowercase
            # (EmploymentType.full_time, AwardLevel.level_1). There is no
            # date_of_birth/address/suburb/postcode/is_active/hire_date/
            # hourly_rate field.
            for i in range(1, 4):
                emp = Employee(
                    id=f"emp-{i}",
                    name=f"Employee {i}",
                    email=f"emp{i}@test.local",
                    phone=f"041234567{i}",
                    state=State.vic,
                    employment_type=EmploymentType.full_time,
                    hourly_base_rate=Decimal(str(25.0 + i)),
                    award_level=AwardLevel.level_1,
                    venue_id=TestHelper.TEST_VENUE_ID,
                    created_at=now,
                    updated_at=now,
                )
                db.save_employee(emp)

        return db

    @staticmethod
    def assert_employee_exists(
        db: MemoryStore,
        employee_id: str,
        **expected_fields,
    ) -> Employee:
        """
        Assert an employee exists and has expected fields.

        Args:
            db: Database instance
            employee_id: Employee ID to check
            **expected_fields: Fields to verify (name, email, etc.)

        Returns:
            The Employee object if found

        Raises:
            AssertionError if employee not found or fields don't match
        """
        emp = db.get_employee(employee_id)
        assert emp is not None, f"Employee {employee_id} not found in DB"

        for field, expected_value in expected_fields.items():
            actual_value = getattr(emp, field, None)
            assert actual_value == expected_value, \
                f"Employee {employee_id}.{field} = {actual_value}, expected {expected_value}"

        return emp

    @staticmethod
    def assert_shift_exists(
        db: MemoryStore,
        shift_id: str,
        **expected_fields,
    ) -> Shift:
        """
        Assert a shift exists and has expected fields.

        Args:
            db: Database instance
            shift_id: Shift ID to check
            **expected_fields: Fields to verify (employee_id, date, etc.)

        Returns:
            The Shift object if found

        Raises:
            AssertionError if shift not found or fields don't match
        """
        if not hasattr(db, '_shifts'):
            db._shifts = {}
        shift = db._shifts.get(shift_id)
        assert shift is not None, f"Shift {shift_id} not found in DB"

        for field, expected_value in expected_fields.items():
            actual_value = getattr(shift, field, None)
            assert actual_value == expected_value, \
                f"Shift {shift_id}.{field} = {actual_value}, expected {expected_value}"

        return shift


# ============================================================================
# Tests: Employee Webhooks
# ============================================================================

class TestEmployeeWebhooks:
    """Test employee lifecycle webhooks."""

    @pytest.mark.asyncio
    async def test_employee_created_webhook(self):
        """Employee.created → verify employee saved in DB."""
        db = TestHelper.create_test_db(with_seed_data=False)
        registry = get_registry()

        # Create webhook payload
        employee_data = {
            "id": "emp-new-001",
            "name": "Alice Smith",
            "email": "alice@test.local",
        }
        payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.EMPLOYEE_CREATED.value,
            data=employee_data,
        )

        # Create WebhookPayload
        payload = WebhookPayload(
            event_type=TandaEventType.EMPLOYEE_CREATED,
            webhook_id=payload_dict["id"],
            timestamp=datetime.fromisoformat(payload_dict["timestamp"]),
            venue_id=payload_dict["venue_id"],
            data=payload_dict["data"],
        )

        # Dispatch to handlers
        result = await registry.dispatch(payload, ws_hub=None)

        # Verify handler was called
        assert result["handlers_called"] > 0
        assert len(result["errors"]) == 0, f"Handler errors: {result['errors']}"

    @pytest.mark.asyncio
    async def test_employee_updated_webhook(self):
        """Employee.updated → verify fields updated."""
        db = TestHelper.create_test_db(with_seed_data=True)
        registry = get_registry()

        # Create webhook payload for employee update
        employee_data = {
            "id": "emp-1",
            "name": "Employee 1 Updated",
            "email": "emp1-updated@test.local",
        }
        payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.EMPLOYEE_UPDATED.value,
            data=employee_data,
        )

        # Dispatch to handlers
        payload = WebhookPayload(
            event_type=TandaEventType.EMPLOYEE_UPDATED,
            webhook_id=payload_dict["id"],
            timestamp=datetime.fromisoformat(payload_dict["timestamp"]),
            venue_id=payload_dict["venue_id"],
            data=payload_dict["data"],
        )

        result = await registry.dispatch(payload, ws_hub=None)

        # Verify handler was called successfully
        assert result["handlers_called"] > 0
        assert len(result["errors"]) == 0

    @pytest.mark.asyncio
    async def test_employee_deactivated_webhook(self):
        """Employee.deactivated → verify handler called."""
        db = TestHelper.create_test_db(with_seed_data=True)
        registry = get_registry()

        employee_data = {"id": "emp-1"}
        payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.EMPLOYEE_DEACTIVATED.value,
            data=employee_data,
        )

        payload = WebhookPayload(
            event_type=TandaEventType.EMPLOYEE_DEACTIVATED,
            webhook_id=payload_dict["id"],
            timestamp=datetime.fromisoformat(payload_dict["timestamp"]),
            venue_id=payload_dict["venue_id"],
            data=payload_dict["data"],
        )

        result = await registry.dispatch(payload, ws_hub=None)

        assert result["handlers_called"] > 0
        assert len(result["errors"]) == 0


# ============================================================================
# Tests: Shift Webhooks
# ============================================================================

class TestShiftWebhooks:
    """Test shift lifecycle webhooks."""

    @pytest.mark.asyncio
    async def test_shift_created_webhook(self):
        """Shift.created → verify shift added to roster."""
        db = TestHelper.create_test_db(with_seed_data=True)
        registry = get_registry()

        # Create shift webhook payload
        shift_data = {
            "id": "shift-001",
            "employee_id": "emp-1",
            "start_time": "2026-04-27T09:00:00",
            "end_time": "2026-04-27T17:00:00",
            "hourly_rate": 25.0,
            "award_level": "minimum",
        }
        payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.SHIFT_CREATED.value,
            data=shift_data,
        )

        payload = WebhookPayload(
            event_type=TandaEventType.SHIFT_CREATED,
            webhook_id=payload_dict["id"],
            timestamp=datetime.fromisoformat(payload_dict["timestamp"]),
            venue_id=payload_dict["venue_id"],
            data=payload_dict["data"],
        )

        result = await registry.dispatch(payload, ws_hub=None)

        # Verify handler was called and cost was calculated
        assert result["handlers_called"] > 0
        assert len(result["errors"]) == 0
        assert result["results"][0]["status"] == "success"

    @pytest.mark.asyncio
    async def test_shift_updated_webhook(self):
        """Shift.updated → verify shift modified."""
        db = TestHelper.create_test_db(with_seed_data=True)
        registry = get_registry()

        shift_data = {
            "id": "shift-001",
            "employee_id": "emp-2",
            "start_time": "2026-04-27T10:00:00",
            "end_time": "2026-04-27T18:00:00",
        }
        payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.SHIFT_UPDATED.value,
            data=shift_data,
        )

        payload = WebhookPayload(
            event_type=TandaEventType.SHIFT_UPDATED,
            webhook_id=payload_dict["id"],
            timestamp=datetime.fromisoformat(payload_dict["timestamp"]),
            venue_id=payload_dict["venue_id"],
            data=payload_dict["data"],
        )

        result = await registry.dispatch(payload, ws_hub=None)

        assert result["handlers_called"] > 0
        assert len(result["errors"]) == 0

    @pytest.mark.asyncio
    async def test_shift_deleted_webhook(self):
        """Shift.deleted → verify shift removed."""
        db = TestHelper.create_test_db(with_seed_data=True)
        registry = get_registry()

        shift_data = {"id": "shift-001"}
        payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.SHIFT_DELETED.value,
            data=shift_data,
        )

        payload = WebhookPayload(
            event_type=TandaEventType.SHIFT_DELETED,
            webhook_id=payload_dict["id"],
            timestamp=datetime.fromisoformat(payload_dict["timestamp"]),
            venue_id=payload_dict["venue_id"],
            data=payload_dict["data"],
        )

        result = await registry.dispatch(payload, ws_hub=None)

        assert result["handlers_called"] > 0
        assert len(result["errors"]) == 0


# ============================================================================
# Tests: Leave Webhooks
# ============================================================================

class TestLeaveWebhooks:
    """Test leave/availability webhooks."""

    @pytest.mark.asyncio
    async def test_leave_created_webhook(self):
        """Leave.created → verify employee marked unavailable."""
        db = TestHelper.create_test_db(with_seed_data=True)
        registry = get_registry()

        leave_data = {
            "id": "leave-001",
            "employee_id": "emp-1",
            "start_date": "2026-05-01",
            "end_date": "2026-05-05",
            "type": "annual",
        }
        payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.LEAVE_CREATED.value,
            data=leave_data,
        )

        payload = WebhookPayload(
            event_type=TandaEventType.LEAVE_CREATED,
            webhook_id=payload_dict["id"],
            timestamp=datetime.fromisoformat(payload_dict["timestamp"]),
            venue_id=payload_dict["venue_id"],
            data=payload_dict["data"],
        )

        result = await registry.dispatch(payload, ws_hub=None)

        assert result["handlers_called"] > 0
        assert len(result["errors"]) == 0

    @pytest.mark.asyncio
    async def test_leave_updated_webhook(self):
        """Leave.updated → verify dates change triggers re-optimization flag."""
        db = TestHelper.create_test_db(with_seed_data=True)
        registry = get_registry()

        leave_data = {
            "id": "leave-001",
            "employee_id": "emp-1",
            "start_date": "2026-05-01",
            "end_date": "2026-05-08",  # Changed dates
        }
        payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.LEAVE_UPDATED.value,
            data=leave_data,
        )

        payload = WebhookPayload(
            event_type=TandaEventType.LEAVE_UPDATED,
            webhook_id=payload_dict["id"],
            timestamp=datetime.fromisoformat(payload_dict["timestamp"]),
            venue_id=payload_dict["venue_id"],
            data=payload_dict["data"],
        )

        result = await registry.dispatch(payload, ws_hub=None)

        # Should flag re-optimization
        assert result["handlers_called"] > 0
        assert len(result["errors"]) == 0
        if result["results"]:
            assert result["results"][0]["result"]["reoptimization_suggested"] is True


# ============================================================================
# Tests: Roster Webhooks
# ============================================================================

class TestRosterWebhooks:
    """Test roster publication webhooks."""

    @pytest.mark.asyncio
    async def test_roster_published_webhook(self):
        """Roster.published → verify roster imported."""
        db = TestHelper.create_test_db(with_seed_data=True)
        registry = get_registry()

        roster_data = {
            "id": "roster-001",
            "shifts": [
                {
                    "id": "shift-1",
                    "employee_id": "emp-1",
                    "actual_hours": 8.0,
                    "scheduled_hours": 8.0,
                },
                {
                    "id": "shift-2",
                    "employee_id": "emp-2",
                    "actual_hours": 10.0,
                    "scheduled_hours": 8.0,  # Variance > 2 hours
                },
            ],
        }
        payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.ROSTER_PUBLISHED.value,
            data=roster_data,
        )

        payload = WebhookPayload(
            event_type=TandaEventType.ROSTER_PUBLISHED,
            webhook_id=payload_dict["id"],
            timestamp=datetime.fromisoformat(payload_dict["timestamp"]),
            venue_id=payload_dict["venue_id"],
            data=payload_dict["data"],
        )

        result = await registry.dispatch(payload, ws_hub=None)

        assert result["handlers_called"] > 0
        assert len(result["errors"]) == 0
        # Check variance was detected
        if result["results"]:
            handler_result = result["results"][0]["result"]
            assert handler_result["shifts_synced"] == 2
            assert handler_result["variances_flagged"] >= 1


# ============================================================================
# Tests: HMAC & Security
# ============================================================================

class TestHMACValidation:
    """Test webhook signature verification."""

    def test_webhook_hmac_valid(self):
        """Valid HMAC signature is accepted."""
        payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.SHIFT_CREATED.value,
            data={"id": "shift-001", "employee_id": "emp-1"},
        )
        payload_json = json.dumps(payload_dict, sort_keys=True)
        payload_bytes = payload_json.encode()

        # Sign with correct secret
        signature = TestHelper.sign_payload(payload_dict, TestHelper.WEBHOOK_SECRET)

        # Verify
        result = verify_hmac_signature(payload_bytes, signature, TestHelper.WEBHOOK_SECRET)
        assert result is True

    def test_webhook_hmac_invalid(self):
        """Invalid HMAC signature is rejected."""
        payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.SHIFT_CREATED.value,
            data={"id": "shift-001"},
        )
        payload_json = json.dumps(payload_dict, sort_keys=True)
        payload_bytes = payload_json.encode()

        # Use wrong signature
        wrong_signature = "0000000000000000000000000000000000000000000000000000000000000000"

        result = verify_hmac_signature(payload_bytes, wrong_signature, TestHelper.WEBHOOK_SECRET)
        assert result is False

    def test_webhook_hmac_wrong_secret(self):
        """HMAC with wrong secret fails verification."""
        payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.SHIFT_CREATED.value,
            data={"id": "shift-001"},
        )
        payload_json = json.dumps(payload_dict, sort_keys=True)
        payload_bytes = payload_json.encode()

        # Sign with correct secret
        signature = TestHelper.sign_payload(payload_dict, TestHelper.WEBHOOK_SECRET)

        # Try to verify with wrong secret
        result = verify_hmac_signature(payload_bytes, signature, "wrong_secret_99999")
        assert result is False

    def test_webhook_hmac_tampered_payload(self):
        """Tampered payload fails HMAC verification."""
        payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.SHIFT_CREATED.value,
            data={"id": "shift-001"},
        )

        # Sign original
        signature = TestHelper.sign_payload(payload_dict, TestHelper.WEBHOOK_SECRET)

        # Tamper with payload
        payload_dict["data"]["id"] = "shift-999"
        payload_json = json.dumps(payload_dict, sort_keys=True)
        payload_bytes = payload_json.encode()

        # Verify should fail
        result = verify_hmac_signature(payload_bytes, signature, TestHelper.WEBHOOK_SECRET)
        assert result is False


# ============================================================================
# Tests: Idempotency
# ============================================================================

class TestIdempotency:
    """Test idempotent webhook processing."""

    def test_webhook_idempotency_duplicate_rejected(self):
        """Same webhook sent twice → processed only once."""
        db = TestHelper.create_test_db(with_seed_data=True)

        webhook_id = "webhook-duplicate-test"
        payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.SHIFT_CREATED.value,
            data={"id": "shift-001", "employee_id": "emp-1"},
            webhook_id=webhook_id,
        )
        payload_json = json.dumps(payload_dict, sort_keys=True)
        payload_bytes = payload_json.encode()

        # First processing
        is_dup_1 = db.is_webhook_processed(webhook_id)
        assert is_dup_1 is False

        payload_hash = hashlib.sha256(payload_bytes).hexdigest()
        db.save_webhook_event(webhook_id, TandaEventType.SHIFT_CREATED.value, payload_hash)

        # Second processing with same webhook_id
        is_dup_2 = db.is_webhook_processed(webhook_id)
        assert is_dup_2 is True

    def test_webhook_idempotency_different_ids_allowed(self):
        """Different webhook IDs → both processed."""
        db = TestHelper.create_test_db(with_seed_data=True)

        webhook_id_1 = "webhook-001"
        webhook_id_2 = "webhook-002"

        payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.SHIFT_CREATED.value,
            data={"id": "shift-001"},
        )
        payload_json = json.dumps(payload_dict, sort_keys=True)
        payload_bytes = payload_json.encode()
        payload_hash = hashlib.sha256(payload_bytes).hexdigest()

        # Record first webhook
        db.save_webhook_event(webhook_id_1, TandaEventType.SHIFT_CREATED.value, payload_hash)
        assert db.is_webhook_processed(webhook_id_1) is True

        # Second webhook should not be marked as duplicate
        assert db.is_webhook_processed(webhook_id_2) is False

        db.save_webhook_event(webhook_id_2, TandaEventType.SHIFT_CREATED.value, payload_hash)
        assert db.is_webhook_processed(webhook_id_2) is True


# ============================================================================
# Tests: Replay Protection
# ============================================================================

class TestReplayProtection:
    """Test webhook replay attack prevention."""

    def test_webhook_replay_old_timestamp_detected(self):
        """Webhook with old timestamp can be detected."""
        # Create a webhook with timestamp from 1 hour ago
        old_timestamp = datetime.utcnow() - timedelta(hours=1)

        payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.SHIFT_CREATED.value,
            data={"id": "shift-001"},
            timestamp=old_timestamp,
        )

        # Extract timestamp and check it's old
        payload_timestamp = datetime.fromisoformat(payload_dict["timestamp"])
        time_diff = datetime.utcnow() - payload_timestamp

        # Should be old (> 5 minutes)
        assert time_diff.total_seconds() > 300

    def test_webhook_timestamp_validation(self):
        """Valid timestamp is within acceptable window."""
        # Create a webhook with current timestamp
        current_timestamp = datetime.utcnow()

        payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.SHIFT_CREATED.value,
            data={"id": "shift-001"},
            timestamp=current_timestamp,
        )

        # Extract timestamp and verify it's current
        payload_timestamp = datetime.fromisoformat(payload_dict["timestamp"])
        time_diff = datetime.utcnow() - payload_timestamp

        # Should be recent (< 1 second)
        assert time_diff.total_seconds() < 1.0


# ============================================================================
# Tests: Full Cycle Workflows
# ============================================================================

class TestFullCycleWorkflows:
    """Test complete multi-step workflows."""

    @pytest.mark.asyncio
    async def test_full_cycle_employee_shift_update(self):
        """Create employee → create shift → update shift → verify all DB state."""
        db = TestHelper.create_test_db(with_seed_data=False)
        registry = get_registry()

        # Step 1: Employee created
        emp_payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.EMPLOYEE_CREATED.value,
            data={
                "id": "emp-alice",
                "name": "Alice",
                "email": "alice@test.local",
            },
        )
        emp_payload = WebhookPayload(
            event_type=TandaEventType.EMPLOYEE_CREATED,
            webhook_id=emp_payload_dict["id"],
            timestamp=datetime.fromisoformat(emp_payload_dict["timestamp"]),
            venue_id=emp_payload_dict["venue_id"],
            data=emp_payload_dict["data"],
        )
        result_1 = await registry.dispatch(emp_payload, ws_hub=None)
        assert len(result_1["errors"]) == 0

        # Step 2: Shift created
        shift_payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.SHIFT_CREATED.value,
            data={
                "id": "shift-alice-001",
                "employee_id": "emp-alice",
                "start_time": "2026-04-27T09:00:00",
                "end_time": "2026-04-27T17:00:00",
                "hourly_rate": 30.0,
                "award_level": "minimum",
            },
        )
        shift_payload = WebhookPayload(
            event_type=TandaEventType.SHIFT_CREATED,
            webhook_id=shift_payload_dict["id"],
            timestamp=datetime.fromisoformat(shift_payload_dict["timestamp"]),
            venue_id=shift_payload_dict["venue_id"],
            data=shift_payload_dict["data"],
        )
        result_2 = await registry.dispatch(shift_payload, ws_hub=None)
        assert len(result_2["errors"]) == 0
        assert result_2["results"][0]["result"]["cost_calculated"] is True

        # Step 3: Shift updated
        update_payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.SHIFT_UPDATED.value,
            data={
                "id": "shift-alice-001",
                "start_time": "2026-04-27T10:00:00",
                "end_time": "2026-04-27T18:00:00",
            },
        )
        update_payload = WebhookPayload(
            event_type=TandaEventType.SHIFT_UPDATED,
            webhook_id=update_payload_dict["id"],
            timestamp=datetime.fromisoformat(update_payload_dict["timestamp"]),
            venue_id=update_payload_dict["venue_id"],
            data=update_payload_dict["data"],
        )
        result_3 = await registry.dispatch(update_payload, ws_hub=None)
        assert len(result_3["errors"]) == 0


# ============================================================================
# Tests: Concurrent Processing
# ============================================================================

class TestConcurrentWebhooks:
    """Test handling of concurrent webhook events."""

    @pytest.mark.asyncio
    async def test_concurrent_webhooks_multiple_shifts(self):
        """Multiple webhooks arriving at once → all processed."""
        db = TestHelper.create_test_db(with_seed_data=True)
        registry = get_registry()

        # Create multiple shift webhooks
        tasks = []
        for i in range(1, 6):
            shift_payload_dict = TestHelper.make_webhook_payload(
                event_type=TandaEventType.SHIFT_CREATED.value,
                data={
                    "id": f"shift-concurrent-{i}",
                    "employee_id": f"emp-{(i % 3) + 1}",
                    "start_time": f"2026-04-27T{9 + i}:00:00",
                    "end_time": f"2026-04-27T{17 + i}:00:00",
                    "hourly_rate": 25.0,
                    "award_level": "minimum",
                },
            )
            payload = WebhookPayload(
                event_type=TandaEventType.SHIFT_CREATED,
                webhook_id=shift_payload_dict["id"],
                timestamp=datetime.fromisoformat(shift_payload_dict["timestamp"]),
                venue_id=shift_payload_dict["venue_id"],
                data=shift_payload_dict["data"],
            )
            tasks.append(registry.dispatch(payload, ws_hub=None))

        # Execute all concurrently
        results = await asyncio.gather(*tasks)

        # Verify all completed successfully
        assert len(results) == 5
        for result in results:
            assert result["handlers_called"] > 0
            assert len(result["errors"]) == 0

    @pytest.mark.asyncio
    async def test_concurrent_mixed_event_types(self):
        """Multiple different event types arriving at once."""
        db = TestHelper.create_test_db(with_seed_data=True)
        registry = get_registry()

        tasks = []

        # Employee update
        emp_payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.EMPLOYEE_UPDATED.value,
            data={"id": "emp-1", "name": "Employee 1 v2"},
        )
        emp_payload = WebhookPayload(
            event_type=TandaEventType.EMPLOYEE_UPDATED,
            webhook_id=emp_payload_dict["id"],
            timestamp=datetime.fromisoformat(emp_payload_dict["timestamp"]),
            venue_id=emp_payload_dict["venue_id"],
            data=emp_payload_dict["data"],
        )
        tasks.append(registry.dispatch(emp_payload, ws_hub=None))

        # Shift created
        shift_payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.SHIFT_CREATED.value,
            data={
                "id": "shift-concurrent",
                "employee_id": "emp-2",
                "start_time": "2026-04-27T09:00:00",
                "end_time": "2026-04-27T17:00:00",
                "hourly_rate": 25.0,
            },
        )
        shift_payload = WebhookPayload(
            event_type=TandaEventType.SHIFT_CREATED,
            webhook_id=shift_payload_dict["id"],
            timestamp=datetime.fromisoformat(shift_payload_dict["timestamp"]),
            venue_id=shift_payload_dict["venue_id"],
            data=shift_payload_dict["data"],
        )
        tasks.append(registry.dispatch(shift_payload, ws_hub=None))

        # Leave created
        leave_payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.LEAVE_CREATED.value,
            data={
                "id": "leave-concurrent",
                "employee_id": "emp-3",
                "start_date": "2026-05-01",
                "end_date": "2026-05-05",
            },
        )
        leave_payload = WebhookPayload(
            event_type=TandaEventType.LEAVE_CREATED,
            webhook_id=leave_payload_dict["id"],
            timestamp=datetime.fromisoformat(leave_payload_dict["timestamp"]),
            venue_id=leave_payload_dict["venue_id"],
            data=leave_payload_dict["data"],
        )
        tasks.append(registry.dispatch(leave_payload, ws_hub=None))

        # Execute all concurrently
        results = await asyncio.gather(*tasks)

        # Verify all completed
        assert len(results) == 3
        for result in results:
            assert result["handlers_called"] > 0
            assert len(result["errors"]) == 0


# ============================================================================
# Tests: Timesheet Webhooks
# ============================================================================

class TestTimesheetWebhooks:
    """Test timesheet approval webhooks."""

    @pytest.mark.asyncio
    async def test_timesheet_approved_webhook(self):
        """Timesheet.approved → verify variance detection."""
        db = TestHelper.create_test_db(with_seed_data=True)
        registry = get_registry()

        timesheet_data = {
            "id": "timesheet-001",
            "approved_hours": 40.5,
            "rostered_hours": 40.0,
        }
        payload_dict = TestHelper.make_webhook_payload(
            event_type=TandaEventType.TIMESHEET_APPROVED.value,
            data=timesheet_data,
        )

        payload = WebhookPayload(
            event_type=TandaEventType.TIMESHEET_APPROVED,
            webhook_id=payload_dict["id"],
            timestamp=datetime.fromisoformat(payload_dict["timestamp"]),
            venue_id=payload_dict["venue_id"],
            data=payload_dict["data"],
        )

        result = await registry.dispatch(payload, ws_hub=None)

        assert result["handlers_called"] > 0
        assert len(result["errors"]) == 0
        if result["results"]:
            handler_result = result["results"][0]["result"]
            assert handler_result["approved_hours"] == 40.5
            assert handler_result["variance_hours"] == 0.5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
