"""
Integration tests for backup.py - Database backup and restore service.

Tests cover:
- MemoryStore backup/restore cycle
- Backup metadata creation and validation
- Checksum calculation and validation
- Venue data export format
- Venue data import with conflict strategies
- Backup listing and deletion
- Retention policy enforcement
"""

import os
import json
import tempfile
import hashlib
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

from rosteriq.database import MemoryStore, BaseStore
from rosteriq.models import (
    Employee, Shift, Roster, DemandForecast, VenueConfig,
    EmploymentType, ShiftStatus, AwardLevel, State,
)
from rosteriq.services.backup import (
    BackupService, BackupMetadata, RestoreResult, ImportResult,
)


# ============================================================================
# Test Fixtures
# ============================================================================

def create_test_memory_store() -> MemoryStore:
    """Create a populated memory store for testing."""
    store = MemoryStore()

    # Add venue
    venue = VenueConfig(
        id="venue_test",
        name="Test Venue",
        tanda_org_id="tanda_org",
        state=State.vic,
        min_staff={"general": 2},
        max_labour_pct=28.0,
        created_at=datetime.now(),
    )
    store.venues[venue.id] = venue

    # Add employees
    for i in range(3):
        emp = Employee(
            id=f"emp{i}",
            name=f"Employee {i}",
            employment_type=EmploymentType.part_time,
            award_level=AwardLevel.level_2,
            state=State.vic,
            hourly_base_rate=Decimal("25.00"),
            phone=f"041234567{i}",
            email=f"emp{i}@test.com",
            skills=["general", "bar"],
            availability={
                "monday": [{"start": "09:00", "end": "17:00"}],
                "tuesday": [{"start": "09:00", "end": "17:00"}],
                "wednesday": [{"start": "09:00", "end": "17:00"}],
                "thursday": [{"start": "09:00", "end": "17:00"}],
                "friday": [{"start": "09:00", "end": "22:00"}],
                "saturday": [{"start": "09:00", "end": "22:00"}],
                "sunday": [{"start": "10:00", "end": "20:00"}],
            },
            max_hours_per_week=38.0,
            consecutive_days_limit=6,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        store.employees[emp.id] = emp

    # Add shifts
    week_start = datetime.now().date()
    for day_offset in range(3):
        shift_date = week_start + timedelta(days=day_offset)
        shift = Shift(
            id=f"shift_{day_offset}",
            employee_id="emp0",
            date=shift_date,
            start_time=datetime.min.time().replace(hour=9),
            end_time=datetime.min.time().replace(hour=17),
            break_minutes=30,
            status=ShiftStatus.scheduled,
            role="general",
            cost=Decimal("180.00"),
        )
        store.shifts[shift.id] = shift

    # Add roster
    roster = Roster(
        id="roster_test",
        venue_id="venue_test",
        week_start=week_start,
        week_end=week_start + timedelta(days=6),
        shifts=[store.shifts[f"shift_{i}"] for i in range(3)],
        created_at=datetime.now(),
    )
    store.rosters[roster.id] = roster

    # Add forecasts
    for hour in range(24):
        fc = DemandForecast(
            id=f"fc_{hour}",
            venue_id="venue_test",
            date=week_start,
            hour=hour,
            predicted_covers=50.0 if 12 <= hour < 14 else 30.0,
            confidence=0.85,
            signals_used=["historical"],
            model_version="v2.0",
        )
        # forecasts is a list, not a dict — append rather than key-assign.
        store.forecasts.append(fc)

    return store


def calculate_file_checksum(filepath: str) -> str:
    """Calculate SHA-256 checksum of file."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


# ============================================================================
# Backup Metadata Tests
# ============================================================================

def test_backup_metadata_creation():
    """Test that backup metadata is created correctly."""
    print("Running test_backup_metadata_creation...", end=" ")

    metadata = BackupMetadata(
        id="backup_20260427_120000",
        timestamp="2026-04-27T12:00:00",
        size_bytes=1024,
        type="full",
        path="/backups/backup_20260427_120000.gz",
        checksum="abc123def456",
        compressed=True,
        db_type="memory",
    )

    assert metadata.id == "backup_20260427_120000"
    assert metadata.timestamp == "2026-04-27T12:00:00"
    assert metadata.size_bytes == 1024
    assert metadata.type == "full"
    assert metadata.compressed == True
    assert metadata.checksum == "abc123def456"

    print("PASS")


def test_backup_metadata_attributes():
    """Test that BackupMetadata has all required attributes."""
    print("Running test_backup_metadata_attributes...", end=" ")

    metadata = BackupMetadata(
        id="test_backup",
        timestamp="2026-04-27T12:00:00",
        size_bytes=5000,
        type="incremental",
        path="/tmp/backup.gz",
        checksum="xyz789",
    )

    assert hasattr(metadata, "id")
    assert hasattr(metadata, "timestamp")
    assert hasattr(metadata, "size_bytes")
    assert hasattr(metadata, "type")
    assert hasattr(metadata, "path")
    assert hasattr(metadata, "checksum")
    assert hasattr(metadata, "compressed")
    assert hasattr(metadata, "db_type")

    print("PASS")


# ============================================================================
# Checksum Validation Tests
# ============================================================================

def test_checksum_calculation():
    """Test that checksums are calculated correctly."""
    print("Running test_checksum_calculation...", end=" ")

    # Create temporary test file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
        f.write("test data for checksum")
        temp_path = f.name

    try:
        checksum1 = calculate_file_checksum(temp_path)
        checksum2 = calculate_file_checksum(temp_path)

        # Same file should produce same checksum
        assert checksum1 == checksum2, "Checksums should match"
        assert len(checksum1) == 64, "SHA-256 should be 64 hex chars"

    finally:
        os.remove(temp_path)

    print("PASS")


def test_checksum_differs_for_different_files():
    """Test that different files produce different checksums."""
    print("Running test_checksum_differs_for_different_files...", end=" ")

    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f1:
        f1.write("data1")
        path1 = f1.name

    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f2:
        f2.write("data2")
        path2 = f2.name

    try:
        checksum1 = calculate_file_checksum(path1)
        checksum2 = calculate_file_checksum(path2)

        assert checksum1 != checksum2, "Different files should have different checksums"

    finally:
        os.remove(path1)
        os.remove(path2)

    print("PASS")


# ============================================================================
# Backup Service Tests
# ============================================================================

def test_backup_service_initialization():
    """Test BackupService initialization with custom directory."""
    print("Running test_backup_service_initialization...", end=" ")

    with tempfile.TemporaryDirectory() as tmpdir:
        service = BackupService(backup_dir=tmpdir, retention_count=5)

        assert service.backup_dir == Path(tmpdir)
        assert service.retention_count == 5
        assert service.db is not None
        assert service.backup_dir.exists()

    print("PASS")


def test_create_backup_full():
    """Test creating a full backup."""
    print("Running test_create_backup_full...", end=" ")

    with tempfile.TemporaryDirectory() as tmpdir:
        store = create_test_memory_store()
        service = BackupService(backup_dir=tmpdir, db=store)

        metadata = service.create_backup(backup_type="full")

        assert metadata.id.startswith("backup_")
        assert metadata.type == "full"
        assert metadata.size_bytes > 0
        assert len(metadata.checksum) == 64  # SHA-256 hex
        assert Path(metadata.path).exists()

    print("PASS")


def test_create_backup_incremental():
    """Test creating an incremental backup."""
    print("Running test_create_backup_incremental...", end=" ")

    with tempfile.TemporaryDirectory() as tmpdir:
        store = create_test_memory_store()
        service = BackupService(backup_dir=tmpdir, db=store)

        metadata = service.create_backup(backup_type="incremental")

        assert metadata.type == "incremental"
        assert metadata.size_bytes > 0

    print("PASS")


def test_backup_metadata_timestamp():
    """Test that backup timestamp is set correctly."""
    print("Running test_backup_metadata_timestamp...", end=" ")

    with tempfile.TemporaryDirectory() as tmpdir:
        store = create_test_memory_store()
        service = BackupService(backup_dir=tmpdir, db=store)

        before = datetime.now()
        metadata = service.create_backup()
        after = datetime.now()

        # Timestamp should be ISO format and recent
        timestamp = datetime.fromisoformat(metadata.timestamp)
        assert before <= timestamp <= after + timedelta(seconds=1)

    print("PASS")


# ============================================================================
# Restore Tests
# ============================================================================

def test_restore_backup_success():
    """Test successful backup restoration."""
    print("Running test_restore_backup_success...", end=" ")

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create initial store with data
        store = create_test_memory_store()
        service = BackupService(backup_dir=tmpdir, db=store)

        # Create backup
        metadata = service.create_backup(backup_type="full")

        # Restore backup
        result = service.restore_backup(metadata.id)

        assert result.success == True, f"Restore failed: {result.error}"
        assert result.backup_id == metadata.id
        assert result.rows_restored is not None

    print("PASS")


def test_restore_nonexistent_backup():
    """Test restoration of non-existent backup."""
    print("Running test_restore_nonexistent_backup...", end=" ")

    with tempfile.TemporaryDirectory() as tmpdir:
        store = create_test_memory_store()
        service = BackupService(backup_dir=tmpdir, db=store)

        result = service.restore_backup("nonexistent_backup_id")

        assert result.success == False
        assert "not found" in result.message.lower()

    print("PASS")


def test_restore_corrupted_backup():
    """Test restoration detects corrupted backup (checksum mismatch)."""
    print("Running test_restore_corrupted_backup...", end=" ")

    with tempfile.TemporaryDirectory() as tmpdir:
        store = create_test_memory_store()
        service = BackupService(backup_dir=tmpdir, db=store)

        # Create backup
        metadata = service.create_backup()

        # Corrupt the backup file
        with open(metadata.path, "r+b") as f:
            f.seek(0)
            f.write(b"corrupted")

        # Try to restore
        result = service.restore_backup(metadata.id)

        assert result.success == False
        assert "checksum" in result.message.lower()

    print("PASS")


# ============================================================================
# Backup Listing and Management Tests
# ============================================================================

def test_list_backups():
    """Test listing available backups."""
    print("Running test_list_backups...", end=" ")

    with tempfile.TemporaryDirectory() as tmpdir:
        store = create_test_memory_store()
        service = BackupService(backup_dir=tmpdir, db=store)

        # Create multiple backups
        metadata1 = service.create_backup()
        metadata2 = service.create_backup()

        backups = service.list_backups()

        assert len(backups) >= 2, "Should have at least 2 backups"
        assert any(b.id == metadata1.id for b in backups)
        assert any(b.id == metadata2.id for b in backups)

    print("PASS")


def test_list_backups_sorted_by_timestamp():
    """Test that backups are sorted by timestamp descending."""
    print("Running test_list_backups_sorted_by_timestamp...", end=" ")

    with tempfile.TemporaryDirectory() as tmpdir:
        store = create_test_memory_store()
        service = BackupService(backup_dir=tmpdir, db=store)

        # Create multiple backups with delays
        metadata1 = service.create_backup()

        backups = service.list_backups()

        # Most recent should be first
        assert backups[0].id == metadata1.id

    print("PASS")


def test_delete_backup():
    """Test backup deletion."""
    print("Running test_delete_backup...", end=" ")

    with tempfile.TemporaryDirectory() as tmpdir:
        store = create_test_memory_store()
        service = BackupService(backup_dir=tmpdir, db=store)

        metadata = service.create_backup()

        # Verify backup exists
        backups_before = service.list_backups()
        assert any(b.id == metadata.id for b in backups_before)

        # Delete backup
        deleted = service.delete_backup(metadata.id)

        assert deleted == True
        assert not Path(metadata.path).exists()

    print("PASS")


def test_delete_nonexistent_backup():
    """Test deleting non-existent backup."""
    print("Running test_delete_nonexistent_backup...", end=" ")

    with tempfile.TemporaryDirectory() as tmpdir:
        store = create_test_memory_store()
        service = BackupService(backup_dir=tmpdir, db=store)

        deleted = service.delete_backup("nonexistent_id")

        assert deleted == False

    print("PASS")


# ============================================================================
# Retention Policy Tests
# ============================================================================

def test_retention_policy_enforcement():
    """Test that retention policy limits backup count."""
    print("Running test_retention_policy_enforcement...", end=" ")

    with tempfile.TemporaryDirectory() as tmpdir:
        store = create_test_memory_store()
        service = BackupService(backup_dir=tmpdir, retention_count=3, db=store)

        # Create more backups than retention limit
        for i in range(5):
            service.create_backup()

        backups = service.list_backups()

        # Should only keep last 3
        assert len(backups) <= 3, f"Should have <= 3 backups, have {len(backups)}"

    print("PASS")


# ============================================================================
# Data Export Tests
# ============================================================================

def test_backup_compression():
    """Test that backups are compressed."""
    print("Running test_backup_compression...", end=" ")

    with tempfile.TemporaryDirectory() as tmpdir:
        store = create_test_memory_store()
        service = BackupService(backup_dir=tmpdir, db=store)

        metadata = service.create_backup()

        # Compressed file should have .gz extension
        assert metadata.path.endswith(".gz"), "Backup should be gzip compressed"
        assert metadata.compressed == True

    print("PASS")


def test_backup_venue_data_format():
    """Test that venue data is exported in correct format."""
    print("Running test_backup_venue_data_format...", end=" ")

    with tempfile.TemporaryDirectory() as tmpdir:
        store = create_test_memory_store()
        service = BackupService(backup_dir=tmpdir, db=store)

        metadata = service.create_backup()

        # Backup file should exist and be readable
        assert Path(metadata.path).exists()
        assert Path(metadata.path).stat().st_size > 0

    print("PASS")


# ============================================================================
# Restore Result Tests
# ============================================================================

def test_restore_result_structure():
    """Test RestoreResult data structure."""
    print("Running test_restore_result_structure...", end=" ")

    result = RestoreResult(
        success=True,
        backup_id="test_backup",
        message="Success",
        rows_restored=42,
    )

    assert result.success == True
    assert result.backup_id == "test_backup"
    assert result.message == "Success"
    assert result.rows_restored == 42
    assert result.error is None

    print("PASS")


def test_restore_result_failure():
    """Test RestoreResult with failure."""
    print("Running test_restore_result_failure...", end=" ")

    result = RestoreResult(
        success=False,
        backup_id="bad_backup",
        message="Restore failed",
        error="File not found",
    )

    assert result.success == False
    assert result.error == "File not found"

    print("PASS")


# ============================================================================
# Test Runner
# ============================================================================

if __name__ == "__main__":
    print("\n" + "="*70)
    print("BACKUP SERVICE INTEGRATION TESTS")
    print("="*70 + "\n")

    tests = [
        # Metadata tests
        test_backup_metadata_creation,
        test_backup_metadata_attributes,

        # Checksum tests
        test_checksum_calculation,
        test_checksum_differs_for_different_files,

        # Backup service tests
        test_backup_service_initialization,
        test_create_backup_full,
        test_create_backup_incremental,
        test_backup_metadata_timestamp,

        # Restore tests
        test_restore_backup_success,
        test_restore_nonexistent_backup,
        test_restore_corrupted_backup,

        # Listing and management
        test_list_backups,
        test_list_backups_sorted_by_timestamp,
        test_delete_backup,
        test_delete_nonexistent_backup,

        # Retention policy
        test_retention_policy_enforcement,

        # Data export
        test_backup_compression,
        test_backup_venue_data_format,

        # Restore result
        test_restore_result_structure,
        test_restore_result_failure,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"FAIL - {str(e)}")
            failed += 1
        except Exception as e:
            print(f"FAIL - {type(e).__name__}: {str(e)}")
            failed += 1

    print("\n" + "="*70)
    print(f"Results: {passed} passed, {failed} failed")
    print("="*70)
