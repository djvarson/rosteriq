"""
Database backup and data export service for RosterIQ.

Provides functionality for:
- Creating full and incremental backups of the database
- Restoring from backups
- Exporting venue data for portability
- Importing venue data
- Uploading backups to cloud storage via presigned URLs

Usage:
    from rosteriq.services.backup import BackupService
    backup_service = BackupService()
    metadata = backup_service.create_backup(backup_type="full")
    result = backup_service.restore_backup(metadata.id)
"""

import os
import json
import gzip
import logging
import hashlib
import subprocess
import tempfile
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, Dict, List, Any
from pathlib import Path

try:
    import httpx
except ImportError:
    httpx = None

from rosteriq.database import get_db, PostgresStore, MemoryStore, BaseStore, DATABASE_URL
from rosteriq.models import VenueConfig, Employee, Shift, Roster, DemandForecast

logger = logging.getLogger(__name__)


# ============================================================================
# Data classes for backup metadata
# ============================================================================

@dataclass
class BackupMetadata:
    """Metadata about a backup."""
    id: str
    timestamp: str  # ISO 8601 format
    size_bytes: int
    type: str  # "full" or "incremental"
    path: str  # Local path to backup file
    checksum: str  # SHA-256 hash for integrity verification
    compressed: bool = True
    db_type: str = "postgres"  # "postgres" or "memory"


@dataclass
class RestoreResult:
    """Result of a restore operation."""
    success: bool
    backup_id: str
    message: str
    rows_restored: Optional[int] = None
    error: Optional[str] = None


@dataclass
class ImportResult:
    """Result of an import operation."""
    success: bool
    message: str
    entities_imported: Dict[str, int] = None
    warnings: List[str] = None
    errors: List[str] = None


# ============================================================================
# BackupService
# ============================================================================

class BackupService:
    """Service for database backups, restores, and data exports."""

    def __init__(
        self,
        backup_dir: Optional[str] = None,
        retention_count: int = 10,
        db: Optional[BaseStore] = None,
    ):
        """
        Initialize the backup service.

        Args:
            backup_dir: Directory to store backups. Defaults to ./backups
            retention_count: Number of backups to keep. Older ones are deleted.
            db: Database store instance. Defaults to get_db()
        """
        self.backup_dir = Path(backup_dir or "./backups")
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.retention_count = retention_count
        self.db = db or get_db()

    def create_backup(self, backup_type: str = "full") -> BackupMetadata:
        """
        Create a backup of the database.

        Args:
            backup_type: "full" or "incremental"

        Returns:
            BackupMetadata with backup details

        Raises:
            Exception on backup failure
        """
        backup_id = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        timestamp = datetime.now().isoformat()

        logger.info(f"Creating {backup_type} backup: {backup_id}")

        try:
            if isinstance(self.db, PostgresStore):
                backup_data = self._backup_postgres(backup_type)
            else:
                backup_data = self._backup_memory()

            # Compress backup
            compressed_path = self._compress_backup(backup_data, backup_id)

            # Calculate checksum
            checksum = self._calculate_checksum(compressed_path)

            # Get file size
            size_bytes = os.path.getsize(compressed_path)

            # Create metadata
            metadata = BackupMetadata(
                id=backup_id,
                timestamp=timestamp,
                size_bytes=size_bytes,
                type=backup_type,
                path=str(compressed_path),
                checksum=checksum,
                compressed=True,
                db_type="postgres" if isinstance(self.db, PostgresStore) else "memory",
            )

            logger.info(f"Backup created: {backup_id} ({size_bytes} bytes)")

            # Enforce retention policy
            self._enforce_retention()

            return metadata

        except Exception as e:
            logger.error(f"Backup creation failed: {e}")
            raise

    def restore_backup(self, backup_id: str) -> RestoreResult:
        """
        Restore the database from a backup.

        Args:
            backup_id: ID of the backup to restore

        Returns:
            RestoreResult with success status and details

        Raises:
            FileNotFoundError if backup not found
            ValueError if backup is corrupted
        """
        metadata = self._find_backup_file(backup_id)
        if not metadata:
            return RestoreResult(
                success=False,
                backup_id=backup_id,
                message=f"Backup not found: {backup_id}",
                error="Backup not found",
            )

        logger.info(f"Restoring backup: {backup_id}")

        try:
            # Verify checksum
            current_checksum = self._calculate_checksum(metadata.path)
            if current_checksum != metadata.checksum:
                return RestoreResult(
                    success=False,
                    backup_id=backup_id,
                    message="Backup is corrupted (checksum mismatch)",
                    error="Checksum validation failed",
                )

            # Decompress backup
            backup_data = self._decompress_backup(metadata.path)

            # Restore based on database type
            if isinstance(self.db, PostgresStore):
                rows_restored = self._restore_postgres(backup_data)
            else:
                rows_restored = self._restore_memory(backup_data)

            logger.info(f"Backup restored: {backup_id} ({rows_restored} rows)")

            return RestoreResult(
                success=True,
                backup_id=backup_id,
                message=f"Backup restored successfully",
                rows_restored=rows_restored,
            )

        except Exception as e:
            logger.error(f"Restore failed: {e}")
            return RestoreResult(
                success=False,
                backup_id=backup_id,
                message=f"Restore failed: {str(e)}",
                error=str(e),
            )

    def list_backups(self) -> List[BackupMetadata]:
        """
        List all available backups.

        Returns:
            List of BackupMetadata objects, sorted by timestamp descending
        """
        backups = []
        for backup_file in sorted(self.backup_dir.glob("backup_*.gz"), reverse=True):
            metadata = self._load_metadata(backup_file)
            if metadata:
                backups.append(metadata)
        return backups

    def delete_backup(self, backup_id: str) -> bool:
        """
        Delete a backup.

        Args:
            backup_id: ID of the backup to delete

        Returns:
            True if deleted, False if not found
        """
        metadata = self._find_backup_file(backup_id)
        if metadata:
            try:
                os.remove(metadata.path)
                # Also try to remove metadata file
                metadata_file = Path(metadata.path).with_suffix(".json")
                if metadata_file.exists():
                    os.remove(metadata_file)
                logger.info(f"Backup deleted: {backup_id}")
                return True
            except Exception as e:
                logger.error(f"Failed to delete backup: {e}")
                return False
        return False

    def upload_to_s3(
        self,
        backup_id: str,
        bucket: str,
        presigned_url: str,
    ) -> bool:
        """
        Upload a backup to S3 using a presigned URL.

        Args:
            backup_id: ID of the backup to upload
            bucket: S3 bucket name (for logging)
            presigned_url: Presigned URL for uploading to S3

        Returns:
            True if upload successful, False otherwise
        """
        if not httpx:
            logger.error("httpx library not available for S3 uploads")
            return False

        metadata = self._find_backup_file(backup_id)
        if not metadata:
            logger.error(f"Backup not found: {backup_id}")
            return False

        try:
            logger.info(f"Uploading backup to S3: {backup_id}")

            with open(metadata.path, "rb") as f:
                file_content = f.read()

            # Use httpx to upload via presigned URL
            with httpx.Client() as client:
                response = client.put(
                    presigned_url,
                    content=file_content,
                    timeout=300,
                )
                response.raise_for_status()

            logger.info(f"Backup uploaded to S3: {backup_id}")
            return True

        except Exception as e:
            logger.error(f"S3 upload failed: {e}")
            return False

    def export_venue_data(self, venue_id: str) -> Dict[str, Any]:
        """
        Export all data for a venue as portable JSON.

        Includes: venue config, employees, rosters, shifts, forecasts, templates

        Args:
            venue_id: ID of the venue to export

        Returns:
            Dictionary with venue data, suitable for JSON export

        Raises:
            ValueError if venue not found
        """
        venue = self.db.get_venue(venue_id)
        if not venue:
            raise ValueError(f"Venue not found: {venue_id}")

        logger.info(f"Exporting venue data: {venue_id}")

        # Collect all venue-related data
        employees = [e for e in self.db.list_employees() if e.venue_id == venue_id]
        rosters = [r for r in self.db.list_rosters() if r.venue_id == venue_id]
        forecasts = self.db.get_forecasts(venue_id=venue_id)

        # Serialize to JSON-compatible format
        export_data = {
            "venue": venue.dict() if hasattr(venue, "dict") else asdict(venue),
            "employees": [
                e.dict() if hasattr(e, "dict") else asdict(e)
                for e in employees
            ],
            "rosters": [
                r.dict() if hasattr(r, "dict") else asdict(r)
                for r in rosters
            ],
            "forecasts": [
                f.dict() if hasattr(f, "dict") else asdict(f)
                for f in forecasts
            ],
            "export_timestamp": datetime.now().isoformat(),
            "export_version": "1.0",
        }

        logger.info(
            f"Venue data exported: {venue_id} "
            f"({len(employees)} employees, {len(rosters)} rosters)"
        )

        return export_data

    def import_venue_data(
        self,
        data: Dict[str, Any],
        conflict_strategy: str = "skip",
    ) -> ImportResult:
        """
        Import venue data from JSON export.

        Args:
            data: Venue data dictionary from export_venue_data
            conflict_strategy: How to handle ID conflicts:
                - "skip": Don't import if ID exists
                - "overwrite": Replace existing entity
                - "generate_new": Generate new IDs

        Returns:
            ImportResult with details of imported entities
        """
        logger.info("Importing venue data")

        warnings = []
        errors = []
        entities_imported = {
            "venues": 0,
            "employees": 0,
            "rosters": 0,
            "forecasts": 0,
        }

        try:
            # Import venue
            if "venue" in data:
                venue_dict = data["venue"]
                venue_id = venue_dict.get("id")

                existing = self.db.get_venue(venue_id)
                if existing:
                    if conflict_strategy == "skip":
                        warnings.append(f"Venue {venue_id} already exists (skipped)")
                    elif conflict_strategy == "generate_new":
                        venue_dict["id"] = f"{venue_id}_import_{datetime.now().timestamp()}"
                        self._save_entity(VenueConfig, venue_dict)
                        entities_imported["venues"] += 1
                    else:  # overwrite
                        self._save_entity(VenueConfig, venue_dict)
                        entities_imported["venues"] += 1
                else:
                    self._save_entity(VenueConfig, venue_dict)
                    entities_imported["venues"] += 1

            # Import employees
            for emp_dict in data.get("employees", []):
                emp_id = emp_dict.get("id")
                existing = self.db.get_employee(emp_id)
                if existing and conflict_strategy == "skip":
                    warnings.append(f"Employee {emp_id} already exists (skipped)")
                else:
                    if conflict_strategy == "generate_new" and existing:
                        emp_dict["id"] = f"{emp_id}_import_{datetime.now().timestamp()}"
                    self._save_entity(Employee, emp_dict)
                    entities_imported["employees"] += 1

            # Import rosters
            for roster_dict in data.get("rosters", []):
                roster_id = roster_dict.get("id")
                existing = self.db.get_roster(roster_id)
                if existing and conflict_strategy == "skip":
                    warnings.append(f"Roster {roster_id} already exists (skipped)")
                else:
                    if conflict_strategy == "generate_new" and existing:
                        roster_dict["id"] = f"{roster_id}_import_{datetime.now().timestamp()}"
                    self._save_entity(Roster, roster_dict)
                    entities_imported["rosters"] += 1

            # Import forecasts
            forecast_list = []
            for forecast_dict in data.get("forecasts", []):
                forecast_list.append(forecast_dict)
            if forecast_list:
                # Convert dicts to DemandForecast objects
                forecasts = [DemandForecast(**f) for f in forecast_list]
                self.db.add_forecasts(forecasts)
                entities_imported["forecasts"] = len(forecasts)

            logger.info(
                f"Venue data imported: {entities_imported['employees']} employees, "
                f"{entities_imported['rosters']} rosters"
            )

            return ImportResult(
                success=True,
                message="Import completed",
                entities_imported=entities_imported,
                warnings=warnings,
                errors=errors,
            )

        except Exception as e:
            logger.error(f"Import failed: {e}")
            errors.append(str(e))
            return ImportResult(
                success=False,
                message=f"Import failed: {str(e)}",
                entities_imported=entities_imported,
                warnings=warnings,
                errors=errors,
            )

    # ========================================================================
    # Private methods
    # ========================================================================

    def _backup_postgres(self, backup_type: str) -> bytes:
        """Create a PostgreSQL backup using pg_dump."""
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL not set")

        try:
            # Extract connection string
            # Format: postgresql://user:password@host:port/dbname
            cmd = [
                "pg_dump",
                "--no-password",
                "--format=custom",
                "--compress=9",
                DATABASE_URL,
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                timeout=3600,
                check=True,
            )

            return result.stdout

        except subprocess.CalledProcessError as e:
            logger.error(f"pg_dump failed: {e.stderr.decode()}")
            raise

    def _backup_memory(self) -> bytes:
        """Serialize in-memory database to JSON."""
        if not isinstance(self.db, MemoryStore):
            raise ValueError("Database is not MemoryStore")

        # Serialize all data structures
        backup_dict = {
            "venues": [
                v.dict() if hasattr(v, "dict") else asdict(v)
                for v in self.db.list_venues()
            ],
            "employees": [
                e.dict() if hasattr(e, "dict") else asdict(e)
                for e in self.db.list_employees()
            ],
            "rosters": [
                r.dict() if hasattr(r, "dict") else asdict(r)
                for r in self.db.list_rosters()
            ],
            "forecasts": [
                f.dict() if hasattr(f, "dict") else asdict(f)
                for f in self.db.get_forecasts()
            ],
            "backup_timestamp": datetime.now().isoformat(),
        }

        return json.dumps(backup_dict).encode("utf-8")

    def _compress_backup(self, data: bytes, backup_id: str) -> str:
        """Compress backup data with gzip and save to file."""
        backup_file = self.backup_dir / f"{backup_id}.gz"

        with gzip.open(backup_file, "wb") as f:
            f.write(data)

        return str(backup_file)

    def _decompress_backup(self, backup_path: str) -> bytes:
        """Decompress a gzipped backup file."""
        with gzip.open(backup_path, "rb") as f:
            return f.read()

    def _calculate_checksum(self, file_path: str) -> str:
        """Calculate SHA-256 checksum of a file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def _restore_postgres(self, backup_data: bytes) -> int:
        """Restore a PostgreSQL backup using pg_restore."""
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL not set")

        try:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(backup_data)
                tmp_path = tmp.name

            cmd = [
                "pg_restore",
                "--no-password",
                "--clean",
                "--if-exists",
                f"--dbname={DATABASE_URL}",
                tmp_path,
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                timeout=3600,
                check=True,
            )

            os.unlink(tmp_path)

            # Return approximate row count (pg_restore doesn't report this)
            return -1  # Unknown count

        except subprocess.CalledProcessError as e:
            logger.error(f"pg_restore failed: {e.stderr.decode()}")
            raise

    def _restore_memory(self, backup_data: bytes) -> int:
        """Restore from in-memory JSON backup."""
        if not isinstance(self.db, MemoryStore):
            raise ValueError("Database is not MemoryStore")

        backup_dict = json.loads(backup_data.decode("utf-8"))
        row_count = 0

        # Restore venues
        for venue_dict in backup_dict.get("venues", []):
            venue = VenueConfig(**venue_dict)
            self.db.save_venue(venue)
            row_count += 1

        # Restore employees
        for emp_dict in backup_dict.get("employees", []):
            employee = Employee(**emp_dict)
            self.db.save_employee(employee)
            row_count += 1

        # Restore rosters
        for roster_dict in backup_dict.get("rosters", []):
            roster = Roster(**roster_dict)
            self.db.save_roster(roster)
            row_count += 1

        # Restore forecasts
        forecasts = [DemandForecast(**f) for f in backup_dict.get("forecasts", [])]
        if forecasts:
            self.db.add_forecasts(forecasts)
            row_count += len(forecasts)

        return row_count

    def _find_backup_file(self, backup_id: str) -> Optional[BackupMetadata]:
        """Find a backup by ID and load its metadata."""
        backup_file = self.backup_dir / f"{backup_id}.gz"

        if not backup_file.exists():
            return None

        return self._load_metadata(backup_file)

    def _load_metadata(self, backup_file: Path) -> Optional[BackupMetadata]:
        """Load metadata from a backup file."""
        try:
            # Try to load metadata from JSON file
            metadata_file = backup_file.with_suffix(".json")
            if metadata_file.exists():
                with open(metadata_file, "r") as f:
                    data = json.load(f)
                    return BackupMetadata(**data)

            # Reconstruct metadata from file
            return BackupMetadata(
                id=backup_file.stem,
                timestamp=datetime.fromtimestamp(backup_file.stat().st_mtime).isoformat(),
                size_bytes=backup_file.stat().st_size,
                type="full",
                path=str(backup_file),
                checksum=self._calculate_checksum(str(backup_file)),
                compressed=True,
                db_type="postgres" if isinstance(self.db, PostgresStore) else "memory",
            )
        except Exception as e:
            logger.error(f"Failed to load metadata for {backup_file}: {e}")
            return None

    def _enforce_retention(self):
        """Delete old backups to maintain retention policy."""
        backups = self.list_backups()

        if len(backups) > self.retention_count:
            for old_backup in backups[self.retention_count :]:
                logger.info(f"Deleting old backup per retention policy: {old_backup.id}")
                self.delete_backup(old_backup.id)

    def _save_entity(self, entity_class, data_dict):
        """Save an entity to the database."""
        try:
            entity = entity_class(**data_dict)

            if entity_class == VenueConfig:
                self.db.save_venue(entity)
            elif entity_class == Employee:
                self.db.save_employee(entity)
            elif entity_class == Roster:
                self.db.save_roster(entity)
            elif entity_class == Shift:
                self.db.save_shift(entity)
        except Exception as e:
            logger.warning(f"Failed to create {entity_class.__name__}: {e}")


# ============================================================================
# Singleton instance
# ============================================================================

_backup_service_instance: Optional[BackupService] = None


def get_backup_service(
    backup_dir: Optional[str] = None,
    retention_count: int = 10,
) -> BackupService:
    """Get the singleton BackupService instance."""
    global _backup_service_instance
    if _backup_service_instance is None:
        _backup_service_instance = BackupService(
            backup_dir=backup_dir,
            retention_count=retention_count,
        )
    return _backup_service_instance
