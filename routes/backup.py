"""
Backup and data export API routes for RosterIQ.

Endpoints:
- POST /api/v1/admin/backup — create backup (admin only)
- GET /api/v1/admin/backups — list backups
- POST /api/v1/admin/restore/{backup_id} — restore from backup (admin only)
- DELETE /api/v1/admin/backups/{backup_id} — delete a backup
- POST /api/v1/admin/backup/{backup_id}/upload — upload to S3
- GET /api/v1/venues/{venue_id}/export — export venue data as JSON
- POST /api/v1/venues/import — import venue data from JSON
"""

import logging
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel

from rosteriq.database import get_db
from rosteriq.services.backup import (
    get_backup_service, BackupService, BackupMetadata,
    RestoreResult, ImportResult,
)
from rosteriq.middleware.auth import require_owner, get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["backup"])


# ============================================================================
# Request/Response Models
# ============================================================================

class CreateBackupRequest(BaseModel):
    """Request to create a backup."""
    backup_type: str = "full"  # "full" or "incremental"


class BackupMetadataResponse(BaseModel):
    """Response with backup metadata."""
    id: str
    timestamp: str
    size_bytes: int
    type: str
    checksum: str
    compressed: bool
    db_type: str


class RestoreBackupRequest(BaseModel):
    """Request to restore a backup."""
    confirmed: bool = False  # Require explicit confirmation


class RestoreBackupResponse(BaseModel):
    """Response from restore operation."""
    success: bool
    backup_id: str
    message: str
    rows_restored: Optional[int] = None
    error: Optional[str] = None


class UploadBackupRequest(BaseModel):
    """Request to upload backup to S3."""
    bucket: str
    presigned_url: str


class UploadBackupResponse(BaseModel):
    """Response from upload operation."""
    success: bool
    backup_id: str
    bucket: str
    message: str


class ExportVenueRequest(BaseModel):
    """Request to export venue data."""
    venue_id: str


class VenueExportResponse(BaseModel):
    """Response with exported venue data."""
    venue: Dict[str, Any]
    employees: List[Dict[str, Any]]
    rosters: List[Dict[str, Any]]
    forecasts: List[Dict[str, Any]]
    export_timestamp: str
    export_version: str


class ImportVenueRequest(BaseModel):
    """Request to import venue data."""
    data: Dict[str, Any]
    conflict_strategy: str = "skip"  # "skip", "overwrite", "generate_new"


class ImportVenueResponse(BaseModel):
    """Response from import operation."""
    success: bool
    message: str
    entities_imported: Dict[str, int]
    warnings: List[str] = []
    errors: List[str] = []


# ============================================================================
# Backup endpoints (admin only)
# ============================================================================

@router.post("/admin/backup", response_model=BackupMetadataResponse)
async def create_backup(
    request: CreateBackupRequest,
    current_user=Depends(require_owner),
):
    """
    Create a backup of the database.

    Requires admin role.

    Args:
        request: CreateBackupRequest with backup_type

    Returns:
        BackupMetadataResponse with backup details
    """
    try:
        backup_service = get_backup_service()
        metadata = backup_service.create_backup(backup_type=request.backup_type)

        return BackupMetadataResponse(
            id=metadata.id,
            timestamp=metadata.timestamp,
            size_bytes=metadata.size_bytes,
            type=metadata.type,
            checksum=metadata.checksum,
            compressed=metadata.compressed,
            db_type=metadata.db_type,
        )

    except Exception as e:
        logger.error(f"Backup creation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/admin/backups", response_model=List[BackupMetadataResponse])
async def list_backups(
    current_user=Depends(require_owner),
):
    """
    List all available backups.

    Requires admin role.

    Returns:
        List of BackupMetadataResponse objects
    """
    try:
        backup_service = get_backup_service()
        backups = backup_service.list_backups()

        return [
            BackupMetadataResponse(
                id=b.id,
                timestamp=b.timestamp,
                size_bytes=b.size_bytes,
                type=b.type,
                checksum=b.checksum,
                compressed=b.compressed,
                db_type=b.db_type,
            )
            for b in backups
        ]

    except Exception as e:
        logger.error(f"List backups failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/admin/restore/{backup_id}", response_model=RestoreBackupResponse)
async def restore_backup(
    backup_id: str,
    request: RestoreBackupRequest,
    current_user=Depends(require_owner),
):
    """
    Restore the database from a backup.

    Requires admin role and explicit confirmation.

    Args:
        backup_id: ID of the backup to restore
        request: RestoreBackupRequest with confirmation flag

    Returns:
        RestoreBackupResponse with result details
    """
    if not request.confirmed:
        raise HTTPException(
            status_code=400,
            detail="Restore must be explicitly confirmed",
        )

    try:
        backup_service = get_backup_service()
        result = backup_service.restore_backup(backup_id)

        return RestoreBackupResponse(
            success=result.success,
            backup_id=result.backup_id,
            message=result.message,
            rows_restored=result.rows_restored,
            error=result.error,
        )

    except Exception as e:
        logger.error(f"Restore failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/admin/backups/{backup_id}")
async def delete_backup(
    backup_id: str,
    current_user=Depends(require_owner),
):
    """
    Delete a backup.

    Requires admin role.

    Args:
        backup_id: ID of the backup to delete

    Returns:
        Response with deletion status
    """
    try:
        backup_service = get_backup_service()
        deleted = backup_service.delete_backup(backup_id)

        if not deleted:
            raise HTTPException(status_code=404, detail="Backup not found")

        return {"success": True, "message": f"Backup {backup_id} deleted"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete backup failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/admin/backup/{backup_id}/upload", response_model=UploadBackupResponse)
async def upload_backup_to_s3(
    backup_id: str,
    request: UploadBackupRequest,
    current_user=Depends(require_owner),
):
    """
    Upload a backup to S3 using a presigned URL.

    Requires admin role.

    Args:
        backup_id: ID of the backup to upload
        request: UploadBackupRequest with bucket and presigned_url

    Returns:
        UploadBackupResponse with upload status
    """
    try:
        backup_service = get_backup_service()
        success = backup_service.upload_to_s3(
            backup_id=backup_id,
            bucket=request.bucket,
            presigned_url=request.presigned_url,
        )

        if not success:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to upload backup {backup_id}",
            )

        return UploadBackupResponse(
            success=True,
            backup_id=backup_id,
            bucket=request.bucket,
            message="Backup uploaded to S3",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"S3 upload failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Venue data export/import endpoints
# ============================================================================

@router.get("/venues/{venue_id}/export", response_model=VenueExportResponse)
async def export_venue_data(
    venue_id: str,
    current_user=Depends(get_current_user),
):
    """
    Export all data for a venue as portable JSON.

    Includes: venue config, employees, rosters, shifts, forecasts.

    Args:
        venue_id: ID of the venue to export
        current_user: Current authenticated user

    Returns:
        VenueExportResponse with all venue data

    Raises:
        404: If venue not found
    """
    # Verify user has access to this venue
    db = get_db()
    venue = db.get_venue(venue_id)
    if not venue:
        raise HTTPException(status_code=404, detail="Venue not found")

    try:
        backup_service = get_backup_service()
        export_data = backup_service.export_venue_data(venue_id)

        return VenueExportResponse(
            venue=export_data["venue"],
            employees=export_data["employees"],
            rosters=export_data["rosters"],
            forecasts=export_data["forecasts"],
            export_timestamp=export_data["export_timestamp"],
            export_version=export_data["export_version"],
        )

    except ValueError as e:
        logger.error(f"Export failed: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Export failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/venues/import", response_model=ImportVenueResponse)
async def import_venue_data(
    request: ImportVenueRequest,
    current_user=Depends(require_owner),
):
    """
    Import venue data from JSON export.

    Requires admin role. Supports handling ID conflicts via conflict_strategy:
    - "skip": Don't import if ID exists (default)
    - "overwrite": Replace existing entity
    - "generate_new": Generate new IDs for conflicts

    Args:
        request: ImportVenueRequest with data and conflict_strategy
        current_user: Current authenticated user (admin only)

    Returns:
        ImportVenueResponse with import results
    """
    try:
        backup_service = get_backup_service()
        result = backup_service.import_venue_data(
            data=request.data,
            conflict_strategy=request.conflict_strategy,
        )

        return ImportVenueResponse(
            success=result.success,
            message=result.message,
            entities_imported=result.entities_imported or {},
            warnings=result.warnings or [],
            errors=result.errors or [],
        )

    except Exception as e:
        logger.error(f"Import failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
