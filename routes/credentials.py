"""
Credential management routes for API keys and webhook secrets.

Endpoints:
- POST /api/credentials/api-keys — create named API key
- GET /api/credentials/api-keys — list user's API keys
- DELETE /api/credentials/api-keys/{key_id} — revoke key
- POST /api/credentials/api-keys/{key_id}/rotate — rotate key
- POST /api/credentials/webhook-secret/rotate — rotate webhook secret for venue
- GET /api/credentials/webhook-secret/status — show rotation status
- GET /api/credentials/api-keys/{key_id}/usage — usage stats for a key
"""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.middleware.tenant import enforce_venue_manager
from rosteriq.services.credential_manager import credential_manager
from rosteriq.database import get_db

router = APIRouter(prefix="/api/credentials", tags=["credentials"])


# ============================================================================
# Schemas
# ============================================================================

class CreateApiKeyRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    expires_in_days: Optional[int] = Field(None, ge=1, le=3650)


class ApiKeyResponse(BaseModel):
    key: str
    key_id: str
    name: str
    created_at: str
    expires_at: Optional[str]


class ApiKeyMetadataResponse(BaseModel):
    id: str
    name: str
    created_at: str
    last_used_at: Optional[str]
    expires_at: Optional[str]
    is_active: bool
    usage_count: int
    revoked_at: Optional[str]


class ApiKeyListResponse(BaseModel):
    keys: list[ApiKeyMetadataResponse]


class RotateApiKeyResponse(BaseModel):
    new_key: str
    new_key_id: str
    old_key_id: str


class WebhookSecretRotateResponse(BaseModel):
    new_secret: str
    grace_expires_at: str
    message: str


class WebhookSecretStatusResponse(BaseModel):
    venue_id: str
    has_active_secret: bool
    has_grace_period_secret: bool
    grace_expires_at: Optional[str]
    rotated_at: Optional[str]


class ApiKeyUsageResponse(BaseModel):
    key_id: str
    usage_count: int
    created_at: str
    last_used_at: Optional[str]
    expires_at: Optional[str]
    is_active: bool
    suspicious_flags: int


# ============================================================================
# API Key Routes
# ============================================================================

@router.post("/api-keys", response_model=ApiKeyResponse)
async def create_api_key(
    request: CreateApiKeyRequest,
    current_user: UserContext = Depends(get_current_user)
):
    """
    Create a new named API key for the current user.

    - max 5 keys per user
    - optional expiration
    - returns plaintext key once (never shown again)
    """
    try:
        plaintext_key, key_id = credential_manager.create_api_key(
            user_id=current_user.user_id,
            name=request.name,
            expires_in_days=request.expires_in_days
        )

        # Get the record to return timestamps
        record = credential_manager.db.get_api_key_record(key_id)

        return ApiKeyResponse(
            key=plaintext_key,
            key_id=key_id,
            name=request.name,
            created_at=record["created_at"].isoformat() if hasattr(record["created_at"], "isoformat") else str(record["created_at"]),
            expires_at=record.get("expires_at").isoformat() if record.get("expires_at") and hasattr(record.get("expires_at"), "isoformat") else str(record.get("expires_at")) if record.get("expires_at") else None
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create API key: {str(e)}")


@router.get("/api-keys", response_model=ApiKeyListResponse)
async def list_api_keys(
    current_user: UserContext = Depends(get_current_user)
):
    """
    List all API keys for the current user.

    Returns metadata only (never the actual key).
    """
    try:
        keys = credential_manager.list_api_keys(current_user.user_id)

        return ApiKeyListResponse(keys=[
            ApiKeyMetadataResponse(
                id=k["id"],
                name=k["name"],
                created_at=k["created_at"].isoformat() if hasattr(k["created_at"], "isoformat") else str(k["created_at"]),
                last_used_at=k.get("last_used_at").isoformat() if k.get("last_used_at") and hasattr(k.get("last_used_at"), "isoformat") else str(k.get("last_used_at")) if k.get("last_used_at") else None,
                expires_at=k.get("expires_at").isoformat() if k.get("expires_at") and hasattr(k.get("expires_at"), "isoformat") else str(k.get("expires_at")) if k.get("expires_at") else None,
                is_active=k["is_active"],
                usage_count=k["usage_count"],
                revoked_at=k.get("revoked_at").isoformat() if k.get("revoked_at") and hasattr(k.get("revoked_at"), "isoformat") else str(k.get("revoked_at")) if k.get("revoked_at") else None
            )
            for k in keys
        ])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list API keys: {str(e)}")


@router.delete("/api-keys/{key_id}", status_code=204)
async def revoke_api_key(
    key_id: str,
    current_user: UserContext = Depends(get_current_user)
):
    """
    Revoke an API key.

    Once revoked, the key cannot be used for authentication.
    """
    try:
        success = credential_manager.revoke_api_key(
            user_id=current_user.user_id,
            key_id=key_id
        )

        if not success:
            raise HTTPException(status_code=404, detail="API key not found")

        return None
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to revoke API key: {str(e)}")


@router.post("/api-keys/{key_id}/rotate", response_model=RotateApiKeyResponse)
async def rotate_api_key(
    key_id: str,
    current_user: UserContext = Depends(get_current_user)
):
    """
    Rotate an API key (revoke old, create new with same name).

    The old key is immediately revoked. A new key is created with the same
    name and expiration settings.
    """
    try:
        new_key, new_key_id = credential_manager.rotate_api_key(
            user_id=current_user.user_id,
            key_id=key_id
        )

        return RotateApiKeyResponse(
            new_key=new_key,
            new_key_id=new_key_id,
            old_key_id=key_id
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to rotate API key: {str(e)}")


@router.get("/api-keys/{key_id}/usage", response_model=ApiKeyUsageResponse)
async def get_api_key_usage(
    key_id: str,
    current_user: UserContext = Depends(get_current_user)
):
    """
    Get usage statistics for an API key.

    Returns: usage count, last used timestamp, suspicious flags, etc.
    """
    try:
        usage = credential_manager.get_key_usage_stats(
            user_id=current_user.user_id,
            key_id=key_id
        )

        if not usage:
            raise HTTPException(status_code=404, detail="API key not found")

        return ApiKeyUsageResponse(
            key_id=usage["key_id"],
            usage_count=usage["usage_count"],
            created_at=usage["created_at"].isoformat() if hasattr(usage["created_at"], "isoformat") else str(usage["created_at"]),
            last_used_at=usage.get("last_used_at").isoformat() if usage.get("last_used_at") and hasattr(usage.get("last_used_at"), "isoformat") else str(usage.get("last_used_at")) if usage.get("last_used_at") else None,
            expires_at=usage.get("expires_at").isoformat() if usage.get("expires_at") and hasattr(usage.get("expires_at"), "isoformat") else str(usage.get("expires_at")) if usage.get("expires_at") else None,
            is_active=usage["is_active"],
            suspicious_flags=usage["suspicious_flags"]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get key usage: {str(e)}")


# ============================================================================
# Webhook Secret Rotation Routes
# ============================================================================

@router.post("/webhook-secret/rotate", response_model=WebhookSecretRotateResponse)
async def rotate_webhook_secret(
    venue_id: str,
    current_user: UserContext = Depends(get_current_user)
):
    """
    Rotate webhook secret for a venue.

    - Old secret remains valid during 24-hour grace period
    - New secret takes effect immediately
    - Both secrets accepted during grace period
    """
    # Rotating a venue's webhook signing secret is a manager action (membership
    # AND role). Before the try (broad except would otherwise mask the 403).
    enforce_venue_manager(venue_id)
    try:
        # Check authorization: user must have access to this venue
        db = get_db()
        user = db.get_user_by_id(current_user.user_id)

        if not user or venue_id not in user.get("venue_ids", []):
            raise HTTPException(status_code=403, detail="Not authorized for this venue")

        new_secret, old_secret, grace_expires = credential_manager.rotate_webhook_secret(
            venue_id=venue_id
        )

        return WebhookSecretRotateResponse(
            new_secret=new_secret,
            grace_expires_at=grace_expires.isoformat(),
            message=f"Webhook secret rotated. Old secret valid until {grace_expires.isoformat()}"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to rotate webhook secret: {str(e)}")


@router.get("/webhook-secret/status", response_model=WebhookSecretStatusResponse)
async def get_webhook_secret_status(
    venue_id: str,
    current_user: UserContext = Depends(get_current_user)
):
    """
    Get webhook secret rotation status for a venue.

    Shows:
    - Whether active secret exists
    - Whether in grace period
    - Grace period expiration time
    """
    try:
        # Check authorization
        db = get_db()
        user = db.get_user_by_id(current_user.user_id)

        if not user or venue_id not in user.get("venue_ids", []):
            raise HTTPException(status_code=403, detail="Not authorized for this venue")

        secrets_list = db.get_webhook_secrets(venue_id)

        has_active = False
        has_grace = False
        grace_expires = None
        rotated_at = None

        for secret in secrets_list:
            if secret.get("is_active"):
                has_active = True
                rotated_at = secret.get("rotated_at")
            if secret.get("grace_expires_at"):
                has_grace = True
                grace_expires = secret.get("grace_expires_at")

        return WebhookSecretStatusResponse(
            venue_id=venue_id,
            has_active_secret=has_active,
            has_grace_period_secret=has_grace,
            grace_expires_at=grace_expires.isoformat() if grace_expires and hasattr(grace_expires, "isoformat") else str(grace_expires) if grace_expires else None,
            rotated_at=rotated_at.isoformat() if rotated_at and hasattr(rotated_at, "isoformat") else str(rotated_at) if rotated_at else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get webhook secret status: {str(e)}")
