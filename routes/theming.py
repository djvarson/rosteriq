"""
API routes for white-label theming and branding customization.

Endpoints:
  GET    /api/theme/{venue_id} — get current theme config
  PUT    /api/theme/{venue_id} — update theme config
  POST   /api/theme/{venue_id}/logo — upload logo as base64
  GET    /api/theme/{venue_id}/css — get CSS variables as text/css
  GET    /api/theme/{venue_id}/preview — get HTML preview of themed header
  DELETE /api/theme/{venue_id} — reset to defaults

All endpoints require JWT auth and venue authorization.
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends, Header, status
from fastapi.responses import PlainTextResponse, HTMLResponse
from pydantic import BaseModel, Field

from rosteriq.services.theming import ThemeService, ThemeConfig
from rosteriq.services.auth import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/theme", tags=["theming"])


# ============================================================================
# Request/Response models
# ============================================================================


class ThemeConfigRequest(BaseModel):
    """Request body for theme updates."""

    company_name: str = Field(..., min_length=1, max_length=100)
    primary_color: Optional[str] = None
    secondary_color: Optional[str] = None
    accent_color: Optional[str] = None
    text_color: Optional[str] = None
    header_bg: Optional[str] = None
    font_family: Optional[str] = None
    logo_url: Optional[str] = None
    favicon_url: Optional[str] = None
    email_header_color: Optional[str] = None
    email_footer_text: Optional[str] = Field(None, max_length=200)


class LogoUploadRequest(BaseModel):
    """Request body for logo upload (base64)."""

    data: str = Field(..., description="Base64-encoded image data")


class ThemeConfigResponse(BaseModel):
    """Response body for theme config."""

    venue_id: str
    company_name: str
    primary_color: str
    secondary_color: str
    accent_color: str
    text_color: str
    header_bg: str
    font_family: str
    logo_url: Optional[str]
    favicon_url: Optional[str]
    email_header_color: str
    email_footer_text: str


# ============================================================================
# Helpers
# ============================================================================


def _check_venue_auth(venue_id: str, current_user: dict) -> None:
    """Verify user has access to the venue."""
    # Check if user is admin or belongs to the venue
    if current_user.get("role") != "admin" and current_user.get("venue_id") != venue_id:
        raise HTTPException(status_code=403, detail="Not authorized for this venue")


def _get_theme_service() -> ThemeService:
    """Get theme service instance."""
    return ThemeService()


# ============================================================================
# Endpoints
# ============================================================================


@router.get("/{venue_id}", response_model=ThemeConfigResponse)
async def get_theme(
    venue_id: str,
    current_user: dict = Depends(get_current_user),
    theme_svc: ThemeService = Depends(_get_theme_service),
):
    """Get theme configuration for a venue."""
    _check_venue_auth(venue_id, current_user)

    config = theme_svc.get_theme(venue_id)
    return ThemeConfigResponse(**config.to_dict())


@router.put("/{venue_id}", response_model=ThemeConfigResponse)
async def update_theme(
    venue_id: str,
    payload: ThemeConfigRequest,
    current_user: dict = Depends(get_current_user),
    theme_svc: ThemeService = Depends(_get_theme_service),
):
    """
    Update theme configuration for a venue.

    Validates colors and other fields before saving.
    """
    _check_venue_auth(venue_id, current_user)

    # Get existing config (or defaults)
    existing = theme_svc.get_theme(venue_id)

    # Merge with updates
    config = ThemeConfig(
        venue_id=venue_id,
        company_name=payload.company_name or existing.company_name,
        primary_color=payload.primary_color or existing.primary_color,
        secondary_color=payload.secondary_color or existing.secondary_color,
        accent_color=payload.accent_color or existing.accent_color,
        text_color=payload.text_color or existing.text_color,
        header_bg=payload.header_bg or existing.header_bg,
        font_family=payload.font_family or existing.font_family,
        logo_url=payload.logo_url if payload.logo_url is not None else existing.logo_url,
        favicon_url=payload.favicon_url if payload.favicon_url is not None else existing.favicon_url,
        email_header_color=payload.email_header_color or existing.email_header_color,
        email_footer_text=payload.email_footer_text or existing.email_footer_text,
    )

    try:
        theme_svc.set_theme(venue_id, config)
        return ThemeConfigResponse(**config.to_dict())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{venue_id}/logo")
async def upload_logo(
    venue_id: str,
    payload: LogoUploadRequest,
    current_user: dict = Depends(get_current_user),
    theme_svc: ThemeService = Depends(_get_theme_service),
):
    """
    Upload a logo as base64-encoded image data.

    Validates format and size (max 500KB).
    Stores as data: URL for immediate use.
    """
    _check_venue_auth(venue_id, current_user)

    # Validate logo
    if not theme_svc.validate_logo(payload.data):
        raise HTTPException(status_code=400, detail="Invalid logo data or format")

    # Get current config
    config = theme_svc.get_theme(venue_id)

    # Create data: URL
    # Detect format from magic bytes
    import base64

    data = base64.b64decode(payload.data)
    if data.startswith(b"\x89PNG"):
        mime = "image/png"
    elif data.startswith(b"\xFF\xD8\xFF"):
        mime = "image/jpeg"
    elif data.startswith(b"<"):
        mime = "image/svg+xml"
    else:
        mime = "image/png"  # Default

    logo_url = f"data:{mime};base64,{payload.data}"

    # Update config
    config.logo_url = logo_url
    theme_svc.set_theme(venue_id, config)

    return {
        "status": "success",
        "logo_url": logo_url,
        "size_kb": len(data) / 1024,
    }


@router.get("/{venue_id}/css")
async def get_theme_css(
    venue_id: str,
    current_user: dict = Depends(get_current_user),
    theme_svc: ThemeService = Depends(_get_theme_service),
):
    """
    Get CSS custom properties for a theme.

    Returns as text/css so it can be directly linked or embedded.
    """
    _check_venue_auth(venue_id, current_user)

    css = theme_svc.generate_css_variables(venue_id)
    return PlainTextResponse(content=css, media_type="text/css")


@router.get("/{venue_id}/preview")
async def preview_theme(
    venue_id: str,
    current_user: dict = Depends(get_current_user),
    theme_svc: ThemeService = Depends(_get_theme_service),
):
    """
    Get HTML preview of the themed header.

    Useful for UI previews before committing changes.
    """
    _check_venue_auth(venue_id, current_user)

    config = theme_svc.get_theme(venue_id)
    html = theme_svc.preview_theme(config)
    return HTMLResponse(content=html)


@router.delete("/{venue_id}", status_code=204)
async def reset_theme(
    venue_id: str,
    current_user: dict = Depends(get_current_user),
    theme_svc: ThemeService = Depends(_get_theme_service),
):
    """Reset a venue's theme to defaults."""
    _check_venue_auth(venue_id, current_user)

    theme_svc.delete_theme(venue_id)
    return None
