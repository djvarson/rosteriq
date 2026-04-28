"""
Data feed configuration routes.

Endpoints for managing which data feeds are enabled, API keys, poll intervals,
and custom parameters per venue.

All endpoints return masked API keys (last 4 chars only) for security.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from typing import Optional

from rosteriq.database import get_db
from rosteriq.services.feed_config import feed_config_service, AVAILABLE_FEEDS
from rosteriq.middleware.auth import get_current_user, UserContext

router = APIRouter(prefix="/api/feeds", tags=["feeds"])


# ============================================================================
# Schemas
# ============================================================================

class FeedConfigRequest(BaseModel):
    """Request to set feed configuration."""
    enabled: Optional[bool] = True
    api_key: Optional[str] = None
    poll_interval_minutes: Optional[int] = None
    custom_params: Optional[dict] = {}


class FeedConfigResponse(BaseModel):
    """Response with feed configuration (API key masked)."""
    venue_id: str
    feed_name: str
    enabled: bool
    api_key_masked: Optional[str] = None
    poll_interval_minutes: int
    last_updated_at: Optional[str] = None
    last_tested_at: Optional[str] = None
    last_test_status: Optional[str] = None
    custom_params: Optional[dict] = {}


class FeedTestResponse(BaseModel):
    """Response from feed connectivity test."""
    success: bool
    message: str
    status_code: int
    latency_ms: float
    feed_name: str


class AvailableFeedResponse(BaseModel):
    """Info about an available feed."""
    feed_name: str
    display_name: str
    description: str
    requires_api_key: bool
    default_poll_interval_minutes: int
    config_params: list[dict]


# ============================================================================
# Routes
# ============================================================================

@router.get("/available")
async def list_available_feeds(
    current_user: UserContext = Depends(get_current_user),
) -> dict[str, AvailableFeedResponse]:
    """
    List all available data feed types with descriptions and configuration requirements.

    Returns a dict mapping feed name to feed details (display name, description, etc).
    """
    try:
        feeds = feed_config_service.get_available_feeds()
        result = {}
        for feed_name, info in feeds.items():
            result[feed_name] = AvailableFeedResponse(
                feed_name=feed_name,
                display_name=info["display_name"],
                description=info["description"],
                requires_api_key=info.get("requires_api_key", False),
                default_poll_interval_minutes=info["default_poll_interval_minutes"],
                config_params=info.get("config_params", []),
            )
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list feeds: {str(e)}",
        )


@router.get("/config/{venue_id}")
async def list_feed_configs(
    venue_id: str,
    current_user: UserContext = Depends(get_current_user),
) -> list[FeedConfigResponse]:
    """
    List all feed configurations for a venue.

    Returns list of feed configs with masked API keys and status.
    """
    try:
        configs = await feed_config_service.list_all_configs(venue_id)

        result = []
        for cfg in configs:
            result.append(FeedConfigResponse(
                venue_id=cfg.get("venue_id"),
                feed_name=cfg.get("feed_name"),
                enabled=cfg.get("enabled", False),
                api_key_masked=cfg.get("api_key_masked"),
                poll_interval_minutes=cfg.get("poll_interval_minutes", 30),
                last_updated_at=str(cfg.get("last_updated_at")) if cfg.get("last_updated_at") else None,
                last_tested_at=str(cfg.get("last_tested_at")) if cfg.get("last_tested_at") else None,
                last_test_status=cfg.get("last_test_status"),
                custom_params={k: v for k, v in cfg.items()
                              if k not in ("venue_id", "feed_name", "enabled", "api_key_masked",
                                         "poll_interval_minutes", "last_updated_at",
                                         "last_tested_at", "last_test_status")},
            ))
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list feed configs: {str(e)}",
        )


@router.put("/config/{venue_id}/{feed_name}")
async def update_feed_config(
    venue_id: str,
    feed_name: str,
    request: FeedConfigRequest,
    current_user: UserContext = Depends(get_current_user),
) -> FeedConfigResponse:
    """
    Update configuration for a specific feed at a venue.

    Request body can include:
    - enabled: bool
    - api_key: string (stored encrypted)
    - poll_interval_minutes: int
    - custom_params: dict of feed-specific settings

    Response includes masked API key (last 4 chars only).
    """
    try:
        config_dict = {}
        if request.enabled is not None:
            config_dict["enabled"] = request.enabled
        if request.api_key:
            config_dict["api_key"] = request.api_key
        if request.poll_interval_minutes is not None:
            config_dict["poll_interval_minutes"] = request.poll_interval_minutes
        if request.custom_params:
            config_dict.update(request.custom_params)

        result = await feed_config_service.set_config(venue_id, feed_name, config_dict)

        return FeedConfigResponse(
            venue_id=result.get("venue_id"),
            feed_name=result.get("feed_name"),
            enabled=result.get("enabled", False),
            api_key_masked=result.get("api_key_masked"),
            poll_interval_minutes=result.get("poll_interval_minutes", 30),
            last_updated_at=str(result.get("last_updated_at")) if result.get("last_updated_at") else None,
            last_tested_at=str(result.get("last_tested_at")) if result.get("last_tested_at") else None,
            last_test_status=result.get("last_test_status"),
            custom_params={k: v for k, v in result.items()
                          if k not in ("venue_id", "feed_name", "enabled", "api_key_masked",
                                     "poll_interval_minutes", "last_updated_at",
                                     "last_tested_at", "last_test_status")},
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update feed config: {str(e)}",
        )


@router.post("/test/{venue_id}/{feed_name}")
async def test_feed_connection(
    venue_id: str,
    feed_name: str,
    current_user: UserContext = Depends(get_current_user),
) -> FeedTestResponse:
    """
    Test connectivity to a specific data feed.

    Attempts to authenticate and reach the feed's API. Updates the feed's
    last_tested_at and last_test_status in the database.

    Returns status (success/failure), message, HTTP status code, and latency.
    """
    try:
        result = await feed_config_service.test_feed(venue_id, feed_name)
        return FeedTestResponse(
            success=result["success"],
            message=result["message"],
            status_code=result["status_code"],
            latency_ms=result["latency_ms"],
            feed_name=result["feed_name"],
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error testing feed: {str(e)}",
        )
