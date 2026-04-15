"""
Deputy Workforce Management Integration.

This module provides HTTP client and credentials management for Deputy API.
Deputy API documentation: https://www.deputy.com/api/v1/
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

DEPUTY_BASE_URL_TEMPLATE = "https://{subdomain}.deputy.com/api/v1"
DEPUTY_REQUEST_TIMEOUT = 30
DEPUTY_RATE_LIMIT = 200  # requests per minute


class DeputyAPIError(Exception):
    """
    Exception raised for Deputy API errors.
    """
    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        detail: Optional[str] = None,
        response_json: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.detail = detail
        self.response_json = response_json


@dataclass
class DeputyCredentials:
    """Deputy API credentials."""
    subdomain: str
    access_token: Optional[str] = None
    permanent_token: Optional[str] = None
    refresh_token: Optional[str] = None
    expires_at: Optional[datetime] = None

    def is_expired(self) -> bool:
        """Check if access token is expired."""
        if not self.expires_at:
            return False
        return datetime.now() >= self.expires_at


class DeputyClient:
    """
    Async HTTP client for Deputy API with OAuth 2.0 support,
    rate limiting, and retry with exponential backoff.

    Supports both OAuth 2.0 bearer token and permanent token authentication.
    """

    def __init__(
        self,
        subdomain: str,
        access_token: Optional[str] = None,
        permanent_token: Optional[str] = None,
        timeout: int = DEPUTY_REQUEST_TIMEOUT,
        rate_limit: int = DEPUTY_RATE_LIMIT,
    ):
        """
        Initialize Deputy API client.

        Args:
            subdomain: Deputy subdomain (e.g., "mycompany")
            access_token: OAuth 2.0 access token
            permanent_token: Permanent token (alternative to OAuth)
            timeout: Request timeout in seconds
            rate_limit: Rate limit in requests per minute
        """
        self.subdomain = subdomain
        self.base_url = DEPUTY_BASE_URL_TEMPLATE.format(subdomain=subdomain)
        self.access_token = access_token
        self.permanent_token = permanent_token
        self.timeout = timeout
        self.rate_limit = rate_limit

        # Token bucket for rate limiting
        self.request_count = 0
        self.last_reset = datetime.now()

    def _get_headers(self) -> Dict[str, str]:
        """Get HTTP headers with authorization token."""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # Use permanent token if available, otherwise access token
        token = self.permanent_token or self.access_token
        if token:
            headers["Authorization"] = f"Bearer {token}"

        return headers

    async def _check_rate_limit(self):
        """Check and enforce token bucket rate limiting (200 req/min)."""
        now = datetime.now()
        if (now - self.last_reset).total_seconds() >= 60:
            self.request_count = 0
            self.last_reset = now

        if self.request_count >= self.rate_limit:
            sleep_time = 60 - (now - self.last_reset).total_seconds()
            if sleep_time > 0:
                logger.debug(f"Rate limit reached, sleeping {sleep_time:.1f}s")
                await asyncio.sleep(sleep_time)
                self.request_count = 0
                self.last_reset = datetime.now()

        self.request_count += 1

    async def _retry_with_backoff(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
    ) -> Dict[str, Any]:
        """
        Execute request with exponential backoff retry (3 retries).

        Args:
            method: HTTP method
            url: Request URL
            headers: Request headers
            json_data: Request body
            params: Query parameters
            max_retries: Maximum number of retries

        Returns:
            Response JSON data

        Raises:
            DeputyAPIError: If all retries fail or non-2xx response
        """
        # Import httpx inside method so module can be imported without httpx
        import httpx

        last_exception = None

        for attempt in range(max_retries + 1):
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.request(
                        method,
                        url,
                        headers=headers,
                        json=json_data,
                        params=params,
                    )

                    # Handle rate limit (429) with backoff
                    if response.status_code == 429:
                        if attempt < max_retries:
                            backoff = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                            logger.warning(
                                f"Rate limited (429), retrying in {backoff}s (attempt {attempt + 1})"
                            )
                            await asyncio.sleep(backoff)
                            continue

                    # Raise for other non-2xx responses
                    if response.status_code >= 400:
                        try:
                            response_json = response.json()
                        except Exception:
                            response_json = None

                        raise DeputyAPIError(
                            f"Deputy API error: {response.status_code}",
                            status_code=response.status_code,
                            detail=response.text,
                            response_json=response_json,
                        )

                    return response.json()

            except DeputyAPIError:
                raise
            except httpx.HTTPError as e:
                last_exception = e
                if attempt < max_retries:
                    backoff = 2 ** attempt
                    logger.warning(
                        f"Request failed (attempt {attempt + 1}), retrying in {backoff}s: {e}"
                    )
                    await asyncio.sleep(backoff)

        if last_exception:
            raise DeputyAPIError(
                f"Deputy API request failed after {max_retries + 1} attempts",
                detail=str(last_exception),
            )

    async def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        GET request to Deputy API.

        Args:
            path: API endpoint path (e.g., "/employee")
            params: Query parameters

        Returns:
            Response JSON data
        """
        await self._check_rate_limit()
        url = f"{self.base_url}{path}"
        headers = self._get_headers()
        return await self._retry_with_backoff("GET", url, headers, params=params)

    async def post(
        self,
        path: str,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        POST request to Deputy API.

        Args:
            path: API endpoint path
            json: Request body

        Returns:
            Response JSON data
        """
        await self._check_rate_limit()
        url = f"{self.base_url}{path}"
        headers = self._get_headers()
        return await self._retry_with_backoff("POST", url, headers, json_data=json)

    async def put(
        self,
        path: str,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        PUT request to Deputy API.

        Args:
            path: API endpoint path
            json: Request body

        Returns:
            Response JSON data
        """
        await self._check_rate_limit()
        url = f"{self.base_url}{path}"
        headers = self._get_headers()
        return await self._retry_with_backoff("PUT", url, headers, json_data=json)

    async def paginate(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> list:
        """
        Retrieve all paginated results from endpoint.

        Deputy pagination uses offset/limit. This method yields individual
        items across all pages.

        Args:
            path: API endpoint path
            params: Query parameters

        Returns:
            List of all items across all pages
        """
        if params is None:
            params = {}

        items = []
        offset = 0
        page_size = 100

        while True:
            query_params = {**params, "offset": offset, "limit": page_size}
            response = await self.get(path, params=query_params)

            # Deputy returns data in various formats; handle common patterns
            page_items = response.get("data", response.get("results", []))
            if not isinstance(page_items, list):
                # Single item response
                page_items = [page_items] if page_items else []

            if not page_items:
                break

            items.extend(page_items)

            # If we got fewer items than requested, we've reached the end
            if len(page_items) < page_size:
                break

            offset += page_size

        return items


async def refresh_token(
    client_id: str,
    client_secret: str,
    refresh_token: str,
    subdomain: str,
) -> DeputyCredentials:
    """
    Refresh Deputy OAuth 2.0 access token using refresh token.

    Args:
        client_id: OAuth 2.0 client ID
        client_secret: OAuth 2.0 client secret
        refresh_token: Refresh token from previous auth
        subdomain: Deputy subdomain

    Returns:
        Updated DeputyCredentials with new access token

    Raises:
        DeputyAPIError: If token refresh fails
    """
    import httpx

    url = f"{DEPUTY_BASE_URL_TEMPLATE.format(subdomain=subdomain)}/oauth/token"
    data = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
    }

    try:
        async with httpx.AsyncClient(timeout=DEPUTY_REQUEST_TIMEOUT) as client:
            response = await client.post(url, data=data)
            response.raise_for_status()

            token_data = response.json()
            new_access_token = token_data.get("access_token")
            expires_in = token_data.get("expires_in", 3600)
            expires_at = datetime.now() + timedelta(seconds=expires_in - 60)

            logger.debug("Deputy OAuth token refreshed successfully")

            return DeputyCredentials(
                subdomain=subdomain,
                access_token=new_access_token,
                refresh_token=token_data.get("refresh_token", refresh_token),
                expires_at=expires_at,
            )

    except Exception as e:
        raise DeputyAPIError(
            "Failed to refresh Deputy OAuth token",
            detail=str(e),
        )
