"""
HumanForce Workforce Management Integration.

This module provides HTTP client and credentials management for HumanForce API.
HumanForce API documentation: https://api.humanforce.com/
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, AsyncIterator

logger = logging.getLogger(__name__)

HUMANFORCE_BASE_URL_TEMPLATE = "https://{region}.humanforce.com/api/v1"
HUMANFORCE_REQUEST_TIMEOUT = 30
HUMANFORCE_RATE_LIMIT = 600  # requests per minute


class HumanForceAPIError(Exception):
    """
    Exception raised for HumanForce API errors.
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
class HumanForceCredentials:
    """HumanForce API credentials."""
    region: str = "apac"
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    api_key: Optional[str] = None
    access_token: Optional[str] = None
    expires_at: Optional[datetime] = None

    def is_expired(self) -> bool:
        """Check if access token is expired."""
        if not self.expires_at:
            return False
        return datetime.now() >= self.expires_at


class HumanForceClient:
    """
    Async HTTP client for HumanForce API with OAuth 2.0 support,
    API key authentication, rate limiting, and retry with exponential backoff.

    Supports three authentication modes:
    1. OAuth 2.0 client credentials (client_id + client_secret)
    2. API key authentication
    3. Access token (pre-authenticated)
    """

    def __init__(
        self,
        region: str = "apac",
        api_key: Optional[str] = None,
        access_token: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        timeout: int = HUMANFORCE_REQUEST_TIMEOUT,
        rate_limit: int = HUMANFORCE_RATE_LIMIT,
    ):
        """
        Initialize HumanForce API client.

        Args:
            region: HumanForce region (default: "apac" for Australia/Pacific)
            api_key: Long-lived API key (alternative to OAuth)
            access_token: Pre-authenticated access token
            client_id: OAuth 2.0 client ID
            client_secret: OAuth 2.0 client secret
            timeout: Request timeout in seconds
            rate_limit: Rate limit in requests per minute
        """
        self.region = region
        self.base_url = HUMANFORCE_BASE_URL_TEMPLATE.format(region=region)
        self.api_key = api_key
        self.access_token = access_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.timeout = timeout
        self.rate_limit = rate_limit

        # Token expiry for OAuth flow
        self.token_expiry: Optional[datetime] = None

        # Token bucket for rate limiting
        self.request_count = 0
        self.last_reset = datetime.now()

    async def authenticate(self) -> str:
        """
        Ensure authentication and return access token.

        If client credentials are provided, runs OAuth client-credentials flow
        and caches the token. If api_key is set, uses bearer token directly.
        If access_token is already set, uses it as-is.

        Returns:
            Access token string

        Raises:
            HumanForceAPIError: If OAuth flow fails
        """
        # If we have an api_key, derive token from it
        if self.api_key:
            return self.api_key

        # If we have a cached access token that hasn't expired, use it
        if self.access_token and self.token_expiry:
            if datetime.now() < self.token_expiry:
                return self.access_token

        # If we have client credentials, perform OAuth flow
        if self.client_id and self.client_secret:
            return await self._oauth_client_credentials_flow()

        # If we have a pre-set access token, use it
        if self.access_token:
            return self.access_token

        raise HumanForceAPIError(
            "No valid credentials provided. Set api_key, access_token, or client_id + client_secret."
        )

    async def _oauth_client_credentials_flow(self) -> str:
        """
        Obtain OAuth 2.0 access token using client credentials flow.

        Returns:
            Access token string

        Raises:
            HumanForceAPIError: If token request fails
        """
        import httpx

        token_url = f"{self.base_url}/oauth/token"

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    token_url,
                    json={
                        "grant_type": "client_credentials",
                        "client_id": self.client_id,
                        "client_secret": self.client_secret,
                    },
                )

                if response.status_code >= 400:
                    try:
                        response_json = response.json()
                    except Exception:
                        response_json = None

                    raise HumanForceAPIError(
                        f"HumanForce OAuth token request failed: {response.status_code}",
                        status_code=response.status_code,
                        detail=response.text,
                        response_json=response_json,
                    )

                token_data = response.json()
                self.access_token = token_data["access_token"]
                expires_in = token_data.get("expires_in", 3600)
                self.token_expiry = datetime.now() + timedelta(seconds=expires_in - 60)

                logger.debug("HumanForce OAuth token obtained successfully")
                return self.access_token

        except HumanForceAPIError:
            raise
        except Exception as e:
            raise HumanForceAPIError(
                "HumanForce OAuth token request failed",
                detail=str(e),
            )

    def _get_headers(self, token: str) -> Dict[str, str]:
        """Get HTTP headers with authorization token."""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # Bearer token (supports both API key and OAuth access token)
        headers["Authorization"] = f"Bearer {token}"

        return headers

    async def _check_rate_limit(self):
        """Check and enforce token bucket rate limiting (600 req/min)."""
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
            HumanForceAPIError: If all retries fail or non-2xx response
        """
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

                        raise HumanForceAPIError(
                            f"HumanForce API error: {response.status_code}",
                            status_code=response.status_code,
                            detail=response.text,
                            response_json=response_json,
                        )

                    return response.json()

            except HumanForceAPIError:
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
            raise HumanForceAPIError(
                f"HumanForce API request failed after {max_retries + 1} attempts",
                detail=str(last_exception),
            )

    async def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        GET request to HumanForce API.

        Args:
            path: API endpoint path (e.g., "/employees")
            params: Query parameters

        Returns:
            Response JSON data
        """
        await self._check_rate_limit()
        token = await self.authenticate()
        url = f"{self.base_url}{path}"
        headers = self._get_headers(token)
        return await self._retry_with_backoff("GET", url, headers, params=params)

    async def post(
        self,
        path: str,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        POST request to HumanForce API.

        Args:
            path: API endpoint path
            json: Request body

        Returns:
            Response JSON data
        """
        await self._check_rate_limit()
        token = await self.authenticate()
        url = f"{self.base_url}{path}"
        headers = self._get_headers(token)
        return await self._retry_with_backoff("POST", url, headers, json_data=json)

    async def put(
        self,
        path: str,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        PUT request to HumanForce API.

        Args:
            path: API endpoint path
            json: Request body

        Returns:
            Response JSON data
        """
        await self._check_rate_limit()
        token = await self.authenticate()
        url = f"{self.base_url}{path}"
        headers = self._get_headers(token)
        return await self._retry_with_backoff("PUT", url, headers, json_data=json)

    async def paginate(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Retrieve all paginated results from endpoint using cursor-based pagination.

        HumanForce uses cursor-based pagination with ?cursor=...&limit=100
        and next_cursor in response headers or data.

        Args:
            path: API endpoint path
            params: Query parameters

        Yields:
            Individual items across all pages
        """
        if params is None:
            params = {}

        cursor = None
        page_size = 100

        while True:
            query_params = {**params, "limit": page_size}
            if cursor:
                query_params["cursor"] = cursor

            response = await self.get(path, params=query_params)

            # HumanForce returns data in various formats
            items = response.get("data", response.get("results", []))
            if not isinstance(items, list):
                items = [items] if items else []

            for item in items:
                yield item

            # Check for next cursor in response
            cursor = response.get("next_cursor") or response.get("pagination", {}).get("next_cursor")
            if not cursor or not items:
                break
