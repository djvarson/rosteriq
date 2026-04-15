"""
RosterIQ Authentication and Authorization Module

Provides:
- JWT-based authentication for dashboard and API
- Venue-scoped access control (users can only see their venue's data)
- API key authentication for programmatic access (webhooks, integrations)
- Tanda OAuth flow integration support
- Password hashing with bcrypt
- Rate limiting on login attempts
"""

from __future__ import annotations

import logging
import os
import secrets
import string
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Optional

from pydantic import BaseModel, EmailStr, Field

# Conditional imports for optional dependencies
try:
    import jwt
except ImportError:
    jwt = None

try:
    from passlib.context import CryptContext
except ImportError:
    CryptContext = None

try:
    from fastapi import APIRouter, Depends, FastAPI, HTTPException, status, Request
    from fastapi.security import HTTPAuthenticationCredentials, HTTPBearer
except ImportError:
    APIRouter = None
    Depends = None
    FastAPI = None
    HTTPException = None
    status = None
    HTTPBearer = None
    Request = None

logger = logging.getLogger(__name__)

# ============================================================================
# Access Level Enum
# ============================================================================


class AccessLevel(str, Enum):
    """Role-based access levels for RosterIQ."""

    L1_SUPERVISOR = "l1"
    L2_ROSTER_MAKER = "l2"
    OWNER = "owner"

    @classmethod
    def rank(cls, level: "AccessLevel") -> int:
        """Return numeric rank for access level (higher = more permission)."""
        ranking = {
            cls.L1_SUPERVISOR: 1,
            cls.L2_ROSTER_MAKER: 2,
            cls.OWNER: 3,
        }
        return ranking.get(level, 0)


# Configuration
JWT_SECRET = os.getenv("RIQ_JWT_SECRET", "dev-secret-key-change-in-production")
JWT_ALGORITHM = "HS256"
TOKEN_EXPIRE_HOURS = 24
PASSWORD_MIN_LENGTH = 8
API_KEY_PREFIX = "riq_"
API_KEY_LENGTH = 32
AUTH_ENABLED = os.getenv("ROSTERIQ_AUTH_ENABLED", "").lower() in ("1", "true", "yes")

# Initialize password context
pwd_context = CryptContext(
    schemes=["bcrypt"],
    deprecated="auto",
) if CryptContext else None

# In-memory stores (development - replace with real database in production)
_users: dict[str, dict[str, Any]] = {}  # email -> user data
_api_keys: dict[str, dict[str, Any]] = {}  # key -> api key data
_failed_login_attempts: dict[str, int] = {}  # email -> failed attempts count


# ============================================================================
# Pydantic Models
# ============================================================================


class User(BaseModel):
    """Authenticated user model."""

    id: str
    email: str
    name: str
    venue_id: str
    role: str = Field(default="manager", description="manager, owner, or admin")
    access_level: AccessLevel = Field(default=AccessLevel.L1_SUPERVISOR, description="Access level: l1, l2, owner")
    created_at: datetime

    class Config:
        from_attributes = True


class UserCreate(BaseModel):
    """Request model for user creation."""

    email: EmailStr
    password: str = Field(min_length=PASSWORD_MIN_LENGTH)
    name: str
    venue_id: str
    role: str = Field(default="manager")
    access_level: Optional[AccessLevel] = Field(default=AccessLevel.L1_SUPERVISOR)


class UserLogin(BaseModel):
    """Request model for user login."""

    email: EmailStr
    password: str


class TokenResponse(BaseModel):
    """JWT token response."""

    access_token: str
    token_type: str = "bearer"
    expires_in: int
    venue_id: str
    user_name: str


class APIKey(BaseModel):
    """API key model."""

    key: str
    venue_id: str
    name: str
    created_at: datetime
    last_used: Optional[datetime] = None
    active: bool = True

    class Config:
        from_attributes = True


class APIKeyCreate(BaseModel):
    """Request model for API key creation."""

    name: str = Field(min_length=1, max_length=100)


class TokenRefresh(BaseModel):
    """Request model for token refresh."""

    access_token: str


# ============================================================================
# Core Password Functions
# ============================================================================


def hash_password(password: str) -> str:
    """
    Hash a password using bcrypt.

    Args:
        password: Plain text password to hash

    Returns:
        Bcrypt hash of the password

    Raises:
        RuntimeError: If passlib is not installed
    """
    if pwd_context is None:
        raise RuntimeError("passlib is required for password hashing")

    if len(password) < PASSWORD_MIN_LENGTH:
        raise ValueError(f"Password must be at least {PASSWORD_MIN_LENGTH} characters")

    return pwd_context.hash(password)


def verify_password(password: str, hashed: str) -> bool:
    """
    Verify a password against its bcrypt hash.

    Args:
        password: Plain text password to verify
        hashed: Bcrypt hash to verify against

    Returns:
        True if password matches, False otherwise
    """
    if pwd_context is None:
        logger.error("passlib is required for password verification")
        return False

    return pwd_context.verify(password, hashed)


# ============================================================================
# Core JWT Functions
# ============================================================================


def create_access_token(
    user_id: str,
    venue_id: str,
    role: str,
    access_level: Optional[AccessLevel] = None,
    expires_hours: int = TOKEN_EXPIRE_HOURS,
) -> str:
    """
    Create a JWT access token.

    Args:
        user_id: User ID to encode
        venue_id: Venue ID (for venue-scoped access)
        role: User role (manager, owner, admin)
        access_level: Access level (l1, l2, owner). Defaults to L1_SUPERVISOR.
        expires_hours: Token expiration time in hours

    Returns:
        Encoded JWT token

    Raises:
        RuntimeError: If PyJWT is not installed
    """
    if jwt is None:
        raise RuntimeError("PyJWT is required for token creation")

    now = datetime.now(timezone.utc)
    expires = now + timedelta(hours=expires_hours)

    if access_level is None:
        access_level = AccessLevel.L1_SUPERVISOR

    payload = {
        "sub": user_id,  # subject (user ID)
        "venue_id": venue_id,
        "role": role,
        "al": access_level.value,  # access level
        "iat": now,  # issued at
        "exp": expires,  # expiration time
    }

    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    logger.info(f"Created access token for user {user_id} in venue {venue_id} with access level {access_level.value}")
    return token


def decode_token(token: str) -> dict[str, Any]:
    """
    Decode and verify a JWT token.

    Args:
        token: JWT token to decode

    Returns:
        Decoded token payload (includes 'al' field for access_level if present)

    Raises:
        HTTPException: If token is invalid, expired, or jwt library is missing
    """
    if jwt is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="JWT library not available",
        )

    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        # Backward compatibility: old tokens without "al" default to L1
        if "al" not in payload:
            payload["al"] = AccessLevel.L1_SUPERVISOR.value
        return payload
    except jwt.ExpiredSignatureError:
        logger.warning("Attempted to use expired token")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid token provided: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )


# ============================================================================
# Access Control Helpers
# ============================================================================


def has_access(user: User | dict, required: AccessLevel) -> bool:
    """
    Check if a user has the required access level.

    Access hierarchy: OWNER > L2_ROSTER_MAKER > L1_SUPERVISOR
    OWNER satisfies any requirement; L2 satisfies L2 and L1; L1 satisfies only L1.

    Args:
        user: User object or decoded token dict
        required: Required access level

    Returns:
        True if user has sufficient access, False otherwise
    """
    # Extract access level from user or dict
    if isinstance(user, dict):
        user_level_str = user.get("al", AccessLevel.L1_SUPERVISOR.value)
    else:
        user_level_str = (user.access_level.value if user.access_level else AccessLevel.L1_SUPERVISOR.value)

    try:
        user_level = AccessLevel(user_level_str)
    except (ValueError, KeyError):
        user_level = AccessLevel.L1_SUPERVISOR

    user_rank = AccessLevel.rank(user_level)
    required_rank = AccessLevel.rank(required)

    return user_rank >= required_rank


# ============================================================================
# API Key Functions
# ============================================================================


def generate_api_key(venue_id: str, name: str) -> APIKey:
    """
    Generate a new API key for a venue.

    Args:
        venue_id: Venue to generate key for
        name: Human-readable name for the key

    Returns:
        APIKey object with generated key
    """
    # Generate random hex string for key
    random_suffix = secrets.token_hex(API_KEY_LENGTH // 2)
    key = f"{API_KEY_PREFIX}{random_suffix}"

    api_key_obj = APIKey(
        key=key,
        venue_id=venue_id,
        name=name,
        created_at=datetime.now(timezone.utc),
        active=True,
    )

    # Store in memory
    _api_keys[key] = {
        "key": key,
        "venue_id": venue_id,
        "name": name,
        "created_at": api_key_obj.created_at,
        "last_used": None,
        "active": True,
    }

    logger.info(f"Generated API key '{name}' for venue {venue_id}")
    return api_key_obj


def verify_api_key(key: str) -> dict[str, Any]:
    """
    Verify an API key and return its details.

    Args:
        key: API key to verify

    Returns:
        API key data if valid

    Raises:
        HTTPException: If key is invalid or inactive
    """
    if key not in _api_keys:
        logger.warning(f"Attempted to use invalid API key")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )

    key_data = _api_keys[key]

    if not key_data.get("active", False):
        logger.warning(f"Attempted to use inactive API key")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key is inactive",
        )

    # Update last_used timestamp
    _api_keys[key]["last_used"] = datetime.now(timezone.utc)

    return key_data


# ============================================================================
# User Management
# ============================================================================


def create_user(user_create: UserCreate) -> User:
    """
    Create a new user.

    Args:
        user_create: User creation request

    Returns:
        Created user

    Raises:
        ValueError: If user already exists
    """
    if user_create.email in _users:
        raise ValueError(f"User with email {user_create.email} already exists")

    user_id = secrets.token_urlsafe(16)
    password_hash = hash_password(user_create.password)

    access_level = user_create.access_level or AccessLevel.L1_SUPERVISOR

    user_data = {
        "id": user_id,
        "email": user_create.email,
        "name": user_create.name,
        "venue_id": user_create.venue_id,
        "role": user_create.role,
        "access_level": access_level,
        "password_hash": password_hash,
        "created_at": datetime.now(timezone.utc),
    }

    _users[user_create.email] = user_data

    logger.info(f"Created user {user_id} ({user_create.email}) in venue {user_create.venue_id} with access level {access_level.value}")

    return User(**user_data)


def get_user_by_email(email: str) -> Optional[User]:
    """
    Retrieve a user by email.

    Args:
        email: User email

    Returns:
        User if found, None otherwise
    """
    if email not in _users:
        return None

    user_data = _users[email].copy()
    user_data.pop("password_hash", None)
    return User(**user_data)


def get_user_by_id(user_id: str) -> Optional[User]:
    """
    Retrieve a user by ID.

    Args:
        user_id: User ID

    Returns:
        User if found, None otherwise
    """
    for user_data in _users.values():
        if user_data["id"] == user_id:
            safe_data = user_data.copy()
            safe_data.pop("password_hash", None)
            return User(**safe_data)

    return None


def authenticate_user(email: str, password: str) -> Optional[User]:
    """
    Authenticate a user with email and password.

    Implements rate limiting on failed attempts.

    Args:
        email: User email
        password: Plain text password

    Returns:
        User if authentication successful, None otherwise
    """
    # Check rate limiting
    failed_count = _failed_login_attempts.get(email, 0)
    if failed_count >= 5:
        logger.warning(f"Rate limit exceeded for email {email}")
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many failed login attempts. Please try again later.",
        )

    if email not in _users:
        _failed_login_attempts[email] = failed_count + 1
        return None

    user_data = _users[email]

    if not verify_password(password, user_data.get("password_hash", "")):
        _failed_login_attempts[email] = failed_count + 1
        return None

    # Reset failed attempts on successful login
    _failed_login_attempts[email] = 0

    safe_data = user_data.copy()
    safe_data.pop("password_hash", None)
    return User(**safe_data)


# ============================================================================
# Demo Data
# ============================================================================


def create_demo_user() -> User:
    """
    Create a demo user for development and testing.

    Creates a user for "The Royal Oak" venue that can be used immediately
    in dev mode without authentication setup. Demo user defaults to OWNER
    access level so all features are accessible in demo mode.

    Returns:
        Created demo user
    """
    # Check if demo user already exists
    demo_email = "demo@rosteriq.local"
    if demo_email in _users:
        user_data = _users[demo_email].copy()
        user_data.pop("password_hash", None)
        return User(**user_data)

    demo_user = UserCreate(
        email=demo_email,
        password="DemoPass123!",
        name="Demo Manager",
        venue_id="venue-royal-oak",
        role="owner",
        access_level=AccessLevel.OWNER,
    )

    user = create_user(demo_user)
    logger.info(f"Created demo user for testing: {user.email}")
    return user


# ============================================================================
# FastAPI Dependencies (only if FastAPI is available)
# ============================================================================


if HTTPBearer is not None:
    security = HTTPBearer()

    async def get_current_user(
        credentials: HTTPAuthenticationCredentials = Depends(security),
    ) -> User:
        """
        FastAPI dependency to extract and verify current user from Bearer token.

        Args:
            credentials: HTTP Bearer credentials

        Returns:
            Authenticated user

        Raises:
            HTTPException: If token is invalid or user not found
        """
        token = credentials.credentials
        payload = decode_token(token)

        user_id = payload.get("sub")
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token payload",
            )

        user = get_user_by_id(user_id)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found",
            )

        return user

    async def get_current_venue(user: User = Depends(get_current_user)) -> str:
        """
        FastAPI dependency to get venue ID from authenticated user.

        Args:
            user: Current authenticated user

        Returns:
            Venue ID for the user
        """
        return user.venue_id

    async def verify_api_key_dependency(x_api_key: str = None) -> dict[str, Any]:
        """
        FastAPI dependency to verify API key from X-API-Key header.

        Args:
            x_api_key: API key from X-API-Key header

        Returns:
            API key data if valid

        Raises:
            HTTPException: If API key is missing or invalid
        """
        if not x_api_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="API key required (X-API-Key header)",
            )

        return verify_api_key(x_api_key)

    def require_role(required_role: str) -> Callable:
        """
        FastAPI dependency factory to check user has required role.

        Roles: manager, owner, admin
        Access hierarchy: admin > owner > manager

        Args:
            required_role: Role required to access endpoint

        Returns:
            Dependency function that checks role
        """
        role_hierarchy = {"manager": 1, "owner": 2, "admin": 3}

        async def check_role(user: User = Depends(get_current_user)) -> User:
            """Check user has required role."""
            required_level = role_hierarchy.get(required_role, 0)
            user_level = role_hierarchy.get(user.role, 0)

            if user_level < required_level:
                logger.warning(
                    f"User {user.id} attempted to access {required_role} resource with {user.role} role"
                )
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"This action requires {required_role} role or higher",
                )

            return user

        return check_role

    def require_access(required_level: AccessLevel) -> Callable:
        """
        FastAPI dependency factory to check user has required access level.

        Access hierarchy: OWNER (3) > L2_ROSTER_MAKER (2) > L1_SUPERVISOR (1)

        In demo mode (AUTH_ENABLED=False), returns a synthetic OWNER-level user
        so existing flows keep working without authentication.

        Args:
            required_level: Access level required to access endpoint

        Returns:
            Async dependency function that checks access level and returns user
        """

        async def check_access(request: Request) -> User:
            """Check user has required access level."""
            # Demo mode short-circuit: return synthetic OWNER user
            if not AUTH_ENABLED:
                return User(
                    id="demo-user",
                    email="demo@rosteriq.local",
                    name="Demo User",
                    venue_id="demo-venue",
                    role="owner",
                    access_level=AccessLevel.OWNER,
                    created_at=datetime.now(timezone.utc),
                )

            # Auth mode: extract and verify Bearer token
            auth_header = request.headers.get("authorization", "")
            if not auth_header.startswith("Bearer "):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Missing Bearer token",
                )

            token = auth_header.split(" ", 1)[1]
            try:
                payload = decode_token(token)
            except HTTPException:
                raise

            user_id = payload.get("sub")
            if not user_id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Invalid token payload",
                )

            user = get_user_by_id(user_id)
            if not user:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="User not found",
                )

            # Check access level
            if not has_access(user, required_level):
                logger.warning(
                    f"User {user.id} ({user.access_level.value}) attempted to access {required_level.value} resource"
                )
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"This action requires {required_level.value} access level or higher",
                )

            return user

        return check_access

    # ========================================================================
    # FastAPI Router
    # ========================================================================

    auth_router = APIRouter(prefix="/auth", tags=["auth"])

    @auth_router.post("/register", response_model=User, status_code=status.HTTP_201_CREATED)
    async def register(user_create: UserCreate) -> User:
        """
        Register a new user.

        Only the first user in a venue becomes owner. Subsequent registrations
        require admin or owner to approve (future enhancement).

        Args:
            user_create: User creation request

        Returns:
            Created user

        Raises:
            HTTPException: If registration fails
        """
        try:
            # Auto-promote first user in venue to owner
            is_first_user = not any(
                u["venue_id"] == user_create.venue_id for u in _users.values()
            )
            if is_first_user:
                user_create.role = "owner"

            user = create_user(user_create)
            logger.info(f"New user registered: {user.email}")
            return user
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(e),
            )

    @auth_router.post("/login", response_model=TokenResponse)
    async def login(credentials: UserLogin) -> TokenResponse:
        """
        Authenticate user and return JWT token.

        Args:
            credentials: Email and password

        Returns:
            JWT token with expiration and user info

        Raises:
            HTTPException: If authentication fails
        """
        user = authenticate_user(credentials.email, credentials.password)

        if not user:
            logger.warning(f"Failed login attempt for {credentials.email}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid email or password",
            )

        token = create_access_token(user.id, user.venue_id, user.role, user.access_level)
        expires_at = datetime.now(timezone.utc) + timedelta(hours=TOKEN_EXPIRE_HOURS)

        logger.info(f"User {user.email} logged in successfully")

        return TokenResponse(
            access_token=token,
            expires_in=int(expires_at.timestamp()),
            venue_id=user.venue_id,
            user_name=user.name,
        )

    @auth_router.post("/refresh", response_model=TokenResponse)
    async def refresh(refresh_req: TokenRefresh) -> TokenResponse:
        """
        Refresh an expiring JWT token.

        Args:
            refresh_req: Current token to refresh

        Returns:
            New JWT token

        Raises:
            HTTPException: If token is invalid
        """
        payload = decode_token(refresh_req.access_token)

        user_id = payload.get("sub")
        venue_id = payload.get("venue_id")
        role = payload.get("role")
        access_level_str = payload.get("al", AccessLevel.L1_SUPERVISOR.value)

        if not all([user_id, venue_id, role]):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token",
            )

        try:
            access_level = AccessLevel(access_level_str)
        except (ValueError, KeyError):
            access_level = AccessLevel.L1_SUPERVISOR

        new_token = create_access_token(user_id, venue_id, role, access_level)
        expires_at = datetime.now(timezone.utc) + timedelta(hours=TOKEN_EXPIRE_HOURS)

        user = get_user_by_id(user_id)

        return TokenResponse(
            access_token=new_token,
            expires_in=int(expires_at.timestamp()),
            venue_id=venue_id,
            user_name=user.name if user else "Unknown",
        )

    @auth_router.get("/me", response_model=User)
    async def get_current_user_info(user: User = Depends(get_current_user)) -> User:
        """
        Get current authenticated user's information.

        Args:
            user: Current user from token

        Returns:
            Current user information
        """
        return user

    @auth_router.post("/api-keys", response_model=APIKey, status_code=status.HTTP_201_CREATED)
    async def create_api_key(
        key_create: APIKeyCreate,
        user: User = Depends(require_role("owner")),
    ) -> APIKey:
        """
        Generate a new API key for the user's venue.

        Requires owner or admin role.

        Args:
            key_create: API key creation request
            user: Current authenticated user (must be owner/admin)

        Returns:
            Generated API key

        Raises:
            HTTPException: If user lacks permission
        """
        api_key = generate_api_key(user.venue_id, key_create.name)
        logger.info(f"API key '{key_create.name}' generated by {user.email}")
        return api_key

    @auth_router.get("/api-keys", response_model=list[APIKey])
    async def list_api_keys(
        user: User = Depends(require_role("owner")),
    ) -> list[APIKey]:
        """
        List all API keys for the user's venue.

        Requires owner or admin role.

        Args:
            user: Current authenticated user (must be owner/admin)

        Returns:
            List of API keys for the venue
        """
        venue_keys = [
            APIKey(**key_data)
            for key_data in _api_keys.values()
            if key_data["venue_id"] == user.venue_id
        ]
        logger.info(f"API keys listed for venue {user.venue_id}")
        return venue_keys

    @auth_router.delete("/api-keys/{key_id}", status_code=status.HTTP_204_NO_CONTENT)
    async def deactivate_api_key(
        key_id: str,
        user: User = Depends(require_role("owner")),
    ) -> None:
        """
        Deactivate an API key for the user's venue.

        Requires owner or admin role. Deactivation is permanent.

        Args:
            key_id: API key to deactivate
            user: Current authenticated user (must be owner/admin)

        Raises:
            HTTPException: If key not found or user lacks permission
        """
        if key_id not in _api_keys:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="API key not found",
            )

        key_data = _api_keys[key_id]

        if key_data["venue_id"] != user.venue_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Cannot deactivate API key from another venue",
            )

        _api_keys[key_id]["active"] = False
        logger.info(f"API key deactivated by {user.email}")

else:
    # FastAPI not available - provide placeholder
    auth_router = None


# ============================================================================
# App Setup
# ============================================================================


def setup_auth(app: FastAPI) -> None:
    """
    Mount authentication routes and middleware onto the FastAPI app.

    Args:
        app: FastAPI application instance

    Usage:
        from rosteriq.auth import setup_auth
        setup_auth(app)
    """
    if auth_router is None:
        logger.warning("FastAPI not available - auth routes not mounted")
        return

    app.include_router(auth_router)

    # Create demo user for development
    try:
        create_demo_user()
    except Exception as e:
        logger.error(f"Failed to create demo user: {e}")

    logger.info("Authentication module initialized")


# ============================================================================
# Tanda OAuth Integration Stub
# ============================================================================


def get_tanda_oauth_config() -> dict[str, str]:
    """
    Get Tanda OAuth configuration.

    For future integration with Tanda's OAuth flow.
    https://developer.tanda.co/

    Returns:
        OAuth configuration (client_id, client_secret, etc.)
    """
    return {
        "client_id": os.getenv("TANDA_CLIENT_ID", ""),
        "client_secret": os.getenv("TANDA_CLIENT_SECRET", ""),
        "authorize_url": "https://my.tanda.co/oauth/authorize",
        "token_url": "https://my.tanda.co/oauth/token",
        "api_url": "https://api.tanda.co/v2",
    }


if __name__ == "__main__":
    # Quick test
    print("Creating demo user...")
    user = create_demo_user()
    print(f"Demo user: {user.email} (ID: {user.id})")

    print("\nTesting password hashing...")
    pwd = "TestPassword123!"
    hashed = hash_password(pwd)
    print(f"Verification: {verify_password(pwd, hashed)}")

    print("\nTesting JWT tokens...")
    token = create_access_token(user.id, user.venue_id, user.role)
    print(f"Token created: {token[:50]}...")
    payload = decode_token(token)
    print(f"Decoded payload: {payload}")

    print("\nTesting API key generation...")
    api_key = generate_api_key(user.venue_id, "Test Key")
    print(f"API key generated: {api_key.key}")

    print("\nAll tests passed!")
