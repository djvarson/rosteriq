"""
Pydantic schemas for request/response validation.

Pure Pydantic models — no ORM or database dependencies.
"""
from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel, EmailStr, Field


# ============================================================================
# Auth Request Schemas
# ============================================================================

class RegisterRequest(BaseModel):
    """User registration request."""
    email: EmailStr
    password: str = Field(..., min_length=8, description="Password must be at least 8 characters")
    name: str = Field(..., min_length=1, max_length=255)


class LoginRequest(BaseModel):
    """User login request."""
    email: EmailStr
    password: str


class RefreshTokenRequest(BaseModel):
    """Refresh token request."""
    refresh_token: str


class LogoutRequest(BaseModel):
    """Logout request."""
    refresh_token: str


class InviteUserRequest(BaseModel):
    """Invite user request (owner only)."""
    email: EmailStr
    name: str
    role: str = Field(..., description="owner, manager, or staff")
    venue_ids: Optional[List[str]] = Field(None, description="Venue IDs to assign (required for managers)")


class ForgotPasswordRequest(BaseModel):
    """Password reset request."""
    email: EmailStr


class ResetPasswordRequest(BaseModel):
    """Password reset token verification and new password."""
    token: str
    new_password: str = Field(..., min_length=8, description="New password must be at least 8 characters")


class VerifyEmailRequest(BaseModel):
    """Email verification token."""
    token: str


# ============================================================================
# Auth Response Schemas
# ============================================================================

class TokenPair(BaseModel):
    """JWT token pair response."""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int = Field(default=3600, description="Seconds until access token expires")


class UserResponse(BaseModel):
    """User profile response."""
    id: str
    email: str
    name: str
    role: str
    is_active: bool
    created_at: datetime
    last_login: Optional[datetime] = None
    venue_ids: List[str] = []

    class Config:
        from_attributes = True


class ApiKeyResponse(BaseModel):
    """API key generation response."""
    api_key: str = Field(..., description="Plaintext API key (show once)")
    created_at: datetime


class UserDetailResponse(BaseModel):
    """Detailed user response with tokens."""
    user: UserResponse
    tokens: Optional[TokenPair] = None


class ErrorResponse(BaseModel):
    """Standard error response."""
    detail: str
    error_code: Optional[str] = None


# ============================================================================
# OAuth Callback Schemas
# ============================================================================

class AuthCallback(BaseModel):
    """OAuth callback state."""
    state: str
    code: str
    provider: str
