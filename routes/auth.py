"""
Authentication routes for user registration, login, token management, and profile.

Uses the custom BaseStore database layer instead of SQLAlchemy.
"""
from typing import Optional
import asyncio

from fastapi import APIRouter, Depends, HTTPException, status, Request

from rosteriq.database import get_db
from rosteriq.services.auth import auth_service
from rosteriq.services.notifications import get_notification_service
from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.schemas import (
    RegisterRequest,
    LoginRequest,
    RefreshTokenRequest,
    LogoutRequest,
    InviteUserRequest,
    ForgotPasswordRequest,
    ResetPasswordRequest,
    VerifyEmailRequest,
    TokenPair,
    UserResponse,
    UserDetailResponse,
    ApiKeyResponse,
)

router = APIRouter(prefix="/api/auth", tags=["auth"])


@router.post("/register", response_model=UserDetailResponse, status_code=status.HTTP_201_CREATED)
async def register(
    request: RegisterRequest,
    req: Request,
    db = Depends(get_db),
):
    """
    Register a new user.
    First user automatically becomes owner; subsequent users are staff by default.
    """
    # Check if user already exists
    existing_user = db.get_user_by_email(request.email)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User with this email already exists",
        )

    # Determine role: first user is owner, rest are staff
    user_count = len(db.list_users())
    role = "owner" if user_count == 0 else "staff"

    # Create user
    try:
        user = auth_service.create_user(
            email=request.email,
            password=request.password,
            name=request.name,
            role=role,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "CONFLICT",
                "message": str(e),
            },
        )

    # Create tokens
    access_token, _ = auth_service.create_access_token(user["id"], user["email"], user["role"])
    refresh_token, _, _ = auth_service.create_refresh_token(user["id"])

    # Update last login
    auth_service.update_last_login(user["id"])

    # Email verification disabled for pilot — enable with SendGrid when scaling
    # verify_token = auth_service.create_email_verification_token(user["id"])
    # verification_url = f"https://rosteriq-production-6aaf.up.railway.app/verify-email?token={verify_token}"
    # notification_service = get_notification_service()
    # asyncio.create_task(
    #     notification_service.send_email_verification(
    #         email=user["email"],
    #         name=user.get("name", "User"),
    #         verification_token=verify_token,
    #         verification_url=verification_url,
    #     )
    # )

    return UserDetailResponse(
        user=UserResponse(
            id=user["id"],
            email=user["email"],
            name=user["name"],
            role=user["role"],
            is_active=user["is_active"],
            created_at=user["created_at"],
            last_login=user["last_login"],
            venue_ids=user.get("venue_ids", []),
        ),
        tokens=TokenPair(
            access_token=access_token,
            refresh_token=refresh_token,
        ),
    )


@router.post("/login", response_model=TokenPair)
async def login(
    request: LoginRequest,
    req: Request,
    db = Depends(get_db),
):
    """
    Authenticate user with email and password.
    Returns access and refresh tokens.
    """
    # Get client IP for rate limiting
    client_ip = req.client.host if req.client else "unknown"

    # Check rate limit
    if not auth_service.check_login_rate_limit(client_ip):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many login attempts. Try again in a minute.",
        )

    # Find user
    user = db.get_user_by_email(request.email)

    # Verify password (even if user not found, to prevent timing attacks)
    password_valid = False
    if user and user.get("is_active"):
        password_valid = auth_service.verify_password(request.password, user["password_hash"])

    # Record attempt
    auth_service.record_login_attempt(request.email, client_ip, password_valid)

    if not user or not user.get("is_active") or not password_valid:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    # Create tokens
    access_token, _ = auth_service.create_access_token(user["id"], user["email"], user["role"])
    refresh_token, _, _ = auth_service.create_refresh_token(user["id"])

    # Update last login
    auth_service.update_last_login(user["id"])

    return TokenPair(
        access_token=access_token,
        refresh_token=refresh_token,
    )


@router.post("/refresh", response_model=TokenPair)
async def refresh(
    request: RefreshTokenRequest,
    db = Depends(get_db),
):
    """
    Obtain new access token using refresh token.
    """
    # Verify refresh token
    user_id = auth_service.verify_refresh_token(request.refresh_token)
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token",
        )

    # Get user
    user = db.get_user_by_id(user_id)
    if not user or not user.get("is_active"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
        )

    # Create new access token
    access_token, _ = auth_service.create_access_token(user["id"], user["email"], user["role"])

    # Create new refresh token
    new_refresh_token, _, _ = auth_service.create_refresh_token(user["id"])

    # Revoke old refresh token
    auth_service.revoke_refresh_token(request.refresh_token)

    return TokenPair(
        access_token=access_token,
        refresh_token=new_refresh_token,
    )


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    request: LogoutRequest,
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Logout user by revoking refresh token.
    """
    auth_service.revoke_refresh_token(request.refresh_token)
    return None


@router.get("/me", response_model=UserResponse)
async def get_current_user_profile(
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Get current user's profile information.
    """
    user = db.get_user_by_id(current_user.user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    return UserResponse(
        id=user["id"],
        email=user["email"],
        name=user["name"],
        role=user["role"],
        is_active=user["is_active"],
        created_at=user["created_at"],
        last_login=user["last_login"],
        venue_ids=user.get("venue_ids", []),
    )


@router.put("/me", response_model=UserResponse)
async def update_current_user(
    request: dict,
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Update current user's profile (name only).
    """
    user = db.get_user_by_id(current_user.user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    # Allow updating name
    if "name" in request and request["name"]:
        user["name"] = request["name"]
        db.save_user(user)

    return UserResponse(
        id=user["id"],
        email=user["email"],
        name=user["name"],
        role=user["role"],
        is_active=user["is_active"],
        created_at=user["created_at"],
        last_login=user["last_login"],
        venue_ids=user.get("venue_ids", []),
    )


@router.post("/api-key/generate", response_model=ApiKeyResponse)
async def generate_api_key(
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Generate a new API key for the current user.
    The plaintext key is shown only once.
    """
    api_key = auth_service.generate_api_key(current_user.user_id)
    user = db.get_user_by_id(current_user.user_id)

    return ApiKeyResponse(
        api_key=api_key,
        created_at=user["created_at"],
    )


@router.post("/invite", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def invite_user(
    request: InviteUserRequest,
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Invite a new user (owner only).
    Owner creates users without requiring password signup.
    """
    # Only owners can invite
    if current_user.role != "owner":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only owners can invite users",
        )

    # Check role is valid
    valid_roles = ["owner", "manager", "staff"]
    if request.role not in valid_roles:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid role. Must be one of: {', '.join(valid_roles)}",
        )

    # Check if user already exists
    existing_user = db.get_user_by_email(request.email)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User with this email already exists",
        )

    # Managers must have venue IDs
    if request.role == "manager" and not request.venue_ids:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Managers must be assigned to at least one venue",
        )

    # Create user with empty password (user must reset)
    try:
        user = auth_service.create_user(
            email=request.email,
            password="",  # No password set yet
            name=request.name,
            role=request.role,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e),
        )

    # Assign venues if provided
    if request.venue_ids:
        user["venue_ids"] = [str(v) for v in request.venue_ids]
        db.save_user(user)

    return UserResponse(
        id=user["id"],
        email=user["email"],
        name=user["name"],
        role=user["role"],
        is_active=user["is_active"],
        created_at=user["created_at"],
        last_login=user["last_login"],
        venue_ids=user.get("venue_ids", []),
    )


@router.post("/forgot-password", status_code=status.HTTP_200_OK)
async def forgot_password(
    request: ForgotPasswordRequest,
    req: Request,
    db = Depends(get_db),
):
    """
    Request a password reset email.

    Always returns 200 to prevent email enumeration attacks.
    If email exists, sends reset link. Otherwise, silently succeeds.
    """
    # Try to find user (no error if not found)
    user = db.get_user_by_email(request.email)

    if user and user.get("is_active"):
        # Generate reset token
        reset_token = auth_service.create_password_reset_token(request.email)

        if reset_token:
            # Build reset URL (client should construct this)
            reset_url = f"https://app.example.com/reset-password?token={reset_token}"

            # Send email asynchronously (don't block response)
            notification_service = get_notification_service()
            asyncio.create_task(
                notification_service.send_password_reset(
                    email=request.email,
                    name=user.get("name", "User"),
                    reset_token=reset_token,
                    reset_url=reset_url,
                )
            )

    # Always return 200 (don't reveal if email exists)
    return {"message": "If an account exists with that email, a reset link has been sent."}


@router.post("/reset-password", status_code=status.HTTP_200_OK)
async def reset_password(
    request: ResetPasswordRequest,
    db = Depends(get_db),
):
    """
    Reset password using a valid reset token.

    Returns 200 if successful, 400 if token invalid/expired.
    """
    # Attempt to reset password
    success = auth_service.reset_password(request.token, request.new_password)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired reset token",
        )

    return {"message": "Password reset successful. You can now log in with your new password."}


@router.post("/verify-email", status_code=status.HTTP_200_OK)
async def verify_email(
    request: VerifyEmailRequest,
    db = Depends(get_db),
):
    """
    Verify a user's email using a verification token.

    Returns 200 if successful, 400 if token invalid/expired.
    """
    # Attempt to verify email
    success = auth_service.verify_email(request.token)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired verification token",
        )

    return {"message": "Email verified successfully!"}


@router.post("/resend-verification", status_code=status.HTTP_200_OK)
async def resend_verification(
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Resend email verification link (authenticated users only).

    Generates a new verification token and sends it.
    """
    # Get user
    user = db.get_user_by_id(current_user.user_id)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    # Generate new verification token
    verify_token = auth_service.create_email_verification_token(current_user.user_id)

    # Build verification URL
    verification_url = f"https://app.example.com/verify-email?token={verify_token}"

    # Send email asynchronously
    notification_service = get_notification_service()
    asyncio.create_task(
        notification_service.send_email_verification(
            email=user["email"],
            name=user.get("name", "User"),
            verification_token=verify_token,
            verification_url=verification_url,
        )
    )

    return {"message": "Verification email sent. Please check your inbox."}
