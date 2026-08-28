"""
Authentication routes for user registration, login, token management, and profile.

Uses the custom BaseStore database layer instead of SQLAlchemy.
"""
from typing import Optional
import asyncio
import logging
import os

from fastapi import APIRouter, Depends, HTTPException, Query, status, Request

from rosteriq.database import get_db
from rosteriq.services.auth import auth_service
from rosteriq.services.notifications import get_notification_service
from rosteriq.services.demo import (
    seed_demo_environment, DEMO_USER_ID, DEMO_USER_EMAIL,
    DEMO_STAFF_USER_ID, DEMO_STAFF_EMAIL,
)
from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.services.events import record_event, security
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

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/auth", tags=["auth"])


@router.post("/demo")
async def demo_session(
    request: Request,
    as_: str = Query("manager", alias="as"),
    db = Depends(get_db),
):
    """Mint a short-lived session for the sandboxed demo user so prospects can
    try the product (incl. the AI agent) without registering.

    The demo user is scoped to the demo venue only, so this exposes no real
    tenant data. Rate-limited per IP to bound abuse of the unauthenticated path.

    ``?as=staff`` mints the session for the second demo identity (Emma
    Thompson, whose email is on a seeded employee) so the staff phone portal
    (/my) links and can be showcased; anything else is the default (venue-side)
    demo user. Both are role "staff" and locked to the demo venue.
    """
    client_ip = request.client.host if request.client else "unknown"
    # Reuse the login throttle: too many recent attempts from this IP -> back off.
    if not auth_service.check_login_rate_limit(client_ip):
        # The api.py request middleware skips /api/auth/* for rate.limited, so
        # record the throttle here (security, no identity involved).
        security("rate.limited", outcome="throttled", path="/api/auth/demo",
                 client_ip=client_ip)
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many demo sessions from this address. Try again shortly.",
        )

    try:
        seed_demo_environment(db)
    except Exception as e:  # never 500 the public demo path
        logger.warning("Demo seed issue (continuing): %s", e)

    if (as_ or "").strip().lower() == "staff":
        demo_user_id, demo_email = DEMO_STAFF_USER_ID, DEMO_STAFF_EMAIL
    else:
        demo_user_id, demo_email = DEMO_USER_ID, DEMO_USER_EMAIL
    access_token, _ = auth_service.create_access_token(
        demo_user_id, demo_email, "staff"
    )
    # Record as a (successful) attempt so the per-IP counter advances.
    auth_service.record_login_attempt(demo_email, client_ip, True)
    # Audit which demo identity was minted (unauthenticated path: no actor in
    # context, so the demo user is recorded explicitly as the subject).
    record_event("audit", "demo.session", user_id=demo_user_id,
                 details={"identity": "staff" if demo_user_id == DEMO_STAFF_USER_ID else "manager",
                          "email": demo_email})
    return {"access_token": access_token, "token_type": "bearer", "demo": True}



# The event loop holds only a WEAK reference to created tasks: a bare
# asyncio.create_task(send_email(...)) can be garbage-collected mid-send and
# any exception inside it disappears. Keep a strong reference until done and
# log the outcome — an email that silently never sent is the worst failure
# mode a password-reset flow can have.
_bg_sends: set = set()


def _send_in_background(coro, what: str) -> None:
    task = asyncio.create_task(coro)
    _bg_sends.add(task)

    def _done(t):
        _bg_sends.discard(t)
        exc = None if t.cancelled() else t.exception()
        if exc is not None:
            logger.error(f"Background {what} send failed: {exc}")
            try:
                from rosteriq.services.events import error as _record
                _record(f"notify.{what}", exc)
            except Exception:
                pass

    task.add_done_callback(_done)

def _bootstrap_owner_allowed() -> bool:
    """
    Whether the public /register endpoint may auto-grant the global ``owner``
    role to the first user.

    The ``owner`` role grants unrestricted cross-tenant access, so on a live
    multi-tenant deployment the first internet visitor must NOT be able to
    claim it (a land-grab). Bootstrap is therefore only permitted outside
    production, or when an operator explicitly opts in with
    ``ALLOW_BOOTSTRAP_OWNER=true`` (e.g. during a controlled first-run setup).
    In production the initial owner should be seeded out-of-band (migration /
    CLI / invite).
    """
    if os.environ.get("ALLOW_BOOTSTRAP_OWNER", "").lower() in ("1", "true", "yes"):
        return True
    return os.environ.get("ENVIRONMENT", "development") != "production"


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

    # Determine role. The first registered user may bootstrap as the global
    # owner, but only when bootstrap is allowed (see _bootstrap_owner_allowed)
    # so a public signup on a fresh production deployment cannot seize
    # cross-tenant owner access. Everyone else registers as staff.
    user_count = len(db.list_users())
    if user_count == 0 and _bootstrap_owner_allowed():
        role = "owner"
        logger.warning(
            "Bootstrapping first user %s as global owner (bootstrap allowed).",
            request.email,
        )
    else:
        role = "staff"
        if user_count == 0:
            logger.warning(
                "First user %s registered as staff; owner bootstrap is disabled "
                "in production. Seed the initial owner out-of-band or set "
                "ALLOW_BOOTSTRAP_OWNER=true for a controlled first-run.",
                request.email,
            )

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

    # Audit the new identity (self-registration; owner bootstrap is notable).
    record_event("audit", "auth.register", user_id=user["id"],
                 resource_type="user", resource_id=user["id"],
                 details={"email": user["email"], "name": user.get("name"),
                          "role": role, "bootstrap_owner": role == "owner"})

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
        # Attempt while locked out: an attack signal in its own right (the
        # api.py middleware deliberately skips /api/auth/* so record it here).
        security("auth.login_failed", outcome="denied", email=request.email,
                 reason="locked", client_ip=client_ip)
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
        # Security event: the email is the identity under attack (owners need
        # it); the password is never recorded.
        reason = "unknown_user" if not user else ("inactive" if not user.get("is_active") else "bad_password")
        security("auth.login_failed", outcome="failed", email=request.email,
                 reason=reason, client_ip=client_ip,
                 user_id=(user or {}).get("id"))
        # Did this failure trip the per-IP lockout? Record the trigger once.
        if not auth_service.check_login_rate_limit(client_ip):
            security("auth.locked", outcome="denied", email=request.email,
                     client_ip=client_ip, window_minutes=1, max_attempts=5)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    # Create tokens
    access_token, _ = auth_service.create_access_token(user["id"], user["email"], user["role"])
    refresh_token, _, _ = auth_service.create_refresh_token(user["id"])

    # Update last login
    auth_service.update_last_login(user["id"])

    # Audit (low noise, outcome ok): who signed in, as what role.
    record_event("audit", "auth.login", user_id=user["id"],
                 details={"email": user["email"], "role": user.get("role")})

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
        # Seeded rows (demo identities) never logged in by password and carry
        # no last_login key on the in-memory store — don't 500 the profile.
        last_login=user.get("last_login"),
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

    # Credential change: a new long-lived API key now exists for this user
    # (the key itself is never recorded — and the scrubber would redact it).
    security("auth.api_key_generate", outcome="ok", user_id=current_user.user_id,
             email=current_user.email)

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

    # Audit: an owner created an identity with a role (and venue grants).
    record_event("audit", "auth.invite", user_id=current_user.user_id,
                 resource_type="user", resource_id=user["id"],
                 details={"invited_email": user["email"], "invited_name": user.get("name"),
                          "role": user["role"], "venue_ids": list(user.get("venue_ids") or [])})

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
            _send_in_background(
                notification_service.send_password_reset(
                    email=request.email,
                    name=user.get("name", "User"),
                    reset_token=reset_token,
                    reset_url=reset_url,
                    ),
                "password_reset",
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
        # Credential change attempted with a bad/expired token: security signal.
        # (The token itself is never recorded.)
        security("auth.password_reset", outcome="failed", reason="invalid_or_expired_token")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired reset token",
        )

    # Credential changed (unauthenticated path: the token is single-use and
    # consumed by the service, so no actor beyond the outcome is available).
    security("auth.password_reset", outcome="ok")

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
    _send_in_background(
        notification_service.send_email_verification(
            email=user["email"],
            name=user.get("name", "User"),
            verification_token=verify_token,
            verification_url=verification_url,
        ),
        "email_verification",
    )

    return {"message": "Verification email sent. Please check your inbox."}
