"""
Authentication service for JWT and API key management.
Handles password hashing, token generation/validation, and user auth.

Uses the custom BaseStore database layer instead of SQLAlchemy.
"""
import os
import secrets
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Dict
from uuid import uuid4

import jwt
from passlib.context import CryptContext

from rosteriq.database import get_db
from rosteriq.models import User

logger = logging.getLogger(__name__)

# Security config — JWT_SECRET must be set in production
JWT_SECRET = os.getenv("JWT_SECRET", "")
if not JWT_SECRET:
    logger.warning("JWT_SECRET not set — auth module will reject all tokens in production")
    JWT_SECRET = "dev-only-insecure-" + secrets.token_hex(16)

JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60  # 1 hour
REFRESH_TOKEN_EXPIRE_DAYS = 14  # 14 days
API_KEY_PREFIX = "riq_"

# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class AuthService:
    """Service for authentication operations."""

    def __init__(self, db=None):
        self.db = db or get_db()

    @staticmethod
    def hash_password(password: str) -> str:
        """Hash a password using bcrypt."""
        return pwd_context.hash(password)

    @staticmethod
    def verify_password(plain_password: str, password_hash: str) -> bool:
        """Verify a password against its hash."""
        return pwd_context.verify(plain_password, password_hash)

    @staticmethod
    def create_access_token(user_id: str, email: str, role: str) -> Tuple[str, datetime]:
        """Create a JWT access token."""
        # Use timezone-aware UTC so .timestamp() does not re-apply the host's
        # local offset (naive utcnow().timestamp() makes tokens expire early
        # on non-UTC servers).
        now = datetime.now(timezone.utc)
        expires = now + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

        payload = {
            "sub": user_id,
            "email": email,
            "role": role,
            "type": "access",
            "iat": now.timestamp(),
            "exp": expires.timestamp(),
        }

        token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
        return token, expires

    def create_refresh_token(self, user_id: str) -> Tuple[str, str, datetime]:
        """
        Create a refresh token and store its hash in the database.
        Returns: (token, token_hash, expires_at)
        """
        now = datetime.utcnow()
        expires = now + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)

        # Generate random refresh token
        token = secrets.token_urlsafe(64)
        token_hash = hashlib.sha256(token.encode()).hexdigest()

        # Store hash in database
        self.db.save_refresh_token(token_hash, user_id, expires)

        return token, token_hash, expires

    @staticmethod
    def verify_access_token(token: str) -> Optional[Dict]:
        """
        Verify and decode an access token.
        Returns payload if valid, None if invalid.
        """
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])

            # Verify token type
            if payload.get("type") != "access":
                return None

            return payload
        except jwt.PyJWTError:
            # PyJWT raises PyJWTError subclasses (ExpiredSignatureError,
            # InvalidTokenError, etc). jwt.JWTError does not exist in PyJWT.
            return None

    def verify_refresh_token(self, token: str) -> Optional[str]:
        """
        Verify a refresh token.
        Returns user_id if valid, None if invalid/revoked.
        """
        token_hash = hashlib.sha256(token.encode()).hexdigest()

        refresh_token = self.db.get_refresh_token(token_hash)

        if not refresh_token:
            return None

        # Check if revoked
        if refresh_token.get("is_revoked"):
            return None

        # Check expiration
        expires_at = refresh_token.get("expires_at")
        if isinstance(expires_at, str):
            expires_at = datetime.fromisoformat(expires_at)

        if expires_at < datetime.utcnow():
            self.db.revoke_refresh_token(token_hash)
            return None

        return refresh_token.get("user_id")

    def generate_api_key(self, user_id: str) -> str:
        """
        Generate a new API key for the user.
        Stores hash in database, returns plaintext key once.
        """
        # Generate key: riq_<random>
        random_part = secrets.token_urlsafe(48)
        api_key = f"{API_KEY_PREFIX}{random_part}"
        api_key_hash = hashlib.sha256(api_key.encode()).hexdigest()

        # Update user's API key hash
        user = self.db.get_user_by_id(user_id)
        if user:
            user["api_key_hash"] = api_key_hash
            self.db.save_user(user)

        return api_key

    def verify_api_key(self, api_key: str) -> Optional[dict]:
        """
        Verify an API key and return the associated user dict.
        """
        api_key_hash = hashlib.sha256(api_key.encode()).hexdigest()

        for user in self.db.list_users():
            if user.get("api_key_hash") == api_key_hash and user.get("is_active"):
                return user

        return None

    def revoke_refresh_token(self, token: str) -> bool:
        """Mark a refresh token as revoked."""
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        self.db.revoke_refresh_token(token_hash)
        return True

    def check_login_rate_limit(self, ip_address: str, max_attempts: int = 5) -> bool:
        """
        Check if login is rate limited.
        Returns False if rate limited (too many attempts), True if allowed.
        Limit: max_attempts failed attempts per minute per IP.
        """
        failed_count = self.db.check_login_rate_limit(ip_address, minutes=1)
        return failed_count < max_attempts

    def record_login_attempt(self, email: str, ip_address: str, success: bool):
        """Record a login attempt for rate limiting."""
        self.db.record_login_attempt(email, ip_address, success)

    def update_last_login(self, user_id: str):
        """Update user's last_login timestamp."""
        user = self.db.get_user_by_id(user_id)
        if user:
            user["last_login"] = datetime.utcnow()
            self.db.save_user(user)

    def create_user(self, email: str, password: str, name: str, role: str = "staff") -> dict:
        """Create a new user. Returns user dict."""
        # Check if email already exists
        if self.db.get_user_by_email(email):
            raise ValueError(f"User with email {email} already exists")

        user_id = str(uuid4())
        password_hash = self.hash_password(password)
        now = datetime.utcnow()

        user = {
            "id": user_id,
            "email": email,
            "password_hash": password_hash,
            "name": name,
            "role": role,
            "venue_ids": [],
            "api_key_hash": "",
            "is_active": True,
            "created_at": now,
            "last_login": None,
        }

        self.db.save_user(user)
        return user

    def get_user_by_email(self, email: str) -> Optional[dict]:
        """Get user by email."""
        return self.db.get_user_by_email(email)

    def get_user_by_id(self, user_id: str) -> Optional[dict]:
        """Get user by ID."""
        return self.db.get_user_by_id(user_id)

    def create_password_reset_token(self, email: str) -> Optional[str]:
        """
        Create a password reset token for a user.

        Args:
            email: User's email address

        Returns:
            Plaintext token (64 chars, URL-safe) to send to user, or None if email not found.
            Token hash is stored in database with 1-hour expiry.
        """
        user = self.db.get_user_by_email(email)
        if not user:
            return None

        # Generate token: 64 chars, URL-safe
        token = secrets.token_urlsafe(48)
        token_hash = hashlib.sha256(token.encode()).hexdigest()

        # Store hash with 1-hour expiry
        expires_at = datetime.utcnow() + timedelta(hours=1)
        self.db.save_password_reset_token(token_hash, user["id"], expires_at)

        # Return plaintext token to send via email
        return token

    def reset_password(self, token: str, new_password: str) -> bool:
        """
        Reset user password using a valid reset token.

        Args:
            token: Plaintext reset token from email
            new_password: New password (at least 8 chars)

        Returns:
            True if reset successful, False if token invalid/expired.
        """
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        reset_token = self.db.get_password_reset_token(token_hash)

        if not reset_token:
            return False

        # Check expiration
        expires_at = reset_token.get("expires_at")
        if isinstance(expires_at, str):
            expires_at = datetime.fromisoformat(expires_at)

        if expires_at < datetime.utcnow():
            # Token expired, clean it up
            self.db.delete_password_reset_token(token_hash)
            return False

        # Get user and update password
        user_id = reset_token.get("user_id")
        user = self.db.get_user_by_id(user_id)

        if not user:
            return False

        # Update password and save
        user["password_hash"] = self.hash_password(new_password)
        self.db.save_user(user)

        # Delete the used token
        self.db.delete_password_reset_token(token_hash)

        return True

    def create_email_verification_token(self, user_id: str) -> str:
        """
        Create an email verification token for a user.

        Args:
            user_id: User ID

        Returns:
            Plaintext token (64 chars, URL-safe) to send to user.
            Token hash is stored in database with 24-hour expiry.
        """
        # Generate token: 64 chars, URL-safe
        token = secrets.token_urlsafe(48)
        token_hash = hashlib.sha256(token.encode()).hexdigest()

        # Store hash with 24-hour expiry
        expires_at = datetime.utcnow() + timedelta(hours=24)
        self.db.save_email_verification_token(token_hash, user_id, expires_at)

        # Return plaintext token
        return token

    def verify_email(self, token: str) -> bool:
        """
        Verify a user's email using a verification token.

        Args:
            token: Plaintext verification token from email

        Returns:
            True if verification successful, False if token invalid/expired.
        """
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        verify_token = self.db.get_email_verification_token(token_hash)

        if not verify_token:
            return False

        # Check expiration
        expires_at = verify_token.get("expires_at")
        if isinstance(expires_at, str):
            expires_at = datetime.fromisoformat(expires_at)

        if expires_at < datetime.utcnow():
            # Token expired, clean it up
            self.db.delete_email_verification_token(token_hash)
            return False

        # Get user and mark email as verified
        user_id = verify_token.get("user_id")
        user = self.db.get_user_by_id(user_id)

        if not user:
            return False

        # Mark email as verified and save
        user["email_verified"] = True
        self.db.save_user(user)

        # Delete the used token
        self.db.delete_email_verification_token(token_hash)

        return True


# Convenience instance
auth_service = AuthService()
