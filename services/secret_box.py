"""
Transparent encryption for stored connector credentials (plugin_installs.tokens).

Sensitive token values (access/refresh tokens, API keys/secrets) are encrypted at
the persistence boundary so they aren't sitting in the database as plaintext.
Encryption is:
  - selective: only sensitive keys are encrypted; non-secret fields like
    token_expires_at / business_id / subdomain stay readable for status lookups.
  - idempotent: an already-encrypted value (prefixed) is not re-encrypted.
  - backward compatible: plaintext (un-prefixed) values decrypt to themselves, so
    existing rows keep working and a key rollout is non-breaking.
  - a no-op when no cipher is configured (dev/test): values pass through untouched.

Reuses the same Fernet cipher as feed_config (FEED_CONFIG_ENCRYPTION_KEY).
"""

from __future__ import annotations

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

# Reuse the configured Fernet cipher (None when the key is the dev default /
# malformed — in which case this module is a pass-through).
try:
    from rosteriq.services.feed_config import cipher as _cipher
except Exception:  # pragma: no cover - feed_config import guarded
    _cipher = None

_ENC_PREFIX = "enc::"

# Token keys whose values are secrets and should be encrypted at rest.
SENSITIVE_KEYS = {
    "access_token", "refresh_token", "api_key", "api_secret",
    "client_secret", "secret", "token", "password",
}


def _enc(value: str) -> str:
    if not _cipher:
        return value
    try:
        return _ENC_PREFIX + _cipher.encrypt(value.encode()).decode()
    except Exception as e:  # pragma: no cover - never lose the value on failure
        logger.error(f"secret_box encrypt failed: {e}")
        return value


def _dec(value: str) -> str:
    if not value.startswith(_ENC_PREFIX):
        return value  # plaintext (legacy) — return as-is
    if not _cipher:
        return value  # can't decrypt without the key; leave marker intact
    try:
        return _cipher.decrypt(value[len(_ENC_PREFIX):].encode()).decode()
    except Exception as e:  # pragma: no cover
        logger.error(f"secret_box decrypt failed: {e}")
        return value


def encrypt_tokens(tokens: Any) -> Any:
    """Return a copy of a tokens dict with sensitive values encrypted."""
    if not isinstance(tokens, dict):
        return tokens
    out: Dict[str, Any] = {}
    for k, v in tokens.items():
        if k in SENSITIVE_KEYS and isinstance(v, str) and v and not v.startswith(_ENC_PREFIX):
            out[k] = _enc(v)
        else:
            out[k] = v
    return out


def decrypt_tokens(tokens: Any) -> Any:
    """Return a copy of a tokens dict with any encrypted values decrypted."""
    if not isinstance(tokens, dict):
        return tokens
    out: Dict[str, Any] = {}
    for k, v in tokens.items():
        out[k] = _dec(v) if isinstance(v, str) else v
    return out


def encrypt_install(install: Any) -> Any:
    """Copy of a plugin_install dict with its `tokens` encrypted."""
    if not isinstance(install, dict) or "tokens" not in install:
        return install
    rec = dict(install)
    rec["tokens"] = encrypt_tokens(rec.get("tokens"))
    return rec


def decrypt_install(install: Any) -> Any:
    """Copy of a plugin_install dict with its `tokens` decrypted."""
    if not isinstance(install, dict) or "tokens" not in install:
        return install
    rec = dict(install)
    rec["tokens"] = decrypt_tokens(rec.get("tokens"))
    return rec
