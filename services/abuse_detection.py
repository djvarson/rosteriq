"""
Abuse detection service for RosterIQ API.

Tracks and detects malicious patterns:
- Brute force login attempts
- Credential stuffing
- Data scraping
- API key probing
- Rapid roster generation
- Sequential ID enumeration

Uses in-memory state with TTL-based cleanup.
Thread-safe with asyncio locks.
"""

import time
import logging
import asyncio
import re
from typing import Dict, Optional, List, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import defaultdict

logger = logging.getLogger(__name__)


# ============================================================================
# Data models
# ============================================================================

@dataclass
class AbuseCheckResult:
    """Result of an abuse check."""
    action: str  # "allow", "warn", "throttle", "block"
    reason: Optional[str] = None
    blocked_until: Optional[datetime] = None
    request_count: int = 0
    threshold: int = 0
    abuse_score: float = 0.0


@dataclass
class RequestPattern:
    """Pattern of a single request for abuse tracking."""
    ip: str
    user_id: Optional[str]
    endpoint: str
    method: str
    timestamp: float
    status_code: Optional[int] = None
    is_failed: bool = False


@dataclass
class BlockedIP:
    """Blocked IP record."""
    ip: str
    reason: str
    blocked_at: datetime
    blocked_until: datetime
    request_count: int


@dataclass
class AbuseStats:
    """Summary statistics for abuse activity."""
    total_requests_tracked: int = 0
    total_blocked_ips: int = 0
    currently_blocked: int = 0
    recent_blocks: List[Tuple[str, str]] = field(default_factory=list)  # (ip, reason)
    detection_hits: Dict[str, int] = field(default_factory=dict)  # detection rule -> count


# ============================================================================
# Abuse detector
# ============================================================================

class AbuseDetector:
    """
    Detects and blocks abusive API usage patterns.

    Maintains per-IP and per-user request history with configurable
    detection rules. Automatically blocks IPs exceeding thresholds.
    """

    def __init__(
        self,
        auto_block_cooldown_minutes: int = 15,
        cleanup_interval_seconds: int = 300,
    ):
        """
        Initialize abuse detector.

        Args:
            auto_block_cooldown_minutes: Duration to block IPs after threshold exceeded
            cleanup_interval_seconds: How often to clean up expired data
        """
        self.auto_block_cooldown = timedelta(minutes=auto_block_cooldown_minutes)
        self.cleanup_interval = cleanup_interval_seconds

        # Request history: ip -> list of RequestPattern
        self._request_history: Dict[str, List[RequestPattern]] = defaultdict(list)

        # User attempt tracking: ip -> dict of user_id -> list of RequestPattern
        self._user_attempts: Dict[str, Dict[str, List[RequestPattern]]] = defaultdict(
            lambda: defaultdict(list)
        )

        # Failed login attempts: ip -> list of timestamps
        self._failed_logins: Dict[str, List[float]] = defaultdict(list)

        # API key probe attempts: ip -> list of timestamps
        self._api_key_probes: Dict[str, List[float]] = defaultdict(list)

        # Blocked IPs: ip -> BlockedIP
        self._blocked_ips: Dict[str, BlockedIP] = {}

        # Last enumeration ID per IP: ip -> last_id
        self._last_enumeration_id: Dict[str, int] = {}

        # Metrics
        self._total_requests = 0
        self._detection_hits: Dict[str, int] = defaultdict(int)

        # Thread safety
        self._lock = asyncio.Lock()
        self._last_cleanup = time.time()

    async def record_request(
        self,
        ip: str,
        user_id: Optional[str],
        endpoint: str,
        method: str,
        status_code: Optional[int] = None,
        is_failed: bool = False,
    ) -> None:
        """
        Record a request for abuse analysis.

        Args:
            ip: Client IP address
            user_id: Authenticated user ID (if known)
            endpoint: API endpoint path
            method: HTTP method
            status_code: Response status code
            is_failed: Whether the request failed (for login tracking)
        """
        async with self._lock:
            pattern = RequestPattern(
                ip=ip,
                user_id=user_id,
                endpoint=endpoint,
                method=method,
                timestamp=time.time(),
                status_code=status_code,
                is_failed=is_failed,
            )

            self._request_history[ip].append(pattern)
            self._total_requests += 1

            # Track user attempts from this IP
            if user_id:
                self._user_attempts[ip][user_id].append(pattern)

            # Track failed logins
            if is_failed and endpoint == "/auth/login":
                self._failed_logins[ip].append(time.time())

            # Track invalid API key attempts
            if status_code == 401 and "X-API-Key" in endpoint:
                self._api_key_probes[ip].append(time.time())

            # Periodic cleanup
            await self._cleanup_if_needed()

    async def check_abuse(
        self,
        ip: str,
        user_id: Optional[str] = None,
    ) -> AbuseCheckResult:
        """
        Check if request from IP/user exhibits abusive patterns.

        Returns AbuseCheckResult with action and details.
        """
        async with self._lock:
            # Check if IP is currently blocked
            if ip in self._blocked_ips:
                blocked_record = self._blocked_ips[ip]
                if datetime.now() < blocked_record.blocked_until:
                    return AbuseCheckResult(
                        action="block",
                        reason=blocked_record.reason,
                        blocked_until=blocked_record.blocked_until,
                        request_count=blocked_record.request_count,
                        abuse_score=1.0,
                    )
                else:
                    # Block expired, clean up
                    del self._blocked_ips[ip]

            # Run detection checks
            abuse_score = 0.0
            detections = []

            # Check 1: Brute force login attempts
            result = self._check_brute_force(ip)
            if result["detected"]:
                abuse_score += result["score"]
                detections.append(result["reason"])
                self._detection_hits["brute_force"] += 1

            # Check 2: Credential stuffing
            result = self._check_credential_stuffing(ip)
            if result["detected"]:
                abuse_score += result["score"]
                detections.append(result["reason"])
                self._detection_hits["credential_stuffing"] += 1

            # Check 3: Rapid roster generation
            result = self._check_rapid_generation(ip)
            if result["detected"]:
                abuse_score += result["score"]
                detections.append(result["reason"])
                self._detection_hits["rapid_generation"] += 1

            # Check 4: Data scraping
            result = self._check_data_scraping(ip)
            if result["detected"]:
                abuse_score += result["score"]
                detections.append(result["reason"])
                self._detection_hits["data_scraping"] += 1

            # Check 5: API key probing
            result = self._check_api_key_probing(ip)
            if result["detected"]:
                abuse_score += result["score"]
                detections.append(result["reason"])
                self._detection_hits["api_key_probing"] += 1

            # Check 6: Enumeration patterns
            result = self._check_enumeration(ip)
            if result["detected"]:
                abuse_score += result["score"]
                detections.append(result["reason"])
                self._detection_hits["enumeration"] += 1

            # Determine action based on abuse score
            if abuse_score >= 3.0:
                # Block immediately
                self._apply_block(ip, " + ".join(detections[:2]))
                return AbuseCheckResult(
                    action="block",
                    reason=" + ".join(detections),
                    blocked_until=datetime.now() + self.auto_block_cooldown,
                    abuse_score=abuse_score,
                    threshold=3,
                )
            elif abuse_score >= 2.0:
                # Throttle
                return AbuseCheckResult(
                    action="throttle",
                    reason=" + ".join(detections),
                    abuse_score=abuse_score,
                    threshold=2,
                )
            elif abuse_score >= 1.0:
                # Warn
                return AbuseCheckResult(
                    action="warn",
                    reason=" + ".join(detections),
                    abuse_score=abuse_score,
                    threshold=1,
                )
            else:
                # Allow
                return AbuseCheckResult(
                    action="allow",
                    abuse_score=abuse_score,
                )

    def _check_brute_force(self, ip: str) -> dict:
        """Check for brute force login attempts: >10 in 5 minutes."""
        now = time.time()
        cutoff = now - (5 * 60)  # 5 minutes ago

        # Count failed logins in time window
        recent_failures = [
            t for t in self._failed_logins.get(ip, [])
            if t > cutoff
        ]

        if len(recent_failures) > 10:
            return {
                "detected": True,
                "score": 1.5,
                "reason": f"Brute force: {len(recent_failures)} failed logins in 5min",
            }

        return {"detected": False, "score": 0, "reason": ""}

    def _check_credential_stuffing(self, ip: str) -> dict:
        """Check for credential stuffing: >5 different users in 2 minutes."""
        now = time.time()
        cutoff = now - (2 * 60)  # 2 minutes ago

        users_attempted = set()
        for user_id, patterns in self._user_attempts.get(ip, {}).items():
            for pattern in patterns:
                if pattern.timestamp > cutoff and pattern.endpoint == "/auth/login":
                    users_attempted.add(user_id)

        if len(users_attempted) > 5:
            return {
                "detected": True,
                "score": 1.5,
                "reason": f"Credential stuffing: {len(users_attempted)} users in 2min",
            }

        return {"detected": False, "score": 0, "reason": ""}

    def _check_rapid_generation(self, ip: str) -> dict:
        """Check for rapid roster generation: >20 POSTs to /rosters/generate in 10 minutes."""
        now = time.time()
        cutoff = now - (10 * 60)  # 10 minutes ago

        posts = [
            p for p in self._request_history.get(ip, [])
            if p.timestamp > cutoff
            and p.method == "POST"
            and "/rosters/generate" in p.endpoint
        ]

        if len(posts) > 20:
            return {
                "detected": True,
                "score": 1.0,
                "reason": f"Rapid generation: {len(posts)} roster POSTs in 10min",
            }

        return {"detected": False, "score": 0, "reason": ""}

    def _check_data_scraping(self, ip: str) -> dict:
        """Check for data scraping: >100 GETs to list endpoints in 1 minute."""
        now = time.time()
        cutoff = now - (1 * 60)  # 1 minute ago

        # List endpoints: /employees, /shifts, /venues, etc.
        list_patterns = re.compile(
            r"/(employees|shifts|venues|rosters|forecasts|schedules)"
        )

        scrape_gets = [
            p for p in self._request_history.get(ip, [])
            if p.timestamp > cutoff
            and p.method == "GET"
            and list_patterns.search(p.endpoint)
        ]

        if len(scrape_gets) > 100:
            return {
                "detected": True,
                "score": 1.0,
                "reason": f"Data scraping: {len(scrape_gets)} GETs in 1min",
            }

        return {"detected": False, "score": 0, "reason": ""}

    def _check_api_key_probing(self, ip: str) -> dict:
        """Check for API key probing: >5 invalid keys in 5 minutes."""
        now = time.time()
        cutoff = now - (5 * 60)  # 5 minutes ago

        recent_probes = [
            t for t in self._api_key_probes.get(ip, [])
            if t > cutoff
        ]

        if len(recent_probes) > 5:
            return {
                "detected": True,
                "score": 0.8,
                "reason": f"API key probing: {len(recent_probes)} invalid keys in 5min",
            }

        return {"detected": False, "score": 0, "reason": ""}

    def _check_enumeration(self, ip: str) -> dict:
        """Check for sequential ID patterns (e.g., /employees/1, /employees/2, ...)."""
        patterns = self._request_history.get(ip, [])

        if len(patterns) < 5:
            return {"detected": False, "score": 0, "reason": ""}

        # Look for sequential ID patterns in recent requests
        id_pattern = re.compile(r"/(\w+)/(\d+)(?:/|$)")
        recent = patterns[-20:]  # Last 20 requests

        ids_by_endpoint = defaultdict(list)
        for pattern in recent:
            match = id_pattern.search(pattern.endpoint)
            if match:
                endpoint_base = match.group(1)
                try:
                    entity_id = int(match.group(2))
                    ids_by_endpoint[endpoint_base].append(entity_id)
                except ValueError:
                    pass

        # Check for sequences
        for endpoint_base, ids in ids_by_endpoint.items():
            if len(ids) >= 5:
                # Check if mostly sequential
                sorted_ids = sorted(ids)
                is_sequential = all(
                    sorted_ids[i+1] - sorted_ids[i] <= 2
                    for i in range(len(sorted_ids) - 1)
                )

                if is_sequential:
                    return {
                        "detected": True,
                        "score": 0.7,
                        "reason": f"Enumeration: Sequential IDs on {endpoint_base}",
                    }

        return {"detected": False, "score": 0, "reason": ""}

    def _apply_block(self, ip: str, reason: str) -> None:
        """Block an IP for the configured cooldown period."""
        blocked_until = datetime.now() + self.auto_block_cooldown
        request_count = len(self._request_history.get(ip, []))

        self._blocked_ips[ip] = BlockedIP(
            ip=ip,
            reason=reason,
            blocked_at=datetime.now(),
            blocked_until=blocked_until,
            request_count=request_count,
        )

        logger.warning(f"Blocked IP {ip}: {reason}")

    async def _cleanup_if_needed(self) -> None:
        """Remove stale data periodically."""
        now = time.time()

        if now - self._last_cleanup < self.cleanup_interval:
            return

        cutoff = now - (1 * 60 * 60)  # 1 hour ago

        # Clean request history (keep last hour only)
        for ip in list(self._request_history.keys()):
            self._request_history[ip] = [
                p for p in self._request_history[ip]
                if p.timestamp > cutoff
            ]
            if not self._request_history[ip]:
                del self._request_history[ip]

        # Clean user attempts
        for ip in list(self._user_attempts.keys()):
            for user_id in list(self._user_attempts[ip].keys()):
                self._user_attempts[ip][user_id] = [
                    p for p in self._user_attempts[ip][user_id]
                    if p.timestamp > cutoff
                ]
                if not self._user_attempts[ip][user_id]:
                    del self._user_attempts[ip][user_id]
            if not self._user_attempts[ip]:
                del self._user_attempts[ip]

        # Clean failed login history
        for ip in list(self._failed_logins.keys()):
            self._failed_logins[ip] = [
                t for t in self._failed_logins[ip]
                if t > cutoff
            ]
            if not self._failed_logins[ip]:
                del self._failed_logins[ip]

        # Clean API key probes
        for ip in list(self._api_key_probes.keys()):
            self._api_key_probes[ip] = [
                t for t in self._api_key_probes[ip]
                if t > cutoff
            ]
            if not self._api_key_probes[ip]:
                del self._api_key_probes[ip]

        # Clean expired blocks
        now_dt = datetime.now()
        for ip in list(self._blocked_ips.keys()):
            if self._blocked_ips[ip].blocked_until < now_dt:
                del self._blocked_ips[ip]

        self._last_cleanup = now
        logger.debug("Cleaned up stale abuse detection data")

    def get_blocked_ips(self) -> List[BlockedIP]:
        """Get list of currently blocked IPs."""
        now_dt = datetime.now()
        return [
            record for record in self._blocked_ips.values()
            if record.blocked_until > now_dt
        ]

    def unblock_ip(self, ip: str) -> bool:
        """Manually unblock an IP. Returns True if was blocked."""
        if ip in self._blocked_ips:
            del self._blocked_ips[ip]
            logger.info(f"Unblocked IP {ip}")
            return True
        return False

    def get_abuse_stats(self) -> AbuseStats:
        """Get summary statistics for abuse activity."""
        blocked_ips = self.get_blocked_ips()
        recent_blocks = [
            (record.ip, record.reason)
            for record in sorted(blocked_ips, key=lambda r: r.blocked_at, reverse=True)[:10]
        ]

        return AbuseStats(
            total_requests_tracked=self._total_requests,
            total_blocked_ips=len(self._blocked_ips),
            currently_blocked=len(blocked_ips),
            recent_blocks=recent_blocks,
            detection_hits=dict(self._detection_hits),
        )

    def reset_counters(self, ip: Optional[str] = None) -> None:
        """Reset counters for testing. If ip is None, reset all."""
        if ip is None:
            self._request_history.clear()
            self._user_attempts.clear()
            self._failed_logins.clear()
            self._api_key_probes.clear()
            self._blocked_ips.clear()
            self._total_requests = 0
            self._detection_hits.clear()
            logger.debug("Reset all abuse detection counters")
        else:
            self._request_history.pop(ip, None)
            self._user_attempts.pop(ip, None)
            self._failed_logins.pop(ip, None)
            self._api_key_probes.pop(ip, None)
            self._blocked_ips.pop(ip, None)
            logger.debug(f"Reset abuse detection counters for IP {ip}")
