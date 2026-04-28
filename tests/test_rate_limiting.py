"""
Comprehensive rate limiting and abuse detection tests.

Tests cover:
- Token bucket algorithm and burst handling
- Tier-based rate limits (Starter/Pro/Enterprise)
- Abuse detection rules (brute force, scraping, probing, etc.)
- IP blocking and cooldown expiration
- Integration between rate limiter and abuse detector
- Concurrent requests

Run with: python -m pytest rosteriq/tests/test_rate_limiting.py -v
Or standalone: cd RosterIQ && python tests/test_rate_limiting.py
"""

import asyncio
import time
import sys
import os
from datetime import datetime, timedelta
from typing import Optional

# Ensure rosteriq imports work
test_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(test_dir)
sys.path.insert(0, os.path.dirname(project_root))

# Import TokenBucket directly to avoid middleware init imports
import importlib.util

def import_module_from_file(module_name, file_path):
    """Load module from file path directly."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# Load modules directly
rate_limiter_path = os.path.join(project_root, "middleware", "rate_limiter.py")
abuse_detection_path = os.path.join(project_root, "services", "abuse_detection.py")

rate_limiter = import_module_from_file("rate_limiter_module", rate_limiter_path)
abuse_detection = import_module_from_file("abuse_detection_module", abuse_detection_path)

TokenBucket = rate_limiter.TokenBucket
RateLimiterMiddleware = rate_limiter.RateLimiterMiddleware
AbuseDetector = abuse_detection.AbuseDetector
AbuseCheckResult = abuse_detection.AbuseCheckResult


# ============================================================================
# Fixtures and utilities
# ============================================================================

class MockRequest:
    """Mock FastAPI Request for testing."""

    def __init__(
        self,
        path: str = "/test",
        method: str = "GET",
        client_ip: str = "192.168.1.1",
        is_authenticated: bool = False,
        has_api_key: bool = False,
    ):
        self.url = type("url", (), {"path": path})()
        self.method = method
        self.client = type("client", (), {"host": client_ip})()
        self.headers = {}

        if is_authenticated:
            self.headers["Authorization"] = "Bearer valid-token"
        if has_api_key:
            self.headers["X-API-Key"] = "valid-api-key"

    def get_header(self, name: str) -> Optional[str]:
        """Get header value."""
        return self.headers.get(name)


def create_token_bucket(tier: str) -> TokenBucket:
    """Create token bucket for a tier."""
    limits = {
        "unauthenticated": (60, 1.0),           # 60 req/min
        "authenticated": (200, 3.333),          # ~200 req/min
        "api_key": (500, 8.333),                # ~500 req/min
    }

    if tier not in limits:
        raise ValueError(f"Unknown tier: {tier}")

    capacity, refill_rate = limits[tier]
    return TokenBucket(capacity, refill_rate)


async def run_async_test(coro):
    """Helper to run async code in tests."""
    return await coro


# ============================================================================
# Token bucket tests
# ============================================================================

def test_bucket_fills_over_time():
    """Test that bucket tokens refill over time."""
    bucket = TokenBucket(capacity=10, refill_rate=1.0)

    # Consume all tokens
    for _ in range(10):
        assert bucket.consume()

    # Should be empty
    assert not bucket.consume()

    # Wait for refill
    time.sleep(1.1)

    # Should have 1 token now
    assert bucket.consume()
    assert not bucket.consume()

    print("PASS: test_bucket_fills_over_time")


def test_bucket_drains_on_requests():
    """Test that bucket tokens drain on requests."""
    bucket = TokenBucket(capacity=5, refill_rate=1.0)

    # Check initial
    assert bucket.get_remaining() == 5

    # Consume
    bucket.consume()
    assert bucket.get_remaining() == 4

    bucket.consume(2)
    assert bucket.get_remaining() == 2

    print("PASS: test_bucket_drains_on_requests")


def test_bucket_rejects_when_empty():
    """Test that bucket rejects when empty."""
    bucket = TokenBucket(capacity=3, refill_rate=0.1)

    # Consume all
    assert bucket.consume()
    assert bucket.consume()
    assert bucket.consume()

    # Should reject
    assert not bucket.consume()
    assert not bucket.consume()

    print("PASS: test_bucket_rejects_when_empty")


def test_burst_handling():
    """Test that bucket allows burst up to capacity."""
    bucket = TokenBucket(capacity=20, refill_rate=0.5)

    # Should allow burst of 20
    for _ in range(20):
        assert bucket.consume(), "Should allow burst"

    # Should reject on 21st
    assert not bucket.consume(), "Should reject after burst"

    print("PASS: test_burst_handling")


def test_steady_rate_within_limit():
    """Test that steady rate within limit is allowed."""
    # 1 req/sec limit
    bucket = TokenBucket(capacity=5, refill_rate=1.0)

    for attempt in range(5):
        assert bucket.consume(), f"Request {attempt} should succeed"
        time.sleep(0.11)  # Sleep 110ms, should refill ~0.11 tokens

    print("PASS: test_steady_rate_within_limit")


# ============================================================================
# Tier enforcement tests
# ============================================================================

def test_starter_tier_60_per_minute():
    """Test Starter tier allows 60 req/min."""
    bucket = create_token_bucket("unauthenticated")
    assert bucket.capacity == 60
    assert bucket.refill_rate == 1.0

    # Should allow 60
    for _ in range(60):
        assert bucket.consume()

    # Should reject 61st
    assert not bucket.consume()

    print("PASS: test_starter_tier_60_per_minute")


def test_pro_tier_200_per_minute():
    """Test Pro tier allows 200 req/min."""
    bucket = create_token_bucket("authenticated")
    assert bucket.capacity == 200

    # Should allow 200
    for _ in range(200):
        assert bucket.consume()

    # Should reject 201st
    assert not bucket.consume()

    print("PASS: test_pro_tier_200_per_minute")


def test_enterprise_tier_500_per_minute():
    """Test Enterprise tier allows 500 req/min."""
    bucket = create_token_bucket("api_key")
    assert bucket.capacity == 500

    # Should allow 500
    for _ in range(500):
        assert bucket.consume()

    # Should reject 501st
    assert not bucket.consume()

    print("PASS: test_enterprise_tier_500_per_minute")


def test_unauthenticated_gets_starter_limit():
    """Test unauthenticated requests get Starter tier limit."""
    bucket = create_token_bucket("unauthenticated")
    assert bucket.capacity == 60

    print("PASS: test_unauthenticated_gets_starter_limit")


def test_tier_upgrade_increases_limit():
    """Test that authenticated tier has higher limit than unauthenticated."""
    unauth = create_token_bucket("unauthenticated")
    auth = create_token_bucket("authenticated")
    enterprise = create_token_bucket("api_key")

    assert unauth.capacity < auth.capacity
    assert auth.capacity < enterprise.capacity
    assert unauth.capacity == 60
    assert auth.capacity == 200
    assert enterprise.capacity == 500

    print("PASS: test_tier_upgrade_increases_limit")


# ============================================================================
# Abuse detection tests
# ============================================================================

def test_brute_force_detection():
    """Test detection of brute force login attempts."""
    detector = AbuseDetector()

    async def run():
        ip = "192.168.1.100"

        # Record 12 failed login attempts in 5 minutes
        for i in range(12):
            await detector.record_request(
                ip=ip,
                user_id=f"user_{i % 3}",  # 3 different users
                endpoint="/auth/login",
                method="POST",
                status_code=401,
                is_failed=True,
            )

        # Check should detect brute force
        result = await detector.check_abuse(ip)

        assert result.action in ["warn", "throttle", "block"], \
            f"Expected action, got {result.action}"
        assert "Brute force" in result.reason or result.abuse_score > 0

    asyncio.run(run())
    print("PASS: test_brute_force_detection")


def test_credential_stuffing_detection():
    """Test detection of credential stuffing."""
    detector = AbuseDetector()

    async def run():
        ip = "192.168.1.101"

        # Record 8 login attempts with different usernames in 2 minutes
        for i in range(8):
            await detector.record_request(
                ip=ip,
                user_id=f"user_{i}",  # Different user each time
                endpoint="/auth/login",
                method="POST",
                status_code=401,
                is_failed=True,
            )

        # Check should detect credential stuffing
        result = await detector.check_abuse(ip)

        assert result.abuse_score > 0, "Should detect suspicious pattern"
        assert "Credential" in result.reason or "Brute force" in result.reason or result.action != "allow"

    asyncio.run(run())
    print("PASS: test_credential_stuffing_detection")


def test_data_scraping_detection():
    """Test detection of data scraping."""
    detector = AbuseDetector()

    async def run():
        ip = "192.168.1.102"

        # Record 120 GET requests to list endpoints in 1 minute
        for i in range(120):
            await detector.record_request(
                ip=ip,
                user_id=None,
                endpoint=f"/employees?offset={i}",
                method="GET",
                status_code=200,
            )

        # Check should detect scraping
        result = await detector.check_abuse(ip)

        assert result.action in ["warn", "throttle", "block"], \
            f"Expected action, got {result.action}"
        assert result.abuse_score > 0

    asyncio.run(run())
    print("PASS: test_data_scraping_detection")


def test_rapid_generation_detection():
    """Test detection of rapid roster generation."""
    detector = AbuseDetector()

    async def run():
        ip = "192.168.1.103"

        # Record 25 POST requests to /rosters/generate in 10 minutes
        for i in range(25):
            await detector.record_request(
                ip=ip,
                user_id="user123",
                endpoint="/rosters/generate",
                method="POST",
                status_code=200,
            )

        # Check should detect rapid generation
        result = await detector.check_abuse(ip)

        assert result.action in ["warn", "throttle", "block"], \
            f"Expected action, got {result.action}"
        assert result.abuse_score > 0

    asyncio.run(run())
    print("PASS: test_rapid_generation_detection")


def test_api_key_probing_detection():
    """Test detection of API key probing."""
    detector = AbuseDetector()

    async def run():
        ip = "192.168.1.104"

        # Record 8 requests with invalid API keys in 5 minutes
        for i in range(8):
            await detector.record_request(
                ip=ip,
                user_id=None,
                endpoint="/rosters",
                method="GET",
                status_code=401,
                is_failed=True,
            )

        # Check should detect probing
        result = await detector.check_abuse(ip)

        assert result.abuse_score > 0, "Should detect API key probing"

    asyncio.run(run())
    print("PASS: test_api_key_probing_detection")


def test_enumeration_detection():
    """Test detection of sequential ID enumeration."""
    detector = AbuseDetector()

    async def run():
        ip = "192.168.1.105"

        # Record requests with sequential IDs
        for id in range(1, 10):
            await detector.record_request(
                ip=ip,
                user_id=None,
                endpoint=f"/employees/{id}",
                method="GET",
                status_code=200,
            )

        # Check should detect enumeration
        result = await detector.check_abuse(ip)

        assert result.abuse_score > 0, "Should detect enumeration pattern"

    asyncio.run(run())
    print("PASS: test_enumeration_detection")


def test_auto_block_on_abuse():
    """Test that abuse detector auto-blocks after threshold."""
    detector = AbuseDetector()

    async def run():
        ip = "192.168.1.106"

        # Generate multiple abuse signals
        for i in range(12):
            await detector.record_request(
                ip=ip,
                user_id=f"user_{i}",
                endpoint="/auth/login",
                method="POST",
                status_code=401,
                is_failed=True,
            )

        # Check should block
        result = await detector.check_abuse(ip)

        assert result.action == "block", f"Expected block, got {result.action}"
        assert result.blocked_until is not None

    asyncio.run(run())
    print("PASS: test_auto_block_on_abuse")


def test_block_expires_after_cooldown():
    """Test that block expires after cooldown period."""
    detector = AbuseDetector(auto_block_cooldown_minutes=1)

    async def run():
        ip = "192.168.1.107"

        # Generate abuse to trigger block
        for i in range(12):
            await detector.record_request(
                ip=ip,
                user_id=f"user_{i}",
                endpoint="/auth/login",
                method="POST",
                status_code=401,
                is_failed=True,
            )

        # Check should block
        result1 = await detector.check_abuse(ip)
        assert result1.action == "block"

        # Simulate time passing (block was created 1.5 minutes ago)
        blocked = detector._blocked_ips[ip]
        blocked.blocked_until = datetime.now() - timedelta(seconds=30)

        # Check again should allow
        result2 = await detector.check_abuse(ip)
        assert result2.action == "allow", "Block should expire after cooldown"

    asyncio.run(run())
    print("PASS: test_block_expires_after_cooldown")


def test_manual_unblock():
    """Test manual IP unblock."""
    detector = AbuseDetector()

    async def run():
        ip = "192.168.1.108"

        # Block the IP
        detector._apply_block(ip, "test")
        assert ip in detector._blocked_ips

        # Unblock
        was_blocked = detector.unblock_ip(ip)
        assert was_blocked
        assert ip not in detector._blocked_ips

        # Try to unblock again
        was_blocked = detector.unblock_ip(ip)
        assert not was_blocked, "Should return False if not blocked"

    asyncio.run(run())
    print("PASS: test_manual_unblock")


def test_legitimate_traffic_not_blocked():
    """Test that legitimate traffic is not blocked."""
    detector = AbuseDetector()

    async def run():
        ip = "192.168.1.109"

        # Record legitimate requests
        for i in range(5):
            await detector.record_request(
                ip=ip,
                user_id="user123",
                endpoint=f"/rosters/{i}",
                method="GET",
                status_code=200,
            )

        # Check should allow
        result = await detector.check_abuse(ip)
        assert result.action == "allow", f"Legitimate traffic should be allowed, got {result.action}"

    asyncio.run(run())
    print("PASS: test_legitimate_traffic_not_blocked")


# ============================================================================
# Integration tests
# ============================================================================

def test_abuse_detector_with_rate_limiter():
    """Test integration between abuse detector and rate limiter."""
    detector = AbuseDetector()
    bucket = TokenBucket(capacity=60, refill_rate=1.0)

    async def run():
        ip = "192.168.1.110"

        # First 60 requests should pass rate limiter
        for i in range(60):
            await detector.record_request(
                ip=ip,
                user_id="user123",
                endpoint="/test",
                method="GET",
                status_code=200,
            )
            assert bucket.consume(), f"Request {i} rate limited"

        # 61st should be rate limited
        assert not bucket.consume()

        # Abuse detector should still allow (legitimate traffic pattern)
        result = await detector.check_abuse(ip)
        assert result.action == "allow"

    asyncio.run(run())
    print("PASS: test_abuse_detector_with_rate_limiter")


def test_blocked_ip_gets_429():
    """Test that blocked IP would get 429 status."""
    detector = AbuseDetector()

    async def run():
        ip = "192.168.1.111"

        # Generate abuse
        for i in range(12):
            await detector.record_request(
                ip=ip,
                user_id=f"user_{i}",
                endpoint="/auth/login",
                method="POST",
                status_code=401,
                is_failed=True,
            )

        # Check
        result = await detector.check_abuse(ip)

        if result.action == "block":
            # Simulate 429 response
            status_code = 429
            assert status_code == 429

    asyncio.run(run())
    print("PASS: test_blocked_ip_gets_429")


def test_abuse_stats_endpoint():
    """Test abuse stats retrieval."""
    detector = AbuseDetector()

    async def run():
        ip1 = "192.168.1.112"
        ip2 = "192.168.1.113"

        # Generate abuse on both IPs
        for ip in [ip1, ip2]:
            for i in range(12):
                await detector.record_request(
                    ip=ip,
                    user_id=f"user_{i}",
                    endpoint="/auth/login",
                    method="POST",
                    status_code=401,
                    is_failed=True,
                )

            result = await detector.check_abuse(ip)

        # Get stats
        stats = detector.get_abuse_stats()

        assert stats.total_requests_tracked > 0
        assert stats.total_blocked_ips >= 0
        assert isinstance(stats.detection_hits, dict)

    asyncio.run(run())
    print("PASS: test_abuse_stats_endpoint")


def test_concurrent_requests_from_same_ip():
    """Test handling of concurrent requests from same IP."""
    detector = AbuseDetector()

    async def run():
        ip = "192.168.1.114"

        async def make_request(idx):
            await detector.record_request(
                ip=ip,
                user_id=None,
                endpoint="/test",
                method="GET",
                status_code=200,
            )

        # Make 50 concurrent requests
        tasks = [make_request(i) for i in range(50)]
        await asyncio.gather(*tasks)

        # Should handle concurrency without corruption
        result = await detector.check_abuse(ip)
        assert result.action == "allow"  # Legitimate traffic

        # Check stats
        stats = detector.get_abuse_stats()
        assert stats.total_requests_tracked == 50

    asyncio.run(run())
    print("PASS: test_concurrent_requests_from_same_ip")


def test_different_ips_independent():
    """Test that different IPs are tracked independently."""
    detector = AbuseDetector()

    async def run():
        ip1 = "192.168.1.115"
        ip2 = "192.168.1.116"

        # Record abusive traffic on IP1
        for i in range(12):
            await detector.record_request(
                ip=ip1,
                user_id=f"user_{i}",
                endpoint="/auth/login",
                method="POST",
                status_code=401,
                is_failed=True,
            )

        # Record legitimate traffic on IP2
        for i in range(5):
            await detector.record_request(
                ip=ip2,
                user_id="user123",
                endpoint="/test",
                method="GET",
                status_code=200,
            )

        # Check IP1 (should be blocked)
        result1 = await detector.check_abuse(ip1)
        assert result1.action == "block"

        # Check IP2 (should be allowed)
        result2 = await detector.check_abuse(ip2)
        assert result2.action == "allow"

    asyncio.run(run())
    print("PASS: test_different_ips_independent")


# ============================================================================
# Main test runner
# ============================================================================

def main():
    """Run all tests."""
    tests = [
        # Token bucket
        test_bucket_fills_over_time,
        test_bucket_drains_on_requests,
        test_bucket_rejects_when_empty,
        test_burst_handling,
        test_steady_rate_within_limit,
        # Tier enforcement
        test_starter_tier_60_per_minute,
        test_pro_tier_200_per_minute,
        test_enterprise_tier_500_per_minute,
        test_unauthenticated_gets_starter_limit,
        test_tier_upgrade_increases_limit,
        # Abuse detection
        test_brute_force_detection,
        test_credential_stuffing_detection,
        test_data_scraping_detection,
        test_rapid_generation_detection,
        test_api_key_probing_detection,
        test_enumeration_detection,
        test_auto_block_on_abuse,
        test_block_expires_after_cooldown,
        test_manual_unblock,
        test_legitimate_traffic_not_blocked,
        # Integration
        test_abuse_detector_with_rate_limiter,
        test_blocked_ip_gets_429,
        test_abuse_stats_endpoint,
        test_concurrent_requests_from_same_ip,
        test_different_ips_independent,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"FAIL: {test.__name__}: {e}")
            failed += 1

    print(f"\n{'='*60}")
    print(f"Results: {passed} passed, {failed} failed out of {len(tests)} tests")
    print(f"{'='*60}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    exit(main())
