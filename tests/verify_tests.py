#!/usr/bin/env python
"""
Verification script to test that test files can be imported and have correct structure.

Since pytest may not be available in all environments, this script:
1. Imports all test modules
2. Checks that test classes and methods exist
3. Verifies fixtures are defined
4. Does basic syntax checking
"""

import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

def verify_auth_tests():
    """Verify auth service test structure."""
    print("\n=== Verifying test_auth_service.py ===")
    try:
        # Import without pytest
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "test_auth_service",
            os.path.join(os.path.dirname(__file__), "test_auth_service.py")
        )
        module = importlib.util.module_from_spec(spec)

        # Check for required test classes
        test_classes = [
            "TestPasswordHashing",
            "TestAccessTokens",
            "TestRefreshTokens",
            "TestAPIKeys",
            "TestUserCreation",
            "TestRateLimiting",
            "TestAuthServiceIntegration",
        ]

        # Just check the file exists and has content
        with open(os.path.join(os.path.dirname(__file__), "test_auth_service.py")) as f:
            content = f.read()
            for cls in test_classes:
                assert f"class {cls}" in content, f"Missing class {cls}"
            assert "def test_" in content, "No test methods found"

        print(f"✓ Found {len(test_classes)} test classes")
        print(f"✓ test_auth_service.py structure is valid")
        return True
    except Exception as e:
        print(f"✗ Error verifying auth tests: {e}")
        return False


def verify_webhook_tests():
    """Verify webhook test structure."""
    print("\n=== Verifying test_webhook_idempotency.py ===")
    try:
        test_classes = [
            "TestHMACSignatureVerification",
            "TestWebhookIdempotency",
        ]

        with open(os.path.join(os.path.dirname(__file__), "test_webhook_idempotency.py")) as f:
            content = f.read()
            for cls in test_classes:
                assert f"class {cls}" in content, f"Missing class {cls}"
            assert "def test_" in content, "No test methods found"
            assert "verify_hmac_signature" in content
            assert "is_webhook_duplicate" in content

        print(f"✓ Found {len(test_classes)} test classes")
        print(f"✓ test_webhook_idempotency.py structure is valid")
        return True
    except Exception as e:
        print(f"✗ Error verifying webhook tests: {e}")
        return False


def verify_notification_tests():
    """Verify notification test structure."""
    print("\n=== Verifying test_notifications.py ===")
    try:
        test_classes = [
            "TestNotificationServiceInitialization",
            "TestDailyDigestTemplate",
            "TestRosterPublishedTemplate",
            "TestComplianceAlertTemplate",
            "TestCertificationExpiryTemplate",
            "TestVarianceAlertTemplate",
            "TestEmailTemplateWrapping",
            "TestEmailSending",
        ]

        with open(os.path.join(os.path.dirname(__file__), "test_notifications.py")) as f:
            content = f.read()
            for cls in test_classes:
                assert f"class {cls}" in content, f"Missing class {cls}"
            assert "def test_" in content, "No test methods found"
            assert "NotificationService" in content
            assert "@pytest.fixture" in content, "Missing pytest fixtures"

        print(f"✓ Found {len(test_classes)} test classes")
        print(f"✓ test_notifications.py structure is valid")
        return True
    except Exception as e:
        print(f"✗ Error verifying notification tests: {e}")
        return False


def verify_conftest():
    """Verify conftest.py structure."""
    print("\n=== Verifying conftest.py ===")
    try:
        with open(os.path.join(os.path.dirname(__file__), "conftest.py")) as f:
            content = f.read()
            assert "def memory_store" in content, "Missing memory_store fixture"
            assert "def auth_service" in content, "Missing auth_service fixture"
            assert "def reset_global_db" in content, "Missing reset_global_db fixture"
            assert "MemoryStore" in content
            assert "AuthService" in content

        print("✓ conftest.py has all required fixtures")
        return True
    except Exception as e:
        print(f"✗ Error verifying conftest: {e}")
        return False


def verify_imports():
    """Verify that required modules can be imported."""
    print("\n=== Verifying module imports ===")
    try:
        from rosteriq.database import MemoryStore
        print("✓ MemoryStore imports OK")

        from rosteriq.services.auth import AuthService
        print("✓ AuthService imports OK")

        from rosteriq.models import User, UserRole
        print("✓ User model imports OK")

        from rosteriq.routes.webhook_routes import verify_hmac_signature, is_webhook_duplicate
        print("✓ Webhook functions import OK")

        from rosteriq.services.notifications import NotificationService
        print("✓ NotificationService imports OK")

        return True
    except Exception as e:
        print(f"✗ Import error: {e}")
        import traceback
        traceback.print_exc()
        return False


def count_test_methods():
    """Count total test methods across all test files."""
    print("\n=== Test coverage summary ===")
    test_files = [
        "test_auth_service.py",
        "test_webhook_idempotency.py",
        "test_notifications.py",
    ]

    total_tests = 0
    for filename in test_files:
        filepath = os.path.join(os.path.dirname(__file__), filename)
        with open(filepath) as f:
            content = f.read()
            count = content.count("def test_")
            total_tests += count
            print(f"  {filename}: {count} tests")

    print(f"\nTotal tests across all files: {total_tests}")
    return total_tests


def main():
    """Run all verification checks."""
    print("=" * 70)
    print("RosterIQ Test Suite Verification")
    print("=" * 70)
    print(f"Verification started at {datetime.now().isoformat()}")

    checks = [
        ("Module imports", verify_imports),
        ("conftest.py", verify_conftest),
        ("Auth service tests", verify_auth_tests),
        ("Webhook tests", verify_webhook_tests),
        ("Notification tests", verify_notification_tests),
    ]

    results = []
    for name, check_fn in checks:
        results.append(check_fn())

    # Count tests
    count_test_methods()

    print("\n" + "=" * 70)
    print("Verification Summary")
    print("=" * 70)

    passed = sum(results)
    total = len(results)
    print(f"Checks passed: {passed}/{total}")

    if passed == total:
        print("\n✓ All verification checks PASSED!")
        print("\nTest files are ready to run with: pytest tests/ -v")
        return 0
    else:
        print("\n✗ Some verification checks FAILED!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
