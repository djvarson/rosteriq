#!/usr/bin/env python3
"""
RosterIQ Smoke Test

Lightweight verification that the app boots and key endpoints respond.
Uses httpx TestClient for in-process testing (no server startup needed).

Exit codes:
    0 = All tests passed
    1 = One or more tests failed
    2 = Import or setup error

Usage:
    python -m rosteriq.scripts.smoke_test
"""

import sys
import logging
from typing import Dict, List, Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("rosteriq.smoke_test")


class SmokeTest:
    """Smoke test runner."""

    def __init__(self):
        """Initialize smoke test."""
        self.results: List[Tuple[str, bool, str]] = []
        self.app = None
        self.client = None

    def setup(self) -> bool:
        """
        Load FastAPI app and create test client.

        Returns:
            True if successful
        """
        try:
            # Import after system is ready
            from rosteriq.api import app
            from httpx import Client

            self.app = app
            self.client = Client(app=self.app, base_url="http://test")
            logger.info("✓ App loaded and test client created")
            return True

        except ImportError as e:
            logger.error(f"Import error: {e}")
            return False
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            return False

    def test_health_endpoint(self) -> bool:
        """Test GET /health returns 200."""
        try:
            response = self.client.get("/health")
            passed = response.status_code == 200
            detail = f"Status {response.status_code}"
            self.results.append(("GET /health", passed, detail))
            return passed
        except Exception as e:
            self.results.append(("GET /health", False, str(e)))
            return False

    def test_venues_endpoint(self) -> bool:
        """Test GET /api/v1/venues returns 200."""
        try:
            response = self.client.get("/api/v1/venues")
            passed = response.status_code == 200
            detail = f"Status {response.status_code}"
            self.results.append(("GET /api/v1/venues", passed, detail))
            return passed
        except Exception as e:
            self.results.append(("GET /api/v1/venues", False, str(e)))
            return False

    def test_auth_login_endpoint(self) -> bool:
        """Test POST /api/v1/auth/login returns 401 with no credentials."""
        try:
            response = self.client.post("/api/v1/auth/login", json={})
            # Should fail auth (401 or 422 validation) but not 500
            passed = response.status_code in [401, 422]
            detail = f"Status {response.status_code} (expected 401 or 422)"
            self.results.append(("POST /api/v1/auth/login (no creds)", passed, detail))
            return passed
        except Exception as e:
            self.results.append(("POST /api/v1/auth/login (no creds)", False, str(e)))
            return False

    def test_docs_endpoint(self) -> bool:
        """Test GET /docs returns 200 (Swagger UI)."""
        try:
            response = self.client.get("/docs")
            passed = response.status_code == 200
            detail = f"Status {response.status_code}"
            self.results.append(("GET /docs", passed, detail))
            return passed
        except Exception as e:
            self.results.append(("GET /docs", False, str(e)))
            return False

    def run_all(self) -> bool:
        """
        Run all smoke tests.

        Returns:
            True if all passed, False if any failed
        """
        logger.info("Starting smoke tests...")
        print("\nRosterIQ Smoke Test")
        print("=" * 70)

        if not self.setup():
            logger.error("Setup failed")
            return False

        all_passed = True
        tests = [
            self.test_health_endpoint,
            self.test_venues_endpoint,
            self.test_auth_login_endpoint,
            self.test_docs_endpoint,
        ]

        for test_func in tests:
            try:
                result = test_func()
                all_passed = all_passed and result
            except Exception as e:
                logger.error(f"Test {test_func.__name__} raised exception: {e}")
                all_passed = False

        # Print results
        print("\nTest Results:")
        print("-" * 70)
        for endpoint, passed, detail in self.results:
            status = "✓ PASS" if passed else "✗ FAIL"
            print(f"{status:8} {endpoint:<35} {detail}")
        print("-" * 70)

        passed_count = sum(1 for _, p, _ in self.results if p)
        total_count = len(self.results)
        print(f"Summary: {passed_count}/{total_count} tests passed\n")

        return all_passed

    def cleanup(self):
        """Clean up test resources."""
        if self.client:
            try:
                self.client.close()
            except Exception as e:
                logger.warning(f"Error closing client: {e}")


def main():
    """CLI entry point."""
    test = SmokeTest()
    try:
        success = test.run_all()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Smoke test error: {e}")
        sys.exit(2)
    finally:
        test.cleanup()


if __name__ == "__main__":
    main()
else:
    # Allow import as module
    pass
