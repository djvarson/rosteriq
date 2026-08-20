"""
Admin endpoints for test reporting and execution.

Provides:
- GET /api/v1/admin/test-report — get last test run results
- POST /api/v1/admin/run-tests — trigger a new test run (async)
- GET /api/v1/admin/test-coverage — get service coverage report

TWO GUARDS, both deliberate:

1. OWNER ONLY. ``_admin_required`` used to be a stub that let everything
   through ("assume all requests to /admin are authorized"), so any signed-in
   user — including a venue's casual staff member — could read internal file
   paths and coverage, and trigger a test run.

2. THE RUNNER IS OFF IN PRODUCTION, FAIL-CLOSED. ``run_all()`` executes the
   whole pytest suite IN-PROCESS on the event loop, and those tests create
   venues, employees and rosters in whatever database the app is pointed at.
   Against prod that means fixtures written into a customer's data and every
   request stalled while it runs. The gate asks "is this provably a dev box?"
   rather than "is this production?" — ENVIRONMENT is not set on Railway, so
   a negative check would have failed open exactly where it matters.
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, status
from pydantic import BaseModel

from rosteriq.middleware.auth import get_current_user, UserContext

logger = logging.getLogger(__name__)

# Import test runner
from rosteriq.tests.run_all_tests import TestRunner, TestRunReport

router = APIRouter(prefix="/api/v1/admin", tags=["admin", "testing"])

# Global test runner instance
_test_runner: Optional[TestRunner] = None
_last_report_json: Optional[str] = None
_test_runner_lock = asyncio.Lock()


def get_test_runner() -> TestRunner:
    """Get or create test runner instance."""
    global _test_runner
    if _test_runner is None:
        _test_runner = TestRunner()
    return _test_runner


# Response models
class TestSummary(BaseModel):
    """Summary of test results."""
    total: int
    passed: int
    failed: int
    errors: int
    skipped: int
    duration_seconds: float
    timestamp: str


class TestResultResponse(BaseModel):
    """Response with test run results."""
    summary: TestSummary
    slowest_tests: list
    file_results: list
    coverage: Dict[str, Any]
    available: bool = True
    message: Optional[str] = None


class TestRunRequest(BaseModel):
    """Request to run tests."""
    verbose: bool = False


class TestRunResponse(BaseModel):
    """Response after triggering test run."""
    message: str
    status: str
    started_at: str


class CoverageResponse(BaseModel):
    """Service coverage report."""
    total_services: int
    covered_services: int
    uncovered_services: int
    coverage_percentage: float
    covered: list
    uncovered: list
    total_test_functions: int


async def _admin_required(user: UserContext = Depends(get_current_user)) -> UserContext:
    """Platform owner only. These endpoints expose the repository's internals
    (file paths, per-file test counts, which services lack coverage) and can
    start a test run — none of that belongs to a venue's manager or staff."""
    if not (getattr(user, "is_owner", False) or user.role == "owner"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This is a platform-owner endpoint",
        )
    return user


def test_runner_enabled() -> bool:
    """May this deployment run the test suite in-process?

    Fail-closed by design: an explicit dev/test ENVIRONMENT enables it, any
    other explicit value disables it, and when ENVIRONMENT is UNSET (which is
    the case on Railway today) it is enabled only where there is no real
    database to damage.
    """
    env = (os.environ.get("ENVIRONMENT") or "").strip().lower()
    if env in ("development", "dev", "test", "testing", "local"):
        return True
    if env:                       # production, staging, anything else: no
        return False
    return not os.environ.get("DATABASE_URL")


@router.get(
    "/test-report",
    response_model=TestResultResponse,
    summary="Get last test run results",
    description="Retrieve the results from the last test run (admin only)",
)
async def get_test_report(
    _=Depends(_admin_required),
) -> TestResultResponse:
    """
    Get the results from the last test run.

    Returns the summary, per-file results, slowest tests, and coverage data.
    """
    global _last_report_json

    runner = get_test_runner()

    if runner.last_report is None:
        return TestResultResponse(
            summary=TestSummary(
                total=0,
                passed=0,
                failed=0,
                errors=0,
                skipped=0,
                duration_seconds=0,
                timestamp=datetime.now().isoformat(),
            ),
            slowest_tests=[],
            file_results=[],
            coverage={
                "total_services": 0,
                "covered_services": 0,
                "coverage_percentage": 0,
            },
            available=False,
            message="No test run available yet. Trigger a test run with POST /api/v1/admin/run-tests",
        )

    report = runner.last_report
    coverage = runner.generate_coverage_report()

    return TestResultResponse(
        summary=TestSummary(
            total=report.total,
            passed=report.passed,
            failed=report.failed,
            errors=report.errors,
            skipped=report.skipped,
            duration_seconds=report.duration_seconds,
            timestamp=report.timestamp,
        ),
        slowest_tests=[
            {"name": name, "duration_ms": duration}
            for name, duration in report.slowest_tests
        ],
        file_results=[
            {
                "file_path": fr.file_path,
                "test_count": fr.test_count,
                "passed": fr.passed,
                "failed": fr.failed,
                "errors": fr.errors,
                "duration_seconds": fr.duration_seconds,
            }
            for fr in report.file_results
        ],
        coverage={
            "total_services": coverage.total_services,
            "covered_services": coverage.covered_services,
            "uncovered_services": coverage.uncovered_services,
            "coverage_percentage": coverage.coverage_percentage,
            "covered": coverage.covered,
            "uncovered": coverage.uncovered,
            "total_test_functions": coverage.total_test_functions,
        },
        available=True,
    )


@router.post(
    "/run-tests",
    response_model=TestRunResponse,
    summary="Run all tests",
    description="Trigger a new test run (runs asynchronously)",
)
async def run_tests(
    request: TestRunRequest,
    background_tasks: BackgroundTasks,
    _=Depends(_admin_required),
) -> TestRunResponse:
    """
    Trigger a new test run.

    The test run happens asynchronously in the background.
    Check GET /api/v1/admin/test-report to see results.

    Args:
        request: TestRunRequest with optional verbose flag

    Returns:
        Confirmation that test run was started
    """
    if not test_runner_enabled():
        logger.warning("Refused an in-process test run (not a development deployment)")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=("The in-process test runner is disabled on this deployment. "
                    "It writes test fixtures into the live database and blocks "
                    "the event loop while it runs — run the suite in CI or "
                    "locally instead."),
        )

    async with _test_runner_lock:
        runner = get_test_runner()

        # Add background task to run tests
        async def _run_tests_bg():
            """Run tests in background."""
            try:
                runner.run_all(verbose=request.verbose)
            except Exception as e:
                print(f"Error running tests: {e}")

        background_tasks.add_task(_run_tests_bg)

    return TestRunResponse(
        message="Test run started asynchronously. Check /api/v1/admin/test-report for results.",
        status="running",
        started_at=datetime.now().isoformat(),
    )


@router.get(
    "/test-coverage",
    response_model=CoverageResponse,
    summary="Get service coverage report",
    description="Get coverage of which services have tests (admin only)",
)
async def get_coverage_report(
    _=Depends(_admin_required),
) -> CoverageResponse:
    """
    Get the service coverage report.

    Shows which RosterIQ services have test files, and which are uncovered.

    Returns:
        CoverageResponse with coverage statistics
    """
    runner = get_test_runner()

    # If no tests have been run yet, run them first
    if runner.last_report is None:
        runner.run_all(verbose=False)

    coverage = runner.generate_coverage_report()

    return CoverageResponse(
        total_services=coverage.total_services,
        covered_services=coverage.covered_services,
        uncovered_services=coverage.uncovered_services,
        coverage_percentage=coverage.coverage_percentage,
        covered=coverage.covered,
        uncovered=coverage.uncovered,
        total_test_functions=coverage.total_test_functions,
    )
