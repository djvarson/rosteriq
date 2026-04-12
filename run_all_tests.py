#!/usr/bin/env python3
"""
Unified test runner for RosterIQ.

Discovers and runs all test files under tests/, handling three runner styles:
  1. Custom standalone runners (bare test_ functions + inline __main__ block)
  2. unittest.TestCase suites with unittest.main()
  3. pytest-dependent suites (skipped if pytest is not installed)

Usage:
    python run_all_tests.py              # run everything
    python run_all_tests.py --fast       # skip pytest-dependent tests
    python run_all_tests.py --only NAME  # run a single test file (e.g. --only accountability)

Exit code 0 if all tests pass, 1 otherwise.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TESTS_DIR = ROOT / "tests"

# ── Classification ──────────────────────────────────────────────────────────
# Files that require pytest (they import pytest at module level).
PYTEST_FILES = {
    "test_ask_context.py",
    "test_lightspeed.py",
    "test_pos_aggregator.py",
    "test_shift_swap.py",
    "test_square.py",
    "test_swiftpos.py",
    "test_tanda_integration.py",
}

# Everything else runs fine with `python <file>` from the project root.
# unittest.TestCase files and custom-runner files both work this way.


def _has_pytest() -> bool:
    """Check if pytest is available in the current environment."""
    try:
        import pytest  # noqa: F401
        return True
    except ImportError:
        return False


def _run_file(path: Path, *, use_pytest: bool = False) -> tuple[bool, float, str]:
    """Run a single test file. Returns (passed, elapsed_seconds, output)."""
    env = {**os.environ, "PYTHONPATH": str(ROOT)}
    t0 = time.monotonic()

    if use_pytest:
        cmd = [sys.executable, "-m", "pytest", str(path), "-q", "--tb=short"]
    else:
        cmd = [sys.executable, str(path)]

    result = subprocess.run(
        cmd,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=120,
        env=env,
    )
    elapsed = time.monotonic() - t0
    output = (result.stdout + result.stderr).strip()
    return result.returncode == 0, elapsed, output


def main() -> int:
    parser = argparse.ArgumentParser(description="RosterIQ unified test runner")
    parser.add_argument("--fast", action="store_true", help="Skip pytest-dependent tests")
    parser.add_argument("--only", type=str, default=None, help="Run a single test file (partial name match)")
    args = parser.parse_args()

    test_files = sorted(TESTS_DIR.glob("test_*.py"))
    if not test_files:
        print("No test files found!")
        return 1

    # Filter if --only
    if args.only:
        needle = args.only.lower()
        test_files = [f for f in test_files if needle in f.stem.lower()]
        if not test_files:
            print(f"No test file matching '{args.only}'")
            return 1

    pytest_available = _has_pytest()
    total_start = time.monotonic()

    passed_files: list[str] = []
    failed_files: list[str] = []
    skipped_files: list[str] = []
    results_detail: list[tuple[str, str, float, str]] = []  # (name, status, time, output)

    print("=" * 68)
    print(f"  RosterIQ Test Runner — {len(test_files)} file(s)")
    print("=" * 68)

    for path in test_files:
        name = path.stem
        is_pytest = path.name in PYTEST_FILES

        # Skip pytest tests if --fast or pytest not available
        if is_pytest and (args.fast or not pytest_available):
            reason = "--fast" if args.fast else "pytest not installed"
            skipped_files.append(name)
            results_detail.append((name, f"SKIP ({reason})", 0.0, ""))
            print(f"  SKIP  {name:<40} ({reason})")
            continue

        try:
            ok, elapsed, output = _run_file(path, use_pytest=is_pytest)
        except subprocess.TimeoutExpired:
            failed_files.append(name)
            results_detail.append((name, "TIMEOUT", 120.0, "Timed out after 120s"))
            print(f"  TIMEOUT {name:<37} (120.0s)")
            continue

        if ok:
            passed_files.append(name)
            results_detail.append((name, "PASS", elapsed, output))
            print(f"  PASS  {name:<40} ({elapsed:.1f}s)")
        else:
            failed_files.append(name)
            results_detail.append((name, "FAIL", elapsed, output))
            print(f"  FAIL  {name:<40} ({elapsed:.1f}s)")

    total_elapsed = time.monotonic() - total_start

    # ── Summary ─────────────────────────────────────────────────────────────
    print()
    print("=" * 68)
    print(f"  PASSED: {len(passed_files)}   FAILED: {len(failed_files)}   SKIPPED: {len(skipped_files)}   ({total_elapsed:.1f}s)")
    print("=" * 68)

    # Show failure details
    if failed_files:
        print("\n── Failure Details ──\n")
        for name, status, elapsed, output in results_detail:
            if status == "FAIL" or status == "TIMEOUT":
                print(f"{'─' * 50}")
                print(f"  {name} ({status})")
                print(f"{'─' * 50}")
                # Show last 30 lines of output
                lines = output.split("\n")
                for line in lines[-30:]:
                    print(f"    {line}")
                print()

    return 0 if len(failed_files) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
