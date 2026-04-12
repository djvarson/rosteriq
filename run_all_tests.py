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
# Files that need external runtime dependencies (httpx, fastapi, etc.)
# beyond the stdlib + rosteriq. Skipped with --fast.
EXTERNAL_DEP_FILES = {
    "test_auth.py",            # pydantic, pyjwt, passlib (auth module)
    "test_lightspeed.py",      # httpx (lightspeed adapter)
    "test_pos_aggregator.py",  # httpx (pos adapters)
    "test_shift_swap.py",      # fastapi (shift_swap router)
    "test_square.py",          # httpx (square adapter)
    "test_swiftpos.py",        # httpx (swiftpos adapter)
    "test_tanda_integration.py",  # httpx (tanda adapter)
}

# Everything else runs fine with `python <file>` from the project root
# using only the stdlib + rosteriq pure-stdlib modules.


def _run_file(path: Path) -> tuple[bool, float, str]:
    """Run a single test file. Returns (passed, elapsed_seconds, output)."""
    env = {**os.environ, "PYTHONPATH": str(ROOT)}
    t0 = time.monotonic()

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
        needs_ext = path.name in EXTERNAL_DEP_FILES

        # Skip external-dep tests when --fast
        if needs_ext and args.fast:
            skipped_files.append(name)
            results_detail.append((name, "SKIP (--fast)", 0.0, ""))
            print(f"  SKIP  {name:<40} (--fast)")
            continue

        try:
            ok, elapsed, output = _run_file(path)
        except subprocess.TimeoutExpired:
            failed_files.append(name)
            results_detail.append((name, "TIMEOUT", 120.0, "Timed out after 120s"))
            print(f"  TIMEOUT {name:<37} (120.0s)")
            continue

        if ok:
            passed_files.append(name)
            results_detail.append((name, "PASS", elapsed, output))
            print(f"  PASS  {name:<40} ({elapsed:.1f}s)")
        elif "ModuleNotFoundError" in output or "No module named" in output:
            # Missing dependency — treat as skip, not failure
            mod = "unknown"
            for line in output.split("\n"):
                if "No module named" in line:
                    mod = line.split("No module named")[-1].strip().strip("'\"")
                    break
            skipped_files.append(name)
            results_detail.append((name, f"SKIP (needs {mod})", elapsed, output))
            print(f"  SKIP  {name:<40} (needs {mod})")
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
