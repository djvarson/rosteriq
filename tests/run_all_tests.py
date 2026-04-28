"""
Automated test runner for RosterIQ.

Discovers and runs all tests, generates coverage-like reports showing:
- Test pass/fail/error counts
- Per-file test results
- Service coverage mapping
- JSON and human-readable reports
"""

import sys
import os
import ast
import json
import time
import traceback
import importlib.util
from pathlib import Path
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any
from io import StringIO

# Ensure rosteriq imports work
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


@dataclass
class TestResult:
    """Result of a single test function."""
    name: str
    status: str  # pass, fail, error, skip
    duration_ms: float
    error_message: Optional[str] = None
    error_type: Optional[str] = None
    traceback_text: Optional[str] = None


@dataclass
class FileResult:
    """Result of running all tests in a test file."""
    file_path: str
    test_count: int
    passed: int
    failed: int
    errors: int
    skipped: int
    duration_seconds: float
    test_results: List[TestResult]


@dataclass
class TestRunReport:
    """Overall test run report."""
    total: int
    passed: int
    failed: int
    errors: int
    skipped: int
    duration_seconds: float
    file_results: List[FileResult]
    timestamp: str
    slowest_tests: List[Tuple[str, float]]  # List of (test_name, duration_ms)


@dataclass
class CoverageReport:
    """Coverage report mapping services to tests."""
    total_services: int
    covered_services: int
    uncovered_services: int
    coverage_percentage: float
    covered: List[str]
    uncovered: List[str]
    total_test_functions: int


class TestRunner:
    """Discovers and runs all tests, generates reports."""

    def __init__(self):
        """Initialize test runner."""
        self.test_dir = Path(__file__).parent
        self.root_dir = self.test_dir.parent
        self.last_report: Optional[TestRunReport] = None
        self.all_slowest_tests: List[Tuple[str, float]] = []

    def discover_tests(self, test_dir: str = "tests") -> List[Path]:
        """
        Discover all test_*.py files in the test directory.

        Args:
            test_dir: Name of test directory (relative to RosterIQ root)

        Returns:
            List of Path objects for test files
        """
        test_path = self.root_dir / test_dir
        if not test_path.exists():
            print(f"Test directory not found: {test_path}")
            return []

        test_files = sorted(test_path.glob("test_*.py"))
        return test_files

    def _extract_test_functions(self, file_path: Path) -> List[str]:
        """
        Use AST to extract all test function names from a file.

        Args:
            file_path: Path to the test file

        Returns:
            List of test function names (starting with test_)
        """
        try:
            with open(file_path, 'r') as f:
                tree = ast.parse(f.read())

            test_functions = []
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef) and node.name.startswith('test_'):
                    test_functions.append(node.name)
                elif isinstance(node, ast.ClassDef):
                    # Also extract test methods from classes
                    for item in node.body:
                        if isinstance(item, ast.FunctionDef) and item.name.startswith('test_'):
                            test_functions.append(f"{node.name}.{item.name}")

            return test_functions
        except Exception as e:
            print(f"Error extracting tests from {file_path}: {e}")
            return []

    def _run_single_test(self, test_func, test_name: str) -> TestResult:
        """
        Run a single test function and capture result.

        Args:
            test_func: The callable test function
            test_name: Name of the test for reporting

        Returns:
            TestResult with status, duration, and any error info
        """
        start_time = time.time()
        duration_ms = 0

        # Capture stdout/stderr
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = StringIO()
        sys.stderr = StringIO()

        try:
            # Run the test
            test_func()

            duration_ms = (time.time() - start_time) * 1000
            status = "pass"
            error_message = None
            error_type = None
            traceback_text = None

        except AssertionError as e:
            duration_ms = (time.time() - start_time) * 1000
            status = "fail"
            error_message = str(e)
            error_type = "AssertionError"
            traceback_text = traceback.format_exc()

        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            status = "error"
            error_message = str(e)
            error_type = type(e).__name__
            traceback_text = traceback.format_exc()

        finally:
            # Restore stdout/stderr
            sys.stdout = old_stdout
            sys.stderr = old_stderr

        return TestResult(
            name=test_name,
            status=status,
            duration_ms=duration_ms,
            error_message=error_message,
            error_type=error_type,
            traceback_text=traceback_text,
        )

    def _run_test_file(self, file_path: Path, verbose: bool = False) -> FileResult:
        """
        Load a test module and run all tests in it.

        Args:
            file_path: Path to the test file
            verbose: If True, print test progress

        Returns:
            FileResult with all test results for this file
        """
        module_name = file_path.stem

        # Load module dynamically
        try:
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            if spec is None or spec.loader is None:
                if verbose:
                    print(f"  SKIP {file_path.name}: Could not load spec")
                return FileResult(
                    file_path=str(file_path),
                    test_count=0,
                    passed=0,
                    failed=0,
                    errors=0,
                    skipped=1,
                    duration_seconds=0,
                    test_results=[],
                )

            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)

        except Exception as e:
            if verbose:
                print(f"  ERROR {file_path.name}: Failed to load module")
                print(f"    {e}")
            return FileResult(
                file_path=str(file_path),
                test_count=0,
                passed=0,
                failed=0,
                errors=1,
                skipped=0,
                duration_seconds=0,
                test_results=[TestResult(
                    name=module_name,
                    status="error",
                    duration_ms=0,
                    error_message=f"Failed to load module: {e}",
                    error_type="ImportError",
                )],
            )

        # Extract test functions using AST
        test_names = self._extract_test_functions(file_path)

        if not test_names:
            if verbose:
                print(f"  SKIP {file_path.name}: No tests found")
            return FileResult(
                file_path=str(file_path),
                test_count=0,
                passed=0,
                failed=0,
                errors=0,
                skipped=1,
                duration_seconds=0,
                test_results=[],
            )

        file_start = time.time()
        test_results = []
        passed = 0
        failed = 0
        errors = 0

        if verbose:
            print(f"\n  Running {file_path.name} ({len(test_names)} tests)")

        for test_name in test_names:
            # Handle class methods
            if "." in test_name:
                class_name, method_name = test_name.rsplit(".", 1)
                test_class = getattr(module, class_name, None)
                if test_class is None:
                    result = TestResult(
                        name=test_name,
                        status="error",
                        duration_ms=0,
                        error_message=f"Class {class_name} not found",
                        error_type="AttributeError",
                    )
                    errors += 1
                    test_results.append(result)
                    if verbose:
                        print(f"    FAIL {test_name}")
                    continue

                # Instantiate class and run method
                try:
                    instance = test_class()
                    test_func = getattr(instance, method_name)
                    result = self._run_single_test(test_func, test_name)
                except Exception as e:
                    result = TestResult(
                        name=test_name,
                        status="error",
                        duration_ms=0,
                        error_message=str(e),
                        error_type=type(e).__name__,
                    )
                    errors += 1
            else:
                # Direct function
                test_func = getattr(module, test_name, None)
                if test_func is None:
                    result = TestResult(
                        name=test_name,
                        status="error",
                        duration_ms=0,
                        error_message=f"Test function {test_name} not found",
                        error_type="AttributeError",
                    )
                    errors += 1
                else:
                    result = self._run_single_test(test_func, test_name)

            test_results.append(result)

            if result.status == "pass":
                passed += 1
                if verbose:
                    print(f"    PASS {test_name} ({result.duration_ms:.1f}ms)")
            elif result.status == "fail":
                failed += 1
                if verbose:
                    print(f"    FAIL {test_name}: {result.error_message}")
            elif result.status == "error":
                errors += 1
                if verbose:
                    print(f"    ERROR {test_name}: {result.error_message}")

        file_duration = time.time() - file_start

        return FileResult(
            file_path=str(file_path),
            test_count=len(test_names),
            passed=passed,
            failed=failed,
            errors=errors,
            skipped=0,
            duration_seconds=file_duration,
            test_results=test_results,
        )

    def run_all(self, verbose: bool = False) -> TestRunReport:
        """
        Discover and run all tests.

        Args:
            verbose: If True, print detailed progress

        Returns:
            TestRunReport with overall results
        """
        test_files = self.discover_tests()

        if not test_files:
            print("No test files found!")
            return TestRunReport(
                total=0,
                passed=0,
                failed=0,
                errors=0,
                skipped=0,
                duration_seconds=0,
                file_results=[],
                timestamp=datetime.now().isoformat(),
                slowest_tests=[],
            )

        if verbose:
            print(f"Discovered {len(test_files)} test files\n")

        start_time = time.time()
        file_results = []
        all_slowest = []

        total = 0
        passed = 0
        failed = 0
        errors = 0
        skipped = 0

        for test_file in test_files:
            file_result = self._run_test_file(test_file, verbose=verbose)
            file_results.append(file_result)

            total += file_result.test_count
            passed += file_result.passed
            failed += file_result.failed
            errors += file_result.errors
            skipped += file_result.skipped

            # Track slowest tests
            for test_result in file_result.test_results:
                all_slowest.append((test_result.name, test_result.duration_ms))

        # Sort and get top 10 slowest
        all_slowest.sort(key=lambda x: x[1], reverse=True)
        slowest_tests = all_slowest[:10]
        self.all_slowest_tests = slowest_tests

        total_duration = time.time() - start_time

        report = TestRunReport(
            total=total,
            passed=passed,
            failed=failed,
            errors=errors,
            skipped=skipped,
            duration_seconds=total_duration,
            file_results=file_results,
            timestamp=datetime.now().isoformat(),
            slowest_tests=slowest_tests,
        )

        self.last_report = report
        return report

    def generate_coverage_report(self) -> CoverageReport:
        """
        Generate a coverage report mapping services to tests.

        Returns:
            CoverageReport showing which services have tests
        """
        services_dir = self.root_dir / "services"
        tests_dir = self.root_dir / "tests"

        # Get all service modules
        service_files = sorted([
            f.stem for f in services_dir.glob("*.py")
            if f.stem != "__init__"
        ])

        # Get all test modules
        test_files = sorted([
            f.stem for f in tests_dir.glob("test_*.py")
        ])

        # Simple heuristic: test_auth.py covers services/auth.py
        covered_services = set()
        for test_file in test_files:
            # Remove "test_" prefix
            service_name = test_file[5:] if test_file.startswith("test_") else test_file
            # Check if corresponding service exists
            if service_name in service_files:
                covered_services.add(service_name)

        uncovered_services = set(service_files) - covered_services

        # Count total test functions
        total_test_functions = 0
        if self.last_report:
            for file_result in self.last_report.file_results:
                total_test_functions += file_result.test_count

        coverage_pct = (len(covered_services) / len(service_files) * 100) if service_files else 0

        return CoverageReport(
            total_services=len(service_files),
            covered_services=len(covered_services),
            uncovered_services=len(uncovered_services),
            coverage_percentage=coverage_pct,
            covered=sorted(list(covered_services)),
            uncovered=sorted(list(uncovered_services)),
            total_test_functions=total_test_functions,
        )

    def generate_report(self, format: str = "text") -> str:
        """
        Generate a formatted report of test results.

        Args:
            format: "text" for human-readable, "json" for machine-readable

        Returns:
            Formatted report string
        """
        if not self.last_report:
            return "No test results available. Run run_all() first."

        report = self.last_report

        if format == "json":
            return self._format_json(report)
        else:
            return self._format_text(report)

    def _format_text(self, report: TestRunReport) -> str:
        """Format report as human-readable text."""
        lines = []
        lines.append("\n" + "=" * 80)
        lines.append("TEST RUN REPORT")
        lines.append("=" * 80)
        lines.append(f"Timestamp: {report.timestamp}")
        lines.append(f"Total Duration: {report.duration_seconds:.2f}s")
        lines.append("")

        # Summary
        lines.append("SUMMARY")
        lines.append("-" * 80)
        lines.append(f"Total Tests:     {report.total}")
        lines.append(f"Passed:          {report.passed} ({report.passed/max(report.total,1)*100:.1f}%)")
        lines.append(f"Failed:          {report.failed}")
        lines.append(f"Errors:          {report.errors}")
        lines.append(f"Skipped:         {report.skipped}")
        lines.append("")

        # Per-file results
        lines.append("PER-FILE RESULTS")
        lines.append("-" * 80)
        for file_result in report.file_results:
            file_name = Path(file_result.file_path).name
            pass_rate = (file_result.passed / max(file_result.test_count, 1) * 100) if file_result.test_count > 0 else 0
            lines.append(f"{file_name:40} {file_result.test_count:3} tests | {file_result.passed:3} pass | {file_result.failed:2} fail | {file_result.errors:2} error ({pass_rate:5.1f}%)")
        lines.append("")

        # Slowest tests
        if report.slowest_tests:
            lines.append("TOP 10 SLOWEST TESTS")
            lines.append("-" * 80)
            for i, (test_name, duration_ms) in enumerate(report.slowest_tests, 1):
                lines.append(f"{i:2}. {test_name:50} {duration_ms:7.1f}ms")
            lines.append("")

        # Coverage
        coverage = self.generate_coverage_report()
        lines.append("SERVICE COVERAGE")
        lines.append("-" * 80)
        lines.append(f"Services with tests: {coverage.covered_services}/{coverage.total_services} ({coverage.coverage_percentage:.1f}%)")
        lines.append(f"Total test functions: {coverage.total_test_functions}")

        if coverage.uncovered:
            lines.append("")
            lines.append("Uncovered services:")
            for service in coverage.uncovered[:10]:
                lines.append(f"  - {service}")
            if len(coverage.uncovered) > 10:
                lines.append(f"  ... and {len(coverage.uncovered) - 10} more")

        lines.append("")
        lines.append("=" * 80)

        return "\n".join(lines)

    def _format_json(self, report: TestRunReport) -> str:
        """Format report as JSON."""
        coverage = self.generate_coverage_report()

        data = {
            "timestamp": report.timestamp,
            "duration_seconds": report.duration_seconds,
            "summary": {
                "total": report.total,
                "passed": report.passed,
                "failed": report.failed,
                "errors": report.errors,
                "skipped": report.skipped,
            },
            "file_results": [
                {
                    "file_path": fr.file_path,
                    "test_count": fr.test_count,
                    "passed": fr.passed,
                    "failed": fr.failed,
                    "errors": fr.errors,
                    "duration_seconds": fr.duration_seconds,
                    "tests": [
                        {
                            "name": tr.name,
                            "status": tr.status,
                            "duration_ms": tr.duration_ms,
                            "error_message": tr.error_message,
                            "error_type": tr.error_type,
                        }
                        for tr in fr.test_results
                    ]
                }
                for fr in report.file_results
            ],
            "slowest_tests": [
                {"name": name, "duration_ms": duration}
                for name, duration in report.slowest_tests
            ],
            "coverage": {
                "total_services": coverage.total_services,
                "covered_services": coverage.covered_services,
                "uncovered_services": coverage.uncovered_services,
                "coverage_percentage": coverage.coverage_percentage,
                "covered": coverage.covered,
                "uncovered": coverage.uncovered,
                "total_test_functions": coverage.total_test_functions,
            },
        }

        return json.dumps(data, indent=2)


if __name__ == "__main__":
    runner = TestRunner()

    # Run all tests with verbose output
    report = runner.run_all(verbose=True)

    # Print text report
    print(runner.generate_report("text"))

    # Save JSON report
    json_report = runner.generate_report("json")
    report_path = Path(__file__).parent / "test_report.json"
    with open(report_path, "w") as f:
        f.write(json_report)

    print(f"\nJSON report saved to: {report_path}")

    # Exit with proper code
    sys.exit(0 if report.failed == 0 and report.errors == 0 else 1)
