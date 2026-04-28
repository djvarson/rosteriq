#!/usr/bin/env python3
"""
RosterIQ Pre-Deployment Checklist

Validates application readiness before production deployment:
- Python syntax check (AST parse all .py files)
- Bare import detection (must use rosteriq.* prefix)
- Environment variable validation
- Migration status verification
- Code metrics (file count, line count)

Exit codes:
    0 = GO (all checks passed)
    1 = NO-GO (one or more checks failed)

Usage:
    python -m rosteriq.scripts.pre_deploy_check
"""

import os
import sys
import ast
import re
from pathlib import Path
from typing import List, Tuple, Dict

ROSTERIQ_ROOT = Path(__file__).parent.parent
REQUIRED_ENV_VARS = [
    "DATABASE_URL",
    "ENVIRONMENT",
]
CRITICAL_CONFIG = [
    "DATABASE_URL",
]


class PreDeployChecker:
    """Pre-deployment validation."""

    def __init__(self):
        """Initialize checker."""
        self.checks: List[Tuple[str, bool, str]] = []
        self.metrics: Dict[str, int] = {}

    def check_syntax(self) -> bool:
        """
        Syntax check all Python files using AST parse.
        Returns False if any file has syntax errors.
        """
        py_files = list(ROSTERIQ_ROOT.rglob("*.py"))
        if not py_files:
            msg = "No Python files found"
            self.checks.append(("Syntax check", False, msg))
            return False

        failures = []
        for filepath in py_files:
            # Skip __pycache__
            if "__pycache__" in filepath.parts:
                continue

            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    ast.parse(f.read(), filename=str(filepath))
            except SyntaxError as e:
                failures.append(f"{filepath}: {e}")
            except Exception as e:
                failures.append(f"{filepath}: {e}")

        if failures:
            msg = f"Syntax errors in {len(failures)} file(s)"
            self.checks.append(("Syntax check", False, msg))
            for failure in failures[:3]:
                print(f"  - {failure}")
            return False
        else:
            msg = f"All {len(py_files)} files valid"
            self.checks.append(("Syntax check", True, msg))
            self.metrics["total_py_files"] = len(py_files)
            return True

    def check_bare_imports(self) -> bool:
        """
        Detect bare imports (not using rosteriq.* prefix).
        Returns False if any bare imports found.
        """
        py_files = list(ROSTERIQ_ROOT.rglob("*.py"))
        failures = []

        # Pattern: import <module> or from <module>
        # Exclude stdlib, third-party packages, relative imports
        for filepath in py_files:
            if "__pycache__" in filepath.parts:
                continue

            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()

                tree = ast.parse(content, filename=str(filepath))

                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        for alias in node.names:
                            if alias.name.startswith("rosteriq"):
                                continue
                            # Skip stdlib and common packages
                            if self._is_allowed_module(alias.name):
                                continue
                            failures.append((filepath, f"import {alias.name}"))

                    elif isinstance(node, ast.ImportFrom):
                        if node.module is None or node.module.startswith("rosteriq"):
                            continue
                        if self._is_allowed_module(node.module):
                            continue
                        failures.append((filepath, f"from {node.module}"))

            except Exception:
                pass

        if failures:
            msg = f"Found {len(failures)} potential bare import(s)"
            self.checks.append(("Bare imports check", False, msg))
            for filepath, imp in failures[:3]:
                print(f"  - {filepath}: {imp}")
            return False
        else:
            msg = "No bare imports detected"
            self.checks.append(("Bare imports check", True, msg))
            return True

    def check_environment(self) -> bool:
        """
        Verify required environment variables are set.
        Returns False if critical vars missing.
        """
        missing = []
        for var in CRITICAL_CONFIG:
            if not os.environ.get(var):
                missing.append(var)

        if missing:
            msg = f"Missing critical: {', '.join(missing)}"
            self.checks.append(("Environment check", False, msg))
            return False
        else:
            msg = f"All {len(CRITICAL_CONFIG)} critical vars set"
            self.checks.append(("Environment check", True, msg))
            return True

    def check_migrations(self) -> bool:
        """
        Verify migrations directory exists and has migration files.
        Does not run migrations, just checks structure.
        """
        migrations_dir = ROSTERIQ_ROOT / "migrations"
        if not migrations_dir.exists():
            msg = "migrations/ directory not found"
            self.checks.append(("Migrations check", False, msg))
            return False

        migration_files = sorted(migrations_dir.glob("*.sql"))
        if not migration_files:
            msg = "No migration files found"
            self.checks.append(("Migrations check", False, msg))
            return False

        # Check naming convention (NNN_*.sql)
        pattern = re.compile(r"^\d{3}_\w+\.sql$")
        invalid = [f.name for f in migration_files if not pattern.match(f.name)]

        if invalid:
            msg = f"Invalid migration names: {invalid[:2]}"
            self.checks.append(("Migrations check", False, msg))
            return False
        else:
            msg = f"Found {len(migration_files)} valid migration(s)"
            self.checks.append(("Migrations check", True, msg))
            self.metrics["migrations"] = len(migration_files)
            return True

    def check_code_metrics(self) -> bool:
        """
        Count total Python files and lines as sanity check.
        Returns True (informational, always passes).
        """
        py_files = list(ROSTERIQ_ROOT.rglob("*.py"))
        total_lines = 0

        for filepath in py_files:
            if "__pycache__" in filepath.parts:
                continue
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    total_lines += len(f.readlines())
            except Exception:
                pass

        self.metrics["total_py_files"] = len(py_files)
        self.metrics["total_lines"] = total_lines

        msg = f"{len(py_files)} files, {total_lines} lines"
        self.checks.append(("Code metrics", True, msg))
        return True

    def run_all(self) -> bool:
        """
        Run all pre-deployment checks.

        Returns:
            True if all critical checks passed (GO)
        """
        print("\nRosterIQ Pre-Deployment Check")
        print("=" * 70)

        results = [
            self.check_syntax(),
            self.check_bare_imports(),
            self.check_environment(),
            self.check_migrations(),
            self.check_code_metrics(),
        ]

        print("\nCheck Results:")
        print("-" * 70)
        for check_name, passed, msg in self.checks:
            status = "✓ PASS" if passed else "✗ FAIL"
            print(f"{status:8} {check_name:<30} {msg}")
        print("-" * 70)

        # Verdict
        critical_passed = all(results[:4])  # syntax, imports, env, migrations
        print(f"\nMetrics:")
        for key, value in self.metrics.items():
            print(f"  {key}: {value}")
        print()

        if critical_passed:
            print("VERDICT: GO - Ready for deployment")
            return True
        else:
            print("VERDICT: NO-GO - Fix failures before deployment")
            return False

    @staticmethod
    def _is_allowed_module(module: str) -> bool:
        """Check if module is in stdlib or approved third-party."""
        stdlib_packages = {
            "os", "sys", "pathlib", "datetime", "json", "logging", "re",
            "argparse", "typing", "dataclasses", "collections", "functools",
            "itertools", "operator", "abc", "enum", "contextlib", "io",
            "csv", "time", "traceback", "hashlib", "hmac", "secrets",
            "ast", "asyncio", "concurrent", "threading", "queue",
        }

        approved_third_party = {
            "fastapi", "uvicorn", "pydantic", "psycopg2", "httpx",
            "sqlalchemy", "starlette", "pytest", "xgboost", "prophet",
            "pandas", "numpy", "scikit", "requests", "aiohttp",
        }

        root_module = module.split('.')[0]
        return (root_module in stdlib_packages or
                any(root_module.startswith(pkg) for pkg in approved_third_party))


def main():
    """CLI entry point."""
    checker = PreDeployChecker()
    try:
        go = checker.run_all()
        sys.exit(0 if go else 1)
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
else:
    # Allow import as module
    pass
