"""End-to-end smoke test for a deployed RosterIQ instance.

Hits the public endpoints in order, validates basic response shape, and
prints a pass/fail report. Designed for use after a Railway deploy.

Usage:
  python scripts/e2e_smoke.py --base-url https://your-app.up.railway.app
  python scripts/e2e_smoke.py --base-url https://your-app.up.railway.app --token <jwt>

Exit code is 0 on all-pass, 1 if any check failed.

Pure stdlib — no external deps. Uses urllib so it runs anywhere Python
runs (including the Railway shell).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class CheckResult:
    name: str
    ok: bool
    status: int
    detail: str
    elapsed_ms: int


def _request(
    method: str,
    url: str,
    *,
    body: Optional[Dict[str, Any]] = None,
    token: Optional[str] = None,
    timeout: float = 10.0,
) -> Tuple[int, Any, int]:
    headers = {"Accept": "application/json"}
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    started = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            elapsed = int((time.time() - started) * 1000)
            try:
                payload = json.loads(r.read().decode("utf-8") or "null")
            except Exception:
                payload = None
            return r.status, payload, elapsed
    except urllib.error.HTTPError as e:
        elapsed = int((time.time() - started) * 1000)
        try:
            payload = json.loads(e.read().decode("utf-8") or "null")
        except Exception:
            payload = None
        return e.code, payload, elapsed
    except Exception as e:
        elapsed = int((time.time() - started) * 1000)
        return 0, {"error": str(e)}, elapsed


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------


def check_health(base: str) -> CheckResult:
    code, payload, ms = _request("GET", f"{base}/")
    ok = code == 200
    return CheckResult("root", ok, code, "" if ok else str(payload), ms)


def check_openapi(base: str) -> CheckResult:
    code, payload, ms = _request("GET", f"{base}/openapi.json")
    ok = code == 200 and isinstance(payload, dict) and "paths" in (payload or {})
    detail = ""
    if ok:
        detail = f"{len(payload.get('paths', {}))} paths"
    else:
        detail = str(payload)[:200]
    return CheckResult("openapi.json", ok, code, detail, ms)


def check_onboarding_spec(base: str) -> CheckResult:
    code, payload, ms = _request("GET", f"{base}/api/v1/onboarding/spec")
    ok = code == 200 and (payload or {}).get("total_steps", 0) >= 6
    return CheckResult("onboarding/spec", ok, code, str(payload)[:120], ms)


def check_concierge_ask(base: str) -> CheckResult:
    code, payload, ms = _request(
        "POST", f"{base}/api/v1/concierge/ask",
        body={"venue_id": "smoke-venue", "query": "are you dog friendly?"},
    )
    ok = code == 200 and isinstance(payload, dict) and "answer" in payload
    return CheckResult("concierge/ask", ok, code, str(payload)[:120], ms)


def check_roi_calculate(base: str) -> CheckResult:
    code, payload, ms = _request(
        "POST", f"{base}/api/v1/roi/calculate",
        body={
            "venue_count": 1, "monthly_revenue": 200000,
            "labour_pct": 32.0, "current_admin_hours": 8,
        },
    )
    ok = code in (200, 201) and isinstance(payload, dict)
    return CheckResult("roi/calculate", ok, code, str(payload)[:120], ms)


def check_roster_generate(base: str, token: Optional[str]) -> CheckResult:
    today = date.today()
    monday = today - timedelta(days=today.weekday())
    code, payload, ms = _request(
        "POST", f"{base}/api/v1/rosters/generate",
        body={"venue_id": "smoke-venue", "week_start": monday.isoformat()},
        token=token,
    )
    # 401/403 acceptable when no token + AUTH_ENABLED — we treat as "auth wired"
    if code in (401, 403):
        return CheckResult(
            "rosters/generate", True, code,
            "auth required (expected without token)", ms,
        )
    ok = code in (200, 201) and isinstance(payload, dict) and "shifts" in payload
    return CheckResult("rosters/generate", ok, code, str(payload)[:120], ms)


def check_tanda_history_status(base: str, token: Optional[str]) -> CheckResult:
    code, payload, ms = _request(
        "GET", f"{base}/api/v1/tanda/history/status?venue_id=smoke-venue",
        token=token,
    )
    if code in (401, 403):
        return CheckResult(
            "tanda/history/status", True, code,
            "auth required (expected without token)", ms,
        )
    ok = code == 200 and isinstance(payload, dict)
    return CheckResult("tanda/history/status", ok, code, str(payload)[:120], ms)


def check_billing_tiers(base: str) -> CheckResult:
    code, payload, ms = _request("GET", f"{base}/api/v1/billing/tiers")
    # Treat 404 as acceptable if endpoint not yet exposed publicly
    if code == 404:
        return CheckResult("billing/tiers", True, code, "not exposed", ms)
    ok = code == 200 and isinstance(payload, (dict, list))
    return CheckResult("billing/tiers", ok, code, str(payload)[:120], ms)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run(base: str, token: Optional[str]) -> List[CheckResult]:
    base = base.rstrip("/")
    return [
        check_health(base),
        check_openapi(base),
        check_onboarding_spec(base),
        check_concierge_ask(base),
        check_roi_calculate(base),
        check_roster_generate(base, token),
        check_tanda_history_status(base, token),
        check_billing_tiers(base),
    ]


def main() -> int:
    p = argparse.ArgumentParser(description="RosterIQ E2E smoke test")
    p.add_argument("--base-url", required=True)
    p.add_argument("--token", default=None, help="Optional Bearer token for auth checks")
    args = p.parse_args()

    results = run(args.base_url, args.token)
    pad = max(len(r.name) for r in results) + 1
    print(f"{'CHECK':<{pad}}  STATUS  TIME    DETAIL")
    print(f"{'-' * pad}  ------  ------  ------")
    failures = 0
    for r in results:
        marker = "PASS" if r.ok else "FAIL"
        if not r.ok:
            failures += 1
        print(f"{r.name:<{pad}}  {r.status:>3} {marker}  {r.elapsed_ms:>4}ms  {r.detail}")

    print()
    if failures:
        print(f"FAILED: {failures}/{len(results)} checks failed")
        return 1
    print(f"OK: all {len(results)} checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
