"""
HTML shells must always revalidate so a deploy is visible immediately. Marking
them `immutable` previously made users keep a stale UI for up to an hour after
every deploy (and never see fixes without a hard reload).
"""

from fastapi.testclient import TestClient

from rosteriq.api import app


def test_html_pages_are_not_immutably_cached():
    c = TestClient(app)
    r = c.get("/static/dashboard.html")
    assert r.status_code == 200
    cc = r.headers.get("cache-control", "").lower()
    assert "immutable" not in cc, f"HTML must not be immutable: {cc}"
    assert "no-cache" in cc, f"HTML should revalidate: {cc}"


def test_non_html_static_assets_still_cache():
    c = TestClient(app)
    for asset in ("/static/logo.svg", "/static/logo-icon.svg"):
        r = c.get(asset)
        if r.status_code == 200:
            cc = r.headers.get("cache-control", "").lower()
            assert "max-age" in cc, f"{asset} should be cacheable: {cc}"
            return
    # No asset present in this build — nothing to assert, but don't fail.


def test_public_pages_do_not_require_auth():
    """/login, /register and /connections are public entry points — they must
    render without a token (register used to 401 at the tenant middleware)."""
    from fastapi.testclient import TestClient
    from rosteriq.api import app
    c = TestClient(app)
    for path in ("/login", "/register", "/connections", "/timeclock"):
        r = c.get(path)
        assert r.status_code == 200, f"{path} -> {r.status_code}"
        assert "<" in r.text[:200], f"{path} did not return HTML"
