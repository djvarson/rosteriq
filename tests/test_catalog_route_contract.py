"""
Contract guard for the connector catalog <-> routes boundary.

The Connections hub renders a "Connect" button per connector from the catalog's
declared `endpoint`. If the catalog advertises an endpoint that isn't actually a
registered route (drift, a typo, a deleted route), the hub would show a button
that 404s. This test fails fast on that: EVERY catalog connect endpoint must
resolve to a real route on the app (matching verb + path template).

This is what keeps the hybrid robust — existing connectors stay on their bespoke
routes (handler="custom"), new ones use the generic dispatcher (handler="generic"),
and either way the advertised endpoint is guaranteed to exist.
"""

from rosteriq.api import app
from rosteriq.services.connector_registry import get_catalog

_HTTP_VERBS = {"get", "post", "put", "delete", "patch"}


def _registered_routes():
    """(methods, path_template) per route, from the OpenAPI spec (authoritative).

    app.routes under-counts this app's routers; app.openapi()['paths'] reflects
    every registered route, with {param} templates (e.g. /api/reservations/{provider}/install).
    """
    out = []
    for path, ops in app.openapi().get("paths", {}).items():
        methods = {m.upper() for m in ops if m.lower() in _HTTP_VERBS}
        out.append((methods, path))
    return out


def _path_matches(template: str, concrete: str) -> bool:
    """A concrete path matches a route template, treating {param} as a wildcard."""
    t = template.strip("/").split("/")
    c = concrete.strip("/").split("/")
    if len(t) != len(c):
        return False
    for ts, cs in zip(t, c):
        if ts.startswith("{") and ts.endswith("}"):
            continue
        if ts != cs:
            return False
    return True


def test_every_catalog_endpoint_resolves_to_a_real_route():
    routes = _registered_routes()
    missing = []
    for conn in get_catalog():
        for method in conn["methods"]:
            verb, _, path = method["endpoint"].partition(" ")
            ok = any(verb in methods and _path_matches(tmpl, path) for methods, tmpl in routes)
            if not ok:
                missing.append(f"{conn['key']}/{method['id']} -> {method['endpoint']}")
    assert not missing, "Catalog advertises endpoints with no matching route:\n" + "\n".join(missing)


def test_generic_handler_methods_point_at_the_dispatcher():
    """A handler='generic' method must target POST /api/connections/{key}/connect."""
    for conn in get_catalog():
        for method in conn["methods"]:
            if method.get("handler") == "generic":
                assert method["endpoint"] == f"POST /api/connections/{conn['key']}/connect", (
                    f"{conn['key']}/{method['id']} is generic but endpoint is {method['endpoint']}"
                )


def test_every_requires_env_var_is_documented_in_env_example():
    """
    Every OAuth env var a connector declares (requires_env) must be documented in
    .env.example — otherwise an operator enabling OAuth has no idea what to set,
    and the hub greys the button out with no breadcrumb. Catches the doc drift.
    """
    import pathlib

    env_example = pathlib.Path(__file__).resolve().parents[1] / ".env.example"
    assert env_example.exists(), ".env.example is missing"
    documented = set()
    for line in env_example.read_text().splitlines():
        s = line.lstrip("#").strip()
        if "=" in s:
            documented.add(s.split("=", 1)[0].strip())

    needed = set()
    for conn in get_catalog():
        for method in conn["methods"]:
            needed.update(method.get("requires_env") or [])

    missing = sorted(needed - documented)
    assert not missing, "requires_env vars not documented in .env.example: " + ", ".join(missing)
