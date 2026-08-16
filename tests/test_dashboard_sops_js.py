"""Behavioural tests for the SOP library JS in static/dashboard.html.

The functions under test (loadSopLibrary / saveSop) are extracted from the
dashboard's second <script> block and executed under node with a stubbed DOM
and fetchAPI, so these tests exercise the real rendering/toast logic rather
than grepping for strings.
"""
import json
import os
import re
import shutil
import subprocess

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
DASHBOARD = os.path.join(HERE, "..", "static", "dashboard.html")

NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="node is required to run the dashboard JS")


def _script_blocks():
    with open(DASHBOARD, encoding="utf-8") as fh:
        html = fh.read()
    return re.findall(r"<script[^>]*>([\s\S]*?)</script>", html)


def _extract(src: str, start: str, end: str) -> str:
    i = src.index(start)
    j = src.index(end, i)
    return src[i:j]


def _sop_js() -> str:
    blocks = _script_blocks()
    assert len(blocks) == 2
    main = blocks[1]
    lib = _extract(main, "let sopDocs = [];", "\nfunction openSopEditor(")
    save = _extract(main, "async function saveSop() {", "\nasync function toggleSopActive(")
    return lib + "\n" + save


HARNESS = r"""
// ---- minimal DOM / app stubs ----
const _els = {};
function el(id) {
  if (!_els[id]) _els[id] = { id, innerHTML: '', textContent: '', value: '', checked: false, style: {},
    scrollIntoView() {}, focus() {} };
  return _els[id];
}
global.document = { getElementById: el };
global.state = { currentVenue: 'v1' };
global.esc = function (s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
  return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); };
global.toasts = [];
global.showToast = function (m, k) { toasts.push({ m: m, k: k }); };
global.calls = [];
global.fetchAPI = async function (url, opts) {
  calls.push({ url: url, opts: opts || {} });
  const h = global.__responses.shift();
  if (h instanceof Error) throw h;
  return h;
};
global.__responses = [];
"""


def _run(js_body: str, scenario: str) -> dict:
    """Run scenario JS after loading the SOP functions; scenario must print JSON to stdout."""
    prog = HARNESS + "\n" + js_body + "\n(async () => {\n" + scenario + "\n})().catch(e => { console.log(JSON.stringify({error: String(e && e.stack || e)})); });"
    out = subprocess.run([NODE, "-e", prog], capture_output=True, text=True, timeout=30)
    assert out.returncode == 0, out.stderr
    data = json.loads(out.stdout.strip().splitlines()[-1])
    assert "error" not in data, data.get("error")
    return data


def _doc(**over):
    d = {
        "id": "sop-1", "title": "Glass breakage", "category": "sop", "body": "x", "applies_to": [],
        "version": 1, "requires_ack": True, "active": True,
        "ack_stats": {"required": 4, "acknowledged_current_version": 2, "outstanding_names": ["A", "B"]},
    }
    d.update(over)
    return d


def _render(docs):
    js = _sop_js()
    scenario = (
        "__responses.push(" + json.dumps({"documents": docs}) + ");\n"
        "await loadSopLibrary();\n"
        "console.log(JSON.stringify({html: document.getElementById('sop-list').innerHTML}));"
    )
    return _run(js, scenario)["html"]


# ---------------------------------------------------------------- Delete gating

def test_delete_button_shown_only_when_server_says_deletable():
    # v1, zero acks (old heuristic would show Delete) but server says NOT deletable -> hidden
    html = _render([_doc(deletable=False,
                         ack_stats={"required": 3, "acknowledged_current_version": 0, "outstanding_names": ["A"]})])
    assert "deleteSop(" not in html
    # server omitted the flag entirely -> hidden (never guess)
    html = _render([_doc(ack_stats={"required": 3, "acknowledged_current_version": 0, "outstanding_names": ["A"]})])
    assert "deleteSop(" not in html
    # v3 with acks on the current version but server says deletable -> shown (server is the authority)
    html = _render([_doc(deletable=True, version=3,
                         ack_stats={"required": 3, "acknowledged_current_version": 2, "outstanding_names": ["A"]})])
    assert "deleteSop(0)" in html
    # truthy-but-not-true must not count
    html = _render([_doc(deletable="yes")])
    assert "deleteSop(" not in html


# ------------------------------------------------------ required===0 rendering

def test_active_ack_doc_that_applies_to_nobody_renders_warning_not_full_green():
    html = _render([_doc(applies_to=["Sommelier"],
                         ack_stats={"required": 0, "acknowledged_current_version": 0, "outstanding_names": []})])
    assert "applies to no current staff" in html
    assert "check the role names" in html
    assert "var(--warning)" in html
    assert "width:0%" in html
    assert "0 of 0 acknowledged" not in html
    assert "var(--success)" not in html
    assert "everyone is across it" not in html


def test_required_zero_but_info_only_or_retired_does_not_warn():
    # info-only docs render no bar/caption at all
    html = _render([_doc(requires_ack=False,
                         ack_stats={"required": 0, "acknowledged_current_version": 0, "outstanding_names": []})])
    assert "applies to no current staff" not in html
    assert "var(--warning)" not in html
    # retired docs render no bar/caption either
    html = _render([_doc(active=False,
                         ack_stats={"required": 0, "acknowledged_current_version": 0, "outstanding_names": []})])
    assert "applies to no current staff" not in html
    assert "var(--warning)" not in html


def test_normal_progress_bar_unchanged():
    html = _render([_doc()])
    assert "2 of 4 acknowledged" in html
    assert "waiting on A, B" in html
    assert "width:50%" in html
    assert "var(--brand-blue)" in html
    html = _render([_doc(ack_stats={"required": 4, "acknowledged_current_version": 4, "outstanding_names": []})])
    assert "4 of 4 acknowledged" in html
    assert "everyone is across it" in html
    assert "var(--success)" in html


# ------------------------------------------------------------- saveSop toast

def _save(put_response, existing_id="sop-1"):
    js = _sop_js()
    scenario = (
        "document.getElementById('sop-id').value = " + json.dumps(existing_id) + ";\n"
        "document.getElementById('sop-title').value = 'T';\n"
        "document.getElementById('sop-body').value = 'B';\n"
        "document.getElementById('sop-category').value = 'sop';\n"
        "document.getElementById('sop-roles').value = '';\n"
        "document.getElementById('sop-req').checked = true;\n"
        "__responses.push(" + json.dumps(put_response) + ");\n"
        "__responses.push({documents: []});\n"  # the loadSopLibrary refresh
        "await saveSop();\n"
        "console.log(JSON.stringify({toasts: toasts, calls: calls}));"
    )
    return _run(js, scenario)


def test_save_toast_reports_new_version_when_bumped():
    r = _save({"id": "sop-1", "version": 3, "active": True, "version_bumped": True, "status": "updated"})
    assert r["calls"][0]["opts"]["method"] == "PUT"
    assert r["toasts"][0]["m"] == "Published v3 — staff will be asked to acknowledge again"
    assert r["toasts"][0]["k"] == "success"


def test_save_toast_reports_no_new_version_when_only_details_changed():
    r = _save({"id": "sop-1", "version": 2, "active": True, "version_bumped": False, "status": "updated"})
    assert r["toasts"][0]["m"] == "Details updated — no new version, existing acknowledgements stand"


def test_save_toast_reports_retired_doc():
    r = _save({"id": "sop-1", "version": 2, "active": False, "version_bumped": True, "status": "updated"})
    assert r["toasts"][0]["m"] == "Saved — retired, staff won't see it until restored"


def test_save_new_doc_toast_unchanged():
    r = _save({"status": "published", "document_id": "sop-9"}, existing_id="")
    assert r["calls"][0]["opts"]["method"] == "POST"
    assert r["toasts"][0]["m"] == "Published to staff"
