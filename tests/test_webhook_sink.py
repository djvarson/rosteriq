"""Tests for the hardened WebhookSink (Moment 14c).

Covers retry/backoff, dead-letter file, and one integration test
against a real in-process HTTP server to prove the sink speaks HTTP
over an actual socket — not just mocked ``_attempt_once`` calls.

Pure stdlib. Uses http.server + threading for the integration test;
no flask, no pytest.
"""
from __future__ import annotations

import json
import os
import socket
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq import brief_dispatcher as bd  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SAMPLE_BRIEF = {
    "venue_id": "v",
    "venue_label": "Mojo's",
    "date": "2026-04-11",
    "headline": "test",
    "traffic_light": "green",
    "rollup": {},
    "top_dismissed": [],
    "recap_context": {},
    "one_thing": "",
    "summary": "",
    "generated_at": "2026-04-12T00:00:00Z",
}


class _RecordingSleep:
    """Captures sleep durations without actually blocking."""

    def __init__(self) -> None:
        self.calls: list = []

    def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


def _install_attempt_once(sink, sequence):
    """Replace sink._attempt_once with a function that returns each
    result in ``sequence`` in order. Cycling past the end raises so
    a test can spot accidental over-retries.
    """
    calls = {"n": 0}

    def fake(data):
        idx = calls["n"]
        calls["n"] += 1
        if idx >= len(sequence):
            raise AssertionError(
                f"_attempt_once called {calls['n']} times, only {len(sequence)} scripted"
            )
        return dict(sequence[idx])

    sink._attempt_once = fake  # type: ignore
    return calls


# ---------------------------------------------------------------------------
# _build_payload
# ---------------------------------------------------------------------------

def test_build_payload_default_shape():
    sink = bd.WebhookSink("http://example.invalid/")
    p = sink._build_payload("v", _SAMPLE_BRIEF, "plain text")
    assert p["venue_id"] == "v"
    assert p["brief"] == _SAMPLE_BRIEF
    assert p["text"] == "plain text"


def test_build_payload_uses_transform_hook():
    sink = bd.WebhookSink(
        "http://example.invalid/",
        transform=lambda brief, text: {"slack_text": text, "label": brief["venue_label"]},
    )
    p = sink._build_payload("v", _SAMPLE_BRIEF, "the body")
    assert p == {"slack_text": "the body", "label": "Mojo's"}


def test_build_payload_wraps_non_dict_transform_result():
    sink = bd.WebhookSink(
        "http://example.invalid/",
        transform=lambda brief, text: "just a string",
    )
    p = sink._build_payload("v", _SAMPLE_BRIEF, "x")
    assert p == {"raw": "just a string"}


# ---------------------------------------------------------------------------
# _compute_backoff
# ---------------------------------------------------------------------------

def test_compute_backoff_exponential():
    sink = bd.WebhookSink(
        "http://x/", backoff_base_s=0.25, backoff_cap_s=10.0, sleep=_RecordingSleep()
    )
    assert sink._compute_backoff(0) == 0.25
    assert sink._compute_backoff(1) == 0.5
    assert sink._compute_backoff(2) == 1.0
    assert sink._compute_backoff(3) == 2.0


def test_compute_backoff_caps_at_max():
    sink = bd.WebhookSink(
        "http://x/", backoff_base_s=1.0, backoff_cap_s=4.0, sleep=_RecordingSleep()
    )
    assert sink._compute_backoff(10) == 4.0


def test_compute_backoff_zero_base_returns_zero():
    sink = bd.WebhookSink("http://x/", backoff_base_s=0.0, sleep=_RecordingSleep())
    assert sink._compute_backoff(5) == 0.0


# ---------------------------------------------------------------------------
# Retry loop — via scripted _attempt_once
# ---------------------------------------------------------------------------

def test_send_succeeds_on_first_attempt_no_retries():
    sleeper = _RecordingSleep()
    sink = bd.WebhookSink("http://x/", max_attempts=3, sleep=sleeper)
    _install_attempt_once(sink, [{"ok": True, "status_code": 200, "retryable": False}])
    result = sink.send(venue_id="v", brief=_SAMPLE_BRIEF, text_body="t")
    assert result["status"] == "ok"
    assert len(result["attempts"]) == 1
    assert result["attempts"][0]["ok"] is True
    # No sleeps on success
    assert sleeper.calls == []


def test_send_retries_on_transient_error_then_succeeds():
    sleeper = _RecordingSleep()
    sink = bd.WebhookSink(
        "http://x/", max_attempts=3, backoff_base_s=0.1, sleep=sleeper
    )
    _install_attempt_once(sink, [
        {"ok": False, "status_code": 503, "retryable": True, "error": "HTTP 503"},
        {"ok": False, "status_code": 0, "retryable": True, "error": "URLError"},
        {"ok": True, "status_code": 200, "retryable": False},
    ])
    result = sink.send(venue_id="v", brief=_SAMPLE_BRIEF, text_body="t")
    assert result["status"] == "ok"
    assert len(result["attempts"]) == 3
    # Sleeps only between attempts (2 sleeps for 3 attempts)
    assert len(sleeper.calls) == 2
    # Exponential: 0.1, then 0.2
    assert sleeper.calls[0] == 0.1
    assert sleeper.calls[1] == 0.2


def test_send_bails_immediately_on_non_retryable_error():
    sleeper = _RecordingSleep()
    sink = bd.WebhookSink("http://x/", max_attempts=5, sleep=sleeper)
    _install_attempt_once(sink, [
        {"ok": False, "status_code": 401, "retryable": False, "error": "HTTP 401"},
    ])
    result = sink.send(venue_id="v", brief=_SAMPLE_BRIEF, text_body="t")
    assert result["status"] == "error"
    assert "401" in result["detail"]
    assert len(result["attempts"]) == 1
    assert sleeper.calls == []  # no retry, no sleep


def test_send_exhausts_retries_and_returns_error():
    sleeper = _RecordingSleep()
    sink = bd.WebhookSink(
        "http://x/", max_attempts=3, backoff_base_s=0.0, sleep=sleeper
    )
    _install_attempt_once(sink, [
        {"ok": False, "status_code": 503, "retryable": True, "error": "HTTP 503"},
        {"ok": False, "status_code": 503, "retryable": True, "error": "HTTP 503"},
        {"ok": False, "status_code": 503, "retryable": True, "error": "HTTP 503"},
    ])
    result = sink.send(venue_id="v", brief=_SAMPLE_BRIEF, text_body="t")
    assert result["status"] == "error"
    assert len(result["attempts"]) == 3
    # All attempts failed
    assert all(not a["ok"] for a in result["attempts"])


def test_send_dead_letters_on_exhausted_retries():
    with tempfile.TemporaryDirectory() as d:
        dl_path = os.path.join(d, "dead.jsonl")
        sink = bd.WebhookSink(
            "http://x/",
            max_attempts=2,
            backoff_base_s=0.0,
            dead_letter_path=dl_path,
            sleep=_RecordingSleep(),
        )
        _install_attempt_once(sink, [
            {"ok": False, "status_code": 503, "retryable": True, "error": "HTTP 503"},
            {"ok": False, "status_code": 503, "retryable": True, "error": "HTTP 503"},
        ])
        result = sink.send(venue_id="venue_9", brief=_SAMPLE_BRIEF, text_body="t")
        assert result["status"] == "error"
        assert result["dead_lettered_to"] == dl_path
        with open(dl_path) as f:
            entry = json.loads(f.readline())
        assert entry["venue_id"] == "venue_9"
        assert entry["url"] == "http://x/"
        assert len(entry["attempts"]) == 2


def test_send_dead_letters_on_non_retryable_error():
    with tempfile.TemporaryDirectory() as d:
        dl_path = os.path.join(d, "dead.jsonl")
        sink = bd.WebhookSink(
            "http://x/",
            max_attempts=3,
            dead_letter_path=dl_path,
            sleep=_RecordingSleep(),
        )
        _install_attempt_once(sink, [
            {"ok": False, "status_code": 400, "retryable": False, "error": "HTTP 400"},
        ])
        result = sink.send(venue_id="v", brief=_SAMPLE_BRIEF, text_body="t")
        assert result["status"] == "error"
        assert result["dead_lettered_to"] == dl_path
        # Only one attempt — bailed out early
        with open(dl_path) as f:
            entry = json.loads(f.readline())
        assert len(entry["attempts"]) == 1


def test_send_does_not_dead_letter_on_success():
    with tempfile.TemporaryDirectory() as d:
        dl_path = os.path.join(d, "dead.jsonl")
        sink = bd.WebhookSink(
            "http://x/",
            dead_letter_path=dl_path,
            sleep=_RecordingSleep(),
        )
        _install_attempt_once(sink, [
            {"ok": True, "status_code": 200, "retryable": False},
        ])
        result = sink.send(venue_id="v", brief=_SAMPLE_BRIEF, text_body="t")
        assert result["status"] == "ok"
        # Dead-letter file should never have been created
        assert not os.path.exists(dl_path)


def test_send_swallows_dead_letter_write_failure():
    # Unwriteable path — dead-letter is best-effort
    sink = bd.WebhookSink(
        "http://x/",
        max_attempts=1,
        dead_letter_path="/proc/1/nope/cannot-write/dl.jsonl",
        sleep=_RecordingSleep(),
    )
    _install_attempt_once(sink, [
        {"ok": False, "status_code": 503, "retryable": True, "error": "503"},
    ])
    result = sink.send(venue_id="v", brief=_SAMPLE_BRIEF, text_body="t")
    # Send returns cleanly even though the dead-letter write failed
    assert result["status"] == "error"
    assert result["dead_lettered_to"] is None


def test_send_returns_payload_build_failed_on_transform_exception():
    def bad_transform(brief, text):
        raise RuntimeError("bang")

    sink = bd.WebhookSink("http://x/", transform=bad_transform, sleep=_RecordingSleep())
    result = sink.send(venue_id="v", brief=_SAMPLE_BRIEF, text_body="t")
    assert result["status"] == "error"
    assert "payload_build_failed" in result["detail"]


def test_send_single_attempt_when_max_attempts_is_one():
    sleeper = _RecordingSleep()
    sink = bd.WebhookSink("http://x/", max_attempts=1, sleep=sleeper)
    _install_attempt_once(sink, [
        {"ok": False, "status_code": 503, "retryable": True, "error": "503"},
    ])
    result = sink.send(venue_id="v", brief=_SAMPLE_BRIEF, text_body="t")
    assert result["status"] == "error"
    assert len(result["attempts"]) == 1
    assert sleeper.calls == []  # no retries, no sleeps


# ---------------------------------------------------------------------------
# read_dead_letter
# ---------------------------------------------------------------------------

def test_read_dead_letter_missing_file_returns_empty():
    assert bd.read_dead_letter("/tmp/rq_webhook_dl_missing_xyz.jsonl") == []


def test_read_dead_letter_filters_by_venue_id():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "dl.jsonl")
        sink = bd.WebhookSink(
            "http://x/",
            max_attempts=1,
            dead_letter_path=path,
            sleep=_RecordingSleep(),
        )
        _install_attempt_once(sink, [
            {"ok": False, "status_code": 503, "retryable": True, "error": "e"},
            {"ok": False, "status_code": 503, "retryable": True, "error": "e"},
            {"ok": False, "status_code": 503, "retryable": True, "error": "e"},
        ])
        for vid in ["v1", "v2", "v1"]:
            sink.send(venue_id=vid, brief=_SAMPLE_BRIEF, text_body="t")
        entries = bd.read_dead_letter(path, venue_id="v1")
        assert len(entries) == 2
        assert all(e["venue_id"] == "v1" for e in entries)


def test_read_dead_letter_tolerates_malformed_lines():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "dl.jsonl")
        with open(path, "w") as f:
            f.write(json.dumps({"venue_id": "v", "attempts": []}) + "\n")
            f.write("this is not json\n")
            f.write(json.dumps({"venue_id": "v", "attempts": []}) + "\n")
        assert len(bd.read_dead_letter(path)) == 2


def test_read_dead_letter_respects_limit():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "dl.jsonl")
        with open(path, "w") as f:
            for i in range(10):
                f.write(json.dumps({"venue_id": f"v{i}", "attempts": []}) + "\n")
        entries = bd.read_dead_letter(path, limit=4)
        assert len(entries) == 4


# ---------------------------------------------------------------------------
# Integration test — real HTTP server
# ---------------------------------------------------------------------------

class _Handler(BaseHTTPRequestHandler):
    """Minimal POST handler. Behavior is driven by class-level state
    so tests can script flaky responses across multiple requests."""

    # Class-level scratch space so the test can configure behavior
    # before spinning up the server. Named `script` (not `responses`)
    # to avoid shadowing BaseHTTPRequestHandler.responses, which the
    # stdlib uses for status-code → message lookup.
    script: list = []
    received: list = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        body_bytes = self.rfile.read(length) if length > 0 else b""
        try:
            body = json.loads(body_bytes.decode("utf-8")) if body_bytes else {}
        except Exception:
            body = {"_raw": body_bytes.decode("utf-8", "replace")}
        _Handler.received.append(body)

        # Pop the next scripted response; default to 200 if empty.
        if _Handler.script:
            code = _Handler.script.pop(0)
        else:
            code = 200
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def log_message(self, format, *args):
        # Silence the default stderr spam.
        pass


def _start_server():
    # Pick a free port
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    server = HTTPServer(("127.0.0.1", port), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, port


def _stop_server(server, thread):
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)


def test_integration_webhook_sink_posts_to_real_server():
    # Reset class state
    _Handler.script = []
    _Handler.received = []
    server, thread, port = _start_server()
    try:
        sink = bd.WebhookSink(
            f"http://127.0.0.1:{port}/",
            max_attempts=1,
            sleep=_RecordingSleep(),
        )
        result = sink.send(venue_id="v", brief=_SAMPLE_BRIEF, text_body="hello")
        assert result["status"] == "ok"
        assert result["attempts"][0]["status_code"] == 200
        assert len(_Handler.received) == 1
        received = _Handler.received[0]
        assert received["venue_id"] == "v"
        assert received["text"] == "hello"
        assert received["brief"]["venue_label"] == "Mojo's"
    finally:
        _stop_server(server, thread)


def test_integration_webhook_sink_retries_5xx_and_succeeds():
    # First two requests return 503, third returns 200.
    _Handler.script = [503, 503]
    _Handler.received = []
    server, thread, port = _start_server()
    try:
        sink = bd.WebhookSink(
            f"http://127.0.0.1:{port}/",
            max_attempts=3,
            backoff_base_s=0.01,
            sleep=_RecordingSleep(),
        )
        result = sink.send(venue_id="v", brief=_SAMPLE_BRIEF, text_body="t")
        assert result["status"] == "ok"
        assert len(result["attempts"]) == 3
        assert result["attempts"][0]["status_code"] == 503
        assert result["attempts"][1]["status_code"] == 503
        assert result["attempts"][2]["status_code"] == 200
        assert len(_Handler.received) == 3
    finally:
        _stop_server(server, thread)


def test_integration_webhook_sink_dead_letters_on_persistent_5xx():
    _Handler.script = [503, 503, 503]
    _Handler.received = []
    server, thread, port = _start_server()
    try:
        with tempfile.TemporaryDirectory() as d:
            dl_path = os.path.join(d, "dl.jsonl")
            sink = bd.WebhookSink(
                f"http://127.0.0.1:{port}/",
                max_attempts=3,
                backoff_base_s=0.01,
                dead_letter_path=dl_path,
                sleep=_RecordingSleep(),
            )
            result = sink.send(venue_id="v_persistent", brief=_SAMPLE_BRIEF, text_body="t")
            assert result["status"] == "error"
            assert result["dead_lettered_to"] == dl_path
            with open(dl_path) as f:
                entry = json.loads(f.readline())
            assert entry["venue_id"] == "v_persistent"
            assert len(entry["attempts"]) == 3
            assert all(a["status_code"] == 503 for a in entry["attempts"])
    finally:
        _stop_server(server, thread)


def test_integration_webhook_sink_bails_on_400_no_retry():
    _Handler.script = [400, 400, 400]  # Only the first should be consumed
    _Handler.received = []
    server, thread, port = _start_server()
    try:
        sink = bd.WebhookSink(
            f"http://127.0.0.1:{port}/",
            max_attempts=3,
            backoff_base_s=0.01,
            sleep=_RecordingSleep(),
        )
        result = sink.send(venue_id="v", brief=_SAMPLE_BRIEF, text_body="t")
        assert result["status"] == "error"
        assert len(result["attempts"]) == 1
        assert "400" in result["detail"]
        assert len(_Handler.received) == 1  # No retries
    finally:
        _stop_server(server, thread)


def test_integration_webhook_sink_retries_429_as_transient():
    _Handler.script = [429, 200]
    _Handler.received = []
    server, thread, port = _start_server()
    try:
        sink = bd.WebhookSink(
            f"http://127.0.0.1:{port}/",
            max_attempts=3,
            backoff_base_s=0.01,
            sleep=_RecordingSleep(),
        )
        result = sink.send(venue_id="v", brief=_SAMPLE_BRIEF, text_body="t")
        assert result["status"] == "ok"
        assert len(result["attempts"]) == 2
        assert result["attempts"][0]["status_code"] == 429
        assert result["attempts"][1]["status_code"] == 200
    finally:
        _stop_server(server, thread)


def test_integration_webhook_sink_connection_refused_is_retryable():
    # Never start a server — use an immediately-closed port
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()

    sleeper = _RecordingSleep()
    sink = bd.WebhookSink(
        f"http://127.0.0.1:{port}/",
        max_attempts=2,
        backoff_base_s=0.01,
        sleep=sleeper,
    )
    result = sink.send(venue_id="v", brief=_SAMPLE_BRIEF, text_body="t")
    assert result["status"] == "error"
    assert len(result["attempts"]) == 2
    # Both attempts got connection errors, each with status_code 0
    assert all(a["status_code"] == 0 for a in result["attempts"])
    # One sleep between the two attempts
    assert len(sleeper.calls) == 1


# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_") and callable(v)]
    passed = failed = 0
    for t in tests:
        try:
            t()
            passed += 1
            print(f"  PASS {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"  FAIL {t.__name__}: {e}")
        except Exception as e:
            failed += 1
            print(f"  ERROR {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{passed}/{passed+failed} tests passed")
    sys.exit(0 if failed == 0 else 1)
