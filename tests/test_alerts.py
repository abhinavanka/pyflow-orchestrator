import asyncio
import json

import httpx

from pyflow.alerts import build_failure_payload, send_alert


def test_payload_names_the_failed_tasks():
    payload = build_failure_payload(
        "abcdef123456", "etl", "2026-11-26T00:00:00+00:00",
        [{"task_id": "load", "error": "boom", "attempts": 3}],
    )
    assert payload["event"] == "run_failed"
    assert "etl" in payload["text"] and "load" in payload["text"] and "abcdef12" in payload["text"]
    assert payload["failed_tasks"][0]["attempts"] == 3


def test_send_alert_posts_json_and_reports_success():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["body"] = json.loads(request.content)
        return httpx.Response(200)

    ok = asyncio.run(send_alert("http://hook.test/x", {"a": 1}, transport=httpx.MockTransport(handler)))

    assert ok is True
    assert seen == {"url": "http://hook.test/x", "body": {"a": 1}}


def test_send_alert_returns_false_on_http_error():
    transport = httpx.MockTransport(lambda request: httpx.Response(500))
    assert asyncio.run(send_alert("http://hook.test/x", {}, transport=transport)) is False


def test_send_alert_never_raises_when_unreachable():
    def handler(request):
        raise httpx.ConnectError("no route")

    assert asyncio.run(send_alert("http://hook.test/x", {}, transport=httpx.MockTransport(handler))) is False
