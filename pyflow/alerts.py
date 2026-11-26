"""Failure alerts: POST a JSON message to a webhook URL.

Works with anything that accepts a webhook (Slack-style incoming webhooks,
webhook.site, your own service). An alert that cannot be delivered is logged
but never crashes a run.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

log = logging.getLogger("pyflow.alerts")


def build_failure_payload(
    run_id: str,
    dag_id: str,
    finished_at: str | None,
    failed_tasks: list[dict[str, Any]],
) -> dict[str, Any]:
    names = ", ".join(t["task_id"] for t in failed_tasks) or "unknown"
    return {
        "event": "run_failed",
        "text": f"PyFlow: run {run_id[:8]} of '{dag_id}' failed (task: {names})",
        "dag_id": dag_id,
        "run_id": run_id,
        "finished_at": finished_at,
        "failed_tasks": failed_tasks,
    }


async def send_alert(
    url: str,
    payload: dict[str, Any],
    *,
    timeout: float = 5.0,
    transport: httpx.AsyncBaseTransport | None = None,
) -> bool:
    """POST `payload` as JSON. Returns True if the webhook answered with 2xx."""
    try:
        async with httpx.AsyncClient(timeout=timeout, transport=transport) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
        return True
    except Exception as exc:  # noqa: BLE001 - alerting must never break a run
        log.warning("Could not deliver alert to %s: %s", url, exc)
        return False
