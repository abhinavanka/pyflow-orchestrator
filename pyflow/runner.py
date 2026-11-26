"""Runs workflows in the background and records their progress in the database.

`start_run` creates a run row (status "running", every task "pending"), then
launches the DAG as a background asyncio task. Every task state change is
written to the database as it happens, so anyone polling the API sees live
progress. Operators can pause a run: tasks already running finish, but no new
task attempt starts until the run is resumed.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from pyflow import alerts, db
from pyflow.config import get_settings
from pyflow.db import RunRecord, TaskRunRecord, utcnow
from pyflow.definitions import DAGDefinition, build_dag
from pyflow.executor import FINAL_STATES, TaskResult, TaskState, run_dag

log = logging.getLogger("pyflow.runner")

FINAL_STATE_VALUES = {state.value for state in FINAL_STATES}


@dataclass
class RunControl:
    """Handle for a run that is currently active in this process."""

    gate: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task[None] | None = None

    def __post_init__(self) -> None:
        self.gate.set()  # open = running, closed = paused


# run_id -> control, for runs that are active right now
controls: dict[str, RunControl] = {}


def _json_safe(value: Any) -> Any:
    """Task results are stored as JSON; fall back to text for anything else."""
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return repr(value)


def create_run(session: Session, dag_id: str, definition: dict[str, Any]) -> RunRecord:
    run = RunRecord(
        run_id=str(uuid.uuid4()),
        dag_id=dag_id,
        status="running",
        started_at=utcnow(),
        finished_at=None,
        tasks=[
            TaskRunRecord(task_id=task["task_id"], state="pending", attempts=0)
            for task in definition["tasks"]
        ],
    )
    session.add(run)
    session.commit()
    return run


def launch(run_id: str, definition: dict[str, Any]) -> RunControl:
    """Start executing the run in the background (must be called inside the event loop)."""
    control = RunControl()
    controls[run_id] = control
    control.task = asyncio.create_task(_execute(run_id, definition, control))
    return control


def _persist_task(run_id: str, result: TaskResult) -> None:
    """Called by the executor on every task state change."""
    try:
        with db.get_session_factory()() as session:
            row = session.scalar(
                select(TaskRunRecord).where(
                    TaskRunRecord.run_id == run_id, TaskRunRecord.task_id == result.task_id
                )
            )
            if row is None:
                return
            row.state = result.state.value
            row.started_at = result.started_at
            row.finished_at = result.finished_at
            row.duration_seconds = result.duration_seconds
            row.attempts = result.attempts
            row.result = _json_safe(result.result)
            row.error = result.error
            session.commit()
    except Exception:  # noqa: BLE001 - a database hiccup must not kill the run
        log.exception("Could not save state of task %s in run %s", result.task_id, run_id)


def _finalize(run_id: str, status: str) -> None:
    with db.get_session_factory()() as session:
        run = session.get(RunRecord, run_id)
        if run is not None:
            run.status = status
            run.finished_at = utcnow()
            session.commit()


async def _execute(run_id: str, definition: dict[str, Any], control: RunControl) -> None:
    status = "failed"
    failed_tasks: list[dict[str, Any]] = []
    try:
        dag = build_dag(DAGDefinition(**definition))
        result = await run_dag(
            dag,
            on_state_change=lambda task: _persist_task(run_id, task),
            gate=control.gate,
        )
        status = "success" if result.success else "failed"
        failed_tasks = [
            {"task_id": t.task_id, "error": t.error, "attempts": t.attempts}
            for t in result.tasks.values()
            if t.state == TaskState.FAILED
        ]
    except Exception:  # noqa: BLE001
        log.exception("Run %s crashed", run_id)
        failed_tasks = [{"task_id": "(run)", "error": "internal error, see server log", "attempts": 0}]
    finally:
        _finalize(run_id, status)
        controls.pop(run_id, None)

    if status == "failed":
        await _send_failure_alert(run_id, definition, failed_tasks)


async def _send_failure_alert(
    run_id: str, definition: dict[str, Any], failed_tasks: list[dict[str, Any]]
) -> None:
    url = definition.get("alert_webhook") or get_settings().alert_webhook
    if not url:
        return
    payload = alerts.build_failure_payload(
        run_id=run_id,
        dag_id=definition["dag_id"],
        finished_at=utcnow().isoformat(),
        failed_tasks=failed_tasks,
    )
    await alerts.send_alert(url, payload)


def set_paused(run_id: str, paused: bool) -> bool:
    """Pause or resume an active run. Returns False if the run is not active."""
    control = controls.get(run_id)
    if control is None:
        return False
    if paused:
        control.gate.clear()
    else:
        control.gate.set()
    with db.get_session_factory()() as session:
        run = session.get(RunRecord, run_id)
        if run is not None and run.status in ("running", "paused"):
            run.status = "paused" if paused else "running"
            session.commit()
    return True


def mark_interrupted_runs() -> int:
    """On startup: runs left 'running' or 'paused' by a previous process can never
    finish (their worker is gone), so mark them failed instead of leaving them
    looking alive forever."""
    count = 0
    with db.get_session_factory()() as session:
        stale = session.scalars(
            select(RunRecord).where(RunRecord.status.in_(["running", "paused"]))
        ).all()
        for run in stale:
            run.status = "failed"
            run.finished_at = utcnow()
            for task in run.tasks:
                if task.state not in FINAL_STATE_VALUES:
                    task.state = "failed"
                    task.error = "interrupted: the API restarted while this run was in progress"
                    task.finished_at = utcnow()
            count += 1
        session.commit()
    return count
