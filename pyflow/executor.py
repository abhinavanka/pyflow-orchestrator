"""In-process asyncio executor.

Runs a DAG inside one Python process, starting each task the moment all of its
upstream tasks have succeeded. Failed tasks are retried with exponential
backoff according to their retry policy. A "gate" lets an operator pause a run:
while the gate is closed no new task attempt starts (tasks already running
finish normally).
"""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Awaitable, Callable

from pyflow.dag import DAG, Task


class TaskState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    RETRYING = "retrying"  # last attempt failed, waiting to try again
    SUCCESS = "success"
    FAILED = "failed"  # failed and out of retries
    UPSTREAM_FAILED = "upstream_failed"  # skipped because a dependency failed


FINAL_STATES = {TaskState.SUCCESS, TaskState.FAILED, TaskState.UPSTREAM_FAILED}


@dataclass
class TaskResult:
    task_id: str
    state: TaskState = TaskState.PENDING
    started_at: datetime | None = None  # start of the first attempt
    finished_at: datetime | None = None
    result: Any = None
    error: str | None = None  # error of the most recent failed attempt
    attempts: int = 0

    @property
    def duration_seconds(self) -> float | None:
        if self.started_at and self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None


@dataclass
class RunResult:
    dag_id: str
    tasks: dict[str, TaskResult] = field(default_factory=dict)
    started_at: datetime | None = None
    finished_at: datetime | None = None

    @property
    def success(self) -> bool:
        return all(t.state == TaskState.SUCCESS for t in self.tasks.values())

    @property
    def states(self) -> dict[str, str]:
        return {tid: t.state.value for tid, t in self.tasks.items()}


StateCallback = Callable[[TaskResult], None]
SleepFn = Callable[[float], Awaitable[None]]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def retry_delay_for(task: Task, retry_number: int) -> float:
    """Seconds to wait before retry number `retry_number` (0 = first retry)."""
    return min(task.retry_delay * (task.retry_backoff**retry_number), task.retry_max_delay)


async def run_dag(
    dag: DAG,
    *,
    max_concurrency: int = 4,
    on_state_change: StateCallback | None = None,
    gate: asyncio.Event | None = None,
    sleep: SleepFn = asyncio.sleep,
) -> RunResult:
    """Execute `dag`, running independent tasks in parallel.

    - A task starts as soon as every upstream task has SUCCEEDED.
    - A failing task is retried up to `task.retries` times, waiting
      retry_delay * backoff**n seconds between attempts.
    - If a task finally fails, everything downstream is marked UPSTREAM_FAILED
      and skipped. Unrelated branches keep running.
    - `max_concurrency` caps how many task attempts execute at the same time.
    - `gate`: when given and cleared, no new attempt starts until it is set again.
    - `sleep` is injectable so tests can fake the backoff waits.
    """
    if max_concurrency < 1:
        raise ValueError("max_concurrency must be >= 1")
    dag.validate()

    run = RunResult(dag.dag_id, {tid: TaskResult(tid) for tid in dag.tasks}, started_at=_now())
    waiting_on = {tid: set(t.upstream) for tid, t in dag.tasks.items()}
    semaphore = asyncio.Semaphore(max_concurrency)
    in_flight: dict[asyncio.Task[None], str] = {}
    launched: set[str] = set()

    def emit(result: TaskResult) -> None:
        if on_state_change:
            on_state_change(result)

    async def execute(task_id: str) -> None:
        result = run.tasks[task_id]
        task = dag.tasks[task_id]
        retry_number = 0
        while True:
            if gate is not None:
                await gate.wait()  # paused runs wait here, state stays as it was

            delay: float | None = None
            async with semaphore:
                result.attempts += 1
                result.state = TaskState.RUNNING
                if result.started_at is None:
                    result.started_at = _now()
                emit(result)
                try:
                    if inspect.iscoroutinefunction(task.fn):
                        result.result = await task.fn()
                    else:
                        result.result = await asyncio.to_thread(task.fn)
                    result.error = None
                    result.state = TaskState.SUCCESS
                except Exception as exc:  # noqa: BLE001 - a task may raise anything
                    result.error = f"{type(exc).__name__}: {exc}"
                    if retry_number < task.retries:
                        result.state = TaskState.RETRYING
                        delay = retry_delay_for(task, retry_number)
                    else:
                        result.state = TaskState.FAILED

            if delay is None:  # finished for good (success, or failed with no retries left)
                result.finished_at = _now()
                emit(result)
                return

            emit(result)  # announce RETRYING, then wait outside the semaphore
            await sleep(delay)
            retry_number += 1

    def skip_descendants(task_id: str) -> None:
        for child in dag.descendants(task_id):
            child_result = run.tasks[child]
            if child_result.state == TaskState.PENDING:
                child_result.state = TaskState.UPSTREAM_FAILED
                child_result.finished_at = _now()
                emit(child_result)

    def launch_ready() -> None:
        for task_id, deps in waiting_on.items():
            if (
                not deps
                and task_id not in launched
                and run.tasks[task_id].state == TaskState.PENDING
            ):
                launched.add(task_id)
                in_flight[asyncio.create_task(execute(task_id))] = task_id

    launch_ready()
    while in_flight:
        done, _ = await asyncio.wait(list(in_flight), return_when=asyncio.FIRST_COMPLETED)
        for future in done:
            task_id = in_flight.pop(future)
            future.result()  # execute() handles task errors itself
            if run.tasks[task_id].state == TaskState.SUCCESS:
                for child in dag.tasks[task_id].downstream:
                    waiting_on[child].discard(task_id)
            else:
                skip_descendants(task_id)
        launch_ready()

    run.finished_at = _now()
    return run
