"""In-process asyncio executor.

This is the Phase 2 engine: it runs a DAG inside one Python process, starting
each task the moment all of its upstream tasks have succeeded. In Phase 4 the
"run this task" step moves out to Redis-backed workers, but the scheduling
logic (what is ready, what must be skipped) stays the same.
"""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable

from pyflow.dag import DAG


class TaskState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    UPSTREAM_FAILED = "upstream_failed"  # skipped because a dependency failed


@dataclass
class TaskResult:
    task_id: str
    state: TaskState = TaskState.PENDING
    started_at: datetime | None = None
    finished_at: datetime | None = None
    result: Any = None
    error: str | None = None

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


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def run_dag(
    dag: DAG,
    *,
    max_concurrency: int = 4,
    on_state_change: StateCallback | None = None,
) -> RunResult:
    """Execute `dag`, running independent tasks in parallel.

    - A task starts as soon as every upstream task has SUCCEEDED.
    - If a task fails, everything downstream of it is marked UPSTREAM_FAILED
      and skipped. Unrelated branches keep running.
    - `max_concurrency` caps how many tasks execute at the same time.
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
        fn = dag.tasks[task_id].fn
        async with semaphore:
            result.state = TaskState.RUNNING
            result.started_at = _now()
            emit(result)
            try:
                if inspect.iscoroutinefunction(fn):
                    result.result = await fn()
                else:
                    result.result = await asyncio.to_thread(fn)
                result.state = TaskState.SUCCESS
            except Exception as exc:  # noqa: BLE001 - a task may raise anything
                result.state = TaskState.FAILED
                result.error = f"{type(exc).__name__}: {exc}"
            finally:
                result.finished_at = _now()
                emit(result)

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
