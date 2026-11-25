"""DAG and Task definitions plus dependency resolution (topological sorting).

A workflow is a set of tasks connected by "runs-after" arrows. The graph must
not contain a cycle (A -> B -> A would never finish), which is why it is a
*Directed Acyclic Graph*.

Usage:

    dag = DAG("etl")

    @dag.task()
    def extract(): ...

    @dag.task()
    def transform(): ...

    extract >> transform          # transform runs after extract
    extract >> [a, b] >> load     # a and b run in parallel, load waits for both
"""

from __future__ import annotations

from collections import deque
from typing import Any, Callable, Iterable


class DAGError(ValueError):
    """Raised when a DAG definition is invalid."""


class CycleError(DAGError):
    """Raised when the dependency graph contains a cycle."""


class Task:
    """A single unit of work inside a DAG."""

    def __init__(self, task_id: str, fn: Callable[..., Any], dag: "DAG") -> None:
        self.task_id = task_id
        self.fn = fn
        self.dag = dag
        self.upstream: set[str] = set()
        self.downstream: set[str] = set()

    # -- dependency wiring -------------------------------------------------

    def set_downstream(self, other: "Task | Iterable[Task]") -> None:
        for task in _as_list(other):
            if task.dag is not self.dag:
                raise DAGError(
                    f"Cannot link '{self.task_id}' (DAG '{self.dag.dag_id}') to "
                    f"'{task.task_id}' (DAG '{task.dag.dag_id}'): different DAGs."
                )
            if task is self:
                raise CycleError(f"Task '{self.task_id}' cannot depend on itself.")
            self.downstream.add(task.task_id)
            task.upstream.add(self.task_id)

    def set_upstream(self, other: "Task | Iterable[Task]") -> None:
        for task in _as_list(other):
            task.set_downstream(self)

    def __rshift__(self, other: "Task | list[Task]") -> "Task | list[Task]":
        """`a >> b` means b runs after a. Returns `other` so chains work."""
        self.set_downstream(other)
        return other

    def __rrshift__(self, other: "list[Task]") -> "Task":
        """Supports `[a, b] >> c` (a list has no >> for Tasks)."""
        self.set_upstream(other)
        return self

    def __repr__(self) -> str:
        return f"Task({self.task_id!r})"


class DAG:
    """A named collection of tasks and the dependencies between them."""

    def __init__(self, dag_id: str, description: str = "", schedule: str | None = None) -> None:
        if not dag_id or not dag_id.strip():
            raise DAGError("dag_id must be a non-empty string.")
        self.dag_id = dag_id
        self.description = description
        self.schedule = schedule  # cron expression, used from Phase 6 onwards
        self.tasks: dict[str, Task] = {}

    # -- building ----------------------------------------------------------

    def add_task(self, task_id: str, fn: Callable[..., Any]) -> Task:
        if task_id in self.tasks:
            raise DAGError(f"Duplicate task_id '{task_id}' in DAG '{self.dag_id}'.")
        if not callable(fn):
            raise DAGError(f"Task '{task_id}' needs a callable, got {type(fn).__name__}.")
        task = Task(task_id, fn, self)
        self.tasks[task_id] = task
        return task

    def task(self, task_id: str | None = None) -> Callable[[Callable[..., Any]], Task]:
        """Decorator form: the function name becomes the task_id by default."""

        def decorator(fn: Callable[..., Any]) -> Task:
            return self.add_task(task_id or fn.__name__, fn)

        return decorator

    # -- dependency resolution ---------------------------------------------

    def validate(self) -> None:
        """Raise DAGError/CycleError if the DAG cannot be executed."""
        if not self.tasks:
            raise DAGError(f"DAG '{self.dag_id}' has no tasks.")
        self.topological_levels()  # raises CycleError on a cycle

    def topological_levels(self) -> list[list[str]]:
        """Group tasks into levels using Kahn's algorithm.

        Every task in a level only depends on tasks from earlier levels, so all
        tasks inside one level can safely run in parallel. Levels run in
        sequence. Ordering inside a level is alphabetical for determinism.
        """
        in_degree = {tid: len(t.upstream) for tid, t in self.tasks.items()}
        current = sorted(tid for tid, deg in in_degree.items() if deg == 0)
        levels: list[list[str]] = []
        visited = 0

        while current:
            levels.append(current)
            visited += len(current)
            nxt: list[str] = []
            for tid in current:
                for child in self.tasks[tid].downstream:
                    in_degree[child] -= 1
                    if in_degree[child] == 0:
                        nxt.append(child)
            current = sorted(nxt)

        if visited != len(self.tasks):
            stuck = sorted(tid for tid, deg in in_degree.items() if deg > 0)
            raise CycleError(
                f"DAG '{self.dag_id}' contains a cycle involving: {', '.join(stuck)}"
            )
        return levels

    def topological_order(self) -> list[str]:
        """A single valid sequential execution order."""
        return [tid for level in self.topological_levels() for tid in level]

    def descendants(self, task_id: str) -> set[str]:
        """All tasks that (transitively) run after `task_id`."""
        seen: set[str] = set()
        queue = deque([task_id])
        while queue:
            for child in self.tasks[queue.popleft()].downstream:
                if child not in seen:
                    seen.add(child)
                    queue.append(child)
        return seen

    def __repr__(self) -> str:
        return f"DAG({self.dag_id!r}, tasks={len(self.tasks)})"


def _as_list(value: "Task | Iterable[Task]") -> list[Task]:
    return [value] if isinstance(value, Task) else list(value)
