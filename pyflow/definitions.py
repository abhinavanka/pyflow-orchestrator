"""Workflow definitions as submitted through the API, and how they become DAGs."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from pyflow.dag import DAG
from pyflow.operations import SUPPORTED_OPERATIONS, make_task_function


class TaskDefinition(BaseModel):
    task_id: str
    operation: str
    params: dict[str, Any] = Field(default_factory=dict)
    depends_on: list[str] = Field(default_factory=list)
    # Retry policy: wait retry_delay * retry_backoff**n seconds (max retry_max_delay)
    # before retry n+1. Defaults give 1s, 2s, 4s, 8s...
    retries: int = Field(default=0, ge=0, le=10)
    retry_delay: float = Field(default=1.0, ge=0, le=3600)
    retry_backoff: float = Field(default=2.0, ge=1, le=10)
    retry_max_delay: float = Field(default=60.0, ge=0, le=3600)


class DAGDefinition(BaseModel):
    dag_id: str
    description: str = ""
    tasks: list[TaskDefinition]
    # Where to send a message when a run of this DAG fails.
    alert_webhook: str | None = None


def build_dag(definition: DAGDefinition) -> DAG:
    """Turn a stored definition into a runnable DAG. Raises ValueError if invalid."""
    if not definition.tasks:
        raise ValueError("A DAG needs at least one task")

    task_ids = [task.task_id for task in definition.tasks]
    if len(set(task_ids)) != len(task_ids):
        raise ValueError("Duplicate task IDs")

    for task in definition.tasks:
        if task.operation not in SUPPORTED_OPERATIONS:
            raise ValueError(
                f"Unsupported operation '{task.operation}' in task '{task.task_id}'. "
                f"Choose from: {', '.join(sorted(SUPPORTED_OPERATIONS))}"
            )

    dag = DAG(definition.dag_id, definition.description)
    for task in definition.tasks:
        dag.add_task(
            task.task_id,
            make_task_function(task.operation, task.params),
            retries=task.retries,
            retry_delay=task.retry_delay,
            retry_backoff=task.retry_backoff,
            retry_max_delay=task.retry_max_delay,
        )

    for task in definition.tasks:
        for dependency in task.depends_on:
            if dependency not in dag.tasks:
                raise ValueError(
                    f"Unknown dependency '{dependency}' for task '{task.task_id}'"
                )
            dag.tasks[dependency].set_downstream(dag.tasks[task.task_id])

    dag.validate()
    return dag
