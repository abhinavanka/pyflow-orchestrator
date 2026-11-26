
"""FastAPI endpoints for the PyFlow Orchestrator."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from pyflow import __version__
from pyflow.dag import DAG
from pyflow.executor import run_dag

app = FastAPI(
    title="PyFlow Orchestrator",
    description="Create, execute, and monitor task workflows.",
    version=__version__,
)

# Temporary in-memory storage. PostgreSQL persistence comes next.
dags: dict[str, DAG] = {}
dag_details: dict[str, dict[str, Any]] = {}
runs: dict[str, dict[str, Any]] = {}


class TaskDefinition(BaseModel):
    task_id: str
    operation: str
    params: dict[str, Any] = Field(default_factory=dict)
    depends_on: list[str] = Field(default_factory=list)


class DAGDefinition(BaseModel):
    dag_id: str
    description: str = ""
    tasks: list[TaskDefinition]


def make_task_function(operation: str, params: dict[str, Any]):
    """Create a callable for a supported task operation."""

    def execute():
        if operation == "echo":
            return params.get("message", "Hello from PyFlow")

        if operation == "add":
            return sum(params["values"])

        if operation == "multiply":
            result = 1
            for value in params["values"]:
                result *= value
            return result

        if operation == "subtract":
            values = params["values"]
            if len(values) != 2:
                raise ValueError("subtract requires exactly two values")
            return values[0] - values[1]

        if operation == "divide":
            values = params["values"]
            if len(values) != 2:
                raise ValueError("divide requires exactly two values")
            return values[0] / values[1]

        raise ValueError(f"Unsupported operation: {operation}")

    return execute


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "version": __version__}


@app.get("/dags")
def list_dags():
    return [
        {
            "dag_id": dag_id,
            **dag_details[dag_id],
        }
        for dag_id in dags
    ]


@app.post("/dags", status_code=201)
def create_dag(definition: DAGDefinition):
    if definition.dag_id in dags:
        raise HTTPException(status_code=409, detail="DAG already exists")

    if not definition.tasks:
        raise HTTPException(status_code=400, detail="A DAG needs at least one task")

    dag = DAG(definition.dag_id, definition.description)
    task_ids = {task.task_id for task in definition.tasks}

    if len(task_ids) != len(definition.tasks):
        raise HTTPException(status_code=400, detail="Duplicate task IDs")

    try:
        for task in definition.tasks:
            dag.add_task(
                task.task_id,
                make_task_function(task.operation, task.params),
            )

        for task in definition.tasks:
            for dependency in task.depends_on:
                if dependency not in task_ids:
                    raise ValueError(
                        f"Unknown dependency '{dependency}' for task '{task.task_id}'"
                    )
                dag.tasks[dependency].set_downstream(dag.tasks[task.task_id])

        dag.validate()

    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    dags[definition.dag_id] = dag
    dag_details[definition.dag_id] = {
        "description": definition.description,
        "task_count": len(definition.tasks),
        "tasks": list(task_ids),
    }

    return {
        "message": "DAG created successfully",
        "dag_id": definition.dag_id,
        "task_count": len(definition.tasks),
        "execution_levels": dag.topological_levels(),
    }


@app.get("/dags/{dag_id}")
def get_dag(dag_id: str):
    if dag_id not in dags:
        raise HTTPException(status_code=404, detail="DAG not found")

    return {
        "dag_id": dag_id,
        **dag_details[dag_id],
        "execution_levels": dags[dag_id].topological_levels(),
    }


@app.post("/dags/{dag_id}/runs", status_code=201)
async def trigger_dag(dag_id: str):
    if dag_id not in dags:
        raise HTTPException(status_code=404, detail="DAG not found")

    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    result = await run_dag(dags[dag_id])

    finished_at = result.finished_at or datetime.now(timezone.utc)

    run_data = {
        "run_id": run_id,
        "dag_id": dag_id,
        "status": "success" if result.success else "failed",
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "tasks": {
            task_id: {
                "state": task.state.value,
                "started_at": (
                    task.started_at.isoformat() if task.started_at else None
                ),
                "finished_at": (
                    task.finished_at.isoformat() if task.finished_at else None
                ),
                "duration_seconds": task.duration_seconds,
                "result": task.result,
                "error": task.error,
            }
            for task_id, task in result.tasks.items()
        },
    }

    runs[run_id] = run_data
    return run_data


@app.get("/runs")
def list_runs():
    return list(runs.values())


@app.get("/runs/{run_id}")
def get_run(run_id: str):
    if run_id not in runs:
        raise HTTPException(status_code=404, detail="Run not found")

    return runs[run_id]
