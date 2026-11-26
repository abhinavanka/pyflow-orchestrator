"""FastAPI endpoints for the PyFlow Orchestrator.

Workflows and run history are stored in the database (PostgreSQL in Docker),
so they survive restarts.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from pyflow import __version__
from pyflow.dag import DAG
from pyflow.db import DagRecord, RunRecord, TaskRunRecord, get_session, init_db
from pyflow.executor import run_dag


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()  # create tables on startup if they do not exist yet
    yield


app = FastAPI(
    title="PyFlow Orchestrator",
    description="Create, execute, and monitor task workflows.",
    version=__version__,
    lifespan=lifespan,
)

SUPPORTED_OPERATIONS = {"echo", "add", "multiply", "subtract", "divide"}


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
        dag.add_task(task.task_id, make_task_function(task.operation, task.params))

    for task in definition.tasks:
        for dependency in task.depends_on:
            if dependency not in dag.tasks:
                raise ValueError(
                    f"Unknown dependency '{dependency}' for task '{task.task_id}'"
                )
            dag.tasks[dependency].set_downstream(dag.tasks[task.task_id])

    dag.validate()
    return dag


def _iso(value: datetime | None) -> str | None:
    """ISO timestamp, treating naive datetimes (some databases) as UTC."""
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def run_to_dict(run: RunRecord) -> dict[str, Any]:
    return {
        "run_id": run.run_id,
        "dag_id": run.dag_id,
        "status": run.status,
        "started_at": _iso(run.started_at),
        "finished_at": _iso(run.finished_at),
        "tasks": {
            task.task_id: {
                "state": task.state,
                "started_at": _iso(task.started_at),
                "finished_at": _iso(task.finished_at),
                "duration_seconds": task.duration_seconds,
                "result": task.result,
                "error": task.error,
            }
            for task in run.tasks
        },
    }


def dag_summary(record: DagRecord) -> dict[str, Any]:
    tasks = [task["task_id"] for task in record.definition["tasks"]]
    return {
        "dag_id": record.dag_id,
        "description": record.description,
        "task_count": len(tasks),
        "tasks": tasks,
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "version": __version__}


@app.get("/dags")
def list_dags(session: Session = Depends(get_session)):
    records = session.scalars(select(DagRecord).order_by(DagRecord.created_at)).all()
    return [dag_summary(record) for record in records]


@app.post("/dags", status_code=201)
def create_dag(definition: DAGDefinition, session: Session = Depends(get_session)):
    if session.get(DagRecord, definition.dag_id) is not None:
        raise HTTPException(status_code=409, detail="DAG already exists")

    try:
        dag = build_dag(definition)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    session.add(
        DagRecord(
            dag_id=definition.dag_id,
            description=definition.description,
            definition=definition.model_dump(),
        )
    )
    session.commit()

    return {
        "message": "DAG created successfully",
        "dag_id": definition.dag_id,
        "task_count": len(definition.tasks),
        "execution_levels": dag.topological_levels(),
    }


@app.get("/dags/{dag_id}")
def get_dag(dag_id: str, session: Session = Depends(get_session)):
    record = session.get(DagRecord, dag_id)
    if record is None:
        raise HTTPException(status_code=404, detail="DAG not found")

    dag = build_dag(DAGDefinition(**record.definition))
    return {**dag_summary(record), "execution_levels": dag.topological_levels()}


@app.post("/dags/{dag_id}/runs", status_code=201)
async def trigger_dag(dag_id: str, session: Session = Depends(get_session)):
    record = session.get(DagRecord, dag_id)
    if record is None:
        raise HTTPException(status_code=404, detail="DAG not found")

    dag = build_dag(DAGDefinition(**record.definition))
    result = await run_dag(dag)

    run = RunRecord(
        run_id=str(uuid.uuid4()),
        dag_id=dag_id,
        status="success" if result.success else "failed",
        started_at=result.started_at,
        finished_at=result.finished_at or datetime.now(timezone.utc),
        tasks=[
            TaskRunRecord(
                task_id=task_id,
                state=task.state.value,
                started_at=task.started_at,
                finished_at=task.finished_at,
                duration_seconds=task.duration_seconds,
                result=task.result,
                error=task.error,
            )
            for task_id, task in result.tasks.items()
        ],
    )
    session.add(run)
    session.commit()
    return run_to_dict(run)


@app.get("/runs")
def list_runs(dag_id: str | None = None, session: Session = Depends(get_session)):
    query = select(RunRecord).order_by(RunRecord.started_at)
    if dag_id:
        query = query.where(RunRecord.dag_id == dag_id)
    return [run_to_dict(run) for run in session.scalars(query).all()]


@app.get("/runs/{run_id}")
def get_run(run_id: str, session: Session = Depends(get_session)):
    run = session.get(RunRecord, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return run_to_dict(run)
