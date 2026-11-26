"""FastAPI endpoints for the PyFlow Orchestrator.

Workflows and run history are stored in the database (PostgreSQL in Docker),
so they survive restarts. Runs execute in the background; their state is
written to the database as they progress.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Response
from sqlalchemy import select
from sqlalchemy.orm import Session

from pyflow import __version__, runner
from pyflow.db import DagRecord, RunRecord, get_session, init_db
from pyflow.definitions import DAGDefinition, build_dag


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()  # create tables / add new columns if needed
    runner.mark_interrupted_runs()
    yield


app = FastAPI(
    title="PyFlow Orchestrator",
    description="Create, execute, and monitor task workflows.",
    version=__version__,
    lifespan=lifespan,
)


def _iso(value: datetime | None) -> str | None:
    """ISO timestamp in UTC, treating naive datetimes (some databases) as UTC."""
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
                "attempts": task.attempts,
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
        "is_paused": record.is_paused,
        "task_count": len(tasks),
        "tasks": tasks,
    }


def _get_dag_or_404(session: Session, dag_id: str) -> DagRecord:
    record = session.get(DagRecord, dag_id)
    if record is None:
        raise HTTPException(status_code=404, detail="DAG not found")
    return record


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "version": __version__}


# -- workflows ----------------------------------------------------------------


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
    record = _get_dag_or_404(session, dag_id)
    dag = build_dag(DAGDefinition(**record.definition))
    return {**dag_summary(record), "execution_levels": dag.topological_levels()}


@app.post("/dags/{dag_id}/pause")
def pause_dag(dag_id: str, session: Session = Depends(get_session)):
    """Stop this workflow from being triggered until it is resumed."""
    record = _get_dag_or_404(session, dag_id)
    record.is_paused = True
    session.commit()
    return {"dag_id": dag_id, "is_paused": True}


@app.post("/dags/{dag_id}/resume")
def resume_dag(dag_id: str, session: Session = Depends(get_session)):
    record = _get_dag_or_404(session, dag_id)
    record.is_paused = False
    session.commit()
    return {"dag_id": dag_id, "is_paused": False}


# -- runs ---------------------------------------------------------------------


@app.post("/dags/{dag_id}/runs", status_code=202)
async def trigger_dag(
    dag_id: str,
    response: Response,
    wait: bool = False,
    session: Session = Depends(get_session),
):
    """Start a run in the background (202). Poll GET /runs/{run_id} for progress.

    With `?wait=true` the request waits for the run to finish and returns the
    final result (201).
    """
    record = _get_dag_or_404(session, dag_id)
    if record.is_paused:
        raise HTTPException(status_code=409, detail="DAG is paused. Resume it before triggering a run.")

    run = runner.create_run(session, dag_id, record.definition)
    control = runner.launch(run.run_id, record.definition)

    if wait and control.task is not None:
        await control.task
        response.status_code = 201
        session.expire_all()
        run = session.get(RunRecord, run.run_id)
    return run_to_dict(run)


@app.get("/runs")
def list_runs(
    dag_id: str | None = None,
    status: str | None = None,
    session: Session = Depends(get_session),
):
    query = select(RunRecord).order_by(RunRecord.started_at)
    if dag_id:
        query = query.where(RunRecord.dag_id == dag_id)
    if status:
        query = query.where(RunRecord.status == status)
    return [run_to_dict(run) for run in session.scalars(query).all()]


@app.get("/runs/{run_id}")
def get_run(run_id: str, session: Session = Depends(get_session)):
    run = session.get(RunRecord, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return run_to_dict(run)


@app.post("/runs/{run_id}/pause")
def pause_run(run_id: str):
    """Pause a running run: tasks already running finish, no new task starts."""
    if not runner.set_paused(run_id, True):
        raise HTTPException(status_code=409, detail="Run is not active (finished, or unknown)")
    return {"run_id": run_id, "status": "paused"}


@app.post("/runs/{run_id}/resume")
def resume_run(run_id: str):
    if not runner.set_paused(run_id, False):
        raise HTTPException(status_code=409, detail="Run is not active (finished, or unknown)")
    return {"run_id": run_id, "status": "running"}


# -- built-in alert receiver (for demos) ---------------------------------------

received_alerts: list[dict[str, Any]] = []


@app.post("/alerts/sink", status_code=204)
def receive_alert(payload: dict[str, Any]):
    """A ready-made webhook target: set a DAG's alert_webhook to
    http://localhost:8000/alerts/sink and its failure alerts show up in GET /alerts/sink."""
    received_alerts.append(payload)
    del received_alerts[:-50]  # keep the latest 50


@app.get("/alerts/sink")
def list_received_alerts():
    return received_alerts
