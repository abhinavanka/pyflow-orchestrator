# PyFlow Orchestrator

A lightweight workflow orchestration platform in Python. Define pipelines as
dependency graphs (DAGs), run them in the right order with parallelism, retry
failures, backfill history, and watch everything in a monitoring console.

> Status: **Phase 3 of 9 complete** (core engine, REST API, PostgreSQL persistence). See the roadmap below.

## Why

Teams often chain scripts with cron: no dependency handling, silent failures,
no history. PyFlow runs tasks in dependency order, skips work downstream of a
failure, and keeps a record of every run.

## Quick start

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

pytest                       # run the test suite
python dags/example_etl.py   # watch the example pipeline run
```

Start the full stack (API + Postgres + Redis):

```bash
docker compose up --build
curl http://localhost:8000/health
```

## Defining a workflow

```python
from pyflow import DAG

dag = DAG("example_etl")

@dag.task()
def extract(): ...

@dag.task()
def clean_users(): ...

@dag.task()
def clean_orders(): ...

@dag.task()
def join(): ...

extract >> [clean_users, clean_orders] >> join
```

`clean_users` and `clean_orders` run in parallel; `join` waits for both. If
`clean_users` fails, `join` is marked `upstream_failed` and skipped, while
unrelated branches keep going.

## How scheduling works

1. `DAG.topological_levels()` uses Kahn's algorithm to group tasks into
   levels and rejects cycles with a clear error.
2. `run_dag()` tracks which upstream tasks each task is still waiting on and
   starts a task the moment that set is empty, up to `max_concurrency`.
3. Task states: `pending -> running -> success | failed`, or `upstream_failed`
   when skipped.

## REST API

With the stack running, interactive docs are at http://localhost:8000/docs.

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | Health check |
| GET | `/dags` | List workflows |
| POST | `/dags` | Create a workflow (validated: cycles, unknown dependencies, bad operations) |
| GET | `/dags/{dag_id}` | Workflow details and execution levels |
| POST | `/dags/{dag_id}/runs` | Run a workflow |
| GET | `/runs` | Run history (optional `?dag_id=`) |
| GET | `/runs/{run_id}` | One run with per-task state, result, error, duration |

Workflows, runs and task results are stored in PostgreSQL, so history survives
restarts. Supported task operations: `echo`, `add`, `multiply`, `subtract`, `divide`.

## Project layout

```
pyflow/          core package
  dag.py         DAG, Task, topological sort, cycle detection
  executor.py    asyncio executor, task states, run results
  db.py          SQLAlchemy models (dags, runs, task_runs)
  api/           FastAPI app
dags/            example workflows
tests/           pytest suite
docker-compose.yml   API + PostgreSQL + Redis
```

## Roadmap

- [x] 1. Repo setup, Docker Compose, health endpoint
- [x] 2. Core DAG engine (topological sort, parallel execution, failure propagation)
- [x] 3. Persistence: DAGs, runs and task states in PostgreSQL
- [ ] 4. Workers + Redis queue
- [ ] 5. Retries with exponential backoff, pause/resume, alerts
- [ ] 6. Cron schedules and historical backfills
- [ ] 7. REST API + CLI
- [ ] 8. React monitoring console (live states, logs, Gantt, durations)
- [ ] 9. Tests, architecture diagram, demo, presentation
