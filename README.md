# PyFlow Orchestrator

A lightweight workflow orchestration platform in Python. Define pipelines as
dependency graphs (DAGs), run them in the right order with parallelism, retry
failures, backfill history, and watch everything in a monitoring console.

> Status: **Phase 4 of 9 complete** (core engine, REST API, PostgreSQL persistence, retries, pause/resume, alerts). See the roadmap below.

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
Ready-to-paste workflows are in [docs/examples.md](docs/examples.md).

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | Health check |
| GET | `/dags` | List workflows |
| POST | `/dags` | Create a workflow (validated: cycles, unknown dependencies, bad operations) |
| GET | `/dags/{dag_id}` | Workflow details and execution levels |
| POST | `/dags/{dag_id}/pause` / `resume` | Block / allow new runs of a workflow |
| POST | `/dags/{dag_id}/runs` | Start a run in the background (202); `?wait=true` waits for the result (201) |
| GET | `/runs` | Run history (optional `?dag_id=` and `?status=`) |
| GET | `/runs/{run_id}` | Live state of one run: per-task state, attempts, result, error, duration |
| POST | `/runs/{run_id}/pause` / `resume` | Pause a running run (running tasks finish, no new task starts) |
| GET/POST | `/alerts/sink` | Built-in webhook receiver for demoing alerts |

### Reliability features

- **Retries with exponential backoff.** Per task: `retries`, `retry_delay` (default 1s),
  `retry_backoff` (default 2), `retry_max_delay` (default 60s). A task waiting to retry shows
  `retrying`; `attempts` counts every try.
- **Failure propagation.** When a task is out of retries, tasks downstream are `upstream_failed`
  and skipped; independent branches still run.
- **Pause / resume.** Whole workflows (no new runs) or an individual run (no new task starts).
- **Alerts.** A failed run POSTs JSON to the DAG's `alert_webhook` (or `PYFLOW_ALERT_WEBHOOK`).
  Delivery problems are logged and never break a run.
- **Live state.** Every task state change is written to PostgreSQL as it happens.
- **Restart safety.** Runs left `running` by a crashed process are marked `failed` on startup,
  and the database schema upgrades itself in place (no data loss).

Workflows, runs and task results are stored in PostgreSQL, so history survives
restarts. Supported task operations: `echo`, `add`, `multiply`, `subtract`, `divide`,
and the demo helpers `sleep`, `fail`, `flaky`.

## Project layout

```
pyflow/          core package
  dag.py         DAG, Task, topological sort, cycle detection
  executor.py    asyncio executor, task states, run results
  db.py          SQLAlchemy models, schema upgrade
  definitions.py workflow definitions from the API -> DAG objects
  operations.py  built-in task operations
  runner.py      background runs, live state, pause/resume, alerts
  alerts.py      webhook delivery
  api/           FastAPI app
dags/            example workflows
tests/           pytest suite
docker-compose.yml   API + PostgreSQL + Redis
```

## Roadmap

- [x] 1. Repo setup, Docker Compose, health endpoint
- [x] 2. Core DAG engine (topological sort, parallel execution, failure propagation)
- [x] 3. Persistence: DAGs, runs and task states in PostgreSQL
- [ ] 4. Workers + Redis queue (planned after the console)
- [x] 5. Retries with exponential backoff, pause/resume, alerts
- [ ] 6. Cron schedules and historical backfills
- [ ] 7. REST API + CLI
- [ ] 8. React monitoring console (live states, logs, Gantt, durations)
- [ ] 9. Tests, architecture diagram, demo, presentation
