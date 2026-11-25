"""Example pipeline: extract -> (clean_users, clean_orders in parallel) -> join -> report.

Run it directly to watch the engine work:

    python dags/example_etl.py
"""

import asyncio
import time

from pyflow import DAG, run_dag

dag = DAG("example_etl", description="Demo ETL pipeline")


@dag.task()
def extract():
    time.sleep(0.5)
    return {"users": 120, "orders": 340}


@dag.task()
def clean_users():
    time.sleep(1.0)
    return "users cleaned"


@dag.task()
def clean_orders():
    time.sleep(1.0)
    return "orders cleaned"


@dag.task()
def join():
    time.sleep(0.5)
    return "joined"


@dag.task()
def report():
    return "report ready"


extract >> [clean_users, clean_orders] >> join >> report


if __name__ == "__main__":

    def show(result):
        print(f"  {result.task_id:<14} {result.state.value}")

    print(f"Execution levels: {dag.topological_levels()}")
    outcome = asyncio.run(run_dag(dag, on_state_change=show))
    total = (outcome.finished_at - outcome.started_at).total_seconds()
    print(f"Run {'succeeded' if outcome.success else 'failed'} in {total:.1f}s")
