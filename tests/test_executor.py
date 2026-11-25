import asyncio
import threading
import time

from pyflow import DAG, TaskState, run_dag


def run(dag, **kwargs):
    return asyncio.run(run_dag(dag, **kwargs))


def test_runs_in_dependency_order():
    log = []
    lock = threading.Lock()
    dag = DAG("t")

    def step(name):
        def fn():
            with lock:
                log.append(name)

        return fn

    a, b, c = (dag.add_task(n, step(n)) for n in "abc")
    a >> b >> c

    result = run(dag)
    assert result.success
    assert log == ["a", "b", "c"]


def test_independent_tasks_run_in_parallel():
    dag = DAG("t")
    for name in ("x", "y", "z"):
        dag.add_task(name, lambda: time.sleep(0.3))

    start = time.perf_counter()
    result = run(dag, max_concurrency=3)
    elapsed = time.perf_counter() - start

    assert result.success
    assert elapsed < 0.6, f"expected parallel execution, took {elapsed:.2f}s"


def test_max_concurrency_limits_parallelism():
    dag = DAG("t")
    for name in ("x", "y", "z"):
        dag.add_task(name, lambda: time.sleep(0.2))

    start = time.perf_counter()
    run(dag, max_concurrency=1)
    elapsed = time.perf_counter() - start

    assert elapsed >= 0.55, f"expected serial execution, took {elapsed:.2f}s"


def test_failure_skips_downstream_but_not_other_branches():
    dag = DAG("t")

    def boom():
        raise RuntimeError("bad data")

    a = dag.add_task("a", lambda: 1)
    b = dag.add_task("b", boom)
    c = dag.add_task("c", lambda: 2)
    d = dag.add_task("d", lambda: 3)
    e = dag.add_task("e", lambda: 4)
    a >> [b, c]
    b >> d  # d must be skipped
    c >> e  # e is on a healthy branch and must still run

    result = run(dag)

    assert not result.success
    assert result.states == {
        "a": "success",
        "b": "failed",
        "c": "success",
        "d": "upstream_failed",
        "e": "success",
    }
    assert result.tasks["b"].error == "RuntimeError: bad data"


def test_diamond_with_one_failed_parent_skips_join():
    dag = DAG("t")
    a = dag.add_task("a", lambda: None)
    b = dag.add_task("b", lambda: None)
    c = dag.add_task("c", lambda: 1 / 0)
    d = dag.add_task("d", lambda: None)
    a >> [b, c] >> d

    result = run(dag)
    assert result.tasks["d"].state == TaskState.UPSTREAM_FAILED
    assert result.tasks["b"].state == TaskState.SUCCESS


def test_supports_async_and_sync_tasks():
    dag = DAG("t")

    async def async_task():
        await asyncio.sleep(0.01)
        return "async"

    a = dag.add_task("a", async_task)
    b = dag.add_task("b", lambda: "sync")
    a >> b

    result = run(dag)
    assert result.success
    assert result.tasks["a"].result == "async"
    assert result.tasks["b"].result == "sync"


def test_state_callback_sees_full_lifecycle():
    dag = DAG("t")
    dag.add_task("a", lambda: None)
    events = []

    run(dag, on_state_change=lambda r: events.append((r.task_id, r.state)))

    assert events == [("a", TaskState.RUNNING), ("a", TaskState.SUCCESS)]


def test_durations_are_recorded():
    dag = DAG("t")
    dag.add_task("a", lambda: time.sleep(0.1))

    result = run(dag)
    assert result.tasks["a"].duration_seconds >= 0.1
    assert result.finished_at >= result.started_at
