import asyncio

import pytest

from pyflow import DAG, DAGError, TaskState, run_dag
from pyflow.executor import retry_delay_for


class FakeSleep:
    """Records requested waits instead of actually sleeping."""

    def __init__(self):
        self.delays = []

    async def __call__(self, seconds):
        self.delays.append(seconds)


def flaky(fail_times):
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        if calls["n"] <= fail_times:
            raise RuntimeError(f"boom {calls['n']}")
        return "ok"

    fn.calls = calls
    return fn


def run(dag, **kwargs):
    return asyncio.run(run_dag(dag, **kwargs))


def test_task_succeeds_after_retries():
    dag = DAG("t")
    fn = flaky(2)
    dag.add_task("a", fn, retries=3, retry_delay=1)
    sleep = FakeSleep()

    result = run(dag, sleep=sleep)

    task = result.tasks["a"]
    assert task.state == TaskState.SUCCESS
    assert task.attempts == 3
    assert task.error is None  # cleared once it finally worked
    assert task.result == "ok"
    assert sleep.delays == [1, 2]  # exponential: 1s then 2s


def test_backoff_doubles_and_is_capped():
    dag = DAG("t")
    dag.add_task("a", flaky(99), retries=5, retry_delay=2, retry_backoff=2, retry_max_delay=10)
    sleep = FakeSleep()

    run(dag, sleep=sleep)

    assert sleep.delays == [2, 4, 8, 10, 10]


def test_gives_up_after_retries_and_skips_downstream():
    dag = DAG("t")
    a = dag.add_task("a", flaky(99), retries=2, retry_delay=0.5)
    b = dag.add_task("b", lambda: None)
    a >> b
    sleep = FakeSleep()

    result = run(dag, sleep=sleep)

    assert result.tasks["a"].state == TaskState.FAILED
    assert result.tasks["a"].attempts == 3  # 1 try + 2 retries
    assert result.tasks["a"].error == "RuntimeError: boom 3"
    assert result.tasks["b"].state == TaskState.UPSTREAM_FAILED
    assert sleep.delays == [0.5, 1.0]


def test_no_retries_by_default():
    dag = DAG("t")
    dag.add_task("a", flaky(99))
    sleep = FakeSleep()

    result = run(dag, sleep=sleep)

    assert result.tasks["a"].attempts == 1
    assert result.tasks["a"].state == TaskState.FAILED
    assert sleep.delays == []


def test_callback_reports_retrying_state():
    dag = DAG("t")
    dag.add_task("a", flaky(1), retries=1)
    events = []

    run(dag, sleep=FakeSleep(), on_state_change=lambda r: events.append((r.state, r.attempts)))

    assert events == [
        (TaskState.RUNNING, 1),
        (TaskState.RETRYING, 1),
        (TaskState.RUNNING, 2),
        (TaskState.SUCCESS, 2),
    ]


def test_duration_covers_all_attempts():
    dag = DAG("t")
    dag.add_task("a", flaky(1), retries=1, retry_delay=0.2)

    result = run(dag)  # real sleep this time

    assert result.tasks["a"].duration_seconds >= 0.2


def test_decorator_accepts_retry_options():
    dag = DAG("t")

    @dag.task(retries=4, retry_delay=3)
    def download():
        pass

    assert dag.tasks["download"].retries == 4
    assert dag.tasks["download"].retry_delay == 3


@pytest.mark.parametrize(
    "options",
    [{"retries": -1}, {"retry_delay": -1}, {"retry_backoff": 0.5}, {"retry_max_delay": -5}],
)
def test_invalid_retry_options_rejected(options):
    with pytest.raises(DAGError):
        DAG("t").add_task("a", lambda: None, **options)


def test_retry_delay_formula():
    dag = DAG("t")
    task = dag.add_task("a", lambda: None, retries=9, retry_delay=1, retry_backoff=3, retry_max_delay=50)
    assert [retry_delay_for(task, n) for n in range(5)] == [1, 3, 9, 27, 50]


# -- pause gate -----------------------------------------------------------------


def test_closed_gate_blocks_new_tasks_until_reopened():
    async def scenario():
        started = []
        dag = DAG("t")
        a = dag.add_task("a", lambda: started.append("a"))
        b = dag.add_task("b", lambda: started.append("b"))
        a >> b

        gate = asyncio.Event()  # closed from the start
        runner = asyncio.create_task(run_dag(dag, gate=gate))
        await asyncio.sleep(0.15)
        assert started == []  # paused: nothing may start

        gate.set()
        result = await asyncio.wait_for(runner, timeout=3)
        return started, result

    started, result = asyncio.run(scenario())
    assert started == ["a", "b"]
    assert result.success


def test_pause_lets_running_task_finish_but_holds_the_next():
    async def scenario():
        order = []
        dag = DAG("t")

        async def slow():
            await asyncio.sleep(0.2)
            order.append("a")

        a = dag.add_task("a", slow)
        b = dag.add_task("b", lambda: order.append("b"))
        a >> b

        gate = asyncio.Event()
        gate.set()
        runner = asyncio.create_task(run_dag(dag, gate=gate))
        await asyncio.sleep(0.05)
        gate.clear()  # pause while a is running
        await asyncio.sleep(0.4)
        snapshot = list(order)  # a finished, b must still be waiting
        gate.set()
        result = await asyncio.wait_for(runner, timeout=3)
        return snapshot, order, result

    snapshot, order, result = asyncio.run(scenario())
    assert snapshot == ["a"]
    assert order == ["a", "b"]
    assert result.success
