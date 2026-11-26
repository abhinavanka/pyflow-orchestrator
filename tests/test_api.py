import time

from sqlalchemy import select

from pyflow.db import DagRecord, RunRecord, TaskRunRecord

DIAMOND = {
    "dag_id": "demo_math",
    "description": "diamond",
    "tasks": [
        {"task_id": "start", "operation": "echo", "params": {"message": "hi"}},
        {"task_id": "add", "operation": "add", "params": {"values": [2, 3, 4]}, "depends_on": ["start"]},
        {"task_id": "mul", "operation": "multiply", "params": {"values": [4, 5]}, "depends_on": ["start"]},
        {
            "task_id": "sub",
            "operation": "subtract",
            "params": {"values": [20, 9]},
            "depends_on": ["add", "mul"],
        },
    ],
}

FAILING = {
    "dag_id": "demo_fail",
    "tasks": [
        {"task_id": "start", "operation": "echo"},
        {"task_id": "bad", "operation": "divide", "params": {"values": [1, 0]}, "depends_on": ["start"]},
        {"task_id": "skipped", "operation": "echo", "depends_on": ["bad"]},
        {"task_id": "independent", "operation": "add", "params": {"values": [1, 2]}, "depends_on": ["start"]},
    ],
}


def run_and_wait(client, dag_id):
    response = client.post(f"/dags/{dag_id}/runs", params={"wait": "true"})
    assert response.status_code == 201, response.text
    return response.json()


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


# -- creating DAGs -------------------------------------------------------------


def test_create_dag_returns_execution_levels(client):
    response = client.post("/dags", json=DIAMOND)
    assert response.status_code == 201
    body = response.json()
    assert body["task_count"] == 4
    assert body["execution_levels"] == [["start"], ["add", "mul"], ["sub"]]


def test_duplicate_dag_rejected(client):
    client.post("/dags", json=DIAMOND)
    assert client.post("/dags", json=DIAMOND).status_code == 409


def test_empty_dag_rejected(client):
    response = client.post("/dags", json={"dag_id": "x", "tasks": []})
    assert response.status_code == 400


def test_unknown_dependency_rejected(client):
    bad = {"dag_id": "x", "tasks": [{"task_id": "a", "operation": "echo", "depends_on": ["ghost"]}]}
    response = client.post("/dags", json=bad)
    assert response.status_code == 400
    assert "ghost" in response.json()["detail"]


def test_cycle_rejected(client):
    cyclic = {
        "dag_id": "loop",
        "tasks": [
            {"task_id": "a", "operation": "echo", "depends_on": ["b"]},
            {"task_id": "b", "operation": "echo", "depends_on": ["a"]},
        ],
    }
    response = client.post("/dags", json=cyclic)
    assert response.status_code == 400
    assert "cycle" in response.json()["detail"].lower()


def test_unknown_operation_rejected_at_creation(client):
    bad = {"dag_id": "x", "tasks": [{"task_id": "a", "operation": "teleport"}]}
    response = client.post("/dags", json=bad)
    assert response.status_code == 400
    assert "teleport" in response.json()["detail"]


def test_duplicate_task_ids_rejected(client):
    bad = {"dag_id": "x", "tasks": [{"task_id": "a", "operation": "echo"}, {"task_id": "a", "operation": "echo"}]}
    assert client.post("/dags", json=bad).status_code == 400


def test_invalid_retry_settings_rejected(client):
    bad = {"dag_id": "x", "tasks": [{"task_id": "a", "operation": "echo", "retries": -1}]}
    assert client.post("/dags", json=bad).status_code == 422
    bad["tasks"][0].update(retries=1, retry_backoff=0.5)
    assert client.post("/dags", json=bad).status_code == 422


# -- reading DAGs --------------------------------------------------------------


def test_list_and_get_dag(client):
    client.post("/dags", json=DIAMOND)

    listing = client.get("/dags").json()
    assert [d["dag_id"] for d in listing] == ["demo_math"]
    assert listing[0]["tasks"] == ["start", "add", "mul", "sub"]  # definition order kept
    assert listing[0]["is_paused"] is False

    detail = client.get("/dags/demo_math").json()
    assert detail["execution_levels"] == [["start"], ["add", "mul"], ["sub"]]


def test_get_unknown_dag_404(client):
    assert client.get("/dags/nope").status_code == 404


# -- running DAGs --------------------------------------------------------------


def test_trigger_with_wait_returns_final_result(client):
    client.post("/dags", json=DIAMOND)
    run = run_and_wait(client, "demo_math")

    assert run["status"] == "success"
    assert {tid: t["state"] for tid, t in run["tasks"].items()} == {
        "start": "success",
        "add": "success",
        "mul": "success",
        "sub": "success",
    }
    assert run["tasks"]["add"]["result"] == 9
    assert run["tasks"]["mul"]["result"] == 20
    assert run["tasks"]["sub"]["result"] == 11
    assert run["tasks"]["sub"]["attempts"] == 1
    assert run["started_at"] and run["finished_at"]


def test_trigger_without_wait_returns_immediately_and_can_be_polled(client, wait_for):
    slow = {"dag_id": "slow", "tasks": [{"task_id": "nap", "operation": "sleep", "params": {"seconds": 0.3}}]}
    client.post("/dags", json=slow)

    response = client.post("/dags/slow/runs")
    assert response.status_code == 202
    run = response.json()
    assert run["status"] == "running"
    assert run["tasks"]["nap"]["state"] == "pending"
    assert run["finished_at"] is None

    # while it runs, the database shows live progress
    time.sleep(0.1)
    assert client.get(f"/runs/{run['run_id']}").json()["tasks"]["nap"]["state"] == "running"

    final = wait_for(client, run["run_id"])
    assert final["status"] == "success"
    assert final["tasks"]["nap"]["result"] == "slept 0.3s"


def test_failure_skips_downstream_only(client):
    client.post("/dags", json=FAILING)
    run = run_and_wait(client, "demo_fail")

    assert run["status"] == "failed"
    states = {tid: t["state"] for tid, t in run["tasks"].items()}
    assert states == {
        "start": "success",
        "bad": "failed",
        "skipped": "upstream_failed",
        "independent": "success",
    }
    assert run["tasks"]["bad"]["error"] == "ZeroDivisionError: division by zero"
    assert run["tasks"]["independent"]["result"] == 3


def test_trigger_unknown_dag_404(client):
    assert client.post("/dags/nope/runs").status_code == 404


# -- retries through the API ---------------------------------------------------


def test_flaky_task_succeeds_after_retries(client):
    dag = {
        "dag_id": "flaky_dag",
        "tasks": [
            {
                "task_id": "download",
                "operation": "flaky",
                "params": {"fail_times": 2, "message": "downloaded"},
                "retries": 3,
                "retry_delay": 0.01,
            },
            {"task_id": "after", "operation": "echo", "depends_on": ["download"]},
        ],
    }
    client.post("/dags", json=dag)
    run = run_and_wait(client, "flaky_dag")

    assert run["status"] == "success"
    assert run["tasks"]["download"]["attempts"] == 3
    assert run["tasks"]["download"]["result"] == "downloaded"
    assert run["tasks"]["after"]["state"] == "success"


def test_task_fails_when_retries_run_out(client):
    dag = {
        "dag_id": "hopeless",
        "tasks": [
            {"task_id": "bad", "operation": "fail", "params": {"message": "nope"}, "retries": 2, "retry_delay": 0.01},
            {"task_id": "after", "operation": "echo", "depends_on": ["bad"]},
        ],
    }
    client.post("/dags", json=dag)
    run = run_and_wait(client, "hopeless")

    assert run["status"] == "failed"
    assert run["tasks"]["bad"]["state"] == "failed"
    assert run["tasks"]["bad"]["attempts"] == 3
    assert run["tasks"]["bad"]["error"] == "RuntimeError: nope"
    assert run["tasks"]["after"]["state"] == "upstream_failed"


def test_retrying_state_is_visible_while_waiting(client, wait_for):
    dag = {
        "dag_id": "slow_retry",
        "tasks": [
            {"task_id": "t", "operation": "flaky", "params": {"fail_times": 1}, "retries": 1, "retry_delay": 0.6}
        ],
    }
    client.post("/dags", json=dag)
    run = client.post("/dags/slow_retry/runs").json()

    seen = None
    deadline = time.time() + 3
    while time.time() < deadline:
        seen = client.get(f"/runs/{run['run_id']}").json()["tasks"]["t"]
        if seen["state"] == "retrying":
            break
        time.sleep(0.02)
    assert seen["state"] == "retrying"
    assert seen["attempts"] == 1
    assert "transient failure" in seen["error"]
    assert wait_for(client, run["run_id"])["status"] == "success"


# -- pause / resume ------------------------------------------------------------


def test_paused_dag_cannot_be_triggered_until_resumed(client):
    client.post("/dags", json=DIAMOND)

    assert client.post("/dags/demo_math/pause").json() == {"dag_id": "demo_math", "is_paused": True}
    assert client.get("/dags").json()[0]["is_paused"] is True
    blocked = client.post("/dags/demo_math/runs")
    assert blocked.status_code == 409
    assert "paused" in blocked.json()["detail"]
    assert client.get("/runs").json() == []  # no run was created

    assert client.post("/dags/demo_math/resume").json()["is_paused"] is False
    assert run_and_wait(client, "demo_math")["status"] == "success"


def test_pause_and_resume_unknown_dag_404(client):
    assert client.post("/dags/nope/pause").status_code == 404
    assert client.post("/dags/nope/resume").status_code == 404


def test_pausing_a_run_holds_back_the_next_task(client, wait_for):
    chain = {
        "dag_id": "chain",
        "tasks": [
            {"task_id": "first", "operation": "sleep", "params": {"seconds": 0.3}},
            {"task_id": "second", "operation": "echo", "params": {"message": "done"}, "depends_on": ["first"]},
        ],
    }
    client.post("/dags", json=chain)
    run_id = client.post("/dags/chain/runs").json()["run_id"]

    time.sleep(0.1)  # `first` is running now
    assert client.post(f"/runs/{run_id}/pause").json()["status"] == "paused"

    time.sleep(0.7)  # long enough for `first` to finish
    paused = client.get(f"/runs/{run_id}").json()
    assert paused["status"] == "paused"
    assert paused["tasks"]["first"]["state"] == "success"  # the running task was allowed to finish
    assert paused["tasks"]["second"]["state"] == "pending"  # but the next one is held back

    assert client.post(f"/runs/{run_id}/resume").json()["status"] == "running"
    final = wait_for(client, run_id)
    assert final["status"] == "success"
    assert final["tasks"]["second"]["result"] == "done"


def test_cannot_pause_a_finished_or_unknown_run(client):
    client.post("/dags", json=DIAMOND)
    run = run_and_wait(client, "demo_math")
    assert client.post(f"/runs/{run['run_id']}/pause").status_code == 409
    assert client.post("/runs/missing/resume").status_code == 409


# -- run history ---------------------------------------------------------------


def test_run_history_and_lookup(client):
    client.post("/dags", json=DIAMOND)
    first = run_and_wait(client, "demo_math")
    second = run_and_wait(client, "demo_math")

    history = client.get("/runs").json()
    assert [r["run_id"] for r in history] == [first["run_id"], second["run_id"]]

    assert client.get(f"/runs/{first['run_id']}").json()["run_id"] == first["run_id"]
    assert client.get("/runs/missing").status_code == 404


def test_runs_can_be_filtered_by_dag_and_status(client):
    client.post("/dags", json=DIAMOND)
    client.post("/dags", json=FAILING)
    run_and_wait(client, "demo_math")
    run_and_wait(client, "demo_fail")

    only_fail = client.get("/runs", params={"dag_id": "demo_fail"}).json()
    assert [r["dag_id"] for r in only_fail] == ["demo_fail"]

    failed = client.get("/runs", params={"status": "failed"}).json()
    assert [r["dag_id"] for r in failed] == ["demo_fail"]
    ok = client.get("/runs", params={"status": "success"}).json()
    assert [r["dag_id"] for r in ok] == ["demo_math"]


# -- persistence ---------------------------------------------------------------


def test_everything_is_stored_in_the_database(client, new_session):
    """Data must be readable from a brand-new session, i.e. it is really saved."""
    client.post("/dags", json=FAILING)
    run_id = run_and_wait(client, "demo_fail")["run_id"]

    with new_session() as session:
        dag = session.get(DagRecord, "demo_fail")
        assert dag is not None and len(dag.definition["tasks"]) == 4

        run = session.get(RunRecord, run_id)
        assert run.status == "failed"

        stored = {t.task_id: t.state for t in session.scalars(select(TaskRunRecord))}
        assert stored["bad"] == "failed"
        assert stored["skipped"] == "upstream_failed"


def test_dag_still_runs_from_stored_definition(client):
    """The DAG object is rebuilt from the database row, not kept in memory."""
    client.post("/dags", json=DIAMOND)
    first = run_and_wait(client, "demo_math")
    second = run_and_wait(client, "demo_math")
    assert first["status"] == second["status"] == "success"
    assert first["run_id"] != second["run_id"]


# -- alerts --------------------------------------------------------------------


def test_failed_run_sends_alert_to_webhook(client, monkeypatch):
    sent = []

    async def fake_send(url, payload, **kwargs):
        sent.append((url, payload))
        return True

    monkeypatch.setattr("pyflow.alerts.send_alert", fake_send)
    dag = {
        "dag_id": "alerting",
        "alert_webhook": "http://hooks.example/notify",
        "tasks": [{"task_id": "bad", "operation": "fail", "params": {"message": "kaput"}}],
    }
    client.post("/dags", json=dag)
    run = run_and_wait(client, "alerting")

    assert len(sent) == 1
    url, payload = sent[0]
    assert url == "http://hooks.example/notify"
    assert payload["event"] == "run_failed"
    assert payload["dag_id"] == "alerting"
    assert payload["run_id"] == run["run_id"]
    assert payload["failed_tasks"] == [{"task_id": "bad", "error": "RuntimeError: kaput", "attempts": 1}]


def test_successful_run_sends_no_alert(client, monkeypatch):
    sent = []

    async def fake_send(url, payload, **kwargs):
        sent.append(payload)
        return True

    monkeypatch.setattr("pyflow.alerts.send_alert", fake_send)
    dag = dict(DIAMOND, dag_id="happy", alert_webhook="http://hooks.example/notify")
    client.post("/dags", json=dag)
    run_and_wait(client, "happy")
    assert sent == []


def test_failure_without_a_webhook_is_fine(client):
    client.post("/dags", json=FAILING)
    assert run_and_wait(client, "demo_fail")["status"] == "failed"


def test_builtin_alert_sink_collects_alerts(client):
    assert client.get("/alerts/sink").json() == []
    assert client.post("/alerts/sink", json={"event": "run_failed", "dag_id": "x"}).status_code == 204
    assert client.get("/alerts/sink").json() == [{"event": "run_failed", "dag_id": "x"}]


# -- restart safety --------------------------------------------------------------


def test_runs_left_running_by_a_dead_process_are_marked_failed(client, new_session):
    from pyflow import runner

    with new_session() as session:
        session.add(DagRecord(dag_id="old", definition={"dag_id": "old", "tasks": []}))
        session.add(
            RunRecord(
                run_id="stale-run",
                dag_id="old",
                status="running",
                tasks=[
                    TaskRunRecord(task_id="done", state="success"),
                    TaskRunRecord(task_id="mid", state="running"),
                    TaskRunRecord(task_id="later", state="pending"),
                ],
            )
        )
        session.commit()

    assert runner.mark_interrupted_runs() == 1

    run = client.get("/runs/stale-run").json()
    assert run["status"] == "failed"
    assert run["finished_at"] is not None
    assert run["tasks"]["done"]["state"] == "success"  # finished work is left alone
    assert run["tasks"]["mid"]["state"] == "failed"
    assert run["tasks"]["later"]["state"] == "failed"
    assert "restart" in run["tasks"]["mid"]["error"]
