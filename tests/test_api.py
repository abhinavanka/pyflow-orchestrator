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


# -- reading DAGs --------------------------------------------------------------


def test_list_and_get_dag(client):
    client.post("/dags", json=DIAMOND)

    listing = client.get("/dags").json()
    assert [d["dag_id"] for d in listing] == ["demo_math"]
    assert listing[0]["tasks"] == ["start", "add", "mul", "sub"]  # definition order kept

    detail = client.get("/dags/demo_math").json()
    assert detail["execution_levels"] == [["start"], ["add", "mul"], ["sub"]]


def test_get_unknown_dag_404(client):
    assert client.get("/dags/nope").status_code == 404


# -- running DAGs --------------------------------------------------------------


def test_trigger_runs_all_tasks(client):
    client.post("/dags", json=DIAMOND)
    response = client.post("/dags/demo_math/runs")
    assert response.status_code == 201

    run = response.json()
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
    assert run["started_at"] and run["finished_at"]


def test_failure_skips_downstream_only(client):
    client.post("/dags", json=FAILING)
    run = client.post("/dags/demo_fail/runs").json()

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


# -- run history ---------------------------------------------------------------


def test_run_history_and_lookup(client):
    client.post("/dags", json=DIAMOND)
    first = client.post("/dags/demo_math/runs").json()
    second = client.post("/dags/demo_math/runs").json()

    history = client.get("/runs").json()
    assert [r["run_id"] for r in history] == [first["run_id"], second["run_id"]]

    assert client.get(f"/runs/{first['run_id']}").json()["run_id"] == first["run_id"]
    assert client.get("/runs/missing").status_code == 404


def test_runs_can_be_filtered_by_dag(client):
    client.post("/dags", json=DIAMOND)
    client.post("/dags", json=FAILING)
    client.post("/dags/demo_math/runs")
    client.post("/dags/demo_fail/runs")

    only_fail = client.get("/runs", params={"dag_id": "demo_fail"}).json()
    assert len(only_fail) == 1
    assert only_fail[0]["dag_id"] == "demo_fail"


# -- persistence ---------------------------------------------------------------


def test_everything_is_stored_in_the_database(client, new_session):
    """Data must be readable from a brand-new session, i.e. it is really saved."""
    client.post("/dags", json=FAILING)
    run_id = client.post("/dags/demo_fail/runs").json()["run_id"]

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
    first = client.post("/dags/demo_math/runs").json()
    second = client.post("/dags/demo_math/runs").json()
    assert first["status"] == second["status"] == "success"
    assert first["run_id"] != second["run_id"]
