from sqlalchemy import inspect, text
from sqlalchemy.orm import sessionmaker

from pyflow.db import DagRecord, TaskRunRecord, add_missing_columns, init_db, make_engine

# The tables exactly as Phase 3 created them (no is_paused, no attempts).
OLD_SCHEMA = [
    """CREATE TABLE dags (
        dag_id VARCHAR(200) PRIMARY KEY, description TEXT, definition JSON, created_at DATETIME)""",
    """CREATE TABLE runs (
        run_id VARCHAR(36) PRIMARY KEY, dag_id VARCHAR(200), status VARCHAR(20),
        started_at DATETIME, finished_at DATETIME)""",
    """CREATE TABLE task_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, run_id VARCHAR(36), task_id VARCHAR(200),
        state VARCHAR(20), started_at DATETIME, finished_at DATETIME,
        duration_seconds FLOAT, result JSON, error TEXT)""",
]


def test_phase3_database_is_upgraded_in_place_without_losing_data(tmp_path):
    engine = make_engine(f"sqlite:///{(tmp_path / 'old.db').as_posix()}")
    with engine.begin() as conn:
        for statement in OLD_SCHEMA:
            conn.execute(text(statement))
        conn.execute(
            text("INSERT INTO dags (dag_id, description, definition) VALUES ('old', 'kept', '{\"tasks\": []}')")
        )
        conn.execute(text("INSERT INTO runs (run_id, dag_id, status) VALUES ('r1', 'old', 'success')"))
        conn.execute(text("INSERT INTO task_runs (run_id, task_id, state) VALUES ('r1', 't', 'success')"))

    init_db(engine)

    columns = {t: {c["name"] for c in inspect(engine).get_columns(t)} for t in ("dags", "task_runs")}
    assert "is_paused" in columns["dags"]
    assert "attempts" in columns["task_runs"]

    with sessionmaker(bind=engine)() as session:
        dag = session.get(DagRecord, "old")
        assert dag.description == "kept"  # old data survived
        assert dag.is_paused is False  # new column got its default
        assert session.query(TaskRunRecord).one().attempts == 0


def test_upgrade_is_a_no_op_when_schema_is_current(engine):
    assert add_missing_columns(engine) == []
