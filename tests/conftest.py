import time

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import sessionmaker

from pyflow import db, runner
from pyflow.api import main
from pyflow.db import init_db, make_engine


@pytest.fixture
def engine(tmp_path):
    """A fresh SQLite database file for every test."""
    engine = make_engine(f"sqlite:///{(tmp_path / 'test.db').as_posix()}")
    init_db(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def client(engine, monkeypatch):
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    # Point the whole app (API requests and background runs) at the test database.
    monkeypatch.setattr(db, "get_session_factory", lambda: factory)
    monkeypatch.setattr(main, "init_db", lambda: init_db(engine))
    runner.controls.clear()
    # The `with` block keeps one event loop alive, so background runs can finish.
    with TestClient(main.app) as test_client:
        yield test_client
    runner.controls.clear()
    main.received_alerts.clear()


@pytest.fixture
def new_session(engine):
    """Open a separate session, to prove data was really written to the database."""
    return sessionmaker(bind=engine, expire_on_commit=False)


@pytest.fixture
def wait_for():
    """Poll a run until it reaches one of the given statuses (or fail the test)."""

    def _wait(client, run_id, statuses=("success", "failed"), timeout=5.0):
        deadline = time.time() + timeout
        run = None
        while time.time() < deadline:
            run = client.get(f"/runs/{run_id}").json()
            if run["status"] in statuses:
                return run
            time.sleep(0.02)
        raise AssertionError(f"run still {run['status'] if run else '?'} after {timeout}s: {run}")

    return _wait
