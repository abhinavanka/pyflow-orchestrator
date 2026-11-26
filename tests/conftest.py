import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import sessionmaker

from pyflow.api.main import app
from pyflow.db import Base, get_session, make_engine


@pytest.fixture
def engine():
    """A fresh in-memory SQLite database for every test."""
    engine = make_engine("sqlite://")
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def client(engine):
    factory = sessionmaker(bind=engine, expire_on_commit=False)

    def override_session():
        with factory() as session:
            yield session

    app.dependency_overrides[get_session] = override_session
    # No `with` block, so the startup hook (which targets Postgres) is skipped.
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def new_session(engine):
    """Open a separate session, to prove data was really written to the database."""
    return sessionmaker(bind=engine, expire_on_commit=False)
