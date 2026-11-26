"""Database layer: tables for workflows, runs and per-task results.

Uses SQLAlchemy so the same code runs on PostgreSQL (the real deployment)
and SQLite (fast, in-memory tests).
"""

from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Iterator

from sqlalchemy import JSON, DateTime, Float, ForeignKey, Integer, String, Text, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    relationship,
    sessionmaker,
)
from sqlalchemy.pool import StaticPool

from pyflow.config import get_settings


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class DagRecord(Base):
    """A saved workflow definition."""

    __tablename__ = "dags"

    dag_id: Mapped[str] = mapped_column(String(200), primary_key=True)
    description: Mapped[str] = mapped_column(Text, default="")
    # The full definition exactly as the user submitted it (tasks, params,
    # dependencies). The DAG object is rebuilt from this whenever it runs.
    definition: Mapped[dict[str, Any]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    runs: Mapped[list["RunRecord"]] = relationship(back_populates="dag")


class RunRecord(Base):
    """One execution of a workflow."""

    __tablename__ = "runs"

    run_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    dag_id: Mapped[str] = mapped_column(ForeignKey("dags.dag_id"), index=True)
    status: Mapped[str] = mapped_column(String(20))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    dag: Mapped[DagRecord] = relationship(back_populates="runs")
    tasks: Mapped[list["TaskRunRecord"]] = relationship(
        back_populates="run", cascade="all, delete-orphan", order_by="TaskRunRecord.id"
    )


class TaskRunRecord(Base):
    """The outcome of one task inside one run."""

    __tablename__ = "task_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.run_id"), index=True)
    task_id: Mapped[str] = mapped_column(String(200))
    state: Mapped[str] = mapped_column(String(20))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    duration_seconds: Mapped[float | None] = mapped_column(Float)
    result: Mapped[Any] = mapped_column(JSON, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    run: Mapped[RunRecord] = relationship(back_populates="tasks")


# -- engine / session plumbing ------------------------------------------------


def normalize_url(url: str) -> str:
    """Make `postgresql://...` use the modern psycopg 3 driver."""
    if url.startswith("postgresql://"):
        return "postgresql+psycopg://" + url[len("postgresql://") :]
    if url.startswith("postgres://"):
        return "postgresql+psycopg://" + url[len("postgres://") :]
    return url


def make_engine(url: str) -> Engine:
    url = normalize_url(url)
    if url.startswith("sqlite"):
        # In-memory SQLite needs one shared connection to keep its data.
        return create_engine(
            url,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
    return create_engine(url, pool_pre_ping=True)


@lru_cache
def get_engine() -> Engine:
    return make_engine(get_settings().database_url)


def init_db(engine: Engine | None = None) -> None:
    """Create any missing tables."""
    Base.metadata.create_all(engine or get_engine())


def get_session() -> Iterator[Session]:
    """FastAPI dependency: one database session per request."""
    factory = sessionmaker(bind=get_engine(), expire_on_commit=False)
    with factory() as session:
        yield session
