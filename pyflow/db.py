"""Database layer: tables for workflows, runs and per-task results.

Uses SQLAlchemy so the same code runs on PostgreSQL (the real deployment)
and SQLite (fast, in-memory tests).
"""

from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Iterator

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    create_engine,
    false,
    inspect,
    text,
)
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
    # A paused DAG cannot be triggered until it is resumed.
    is_paused: Mapped[bool] = mapped_column(Boolean, default=False, server_default=false())

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
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))
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
        kwargs: dict[str, Any] = {"connect_args": {"check_same_thread": False}}
        if url in ("sqlite://", "sqlite:///:memory:"):
            # In-memory SQLite needs one shared connection to keep its data.
            kwargs["poolclass"] = StaticPool
        return create_engine(url, **kwargs)
    return create_engine(url, pool_pre_ping=True)


@lru_cache
def get_engine() -> Engine:
    return make_engine(get_settings().database_url)


def add_missing_columns(engine: Engine) -> list[str]:
    """Light schema upgrade: add columns that exist in the models but not yet in
    the database (create_all only creates whole missing tables). Returns what it added.
    """
    added: list[str] = []
    inspector = inspect(engine)
    with engine.begin() as conn:
        for table in Base.metadata.sorted_tables:
            if not inspector.has_table(table.name):
                continue
            existing = {col["name"] for col in inspector.get_columns(table.name)}
            for column in table.columns:
                if column.name in existing:
                    continue
                ddl = (
                    f"ALTER TABLE {table.name} ADD COLUMN {column.name} "
                    f"{column.type.compile(dialect=engine.dialect)}"
                )
                if column.server_default is not None:
                    default = column.server_default.arg
                    if hasattr(default, "compile"):
                        default = default.compile(
                            dialect=engine.dialect, compile_kwargs={"literal_binds": True}
                        )
                    ddl += f" DEFAULT {default}"
                conn.execute(text(ddl))
                added.append(f"{table.name}.{column.name}")
    return added


def init_db(engine: Engine | None = None) -> None:
    """Create missing tables and add any missing columns."""
    engine = engine or get_engine()
    Base.metadata.create_all(engine)
    add_missing_columns(engine)


@lru_cache
def get_session_factory() -> sessionmaker[Session]:
    return sessionmaker(bind=get_engine(), expire_on_commit=False)


def get_session() -> Iterator[Session]:
    """FastAPI dependency: one database session per request."""
    with get_session_factory()() as session:
        yield session
