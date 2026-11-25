"""Runtime configuration, read from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    redis_url: str
    dags_folder: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_url=os.getenv(
                "PYFLOW_DATABASE_URL",
                "postgresql://pyflow:pyflow@localhost:5432/pyflow",
            ),
            redis_url=os.getenv("PYFLOW_REDIS_URL", "redis://localhost:6379/0"),
            dags_folder=os.getenv("PYFLOW_DAGS_FOLDER", "dags"),
        )


def get_settings() -> Settings:
    return Settings.from_env()
