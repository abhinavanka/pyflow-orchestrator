"""The built-in task operations that workflows submitted through the API can use."""

from __future__ import annotations

import time
from typing import Any, Callable

SUPPORTED_OPERATIONS = {
    "echo",
    "add",
    "multiply",
    "subtract",
    "divide",
    "sleep",  # wait `seconds` (max 30): handy for watching a run progress or pausing it
    "fail",  # always fails with `message`: handy for testing alerts
    "flaky",  # fails the first `fail_times` attempts, then succeeds: handy for testing retries
}

MAX_SLEEP_SECONDS = 30


def make_task_function(operation: str, params: dict[str, Any]) -> Callable[[], Any]:
    """Create a callable for a supported task operation."""
    # `flaky` needs memory between attempts. A fresh function is built for every
    # run, so this counter starts at zero for each run.
    attempts = {"n": 0}

    def execute():
        if operation == "echo":
            return params.get("message", "Hello from PyFlow")

        if operation == "add":
            return sum(params["values"])

        if operation == "multiply":
            result = 1
            for value in params["values"]:
                result *= value
            return result

        if operation == "subtract":
            values = params["values"]
            if len(values) != 2:
                raise ValueError("subtract requires exactly two values")
            return values[0] - values[1]

        if operation == "divide":
            values = params["values"]
            if len(values) != 2:
                raise ValueError("divide requires exactly two values")
            return values[0] / values[1]

        if operation == "sleep":
            seconds = min(float(params.get("seconds", 1)), MAX_SLEEP_SECONDS)
            time.sleep(max(seconds, 0))
            return f"slept {seconds}s"

        if operation == "fail":
            raise RuntimeError(params.get("message", "task failed on purpose"))

        if operation == "flaky":
            attempts["n"] += 1
            fail_times = int(params.get("fail_times", 1))
            if attempts["n"] <= fail_times:
                raise RuntimeError(f"transient failure {attempts['n']} of {fail_times}")
            return params.get("message", f"succeeded on attempt {attempts['n']}")

        raise ValueError(f"Unsupported operation: {operation}")

    return execute
