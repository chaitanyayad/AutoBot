"""What a task actually *does*.

The orchestrator does not care what work a task performs — it only needs a
callable per task name and whether it raised. Handlers are registered by name;
anything unregistered falls through to `default_handler`, so a DAG can be wired
up and its ordering verified before any real work exists.

    from app.executors import register

    @register("parse_resume")
    def parse_resume(ctx):
        ...
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TaskContext:
    """Everything a handler is told about the task it is running."""

    task_id: str
    run_id: str
    task_name: str
    attempt: int
    worker_id: str


Handler = Callable[[TaskContext], None]

_REGISTRY: dict[str, Handler] = {}


def register(task_name: str) -> Callable[[Handler], Handler]:
    def decorator(func: Handler) -> Handler:
        _REGISTRY[task_name] = func
        return func

    return decorator


def default_handler(ctx: TaskContext) -> None:
    """Stand-in for unregistered tasks: log, pause briefly, succeed."""
    logger.info("executing %s (run %s) on %s", ctx.task_name, ctx.run_id, ctx.worker_id)
    time.sleep(float(_DEFAULT_DURATION))


def get_handler(task_name: str) -> Handler:
    return _REGISTRY.get(task_name, default_handler)


def registered_names() -> list[str]:
    return sorted(_REGISTRY)


def clear_registry() -> None:
    _REGISTRY.clear()


# Seconds the default handler sleeps. Zero in tests.
_DEFAULT_DURATION = 0.0


def set_default_duration(seconds: float) -> None:
    global _DEFAULT_DURATION
    _DEFAULT_DURATION = seconds
