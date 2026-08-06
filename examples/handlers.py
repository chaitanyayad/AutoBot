"""Example task handlers, loaded by a worker with:

    python -m app.worker --handlers examples.handlers

`flaky_task` fails its first two attempts and then succeeds, which is what makes
the retry/backoff path visible in a live run. `doomed_task` always fails, so it
exhausts its retries and lands on the dead letter queue.
"""

import logging

from app.executors import TaskContext, register

logger = logging.getLogger(__name__)


@register("parse_resume")
def parse_resume(ctx: TaskContext) -> None:
    logger.info("parsing resume (attempt %s)", ctx.attempt)


@register("flaky_task")
def flaky_task(ctx: TaskContext) -> None:
    """Succeeds only on the third attempt."""
    if ctx.attempt < 2:
        raise RuntimeError(f"transient failure on attempt {ctx.attempt}")
    logger.info("flaky_task finally succeeded on attempt %s", ctx.attempt)


@register("doomed_task")
def doomed_task(ctx: TaskContext) -> None:
    raise RuntimeError("this task never works")


@register("slow_task")
def slow_task(ctx: TaskContext) -> None:
    """Long enough to kill the worker mid-execution and watch recovery."""
    import time

    logger.info("slow_task starting on %s", ctx.worker_id)
    time.sleep(60)
