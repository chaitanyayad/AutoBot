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


# --- examples/ml_pipeline.json -----------------------------------------------


@register("ingest_data")
def ingest_data(ctx: TaskContext) -> None:
    logger.info("ingesting training data (attempt %s)", ctx.attempt)


@register("validate_data")
def validate_data(ctx: TaskContext) -> None:
    logger.info("validating schema and null rates")


@register("engineer_features")
def engineer_features(ctx: TaskContext) -> None:
    logger.info("building feature matrix")


@register("train_model")
def train_model(ctx: TaskContext) -> None:
    logger.info("training model (attempt %s)", ctx.attempt)


@register("evaluate_model")
def evaluate_model(ctx: TaskContext) -> None:
    logger.info("scoring model against the holdout set")


@register("deploy_model")
def deploy_model(ctx: TaskContext) -> None:
    logger.info("promoting model to serving")


@register("publish_report")
def publish_report(ctx: TaskContext) -> None:
    logger.info("publishing evaluation report")
