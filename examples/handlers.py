"""Example task handlers, loaded by a worker with:

    python -m app.worker --handlers examples.handlers

`flaky_task` fails its first two attempts and then succeeds, which is what makes
the retry/backoff path visible in a live run. `doomed_task` always fails, so it
exhausts its retries and lands on the dead letter queue.

The rest back the three example pipelines in this directory
(resume_pipeline.json, ml_pipeline.json, notification_pipeline.json) — each
handler just logs what it would do; none of this does real work. Anything in a
pipeline's `workflow` list without a handler here still runs, falling through to
`default_handler`, so a DAG's shape can be verified before its handlers exist.
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


# --- examples/notification_pipeline.json -------------------------------------


@register("fetch_recipients")
def fetch_recipients(ctx: TaskContext) -> None:
    logger.info("fetching recipient list")


@register("render_email")
def render_email(ctx: TaskContext) -> None:
    logger.info("rendering email template")


@register("render_sms")
def render_sms(ctx: TaskContext) -> None:
    logger.info("rendering sms template")


@register("send_email")
def send_email(ctx: TaskContext) -> None:
    logger.info("sending email")


@register("send_sms")
def send_sms(ctx: TaskContext) -> None:
    """Flaky on purpose: notification_pipeline.json gives this node a bigger
    retry budget (max_retries: 5), so failures here are worth watching retry
    rather than immediately failing the run."""
    if ctx.attempt < 1:
        raise RuntimeError("sms gateway timeout")
    logger.info("sms sent on attempt %s", ctx.attempt)


@register("log_delivery")
def log_delivery(ctx: TaskContext) -> None:
    logger.info("logging delivery receipt")
