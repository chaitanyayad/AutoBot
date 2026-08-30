"""Phase 5: cron-triggered runs.

A workflow may carry a `schedule` (a five-field cron expression). This module
wires that into APScheduler:

- `validate_cron` — rejected at registration time (`schemas.py`), same principle
  as an invalid DAG: a workflow that can never fire correctly never gets stored.
- `start()` — called once from the API's lifespan. Loads every workflow that has
  a schedule and arms a job for it, so schedules survive an API restart without
  a separate migration step.
- `add_job` / `remove_job` — called from the registration endpoint so a newly
  scheduled workflow is armed immediately, without waiting for the next restart.

The job itself does exactly what a manual trigger does — `create_run` then
`resolve` — so a scheduled run and a POST /trigger run are indistinguishable
once they exist.
"""

from __future__ import annotations

import logging
import uuid
from typing import Callable

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app import config
from app.models import Workflow

logger = logging.getLogger(__name__)

_JOB_PREFIX = "workflow:"


def validate_cron(expression: str) -> None:
    """Raise ValueError if `expression` is not a valid five-field cron string."""
    CronTrigger.from_crontab(expression, timezone=config.SCHEDULER_TIMEZONE)


def _job_id(workflow_id: uuid.UUID | str) -> str:
    return f"{_JOB_PREFIX}{workflow_id}"


def _run_scheduled(workflow_id: str, session_factory: Callable) -> None:
    """The job body: trigger a fresh run of `workflow_id`.

    Runs in APScheduler's own thread pool, so it opens its own session rather
    than borrowing one from a request — the same pattern as the worker handling
    one delivery per session.
    """
    from app import scheduler as run_scheduler  # local import: avoid a cycle at module load

    session = session_factory()
    try:
        workflow = session.get(Workflow, uuid.UUID(workflow_id))
        if workflow is None:
            logger.warning("scheduled workflow %s no longer exists; skipping", workflow_id)
            return
        run = run_scheduler.create_run(session, workflow)
        run_scheduler.resolve(session, run.id)
        session.commit()
        logger.info("cron-triggered run %s for workflow %r", run.id, workflow.name)
    except Exception:
        session.rollback()
        logger.exception("scheduled trigger failed for workflow %s", workflow_id)
    finally:
        session.close()


def add_job(sched: BackgroundScheduler, workflow: Workflow, session_factory: Callable) -> None:
    """Arm (or replace) the cron job for one workflow. No-op if unscheduled."""
    if not workflow.schedule:
        return
    trigger = CronTrigger.from_crontab(workflow.schedule, timezone=config.SCHEDULER_TIMEZONE)
    sched.add_job(
        _run_scheduled,
        trigger=trigger,
        id=_job_id(workflow.id),
        args=[str(workflow.id), session_factory],
        replace_existing=True,
        misfire_grace_time=300,
    )
    logger.info("armed schedule %r for workflow %s", workflow.schedule, workflow.id)


def remove_job(sched: BackgroundScheduler, workflow_id: uuid.UUID | str) -> None:
    try:
        sched.remove_job(_job_id(workflow_id))
    except Exception:
        pass  # no job to remove is not an error


def start(session_factory: Callable) -> BackgroundScheduler:
    """Build, populate and start the process-wide cron scheduler.

    Called once from the API's lifespan. Every workflow with a non-null
    `schedule` gets a job — this is what makes schedules durable across a
    restart, since nothing else persists APScheduler's own job store.
    """
    sched = BackgroundScheduler(timezone=config.SCHEDULER_TIMEZONE)
    session = session_factory()
    try:
        from sqlalchemy import select

        workflows = session.scalars(
            select(Workflow).where(Workflow.schedule.is_not(None))
        ).all()
        for workflow in workflows:
            add_job(sched, workflow, session_factory)
    finally:
        session.close()
    sched.start()
    logger.info("cron scheduler started with %d job(s)", len(sched.get_jobs()))
    return sched
