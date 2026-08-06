"""Run lifecycle: materialise task rows, resolve what is runnable, close out runs.

Phase 1 stops at "which tasks are runnable". Phase 2 replaces `dispatch` with a
real RabbitMQ publish; nothing else in here needs to change.
"""

import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app import config
from app.broker import TaskMessage, get_broker
from app.dag import get_runnable_tasks, is_run_failed, is_run_finished, validate_definition
from app.retry import backoff_delay, should_retry
from app.models import (
    RUN_COMPLETED,
    RUN_FAILED,
    RUN_RUNNING,
    TASK_FAILED,
    TASK_QUEUED,
    TASK_RUNNING,
    TASK_SUCCESS,
    Task,
    Workflow,
    WorkflowRun,
)

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def create_run(session: Session, workflow: Workflow) -> WorkflowRun:
    """Create a run and one pending task row per node in the definition."""
    specs = validate_definition(workflow.definition)

    run = WorkflowRun(workflow_id=workflow.id, status=RUN_RUNNING, triggered_at=_utcnow())
    session.add(run)
    session.flush()  # assign run.id

    for spec in specs:
        session.add(
            Task(
                run_id=run.id,
                task_name=spec.id,
                depends_on=list(spec.depends_on),
                max_retries=(
                    spec.max_retries
                    if spec.max_retries is not None
                    else config.DEFAULT_MAX_RETRIES
                ),
            )
        )
    session.flush()
    return run


def load_tasks(session: Session, run_id: uuid.UUID) -> list[Task]:
    """All task rows for a run, in stable creation order."""
    return list(
        session.scalars(
            select(Task).where(Task.run_id == run_id).order_by(Task.task_name)
        )
    )


def resolve(session: Session, run_id: uuid.UUID) -> list[Task]:
    """The scheduler tick: find newly runnable tasks and hand them to dispatch.

    Called after a run is triggered and after every task completion.
    """
    tasks = load_tasks(session, run_id)
    runnable = get_runnable_tasks(tasks)
    if runnable:
        dispatch(session, runnable)
    else:
        finalize_if_done(session, run_id, tasks)
    return runnable


def dispatch(session: Session, tasks: list[Task]) -> None:
    """Mark tasks `queued` and publish them to the broker.

    The status change is flushed *before* publishing, so a worker that picks the
    message up immediately never sees the task as still `pending`. If publishing
    then fails the task is left `queued` with nothing to run it — Phase 3's
    heartbeat/timeout recovery is what reclaims that case.
    """
    broker = get_broker()
    for task in tasks:
        task.status = TASK_QUEUED
    session.flush()

    for task in tasks:
        broker.publish(
            TaskMessage(
                task_id=str(task.id),
                run_id=str(task.run_id),
                task_name=task.task_name,
                attempt=task.retry_count,
            )
        )
    logger.debug("dispatched %s", [t.task_name for t in tasks])


def report_result(
    session: Session,
    task: Task,
    *,
    succeed: bool,
    error: str | None = None,
) -> WorkflowRun:
    """Record a task outcome and release whatever it unblocked.

    The single place a task leaves the running state — used by the worker and by
    the development `/simulate` endpoint, so both drive the state machine
    identically.

    A failure with retries remaining is re-queued with exponential backoff rather
    than failing the run. Only an exhausted task is marked `failed`, and it is
    also parked on the dead letter queue.
    """
    if succeed:
        task.status = TASK_SUCCESS
        task.completed_at = _utcnow()
        task.error_message = None
        session.flush()
        resolve(session, task.run_id)
        return session.get(WorkflowRun, task.run_id)

    task.error_message = error

    if should_retry(task.retry_count, task.max_retries):
        return _requeue_for_retry(session, task)

    task.status = TASK_FAILED
    task.completed_at = _utcnow()
    session.flush()

    get_broker().dead_letter(
        TaskMessage(
            task_id=str(task.id),
            run_id=str(task.run_id),
            task_name=task.task_name,
            attempt=task.retry_count,
            reason=f"exhausted {task.max_retries} retries: {error}",
        )
    )
    logger.warning(
        "task %s failed permanently after %s retries", task.task_name, task.retry_count
    )

    resolve(session, task.run_id)
    return session.get(WorkflowRun, task.run_id)


def _requeue_for_retry(session: Session, task: Task) -> WorkflowRun:
    """Put a failed task back on the queue after a backoff delay."""
    task.retry_count += 1
    task.status = TASK_QUEUED
    # Clear the previous attempt's execution record; the task has not completed.
    task.worker_id = None
    task.started_at = None
    task.completed_at = None
    session.flush()

    delay = backoff_delay(task.retry_count)
    get_broker().publish(
        TaskMessage(
            task_id=str(task.id),
            run_id=str(task.run_id),
            task_name=task.task_name,
            attempt=task.retry_count,
        ),
        delay=delay,
    )
    logger.info(
        "retry %s/%s for %s in %.2fs",
        task.retry_count,
        task.max_retries,
        task.task_name,
        delay,
    )
    return session.get(WorkflowRun, task.run_id)


def claim(session: Session, task: Task, worker_id: str) -> None:
    """Move a dispatched task into `running` on behalf of a worker."""
    task.status = TASK_RUNNING
    task.worker_id = worker_id
    task.started_at = _utcnow()
    session.flush()


def finalize_if_done(session: Session, run_id: uuid.UUID, tasks: list[Task] | None = None) -> WorkflowRun:
    """Close out the run if nothing can make progress any more."""
    run = session.get(WorkflowRun, run_id)
    if tasks is None:
        tasks = load_tasks(session, run_id)

    if is_run_finished(tasks):
        run.status = RUN_FAILED if is_run_failed(tasks) else RUN_COMPLETED
        run.completed_at = _utcnow()
        session.flush()
    return run
