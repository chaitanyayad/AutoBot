"""Run lifecycle: materialise task rows, resolve what is runnable, close out runs.

Phase 1 stops at "which tasks are runnable". Phase 2 replaces `dispatch` with a
real RabbitMQ publish; nothing else in here needs to change.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app import config
from app.dag import get_runnable_tasks, is_run_failed, is_run_finished, validate_definition
from app.models import (
    RUN_COMPLETED,
    RUN_FAILED,
    RUN_RUNNING,
    TASK_QUEUED,
    Task,
    Workflow,
    WorkflowRun,
)


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
    """Hand tasks to the executor.

    Phase 1: mark them `queued` so the state machine advances and the same task is
    not resolved twice. Phase 2: publish to RabbitMQ inside this function.
    """
    for task in tasks:
        task.status = TASK_QUEUED
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
