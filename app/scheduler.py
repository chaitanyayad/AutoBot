"""Run lifecycle: materialise task rows, resolve what is runnable, close out runs.

Phase 1 stops at "which tasks are runnable". Phase 2 replaces `dispatch` with a
real RabbitMQ publish. Phase 4 makes the whole thing safe to run from several
workers at once, which costs three rules:

1. **A claim is a compare-and-set** — `claim()` writes the status precondition
   into the UPDATE, so exactly one worker can take a task.
2. **The resolve step is serialised per run** — `resolve()` locks the run row, so
   two workers finishing sibling branches cannot both miss the fan-in they
   jointly unblocked.
3. **Publishes happen after commit** — a message is only visible once the row
   that authorises it is.
"""

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import event, select, update
from sqlalchemy.orm import Session

from app import config
from app.broker import TaskMessage, get_broker
from app.dag import get_runnable_tasks, is_run_failed, is_run_finished, validate_definition
from app.retry import backoff_delay, should_retry
from app.models import (
    RUN_CANCELLED,
    RUN_COMPLETED,
    RUN_FAILED,
    RUN_RUNNING,
    TASK_CANCELLED,
    TASK_FAILED,
    TASK_PENDING,
    TASK_QUEUED,
    TASK_RUNNING,
    TASK_SUCCESS,
    TERMINAL_RUN_STATUSES,
    Task,
    Workflow,
    WorkflowRun,
)

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# --- publish after commit ---------------------------------------------------

_OUTBOX = "scheduler_outbox"


@dataclass(frozen=True)
class _Publication:
    message: TaskMessage
    delay: float = 0.0
    dead_letter: bool = False


def _publish_on_commit(
    session: Session, message: TaskMessage, *, delay: float = 0.0, dead_letter: bool = False
) -> None:
    """Hold a message until the transaction that authorised it commits.

    Publishing inline is a dual write across two systems: the message is visible
    to workers before — or, if the transaction rolls back, *without* — the row
    that says the task was dispatched. A worker that wins that race reads the
    task as still `pending`, declines it, and the delivery is gone for good,
    leaving the run stalled. The window is a single network round trip, which is
    nothing until several idle workers are racing to be first.

    Deferring the publish makes Postgres the sole authority on what was
    dispatched, at the cost of the opposite risk: a broker outage between commit
    and publish leaves a `queued` task with no message. That one is at least
    visible in the table and recoverable; the reverse is not.
    """
    session.info.setdefault(_OUTBOX, []).append(
        _Publication(message=message, delay=delay, dead_letter=dead_letter)
    )


@event.listens_for(Session, "after_commit")
def _flush_outbox(session: Session) -> None:
    broker = None
    for publication in session.info.pop(_OUTBOX, ()):
        try:
            broker = broker or get_broker()
            if publication.dead_letter:
                broker.dead_letter(publication.message)
            else:
                broker.publish(publication.message, delay=publication.delay)
        except Exception:
            # The row is committed either way; losing the message is what the
            # `queued` status is there to make visible.
            logger.exception(
                "failed to publish %s after commit", publication.message.task_name
            )


@event.listens_for(Session, "after_rollback")
@event.listens_for(Session, "after_soft_rollback")
def _discard_outbox(session: Session, *_args) -> None:
    session.info.pop(_OUTBOX, None)


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


def lock_run(session: Session, run_id: uuid.UUID) -> WorkflowRun | None:
    """Take the run's row lock, serialising every scheduler tick for that run.

    Two workers finishing sibling branches at the same moment both ask "is the
    fan-in runnable now?". Under READ COMMITTED neither sees the other's
    uncommitted success, so both can answer no and the run stalls forever with
    nothing left to wake it. Locking the run row first means the second worker
    waits, then reads a snapshot that includes the first one's commit — so
    whoever finishes last always sees the completed set and releases the fan-in.

    Postgres only: SQLite serialises writers globally, so the lock is redundant
    there and `FOR UPDATE` is not supported anyway.
    """
    statement = select(WorkflowRun).where(WorkflowRun.id == run_id)
    if session.get_bind().dialect.name == "postgresql":
        statement = statement.with_for_update()
    return session.scalar(statement)


def resolve(session: Session, run_id: uuid.UUID) -> list[Task]:
    """The scheduler tick: find newly runnable tasks and hand them to dispatch.

    Called after a run is triggered and after every task completion. Every path
    that dispatches work goes through here, which is what makes the run lock a
    single choke point rather than something each caller has to remember.
    """
    lock_run(session, run_id)
    tasks = load_tasks(session, run_id)
    runnable = get_runnable_tasks(tasks)
    if runnable:
        dispatch(session, runnable)
    else:
        finalize_if_done(session, run_id, tasks)
    return runnable


def dispatch(session: Session, tasks: list[Task]) -> None:
    """Mark tasks `queued` and hand their messages to the post-commit outbox.

    The status change is flushed first, so the row that authorises the work is
    written before the message announcing it exists — see `_publish_on_commit`.
    If the publish then fails, the task is left `queued` with nothing to run it,
    which is at least visible in the table.
    """
    for task in tasks:
        task.status = TASK_QUEUED
    session.flush()

    for task in tasks:
        _publish_on_commit(
            session,
            TaskMessage(
                task_id=str(task.id),
                run_id=str(task.run_id),
                task_name=task.task_name,
                attempt=task.retry_count,
            ),
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

    _publish_on_commit(
        session,
        TaskMessage(
            task_id=str(task.id),
            run_id=str(task.run_id),
            task_name=task.task_name,
            attempt=task.retry_count,
            reason=f"exhausted {task.max_retries} retries: {error}",
        ),
        dead_letter=True,
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
    _publish_on_commit(
        session,
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


def claim(session: Session, task: Task, worker_id: str) -> bool:
    """Try to move a dispatched task into `running`. True if this worker won it.

    Compare-and-set: the precondition (`status = 'queued'`) lives in the UPDATE's
    WHERE clause, so checking and taking the task are the same statement and
    nothing can slip between them. Read-then-write cannot do this — under READ
    COMMITTED two workers can both read `queued`, and RabbitMQ's own
    at-least-once delivery makes that a matter of when, not if.

    Racing workers serialise on the row lock; the loser's UPDATE is then
    re-evaluated against the committed row, matches nothing, and reports False so
    the caller declines the delivery instead of running the task a second time.
    """
    result = session.execute(
        update(Task)
        .where(Task.id == task.id, Task.status == TASK_QUEUED)
        .values(status=TASK_RUNNING, worker_id=worker_id, started_at=_utcnow())
        .execution_options(synchronize_session=False)
    )
    # The row now holds whatever the winner wrote — reload rather than assume.
    session.expire(task)
    return result.rowcount == 1


def cancel_run(session: Session, run_id: uuid.UUID) -> WorkflowRun:
    """Mark a run cancelled. Raises ValueError if it is already terminal.

    This does not reach into a worker process to stop a `running` task — there
    is no channel to do that over — so a task already `running` is left alone
    and still reports its result normally; it just no longer unblocks anything
    new (`resolve()` checks for a cancelled run before dispatching).

    Everything *not* already running is moved to `cancelled` directly, rather
    than left `pending`/`queued` forever with no status of its own:

    - `pending` tasks were never going to run anyway once cancellation stops
      `resolve()`, so this just makes that visible instead of implicit.
    - `queued` tasks already have a message in flight, but marking the row
      `cancelled` means a worker's `claim()` — a compare-and-set gated on
      `status = 'queued'` — simply won't match it, so the delivery is declined
      and the task never executes even though its message still exists.
    """
    run = lock_run(session, run_id)
    if run is None:
        raise ValueError(f"run {run_id} not found")
    if run.status in TERMINAL_RUN_STATUSES:
        raise ValueError(f"run is already {run.status}")
    run.status = RUN_CANCELLED
    run.completed_at = _utcnow()
    session.execute(
        update(Task)
        .where(Task.run_id == run_id, Task.status.in_((TASK_PENDING, TASK_QUEUED)))
        .values(status=TASK_CANCELLED)
        .execution_options(synchronize_session=False)
    )
    session.flush()
    return run


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
