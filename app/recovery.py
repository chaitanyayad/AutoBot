"""Worker liveness and recovery of tasks orphaned by a dead worker.

A worker that dies mid-task leaves the row `running` forever: the message was
acked, so RabbitMQ will not redeliver it, and no other worker will touch it
because it is no longer `queued`. The heartbeat table is what makes that
detectable — a worker silent past `HEARTBEAT_TIMEOUT` is presumed dead and its
in-flight tasks are re-queued (or failed, if they are out of retries).
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app import config, scheduler
from app.models import TASK_RUNNING, Task, Worker as WorkerRow

logger = logging.getLogger(__name__)

WORKER_IDLE = "idle"
WORKER_BUSY = "busy"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: datetime | None) -> datetime | None:
    """Normalise a timestamp read back from the database.

    Postgres returns tz-aware values; SQLite returns naive ones. Treating the
    naive case as UTC keeps the comparison correct on both.
    """
    if value is None:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


# --- heartbeats -------------------------------------------------------------


def register_worker(session: Session, worker_id: str, status: str = WORKER_IDLE) -> WorkerRow:
    """Insert or refresh this worker's row."""
    row = session.get(WorkerRow, worker_id)
    if row is None:
        row = WorkerRow(id=worker_id)
        session.add(row)
    row.status = status
    row.last_heartbeat = _utcnow()
    session.flush()
    return row


def heartbeat(session: Session, worker_id: str, status: str | None = None) -> None:
    row = session.get(WorkerRow, worker_id)
    if row is None:
        register_worker(session, worker_id, status or WORKER_IDLE)
        return
    row.last_heartbeat = _utcnow()
    if status is not None:
        row.status = status
    session.flush()


def stale_workers(session: Session, timeout: float | None = None) -> list[WorkerRow]:
    timeout = config.HEARTBEAT_TIMEOUT if timeout is None else timeout
    cutoff = _utcnow() - timedelta(seconds=timeout)
    return [
        row
        for row in session.scalars(select(WorkerRow))
        if _as_utc(row.last_heartbeat) is None or _as_utc(row.last_heartbeat) < cutoff
    ]


# --- recovery ---------------------------------------------------------------


def reclaim_orphaned_tasks(session: Session, timeout: float | None = None) -> list[Task]:
    """Re-queue tasks whose worker stopped heartbeating. Returns those reclaimed.

    A task is orphaned when it is `running` and its worker is either unknown to
    the registry or has gone silent. Recovery runs it through the ordinary retry
    path, so a task that repeatedly kills its worker still exhausts its budget
    and lands in the dead letter queue rather than looping forever.
    """
    timeout = config.HEARTBEAT_TIMEOUT if timeout is None else timeout
    cutoff = _utcnow() - timedelta(seconds=timeout)

    alive = {
        row.id
        for row in session.scalars(select(WorkerRow))
        if _as_utc(row.last_heartbeat) is not None and _as_utc(row.last_heartbeat) >= cutoff
    }

    running = session.scalars(select(Task).where(Task.status == TASK_RUNNING)).all()

    reclaimed: list[Task] = []
    for task in running:
        if task.worker_id in alive:
            continue
        logger.warning(
            "reclaiming %s: worker %s is gone", task.task_name, task.worker_id
        )
        scheduler.report_result(
            session,
            task,
            succeed=False,
            error=f"worker {task.worker_id} stopped responding",
        )
        reclaimed.append(task)

    if reclaimed:
        session.flush()
    return reclaimed


# --- background thread ------------------------------------------------------


class HeartbeatThread(threading.Thread):
    """Beats for this worker and periodically sweeps for orphaned tasks."""

    def __init__(self, worker_id: str, session_factory, sweep: bool = True):
        super().__init__(name=f"heartbeat-{worker_id}", daemon=True)
        self.worker_id = worker_id
        self.session_factory = session_factory
        self.sweep = sweep
        self._stop = threading.Event()
        self._last_sweep = 0.0

    def run(self) -> None:
        import time

        while not self._stop.is_set():
            try:
                with self.session_factory() as session:
                    heartbeat(session, self.worker_id)
                    now = time.monotonic()
                    if self.sweep and now - self._last_sweep >= config.RECOVERY_INTERVAL:
                        reclaim_orphaned_tasks(session)
                        self._last_sweep = now
                    session.commit()
            except Exception:
                # A heartbeat failure must never take the worker down; the next
                # tick retries, and a genuinely dead worker is reclaimed anyway.
                logger.exception("heartbeat tick failed")
            self._stop.wait(config.HEARTBEAT_INTERVAL)

    def stop(self) -> None:
        self._stop.set()
