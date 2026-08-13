"""Worker process: consume tasks, execute them, report back, release the next wave.

    python -m app.worker

Each delivery is handled in its own database session and its own transaction, so
one poisoned task cannot corrupt the state of another. The worker is the only
component that executes user code; the scheduler stays pure.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import socket
import sys
import uuid

from app import config, executors, recovery, scheduler
from app.broker import TaskMessage, get_broker
from app.db import SessionLocal
from app.models import Task

logger = logging.getLogger("worker")


def make_worker_id() -> str:
    """hostname + pid, matching the `workers.id` convention in the schema."""
    return f"{socket.gethostname()}-{os.getpid()}"


class Worker:
    def __init__(self, worker_id: str | None = None, session_factory=SessionLocal):
        self.id = worker_id or make_worker_id()
        self.session_factory = session_factory
        self._stopping = False
        self._heartbeat: recovery.HeartbeatThread | None = None

    # --- message handling ---------------------------------------------------

    def handle(self, message: TaskMessage) -> bool:
        """Execute one task. Returns True so the delivery is acked.

        A task that raises is a *recorded outcome*, not a delivery failure — it is
        acked and marked failed. Only an unreadable message or a database problem
        should leave a delivery unacked, and those propagate as exceptions.
        """
        session = self.session_factory()
        try:
            task = session.get(Task, uuid.UUID(message.task_id))

            if task is None:
                logger.warning("task %s no longer exists; dropping", message.task_id)
                return True

            # The database, not the message, decides whether this is still work,
            # and it decides atomically. A duplicate delivery, a task another
            # worker took first, and a message that outlived its task all land
            # here as a lost claim.
            if not scheduler.claim(session, task, self.id):
                logger.info(
                    "declining %s: already %s on %s",
                    task.task_name,
                    task.status,
                    task.worker_id or "no worker",
                )
                session.rollback()
                return True

            recovery.heartbeat(session, self.id, status=recovery.WORKER_BUSY)
            session.commit()

            ctx = executors.TaskContext(
                task_id=str(task.id),
                run_id=str(task.run_id),
                task_name=task.task_name,
                attempt=task.retry_count,
                worker_id=self.id,
            )

            try:
                executors.get_handler(task.task_name)(ctx)
            except Exception as exc:
                logger.exception("task %s failed", task.task_name)
                scheduler.report_result(
                    session, task, succeed=False, error=f"{type(exc).__name__}: {exc}"
                )
                session.commit()
                return True

            scheduler.report_result(session, task, succeed=True)
            recovery.heartbeat(session, self.id, status=recovery.WORKER_IDLE)
            session.commit()
            logger.info("task %s succeeded", task.task_name)
            return True
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # --- lifecycle ----------------------------------------------------------

    def start_heartbeat(self, sweep: bool = True) -> None:
        with self.session_factory() as session:
            recovery.register_worker(session, self.id)
            session.commit()
        self._heartbeat = recovery.HeartbeatThread(
            self.id, self.session_factory, sweep=sweep
        )
        self._heartbeat.start()

    def run(self, heartbeat: bool = True) -> None:
        broker = get_broker()
        if heartbeat:
            self.start_heartbeat()
        logger.info("worker %s consuming from %s", self.id, config.TASK_QUEUE)
        try:
            broker.consume(self.handle)
        finally:
            self._stop_heartbeat()

    def _stop_heartbeat(self) -> None:
        if self._heartbeat is not None:
            self._heartbeat.stop()
            self._heartbeat = None

    def stop(self, *_args) -> None:
        self._stopping = True
        self._stop_heartbeat()
        logger.info("worker %s stopping", self.id)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Workflow task worker")
    parser.add_argument("--id", dest="worker_id", default=None, help="override worker id")
    parser.add_argument(
        "--task-duration",
        type=float,
        default=config.TASK_DURATION,
        help="seconds the default handler sleeps, to make parallelism visible "
        "(defaults to TASK_DURATION)",
    )
    parser.add_argument(
        "--handlers",
        default=None,
        help="comma-separated modules to import for @register handlers "
        "(defaults to WORKER_HANDLERS)",
    )
    parser.add_argument(
        "--no-heartbeat",
        action="store_true",
        help="skip the heartbeat/recovery thread",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    executors.set_default_duration(args.task_duration)

    modules = (
        [m.strip() for m in args.handlers.split(",") if m.strip()]
        if args.handlers
        else config.WORKER_HANDLERS
    )
    executors.load_handler_modules(modules)

    worker = Worker(worker_id=args.worker_id)
    signal.signal(signal.SIGINT, worker.stop)
    signal.signal(signal.SIGTERM, worker.stop)

    try:
        worker.run(heartbeat=not args.no_heartbeat)
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
