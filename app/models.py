"""SQLAlchemy models mirroring schema.sql.

Column types are declared portably: the Postgres variants (JSONB, TEXT[]) are used
against Postgres, and JSON is substituted on SQLite so the test suite can run
without a database server.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base

# --- status constants -------------------------------------------------------

RUN_PENDING = "pending"
RUN_RUNNING = "running"
RUN_COMPLETED = "completed"
RUN_FAILED = "failed"
RUN_CANCELLED = "cancelled"

TASK_PENDING = "pending"
TASK_QUEUED = "queued"
TASK_RUNNING = "running"
TASK_SUCCESS = "success"
TASK_FAILED = "failed"
TASK_CANCELLED = "cancelled"

TERMINAL_RUN_STATUSES = {RUN_COMPLETED, RUN_FAILED, RUN_CANCELLED}
TERMINAL_TASK_STATUSES = {TASK_SUCCESS, TASK_FAILED, TASK_CANCELLED}

RUN_STATUSES = (RUN_PENDING, RUN_RUNNING, RUN_COMPLETED, RUN_FAILED, RUN_CANCELLED)
TASK_STATUSES = (
    TASK_PENDING, TASK_QUEUED, TASK_RUNNING, TASK_SUCCESS, TASK_FAILED, TASK_CANCELLED,
)


def _in_clause(column: str, values: tuple[str, ...]) -> str:
    allowed = ",".join(f"'{v}'" for v in values)
    return f"{column} IN ({allowed})"

JSONType = JSON().with_variant(JSONB, "postgresql")
StringArray = JSON().with_variant(ARRAY(Text), "postgresql")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Workflow(Base):
    """A workflow definition — the DAG template."""

    __tablename__ = "workflows"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    definition: Mapped[dict] = mapped_column(JSONType, nullable=False)
    # Cron expression ("0 9 * * *"), or NULL for a workflow that is only ever
    # triggered manually. Kept as its own column (rather than read out of
    # `definition` on every startup) so the scheduler can find every scheduled
    # workflow with a plain WHERE clause.
    schedule: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), default=_utcnow
    )

    runs: Mapped[list["WorkflowRun"]] = relationship(
        back_populates="workflow", cascade="all, delete-orphan"
    )


class WorkflowRun(Base):
    """A specific execution of a workflow definition."""

    __tablename__ = "workflow_runs"
    __table_args__ = (
        CheckConstraint(_in_clause("status", RUN_STATUSES), name="ck_runs_status"),
        Index("idx_runs_status", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    workflow_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("workflows.id", ondelete="CASCADE"), nullable=False
    )
    status: Mapped[str] = mapped_column(
        Text, nullable=False, default=RUN_PENDING, server_default=RUN_PENDING
    )
    triggered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), default=_utcnow
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    workflow: Mapped[Workflow] = relationship(back_populates="runs")
    tasks: Mapped[list["Task"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )


class Task(Base):
    """A task instance belonging to one run."""

    __tablename__ = "tasks"
    __table_args__ = (
        CheckConstraint(_in_clause("status", TASK_STATUSES), name="ck_tasks_status"),
        UniqueConstraint("run_id", "task_name", name="uq_tasks_run_task_name"),
        Index("idx_tasks_run_id", "run_id"),
        Index("idx_tasks_status", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("workflow_runs.id", ondelete="CASCADE"), nullable=False
    )
    task_name: Mapped[str] = mapped_column(Text, nullable=False)
    depends_on: Mapped[list[str]] = mapped_column(
        StringArray, nullable=False, default=list
    )
    status: Mapped[str] = mapped_column(
        Text, nullable=False, default=TASK_PENDING, server_default=TASK_PENDING
    )
    retry_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    max_retries: Mapped[int] = mapped_column(
        Integer, nullable=False, default=3, server_default=text("3")
    )
    worker_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    # The moment this attempt becomes claimable: set to "now" on a fresh
    # dispatch, or to "now + backoff delay" on a retry, since the message for a
    # delayed retry is not actually on the main queue until then. Recovery uses
    # it to catch a task stuck `queued` well past that point — the symptom of a
    # publish that failed *after* its transaction committed (Phase 5/6 known
    # gap, closed in Phase 7; see `recovery.reclaim_stuck_queued_tasks`).
    dispatched_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Captured stdout/stderr from the handler's most recent attempt (Phase 6
    # dashboard). Overwritten on each retry — only the latest attempt's output
    # is kept, since `error_message` already carries the history of why prior
    # attempts failed.
    logs: Mapped[str | None] = mapped_column(Text, nullable=True)

    run: Mapped[WorkflowRun] = relationship(back_populates="tasks")


class Worker(Base):
    """Heartbeat registry. Populated from Phase 2 onward."""

    __tablename__ = "workers"
    __table_args__ = (
        CheckConstraint("status IN ('idle','busy')", name="ck_workers_status"),
    )

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    last_heartbeat: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    status: Mapped[str] = mapped_column(
        Text, nullable=False, default="idle", server_default="idle"
    )
