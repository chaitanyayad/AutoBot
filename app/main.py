"""FastAPI application — Phase 1 surface.

Registration, triggering and inspection. Task execution arrives in Phase 2; until
then `/runs/{id}/tasks/{task_name}/simulate` stands in for a worker so the DAG
resolution order can be stepped through by hand.
"""

import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app import config, cron, db, scheduler
from app.dag import DagValidationError, execution_levels, get_runnable_tasks, parse_definition
from app.db import get_session, init_db
from app.models import (
    TASK_QUEUED,
    TASK_RUNNING,
    TERMINAL_RUN_STATUSES,
    Task,
    Workflow,
    WorkflowRun,
)
from app.schemas import RunDetail, RunOut, TaskOut, WorkflowCreate, WorkflowOut


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    # The cron scheduler is process-wide: one BackgroundScheduler per API
    # process, populated from every workflow that has a `schedule` at startup
    # so schedules survive a restart (Phase 5). `db.SessionLocal` is looked up
    # here rather than imported by name, so tests that swap it in before
    # startup (see conftest.client) are honoured.
    app.state.cron_scheduler = cron.start(db.SessionLocal) if config.ENABLE_SCHEDULER else None
    yield
    if app.state.cron_scheduler is not None:
        app.state.cron_scheduler.shutdown(wait=False)


app = FastAPI(
    title="Workflow Orchestration Engine",
    version="0.1.0",
    description="DAG-based workflow orchestration. Phase 1: core DAG engine.",
    lifespan=lifespan,
)


# --- helpers ----------------------------------------------------------------


def _get_run_or_404(session: Session, run_id: uuid.UUID) -> WorkflowRun:
    run = session.get(WorkflowRun, run_id)
    if run is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"run {run_id} not found")
    return run


def _run_detail(session: Session, run: WorkflowRun) -> RunDetail:
    tasks = scheduler.load_tasks(session, run.id)
    return RunDetail(
        id=run.id,
        workflow_id=run.workflow_id,
        status=run.status,
        triggered_at=run.triggered_at,
        completed_at=run.completed_at,
        tasks=[TaskOut.model_validate(t) for t in tasks],
        runnable_now=[t.task_name for t in get_runnable_tasks(tasks)],
    )


# --- endpoints --------------------------------------------------------------


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/workflows", response_model=WorkflowOut, status_code=status.HTTP_201_CREATED)
def register_workflow(payload: WorkflowCreate, session: Session = Depends(get_session)):
    """Register a workflow definition. The DAG is validated before it is stored."""
    definition = payload.model_dump()
    try:
        scheduler.validate_definition(definition)
    except DagValidationError as exc:
        raise HTTPException(422, str(exc)) from exc

    workflow = Workflow(name=payload.name, definition=definition, schedule=payload.schedule)
    session.add(workflow)
    session.commit()

    if workflow.schedule and app.state.cron_scheduler is not None:
        cron.add_job(app.state.cron_scheduler, workflow, db.SessionLocal)

    return workflow


@app.get("/workflows", response_model=list[WorkflowOut])
def list_workflows(session: Session = Depends(get_session)):
    return list(session.scalars(select(Workflow).order_by(Workflow.created_at.desc())))


@app.get("/workflows/{workflow_id}", response_model=WorkflowOut)
def get_workflow(workflow_id: uuid.UUID, session: Session = Depends(get_session)):
    workflow = session.get(Workflow, workflow_id)
    if workflow is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"workflow {workflow_id} not found")
    return workflow


@app.get("/workflows/{workflow_id}/graph")
def get_workflow_graph(workflow_id: uuid.UUID, session: Session = Depends(get_session)):
    """The DAG's static shape — levels for layout, edges for drawing them.

    Decoupled from any particular run: the dashboard fetches this once per
    workflow and overlays live task status from `/runs/{run_id}` on top of it.
    """
    workflow = session.get(Workflow, workflow_id)
    if workflow is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"workflow {workflow_id} not found")

    specs = parse_definition(workflow.definition)
    levels = execution_levels(specs)
    edges = [[dep, spec.id] for spec in specs for dep in spec.depends_on]
    return {"levels": levels, "edges": edges}


@app.post(
    "/workflows/{workflow_id}/trigger",
    response_model=RunDetail,
    status_code=status.HTTP_201_CREATED,
)
def trigger_workflow(workflow_id: uuid.UUID, session: Session = Depends(get_session)):
    """Start a new run: materialise task rows and resolve the first wave."""
    workflow = session.get(Workflow, workflow_id)
    if workflow is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"workflow {workflow_id} not found")

    try:
        run = scheduler.create_run(session, workflow)
    except DagValidationError as exc:
        raise HTTPException(422, str(exc)) from exc

    scheduler.resolve(session, run.id)
    session.commit()
    return _run_detail(session, run)


@app.get("/runs/{run_id}", response_model=RunDetail)
def get_run(run_id: uuid.UUID, session: Session = Depends(get_session)):
    return _run_detail(session, _get_run_or_404(session, run_id))


@app.get("/runs/{run_id}/tasks", response_model=list[TaskOut])
def get_run_tasks(run_id: uuid.UUID, session: Session = Depends(get_session)):
    _get_run_or_404(session, run_id)
    return scheduler.load_tasks(session, run_id)


@app.get("/runs", response_model=list[RunOut])
def list_runs(session: Session = Depends(get_session)):
    return list(session.scalars(select(WorkflowRun).order_by(WorkflowRun.triggered_at.desc())))


@app.get("/runs/{run_id}/tasks/{task_name}/logs")
def get_task_logs(run_id: uuid.UUID, task_name: str, session: Session = Depends(get_session)):
    """Captured stdout/stderr from the task's most recent attempt."""
    _get_run_or_404(session, run_id)
    task = session.scalar(
        select(Task).where(Task.run_id == run_id, Task.task_name == task_name)
    )
    if task is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"task {task_name!r} not found in run {run_id}"
        )
    return {"task_name": task_name, "attempt": task.retry_count, "logs": task.logs}


@app.post("/runs/{run_id}/cancel", response_model=RunDetail)
def cancel_workflow_run(run_id: uuid.UUID, session: Session = Depends(get_session)):
    """Stop dispatching further work for a run.

    Tasks already queued or running are not force-stopped — nothing here can
    reach into a worker process mid-execution — but no task newly unblocked by
    their completion will be dispatched. See `scheduler.cancel_run`.
    """
    _get_run_or_404(session, run_id)
    try:
        run = scheduler.cancel_run(session, run_id)
    except ValueError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    session.commit()
    return _run_detail(session, run)


@app.post("/runs/{run_id}/tasks/{task_name}/simulate", response_model=RunDetail)
def simulate_task_result(
    run_id: uuid.UUID,
    task_name: str,
    succeed: bool = True,
    session: Session = Depends(get_session),
):
    """Development-only stand-in for a worker.

    Real workers exist as of Phase 2, so this is disabled unless
    ENABLE_SIMULATE_ENDPOINT is set — it mutates run state without authentication.
    It drives the same `scheduler.report_result` path a worker does, so it cannot
    diverge from real execution.
    """
    if not config.ENABLE_SIMULATE_ENDPOINT:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            "simulate endpoint is disabled; run a worker (python -m app.worker) "
            "or set ENABLE_SIMULATE_ENDPOINT=true",
        )

    run = _get_run_or_404(session, run_id)
    if run.status in TERMINAL_RUN_STATUSES:
        raise HTTPException(status.HTTP_409_CONFLICT, f"run is already {run.status}")

    task = session.scalar(
        select(Task).where(Task.run_id == run_id, Task.task_name == task_name)
    )
    if task is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"task {task_name!r} not found in run {run_id}"
        )

    # Only a dispatched task may report a result. Without this the endpoint can
    # drive a run into a state the DAG says is impossible (completing a task
    # before its dependencies have run).
    if task.status not in (TASK_QUEUED, TASK_RUNNING):
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"task {task_name!r} is {task.status}, not dispatched — "
            "only queued or running tasks can report a result",
        )

    scheduler.report_result(
        session, task, succeed=succeed, error=None if succeed else "simulated failure"
    )
    session.commit()
    return _run_detail(session, run)
