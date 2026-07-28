"""FastAPI application — Phase 1 surface.

Registration, triggering and inspection. Task execution arrives in Phase 2; until
then `/runs/{id}/tasks/{task_name}/simulate` stands in for a worker so the DAG
resolution order can be stepped through by hand.
"""

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app import scheduler
from app.dag import DagValidationError, get_runnable_tasks
from app.db import get_session, init_db
from app.models import (
    TASK_FAILED,
    TASK_QUEUED,
    TASK_RUNNING,
    TASK_SUCCESS,
    TERMINAL_RUN_STATUSES,
    Task,
    Workflow,
    WorkflowRun,
)
from app.schemas import RunDetail, RunOut, TaskOut, WorkflowCreate, WorkflowOut


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


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

    workflow = Workflow(name=payload.name, definition=definition)
    session.add(workflow)
    session.commit()
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


@app.post("/runs/{run_id}/tasks/{task_name}/simulate", response_model=RunDetail)
def simulate_task_result(
    run_id: uuid.UUID,
    task_name: str,
    succeed: bool = True,
    session: Session = Depends(get_session),
):
    """Stand-in for a worker until Phase 2.

    Marks a task success/failed, then runs the scheduler so the next wave of
    runnable tasks appears — this is how the resolution order is verified by hand.
    """
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

    task.status = TASK_SUCCESS if succeed else TASK_FAILED
    task.completed_at = datetime.now(timezone.utc)
    if not succeed:
        task.error_message = "simulated failure"
    session.flush()

    scheduler.resolve(session, run_id)
    session.commit()
    return _run_detail(session, run)
