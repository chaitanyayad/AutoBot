"""Phase 5: cron-triggered runs.

A `schedule` on a workflow definition is validated at registration (same
principle as an invalid DAG — reject at the door, not at 3am when it fires),
armed immediately in the process-wide APScheduler instance, and re-armed from
the database on every API startup so it survives a restart.
"""

import uuid

import pytest

from app import cron
from app.models import Workflow, WorkflowRun

LINEAR = {
    "name": "daily_report",
    "workflow": [
        {"id": "a", "depends_on": []},
        {"id": "b", "depends_on": ["a"]},
    ],
}


def scheduled(cron_expr: str) -> dict:
    return {**LINEAR, "schedule": cron_expr}


# --- validation at registration ----------------------------------------------


def test_valid_cron_expression_is_accepted(client):
    response = client.post("/workflows", json=scheduled("0 9 * * *"))
    assert response.status_code == 201
    assert response.json()["schedule"] == "0 9 * * *"


def test_invalid_cron_expression_is_rejected(client):
    response = client.post("/workflows", json=scheduled("not a cron expression"))
    assert response.status_code == 422


def test_workflow_without_a_schedule_defaults_to_manual_only(client):
    response = client.post("/workflows", json=LINEAR)
    assert response.status_code == 201
    assert response.json()["schedule"] is None


def test_blank_schedule_is_treated_as_manual_only(client):
    response = client.post("/workflows", json=scheduled("   "))
    assert response.status_code == 201
    assert response.json()["schedule"] is None


@pytest.mark.parametrize(
    "expr", ["61 * * * *", "* * * *", "* * * * * *", ""]
)
def test_malformed_cron_fields_are_rejected(client, expr):
    payload = {**LINEAR, "schedule": expr}
    if expr == "":
        # empty string is treated as "no schedule", not malformed — covered above
        assert client.post("/workflows", json=payload).status_code == 201
    else:
        assert client.post("/workflows", json=payload).status_code == 422


# --- cron.py unit behaviour ---------------------------------------------------


def test_validate_cron_accepts_a_five_field_expression():
    cron.validate_cron("0 9 * * *")  # does not raise


def test_validate_cron_rejects_garbage():
    with pytest.raises(ValueError):
        cron.validate_cron("definitely not cron")


# --- arming on registration ----------------------------------------------------


def test_registering_a_scheduled_workflow_arms_a_job(client):
    workflow_id = client.post("/workflows", json=scheduled("0 9 * * *")).json()["id"]
    jobs = client.app.state.cron_scheduler.get_jobs()
    assert any(job.id == f"workflow:{workflow_id}" for job in jobs)


def test_registering_an_unscheduled_workflow_arms_nothing(client):
    before = len(client.app.state.cron_scheduler.get_jobs())
    client.post("/workflows", json=LINEAR)
    after = len(client.app.state.cron_scheduler.get_jobs())
    assert after == before


# --- durability across a restart ----------------------------------------------


def test_scheduled_workflows_are_re_armed_on_startup(session_factory):
    """A schedule stored in the database is picked up by a fresh scheduler
    instance with no registration call — this is what makes it survive a
    process restart rather than only living in the first process's memory."""
    with session_factory() as session:
        workflow = Workflow(name="restart_test", definition=LINEAR, schedule="0 9 * * *")
        session.add(workflow)
        session.commit()
        workflow_id = workflow.id

    sched = cron.start(session_factory)
    try:
        jobs = sched.get_jobs()
        assert any(job.id == f"workflow:{workflow_id}" for job in jobs)
    finally:
        sched.shutdown(wait=False)


def test_unscheduled_workflows_are_not_armed_on_startup(session_factory):
    with session_factory() as session:
        session.add(Workflow(name="manual_only", definition=LINEAR, schedule=None))
        session.commit()

    sched = cron.start(session_factory)
    try:
        assert sched.get_jobs() == []
    finally:
        sched.shutdown(wait=False)


# --- the job itself behaves like a manual trigger ------------------------------


def test_scheduled_job_creates_and_resolves_a_run(session_factory):
    with session_factory() as session:
        workflow = Workflow(name="job_test", definition=LINEAR, schedule="0 9 * * *")
        session.add(workflow)
        session.commit()
        workflow_id = workflow.id

    cron._run_scheduled(str(workflow_id), session_factory)

    with session_factory() as session:
        runs = session.query(WorkflowRun).filter_by(workflow_id=workflow_id).all()
        assert len(runs) == 1
        assert runs[0].status == "running"


def test_scheduled_job_for_a_deleted_workflow_does_not_raise(session_factory):
    cron._run_scheduled(str(uuid.uuid4()), session_factory)  # must not raise
