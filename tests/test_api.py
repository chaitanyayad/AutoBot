"""End-to-end API tests, including the Phase 1 manual-test scenario."""

RESUME_PIPELINE = {
    "name": "resume_pipeline",
    "workflow": [
        {"id": "parse_resume", "depends_on": []},
        {"id": "extract_skills", "depends_on": ["parse_resume"]},
        {"id": "generate_embeddings", "depends_on": ["parse_resume"]},
        {"id": "save_results", "depends_on": ["extract_skills", "generate_embeddings"]},
        {"id": "send_notification", "depends_on": ["save_results"]},
    ],
}

THREE_TASK_LINEAR = {
    "name": "linear",
    "workflow": [
        {"id": "a", "depends_on": []},
        {"id": "b", "depends_on": ["a"]},
        {"id": "c", "depends_on": ["b"]},
    ],
}

# Retries are Phase 3 behaviour; tests that assert a failure is *permanent* use
# this variant so the intent is explicit rather than inherited.
THREE_TASK_NO_RETRIES = {
    "name": "linear_no_retries",
    "workflow": [dict(node, max_retries=0) for node in THREE_TASK_LINEAR["workflow"]],
}


def register(client, definition):
    response = client.post("/workflows", json=definition)
    assert response.status_code == 201, response.text
    return response.json()["id"]


def complete(client, run_id, task_name, succeed=True):
    response = client.post(
        f"/runs/{run_id}/tasks/{task_name}/simulate", params={"succeed": succeed}
    )
    assert response.status_code == 200, response.text
    return response.json()


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_register_workflow_persists_definition(client):
    workflow_id = register(client, RESUME_PIPELINE)
    body = client.get(f"/workflows/{workflow_id}").json()
    assert body["name"] == "resume_pipeline"
    assert len(body["definition"]["workflow"]) == 5


def test_register_rejects_cycle(client):
    bad = {
        "name": "cyclic",
        "workflow": [
            {"id": "a", "depends_on": ["b"]},
            {"id": "b", "depends_on": ["a"]},
        ],
    }
    response = client.post("/workflows", json=bad)
    assert response.status_code == 422
    assert "cycle" in response.json()["detail"]


def test_register_rejects_unknown_dependency(client):
    bad = {"name": "dangling", "workflow": [{"id": "a", "depends_on": ["ghost"]}]}
    response = client.post("/workflows", json=bad)
    assert response.status_code == 422
    assert "unknown task" in response.json()["detail"]


def test_trigger_creates_one_task_per_node_and_queues_the_root(client):
    workflow_id = register(client, RESUME_PIPELINE)
    run = client.post(f"/workflows/{workflow_id}/trigger").json()

    assert run["status"] == "running"
    assert len(run["tasks"]) == 5

    statuses = {t["task_name"]: t["status"] for t in run["tasks"]}
    assert statuses["parse_resume"] == "queued"
    assert all(
        statuses[name] == "pending"
        for name in statuses
        if name != "parse_resume"
    )


def test_trigger_unknown_workflow_is_404(client):
    response = client.post("/workflows/00000000-0000-0000-0000-000000000000/trigger")
    assert response.status_code == 404


def test_three_task_workflow_resolution_order(client):
    """Phase 1 acceptance check: a 3-task workflow resolves a -> b -> c."""
    workflow_id = register(client, THREE_TASK_LINEAR)
    run_id = client.post(f"/workflows/{workflow_id}/trigger").json()["id"]

    observed = []
    for expected in ["a", "b", "c"]:
        tasks = client.get(f"/runs/{run_id}/tasks").json()
        queued = [t["task_name"] for t in tasks if t["status"] == "queued"]
        assert queued == [expected]
        observed.append(expected)
        complete(client, run_id, expected)

    assert observed == ["a", "b", "c"]
    assert client.get(f"/runs/{run_id}").json()["status"] == "completed"


def test_fan_out_fan_in_resolution_order(client):
    workflow_id = register(client, RESUME_PIPELINE)
    run_id = client.post(f"/workflows/{workflow_id}/trigger").json()["id"]

    run = complete(client, run_id, "parse_resume")
    queued = sorted(t["task_name"] for t in run["tasks"] if t["status"] == "queued")
    assert queued == ["extract_skills", "generate_embeddings"]

    # Fan-in must wait for *both* parallel branches.
    run = complete(client, run_id, "extract_skills")
    assert [t["status"] for t in run["tasks"] if t["task_name"] == "save_results"] == [
        "pending"
    ]

    run = complete(client, run_id, "generate_embeddings")
    assert [t["status"] for t in run["tasks"] if t["task_name"] == "save_results"] == [
        "queued"
    ]

    complete(client, run_id, "save_results")
    run = complete(client, run_id, "send_notification")
    assert run["status"] == "completed"
    assert run["completed_at"] is not None


def test_failed_task_fails_the_run_and_halts_downstream(client):
    workflow_id = register(client, THREE_TASK_NO_RETRIES)
    run_id = client.post(f"/workflows/{workflow_id}/trigger").json()["id"]

    run = complete(client, run_id, "a", succeed=False)
    assert run["status"] == "failed"
    statuses = {t["task_name"]: t["status"] for t in run["tasks"]}
    assert statuses == {"a": "failed", "b": "pending", "c": "pending"}


def test_run_detail_exposes_runnable_now(client):
    workflow_id = register(client, THREE_TASK_LINEAR)
    run_id = client.post(f"/workflows/{workflow_id}/trigger").json()["id"]

    # 'a' is queued, so nothing is runnable until it reports back.
    assert client.get(f"/runs/{run_id}").json()["runnable_now"] == []


def test_undispatched_task_cannot_report_a_result(client):
    """A pending task must not be completable — that would break DAG order."""
    workflow_id = register(client, THREE_TASK_LINEAR)
    run_id = client.post(f"/workflows/{workflow_id}/trigger").json()["id"]

    response = client.post(f"/runs/{run_id}/tasks/c/simulate")
    assert response.status_code == 409
    assert "not dispatched" in response.json()["detail"]

    statuses = {t["task_name"]: t["status"] for t in client.get(f"/runs/{run_id}/tasks").json()}
    assert statuses == {"a": "queued", "b": "pending", "c": "pending"}


def test_task_cannot_report_twice(client):
    workflow_id = register(client, THREE_TASK_LINEAR)
    run_id = client.post(f"/workflows/{workflow_id}/trigger").json()["id"]

    complete(client, run_id, "a")
    assert client.post(f"/runs/{run_id}/tasks/a/simulate").status_code == 409


def test_failure_waits_for_in_flight_siblings(client):
    """A failure must not close the run while a sibling is still executing."""
    fan_out = {
        "name": "fan_out",
        "workflow": [
            {"id": "root", "depends_on": [], "max_retries": 0},
            {"id": "x", "depends_on": ["root"], "max_retries": 0},
            {"id": "y", "depends_on": ["root"], "max_retries": 0},
            {"id": "end", "depends_on": ["x", "y"], "max_retries": 0},
        ],
    }
    workflow_id = register(client, fan_out)
    run_id = client.post(f"/workflows/{workflow_id}/trigger").json()["id"]
    complete(client, run_id, "root")

    # x fails while y is still queued -> run stays running so y can report back.
    run = complete(client, run_id, "x", succeed=False)
    assert run["status"] == "running"

    run = complete(client, run_id, "y")
    assert run["status"] == "failed"
    statuses = {t["task_name"]: t["status"] for t in run["tasks"]}
    assert statuses == {"root": "success", "x": "failed", "y": "success", "end": "pending"}


def test_models_and_schema_sql_agree_on_constraints(client, engine):
    """create_all() must not produce a weaker schema than schema.sql."""
    from sqlalchemy import inspect

    inspector = inspect(engine)
    task_indexes = {i["name"] for i in inspector.get_indexes("tasks")}
    assert {"idx_tasks_run_id", "idx_tasks_status"} <= task_indexes

    unique = {c["name"] for c in inspector.get_unique_constraints("tasks")}
    assert "uq_tasks_run_task_name" in unique

    check_names = {c["name"] for c in inspector.get_check_constraints("tasks")}
    assert "ck_tasks_status" in check_names


def test_model_ddl_carries_the_same_defaults_as_schema_sql():
    """create_all() must emit the DEFAULT clauses schema.sql declares.

    Without these, any insert that does not go through SQLAlchemy (psql, a
    backfill, a non-Python worker) hits NOT NULL with no default.
    """
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.schema import CreateTable

    from app.db import Base

    ddl = {
        t.name: str(CreateTable(t).compile(dialect=postgresql.dialect()))
        for t in Base.metadata.sorted_tables
    }

    assert "status TEXT DEFAULT 'pending' NOT NULL" in ddl["workflow_runs"]
    assert "status TEXT DEFAULT 'pending' NOT NULL" in ddl["tasks"]
    assert "retry_count INTEGER DEFAULT 0 NOT NULL" in ddl["tasks"]
    assert "max_retries INTEGER DEFAULT 3 NOT NULL" in ddl["tasks"]
    assert "status TEXT DEFAULT 'idle' NOT NULL" in ddl["workers"]

    # Known, deliberate divergence: schema.sql gives depends_on DEFAULT '{}',
    # which is Postgres array syntax and invalid for the SQLite JSON variant.
    # The engine always supplies depends_on explicitly, so nothing relies on it.
    assert "depends_on TEXT[] NOT NULL" in ddl["tasks"]


def test_invalid_status_rejected_by_database(client, session_factory):
    """The status CHECK constraint is enforced, not just asserted in Python."""
    import uuid as uuid_mod

    import pytest
    from sqlalchemy.exc import IntegrityError

    from app.models import Task, Workflow, WorkflowRun

    session = session_factory()
    workflow = Workflow(name="w", definition={"workflow": [{"id": "a"}]})
    session.add(workflow)
    session.flush()
    run = WorkflowRun(workflow_id=workflow.id, status="running")
    session.add(run)
    session.flush()

    session.add(Task(run_id=run.id, task_name="a", depends_on=[], status="banana"))
    with pytest.raises(IntegrityError):
        session.commit()
    session.close()


def test_run_404(client):
    response = client.get("/runs/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 404
