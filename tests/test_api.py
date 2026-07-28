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
    workflow_id = register(client, THREE_TASK_LINEAR)
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


def test_run_404(client):
    response = client.get("/runs/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 404
