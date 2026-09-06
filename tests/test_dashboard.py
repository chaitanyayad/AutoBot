"""Phase 6: cancellation, per-task logs, the DAG graph endpoint, and the
dashboard page itself. The live WebSocket push is exercised end-to-end; the
JS that renders it is not (there is no browser in this suite)."""

import uuid

import pytest

from app import executors

LINEAR = {
    "name": "linear",
    "workflow": [
        {"id": "a", "depends_on": []},
        {"id": "b", "depends_on": ["a"]},
    ],
}

FAN_OUT = {
    "name": "fan_out",
    "workflow": [
        {"id": "root", "depends_on": []},
        {"id": "x", "depends_on": ["root"]},
        {"id": "y", "depends_on": ["root"]},
        {"id": "end", "depends_on": ["x", "y"]},
    ],
}


@pytest.fixture()
def worker(session_factory):
    from app.worker import Worker

    return Worker(worker_id="dashboard-worker", session_factory=session_factory)


def trigger(client, definition):
    workflow_id = client.post("/workflows", json=definition).json()["id"]
    return client.post(f"/workflows/{workflow_id}/trigger").json()["id"]


# --- cancel -------------------------------------------------------------------


def test_cancel_marks_the_run_cancelled(client):
    run_id = trigger(client, LINEAR)
    response = client.post(f"/runs/{run_id}/cancel")
    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"
    assert client.get(f"/runs/{run_id}").json()["status"] == "cancelled"


def test_cancelling_an_already_terminal_run_is_a_conflict(client, broker, worker):
    run_id = trigger(client, LINEAR)
    broker.consume(worker.handle)  # runs to completion

    response = client.post(f"/runs/{run_id}/cancel")
    assert response.status_code == 409


def test_cancel_on_unknown_run_is_404(client):
    response = client.post(f"/runs/{uuid.uuid4()}/cancel")
    assert response.status_code == 404


def test_cancel_marks_pending_and_queued_tasks_cancelled(client, broker, worker):
    """root -> {x, y} -> end. Cancel after root dispatches x and y but before
    either is claimed: `end` (still `pending`) and both of x/y (`queued`, never
    claimed) all become `cancelled` outright — there is no reason to leave them
    in limbo when `resolve()` was never going to advance them again anyway.

    `broker.consume()` drains transitively (a handler's own completion can
    make the next wave due within the same call), so root is handled directly
    from the queue rather than through `consume()` — that is the only way to
    get a cancel in before x and y are claimed.
    """
    run_id = trigger(client, FAN_OUT)
    worker.handle(broker.messages()[0])  # root only; its completion queues x and y

    assert client.post(f"/runs/{run_id}/cancel").status_code == 200

    run = client.get(f"/runs/{run_id}").json()
    statuses = {t["task_name"]: t["status"] for t in run["tasks"]}
    assert statuses["root"] == "success"
    assert statuses["x"] == "cancelled"
    assert statuses["y"] == "cancelled"
    assert statuses["end"] == "cancelled"
    assert run["status"] == "cancelled"


def test_a_cancelled_queued_task_declines_its_stray_message(client, broker, worker):
    """x's message is still sitting on the queue when it is cancelled — the
    worker must decline it (claim() no longer matches `status = 'queued'`)
    rather than execute a task the run has already given up on."""
    executed = []
    executors.register("x")(lambda ctx: executed.append(ctx.task_name))

    run_id = trigger(client, FAN_OUT)
    worker.handle(broker.messages()[0])  # root; queues x and y
    client.post(f"/runs/{run_id}/cancel")

    broker.consume(worker.handle)  # delivers the now-stale x/y messages

    assert executed == [], "a cancelled task must never execute"
    task = next(t for t in client.get(f"/runs/{run_id}/tasks").json() if t["task_name"] == "x")
    assert task["status"] == "cancelled"


def test_cancel_leaves_an_already_running_task_to_finish(client, session):
    """A task a worker has already claimed keeps running — cancel has no
    channel to stop it — but cancellation still records on the run itself and
    does not touch the running row."""
    from app import scheduler

    run_id = trigger(client, LINEAR)
    task = next(t for t in scheduler.load_tasks(session, uuid.UUID(run_id)) if t.task_name == "a")
    assert scheduler.claim(session, task, "w1")  # queued -> running
    session.commit()

    client.post(f"/runs/{run_id}/cancel")

    task = client.get(f"/runs/{run_id}/tasks").json()[0]
    assert task["status"] == "running", "cancel must not touch a task already in flight"


def test_cancel_does_not_retry_a_failure_on_the_cancelled_run(client, session):
    """Exercises `report_result` directly — the path a worker takes once its
    handler raises — for a task that was already `running` (so untouched by
    cancel's bulk pending/queued -> cancelled update) when its run was
    cancelled out from under it."""
    from app import scheduler

    run_id = trigger(client, {**LINEAR, "workflow": [
        {"id": "a", "depends_on": [], "max_retries": 5},
    ]})
    task = next(t for t in scheduler.load_tasks(session, uuid.UUID(run_id)) if t.task_name == "a")
    assert scheduler.claim(session, task, "w1")
    session.commit()

    client.post(f"/runs/{run_id}/cancel")

    scheduler.report_result(session, task, succeed=False, error="boom")
    session.commit()

    task = client.get(f"/runs/{run_id}/tasks").json()[0]
    assert task["status"] == "failed", "a cancelled run must not requeue a retry"
    assert task["retry_count"] == 0


# --- per-task logs --------------------------------------------------------------


def test_logs_capture_stdout_and_stderr(client, broker, worker):
    @executors.register("a")
    def noisy(ctx):
        print("hello from stdout")
        import sys
        print("uh oh", file=sys.stderr)

    run_id = trigger(client, {"name": "n", "workflow": [{"id": "a", "depends_on": []}]})
    broker.consume(worker.handle)

    logs = client.get(f"/runs/{run_id}/tasks/a/logs").json()
    assert "hello from stdout" in logs["logs"]
    assert "uh oh" in logs["logs"]


def test_logs_are_null_for_a_task_that_has_not_run(client):
    run_id = trigger(client, LINEAR)
    logs = client.get(f"/runs/{run_id}/tasks/b/logs").json()
    assert logs["logs"] is None


def test_logs_for_unknown_task_is_404(client):
    run_id = trigger(client, LINEAR)
    assert client.get(f"/runs/{run_id}/tasks/nope/logs").status_code == 404


def test_logs_survive_across_a_retry_as_the_latest_attempt(client, broker, worker, monkeypatch):
    from app import config

    monkeypatch.setattr(config, "RETRY_BASE_DELAY", 0.0)
    monkeypatch.setattr(config, "RETRY_MAX_DELAY", 0.0)
    monkeypatch.setattr(config, "RETRY_JITTER", 0.0)

    @executors.register("a")
    def flaky(ctx):
        print(f"attempt {ctx.attempt}")
        if ctx.attempt < 1:
            raise RuntimeError("not yet")

    run_id = trigger(client, {"name": "n", "workflow": [
        {"id": "a", "depends_on": [], "max_retries": 2},
    ]})
    broker.consume(worker.handle, wait=True)

    logs = client.get(f"/runs/{run_id}/tasks/a/logs").json()["logs"]
    assert "attempt 1" in logs
    assert "attempt 0" not in logs


# --- graph endpoint --------------------------------------------------------------


def test_graph_endpoint_returns_levels_and_edges(client):
    workflow_id = client.post("/workflows", json=FAN_OUT).json()["id"]
    graph = client.get(f"/workflows/{workflow_id}/graph").json()

    assert graph["levels"][0] == ["root"]
    assert set(graph["levels"][1]) == {"x", "y"}
    assert graph["levels"][2] == ["end"]

    edges = {tuple(e) for e in graph["edges"]}
    assert edges == {("root", "x"), ("root", "y"), ("x", "end"), ("y", "end")}


def test_graph_endpoint_404_for_unknown_workflow(client):
    assert client.get(f"/workflows/{uuid.uuid4()}/graph").status_code == 404


# --- dashboard page + static assets ----------------------------------------------


def test_dashboard_page_is_served(client):
    response = client.get("/dashboard")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


def test_dashboard_static_assets_are_served(client):
    for path in ("/static/dashboard.css", "/static/dashboard.js"):
        assert client.get(path).status_code == 200


# --- live updates over WebSocket --------------------------------------------------


def test_websocket_pushes_final_state_and_closes_for_a_finished_run(client, broker, worker):
    run_id = trigger(client, LINEAR)
    broker.consume(worker.handle)

    with client.websocket_connect(f"/ws/runs/{run_id}") as ws:
        detail = ws.receive_json()
    assert detail["status"] == "completed"
    assert {t["task_name"] for t in detail["tasks"]} == {"a", "b"}


def test_websocket_reports_an_unknown_run(client):
    with client.websocket_connect(f"/ws/runs/{uuid.uuid4()}") as ws:
        message = ws.receive_json()
    assert "error" in message
