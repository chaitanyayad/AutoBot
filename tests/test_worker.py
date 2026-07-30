"""Phase 2: worker consumes from the broker, executes, and releases the next wave."""

import os

import pytest

from app import broker as broker_module
from app import config, executors
from app.broker import RabbitMQBroker, TaskMessage
from app.models import Task, WorkflowRun
from app.worker import Worker, make_worker_id

LINEAR = {
    "name": "linear",
    "workflow": [
        {"id": "a", "depends_on": []},
        {"id": "b", "depends_on": ["a"]},
        {"id": "c", "depends_on": ["b"]},
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
    return Worker(worker_id="test-worker-1", session_factory=session_factory)


def trigger(client, definition):
    workflow_id = client.post("/workflows", json=definition).json()["id"]
    return client.post(f"/workflows/{workflow_id}/trigger").json()["id"]


def drain(broker, worker):
    """Run every queued message, including work the handlers themselves release."""
    broker.consume(worker.handle)


# --- dispatch -> queue ------------------------------------------------------


def test_trigger_publishes_the_root_task(client, broker):
    trigger(client, LINEAR)
    assert broker.pending() == 1


def test_message_carries_task_identity(client, broker):
    run_id = trigger(client, LINEAR)
    message = broker._queue[0]
    assert message.task_name == "a"
    assert message.run_id == run_id
    assert message.attempt == 0


# --- end-to-end -------------------------------------------------------------


def test_linear_workflow_runs_to_completion(client, broker, worker):
    """Phase 2 acceptance check: a -> b -> c, driven entirely by the worker."""
    run_id = trigger(client, LINEAR)
    drain(broker, worker)

    run = client.get(f"/runs/{run_id}").json()
    assert run["status"] == "completed"
    assert [t["status"] for t in run["tasks"]] == ["success"] * 3
    assert broker.pending() == 0


def test_worker_records_identity_and_timings(client, broker, worker):
    run_id = trigger(client, LINEAR)
    drain(broker, worker)

    for task in client.get(f"/runs/{run_id}/tasks").json():
        assert task["worker_id"] == "test-worker-1"
        assert task["started_at"] is not None
        assert task["completed_at"] is not None


def test_fan_out_and_fan_in_execute_in_order(client, broker, worker):
    executed = []

    for name in ("root", "x", "y", "end"):
        executors.register(name)(lambda ctx: executed.append(ctx.task_name))

    run_id = trigger(client, FAN_OUT)
    drain(broker, worker)

    assert client.get(f"/runs/{run_id}").json()["status"] == "completed"
    assert executed[0] == "root"          # root first
    assert set(executed[1:3]) == {"x", "y"}  # both branches, either order
    assert executed[3] == "end"           # fan-in last


def test_registered_handler_receives_context(client, broker, worker):
    seen = {}

    @executors.register("a")
    def handler(ctx):
        seen.update(
            task_name=ctx.task_name, worker_id=ctx.worker_id, attempt=ctx.attempt
        )

    trigger(client, LINEAR)
    drain(broker, worker)

    assert seen == {"task_name": "a", "worker_id": "test-worker-1", "attempt": 0}


# --- failure handling -------------------------------------------------------


def test_raising_handler_marks_task_failed_and_halts_the_run(client, broker, worker):
    @executors.register("b")
    def explode(ctx):
        raise RuntimeError("kaboom")

    run_id = trigger(client, LINEAR)
    drain(broker, worker)

    run = client.get(f"/runs/{run_id}").json()
    assert run["status"] == "failed"

    statuses = {t["task_name"]: t["status"] for t in run["tasks"]}
    assert statuses == {"a": "success", "b": "failed", "c": "pending"}

    failed = next(t for t in run["tasks"] if t["task_name"] == "b")
    assert "RuntimeError: kaboom" in failed["error_message"]


def test_failure_does_not_stall_an_independent_branch(client, broker, worker):
    @executors.register("x")
    def explode(ctx):
        raise RuntimeError("branch x is broken")

    run_id = trigger(client, FAN_OUT)
    drain(broker, worker)

    run = client.get(f"/runs/{run_id}").json()
    statuses = {t["task_name"]: t["status"] for t in run["tasks"]}
    # y still ran to completion; only the fan-in is blocked.
    assert statuses == {"root": "success", "x": "failed", "y": "success", "end": "pending"}
    assert run["status"] == "failed"


# --- delivery robustness ----------------------------------------------------


def test_duplicate_delivery_executes_once(client, broker, worker):
    runs = []
    executors.register("a")(lambda ctx: runs.append(1))

    trigger(client, LINEAR)
    duplicate = broker._queue[0]
    broker.publish(duplicate)  # same task delivered twice

    drain(broker, worker)
    assert runs == [1], "a non-queued task must not be executed again"


def test_message_for_a_deleted_task_is_dropped(worker):
    import uuid

    ghost = TaskMessage(
        task_id=str(uuid.uuid4()), run_id=str(uuid.uuid4()), task_name="ghost"
    )
    assert worker.handle(ghost) is True  # acked, not retried forever


def test_worker_id_defaults_to_host_and_pid():
    worker_id = make_worker_id()
    assert str(os.getpid()) in worker_id


# --- the Phase 1 stand-in is now gated --------------------------------------


def test_simulate_endpoint_is_disabled_by_default(client, monkeypatch):
    monkeypatch.setattr(config, "ENABLE_SIMULATE_ENDPOINT", False)
    run_id = trigger(client, LINEAR)

    response = client.post(f"/runs/{run_id}/tasks/a/simulate")
    assert response.status_code == 404
    assert "worker" in response.json()["detail"]


# --- real broker (skipped unless RabbitMQ is reachable) ---------------------


def rabbitmq_available() -> bool:
    try:
        import pika

        url = os.getenv("TEST_RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
        pika.BlockingConnection(pika.URLParameters(url)).close()
        return True
    except Exception:
        return False


@pytest.mark.skipif(not rabbitmq_available(), reason="RabbitMQ not reachable")
def test_round_trip_through_real_rabbitmq(client, session_factory, monkeypatch):
    """The same worker logic, over a real durable queue."""
    url = os.getenv("TEST_RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
    queue = "test_task_queue"
    monkeypatch.setattr(config, "TASK_QUEUE", queue)

    real = RabbitMQBroker(url=url, queue=queue)
    real.purge()
    broker_module.set_broker(real)

    executed = []
    for name in ("a", "b", "c"):
        executors.register(name)(lambda ctx: executed.append(ctx.task_name))

    run_id = trigger(client, LINEAR)
    worker = Worker(worker_id="rabbit-worker", session_factory=session_factory)

    # Pull deliveries one at a time rather than blocking in start_consuming().
    channel = real._ensure_channel()
    for _ in range(3):
        method, _props, body = channel.basic_get(queue=queue, auto_ack=False)
        assert method is not None, f"expected a delivery, got none (so far: {executed})"
        worker.handle(TaskMessage.from_json(body))
        channel.basic_ack(method.delivery_tag)

    assert executed == ["a", "b", "c"]
    assert client.get(f"/runs/{run_id}").json()["status"] == "completed"

    real.purge()
    real.close()
