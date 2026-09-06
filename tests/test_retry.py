"""Phase 3: retries, exponential backoff, dead letters, and worker recovery."""

import random
from datetime import datetime, timedelta, timezone

import pytest

from app import config, executors, recovery
from app.models import TASK_RUNNING, Task, Worker as WorkerRow
from app.retry import backoff_delay, should_retry
from app.worker import Worker

ONE_TASK = {"name": "single", "workflow": [{"id": "a", "depends_on": []}]}


def one_task(max_retries: int) -> dict:
    return {
        "name": f"single_{max_retries}",
        "workflow": [{"id": "a", "depends_on": [], "max_retries": max_retries}],
    }


@pytest.fixture()
def worker(session_factory):
    return Worker(worker_id="retry-worker", session_factory=session_factory)


@pytest.fixture(autouse=True)
def _fast_backoff(monkeypatch):
    """Keep the delays real but tiny, so the timing path still executes."""
    monkeypatch.setattr(config, "RETRY_BASE_DELAY", 0.01)
    monkeypatch.setattr(config, "RETRY_MAX_DELAY", 0.05)
    monkeypatch.setattr(config, "RETRY_JITTER", 0.0)


def trigger(client, definition):
    workflow_id = client.post("/workflows", json=definition).json()["id"]
    return client.post(f"/workflows/{workflow_id}/trigger").json()["id"]


# --- backoff policy ---------------------------------------------------------


def test_backoff_doubles_per_attempt():
    delays = [backoff_delay(n, base=1, maximum=1000, jitter=0) for n in range(1, 6)]
    assert delays == [1, 2, 4, 8, 16]


def test_backoff_is_capped():
    assert backoff_delay(20, base=1, maximum=60, jitter=0) == 60


def test_backoff_jitter_stays_within_bounds():
    rng = random.Random(0)
    for _ in range(50):
        delay = backoff_delay(3, base=1, maximum=60, jitter=0.1, rng=rng)
        assert 3.6 <= delay <= 4.4  # 4s +/- 10%


def test_backoff_rejects_zero_attempt():
    with pytest.raises(ValueError):
        backoff_delay(0)


def test_should_retry_counts_retries_not_attempts():
    assert should_retry(0, 3) and should_retry(2, 3)
    assert not should_retry(3, 3)
    assert not should_retry(0, 0)


# --- retry behaviour --------------------------------------------------------


def test_failed_task_is_requeued_not_failed(client, broker, worker, monkeypatch):
    # A long backoff, so the retry cannot come due inside this drain and the
    # assertion is about state rather than timing.
    monkeypatch.setattr(config, "RETRY_BASE_DELAY", 30.0)
    monkeypatch.setattr(config, "RETRY_MAX_DELAY", 30.0)

    attempts = []

    @executors.register("a")
    def flaky(ctx):
        attempts.append(ctx.attempt)
        raise RuntimeError("nope")

    run_id = trigger(client, one_task(max_retries=2))
    broker.consume(worker.handle)  # first attempt only; the retry is delayed

    run = client.get(f"/runs/{run_id}").json()
    task = run["tasks"][0]
    assert task["status"] == "queued", "a retryable failure must not fail the task"
    assert task["retry_count"] == 1
    assert run["status"] == "running"
    assert broker.pending() == 1


def test_retry_is_delayed(client, broker, worker, monkeypatch):
    monkeypatch.setattr(config, "RETRY_BASE_DELAY", 30.0)
    monkeypatch.setattr(config, "RETRY_MAX_DELAY", 30.0)

    @executors.register("a")
    def flaky(ctx):
        raise RuntimeError("nope")

    trigger(client, one_task(max_retries=2))
    broker.consume(worker.handle)

    assert broker.pending() == 1
    assert broker.due() == 0, "the retry must not be immediately available"


def test_task_succeeds_on_a_later_attempt(client, broker, worker):
    attempts = []

    @executors.register("a")
    def flaky(ctx):
        attempts.append(ctx.attempt)
        if ctx.attempt < 2:
            raise RuntimeError("not yet")

    run_id = trigger(client, one_task(max_retries=3))
    broker.consume(worker.handle, wait=True)

    run = client.get(f"/runs/{run_id}").json()
    assert attempts == [0, 1, 2]
    assert run["status"] == "completed"
    assert run["tasks"][0]["status"] == "success"
    assert run["tasks"][0]["retry_count"] == 2


def test_attempt_number_reaches_the_handler(client, broker, worker):
    seen = []
    executors.register("a")(lambda ctx: seen.append(ctx.attempt) or (_ for _ in ()).throw(RuntimeError()))

    trigger(client, one_task(max_retries=2))
    broker.consume(worker.handle, wait=True)
    assert seen == [0, 1, 2]


def test_retry_clears_the_previous_attempts_worker(client, broker, worker, monkeypatch):
    monkeypatch.setattr(config, "RETRY_BASE_DELAY", 30.0)
    monkeypatch.setattr(config, "RETRY_MAX_DELAY", 30.0)

    @executors.register("a")
    def flaky(ctx):
        raise RuntimeError("nope")

    run_id = trigger(client, one_task(max_retries=1))
    broker.consume(worker.handle)  # one attempt

    task = client.get(f"/runs/{run_id}/tasks").json()[0]
    assert task["worker_id"] is None
    assert task["started_at"] is None
    assert task["completed_at"] is None


# --- exhaustion + dead letters ----------------------------------------------


def test_exhausted_task_fails_the_run(client, broker, worker):
    @executors.register("a")
    def always_fails(ctx):
        raise RuntimeError("permanently broken")

    run_id = trigger(client, one_task(max_retries=2))
    broker.consume(worker.handle, wait=True)

    run = client.get(f"/runs/{run_id}").json()
    assert run["status"] == "failed"
    task = run["tasks"][0]
    assert task["status"] == "failed"
    assert task["retry_count"] == 2, "retries stop at max_retries"
    assert "permanently broken" in task["error_message"]


def test_exhausted_task_goes_to_the_dead_letter_queue(client, broker, worker):
    @executors.register("a")
    def always_fails(ctx):
        raise RuntimeError("permanently broken")

    trigger(client, one_task(max_retries=1))
    broker.consume(worker.handle, wait=True)

    dead = broker.dead_letters()
    assert len(dead) == 1
    assert dead[0].task_name == "a"
    assert "exhausted 1 retries" in dead[0].reason


def test_successful_task_is_never_dead_lettered(client, broker, worker):
    trigger(client, ONE_TASK)
    broker.consume(worker.handle, wait=True)
    assert broker.dead_letters() == []


def test_zero_retries_fails_immediately(client, broker, worker):
    calls = []

    @executors.register("a")
    def always_fails(ctx):
        calls.append(1)
        raise RuntimeError("no retries configured")

    run_id = trigger(client, one_task(max_retries=0))
    broker.consume(worker.handle, wait=True)

    assert calls == [1]
    assert client.get(f"/runs/{run_id}").json()["status"] == "failed"
    assert len(broker.dead_letters()) == 1


# --- heartbeats -------------------------------------------------------------


def test_worker_registers_and_beats(session):
    recovery.register_worker(session, "w1")
    session.commit()

    row = session.get(WorkerRow, "w1")
    assert row.status == "idle"
    assert row.last_heartbeat is not None

    # Normalise: Postgres returns tz-aware timestamps, SQLite naive ones.
    first = recovery._as_utc(row.last_heartbeat)
    recovery.heartbeat(session, "w1", status=recovery.WORKER_BUSY)
    session.commit()
    assert session.get(WorkerRow, "w1").status == "busy"
    assert recovery._as_utc(session.get(WorkerRow, "w1").last_heartbeat) >= first


def test_heartbeat_registers_an_unknown_worker(session):
    recovery.heartbeat(session, "never-seen")
    session.commit()
    assert session.get(WorkerRow, "never-seen") is not None


def test_stale_workers_are_identified(session):
    recovery.register_worker(session, "alive")
    stale = recovery.register_worker(session, "dead")
    stale.last_heartbeat = datetime.now(timezone.utc) - timedelta(seconds=120)
    session.commit()

    names = {row.id for row in recovery.stale_workers(session, timeout=30)}
    assert names == {"dead"}


def test_worker_execution_updates_its_heartbeat(client, broker, worker, session):
    trigger(client, ONE_TASK)
    broker.consume(worker.handle, wait=True)

    row = session.get(WorkerRow, "retry-worker")
    assert row is not None, "executing a task must register the worker"
    assert row.status == "idle", "worker returns to idle after finishing"


# --- recovery ---------------------------------------------------------------


def _orphan_a_running_task(client, broker, session, session_factory, age_seconds=120):
    """Start a task on a worker that then 'dies' mid-execution."""
    dying = Worker(worker_id="doomed", session_factory=session_factory)
    executors.register("a")(lambda ctx: None)

    run_id = trigger(client, one_task(max_retries=2))
    message = broker.messages()[0]

    # Claim the task exactly as the worker does, then stop without reporting.
    with session_factory() as s:
        task = s.get(Task, __import__("uuid").UUID(message.task_id))
        recovery.register_worker(s, dying.id)
        from app import scheduler

        scheduler.claim(s, task, dying.id)
        row = s.get(WorkerRow, dying.id)
        row.last_heartbeat = datetime.now(timezone.utc) - timedelta(seconds=age_seconds)
        s.commit()
    return run_id


def test_task_orphaned_by_a_dead_worker_is_reclaimed(
    client, broker, session, session_factory
):
    run_id = _orphan_a_running_task(client, broker, session, session_factory)

    with session_factory() as s:
        assert s.query(Task).filter(Task.status == TASK_RUNNING).count() == 1
        reclaimed = recovery.reclaim_orphaned_tasks(s, timeout=30)
        s.commit()

    assert [t.task_name for t in reclaimed] == ["a"]
    task = client.get(f"/runs/{run_id}/tasks").json()[0]
    assert task["status"] == "queued", "the task is back on the queue"
    assert task["retry_count"] == 1
    assert "stopped responding" in task["error_message"]


def test_live_workers_tasks_are_left_alone(client, broker, session, session_factory):
    run_id = _orphan_a_running_task(
        client, broker, session, session_factory, age_seconds=0
    )

    with session_factory() as s:
        assert recovery.reclaim_orphaned_tasks(s, timeout=30) == []
        s.commit()

    assert client.get(f"/runs/{run_id}/tasks").json()[0]["status"] == "running"


# --- recovery: a lost publish (Phase 7 gap closure) -------------------------


def _backdate_dispatch(session_factory, task_id, age_seconds):
    """Simulate a task whose `dispatched_at` is old — as if its publish, which
    happens after commit and is not itself transactional, never landed."""
    with session_factory() as s:
        task = s.get(Task, task_id)
        task.dispatched_at = datetime.now(timezone.utc) - timedelta(seconds=age_seconds)
        s.commit()


def test_stuck_queued_task_is_reclaimed_and_retried(client, broker, session_factory):
    run_id = trigger(client, one_task(max_retries=2))
    task_id = broker.messages()[0].task_id
    _backdate_dispatch(session_factory, __import__("uuid").UUID(task_id), age_seconds=600)

    with session_factory() as s:
        reclaimed = recovery.reclaim_stuck_queued_tasks(s, timeout=300)
        s.commit()

    assert [t.task_name for t in reclaimed] == ["a"]
    task = client.get(f"/runs/{run_id}/tasks").json()[0]
    assert task["status"] == "queued", "back on the queue for another attempt"
    assert task["retry_count"] == 1
    assert "stuck queued" in task["error_message"]


def test_recently_queued_task_is_left_alone(client, session_factory):
    trigger(client, one_task(max_retries=2))  # dispatched_at is "now"

    with session_factory() as s:
        assert recovery.reclaim_stuck_queued_tasks(s, timeout=300) == []


def test_stuck_queued_task_exhausts_its_retry_budget(client, broker, session_factory):
    run_id = trigger(client, one_task(max_retries=0))
    task_id = broker.messages()[0].task_id
    _backdate_dispatch(session_factory, __import__("uuid").UUID(task_id), age_seconds=600)

    with session_factory() as s:
        recovery.reclaim_stuck_queued_tasks(s, timeout=300)
        s.commit()

    run = client.get(f"/runs/{run_id}").json()
    assert run["status"] == "failed"
    assert run["tasks"][0]["status"] == "failed"
    assert len(broker.dead_letters()) == 1


def test_a_freshly_delayed_retry_is_not_mistaken_for_stuck(
    client, broker, worker, monkeypatch, session_factory
):
    """A retry's `dispatched_at` is set to when its backoff delay elapses, not
    to "now" — otherwise every delayed retry would look stuck the moment the
    sweep's timeout is shorter than the backoff itself."""
    monkeypatch.setattr(config, "RETRY_BASE_DELAY", 30.0)
    monkeypatch.setattr(config, "RETRY_MAX_DELAY", 30.0)

    @executors.register("a")
    def flaky(ctx):
        raise RuntimeError("nope")

    trigger(client, one_task(max_retries=2))
    broker.consume(worker.handle)  # first attempt fails, requeues with a 30s delay

    with session_factory() as s:
        # A sweep timeout shorter than the retry delay must not touch it.
        assert recovery.reclaim_stuck_queued_tasks(s, timeout=1) == []


# --- real RabbitMQ delay path -----------------------------------------------


def rabbitmq_available() -> bool:
    try:
        import pika

        pika.BlockingConnection(pika.URLParameters(RABBIT_URL)).close()
        return True
    except Exception:
        return False


RABBIT_URL = __import__("os").getenv(
    "TEST_RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"
)


@pytest.mark.skipif(not rabbitmq_available(), reason="RabbitMQ not reachable")
def test_delayed_publish_round_trips_through_rabbitmq():
    """A delayed message waits in a TTL queue, then dead-letters back to the main one.

    This is the one part of the retry path that plugins usually provide; the
    in-memory broker cannot prove it works, so it is exercised for real here.
    """
    import time

    from app.broker import RabbitMQBroker, TaskMessage

    queue = "test_retry_queue"
    real = RabbitMQBroker(url=RABBIT_URL, queue=queue, dead_letter_queue=f"{queue}.dead")
    channel = real._ensure_channel()
    real.purge()

    message = TaskMessage(task_id="t1", run_id="r1", task_name="delayed", attempt=1)
    real.publish(message, delay=1.0)

    # It is parked in the 1s retry queue, not immediately runnable.
    assert channel.basic_get(queue=queue, auto_ack=True)[0] is None
    retry_queue = f"{queue}.retry.1s"
    declared = channel.queue_declare(queue=retry_queue, durable=True, passive=True)
    assert declared.method.message_count == 1

    # After the TTL, RabbitMQ routes it back to the main queue on its own.
    deadline = time.monotonic() + 10
    body = None
    while time.monotonic() < deadline:
        method, _props, body = channel.basic_get(queue=queue, auto_ack=True)
        if method is not None:
            break
        time.sleep(0.2)

    assert body is not None, "delayed message never returned to the main queue"
    assert TaskMessage.from_json(body).task_name == "delayed"

    real.purge()
    channel.queue_delete(retry_queue)
    channel.queue_delete(queue)
    channel.queue_delete(f"{queue}.dead")
    real.close()


def test_rabbitmq_brokers_are_per_thread(monkeypatch):
    """Each thread gets its own connection; pika channels are not thread-safe.

    Regression: the worker's heartbeat thread and its consumer thread shared one
    broker, and their concurrent connection setup tore the stream down with
    `Unexpected frame` / `pop from an empty deque`.
    """
    import threading

    from app import broker as broker_module

    monkeypatch.setattr(config, "BROKER_URL", "amqp://guest:guest@localhost:5672/")
    broker_module.set_broker(None)

    seen = {}
    barrier = threading.Barrier(2)

    def capture(name):
        # Hold the object itself: comparing id() alone is unsound, since a
        # collected broker's address can be reused by the next one.
        broker = broker_module.get_broker()
        seen[name] = broker
        barrier.wait(timeout=5)  # keep both alive simultaneously

    threads = [threading.Thread(target=capture, args=(n,)) for n in ("t1", "t2")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(seen) == 2
    assert seen["t1"] is not seen["t2"], "threads must not share a RabbitMQ connection"
    broker_module.set_broker(None)


def test_memory_broker_is_shared_across_threads(monkeypatch):
    """The in-process broker is the opposite case: one queue for every thread."""
    import threading

    from app import broker as broker_module

    monkeypatch.setattr(config, "BROKER_URL", "memory://")
    broker_module.set_broker(None)

    seen = []
    threads = [
        threading.Thread(target=lambda: seen.append(broker_module.get_broker()))
        for _ in range(2)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert seen[0] is seen[1]
    broker_module.set_broker(None)


@pytest.mark.skipif(not rabbitmq_available(), reason="RabbitMQ not reachable")
def test_publish_survives_a_dropped_connection():
    """An idle publisher's connection dies; the next publish must reconnect.

    Regression: a long-lived API process sat idle between triggers, its pika
    stream was reclaimed, and the next trigger raised StreamLostError as a 500.
    """
    from app.broker import RabbitMQBroker, TaskMessage

    queue = "test_reconnect_queue"
    real = RabbitMQBroker(url=RABBIT_URL, queue=queue, dead_letter_queue=f"{queue}.dead")
    real.publish(TaskMessage(task_id="t0", run_id="r0", task_name="before"))

    # Kill the connection out from under the broker, as an idle timeout would.
    real._connection.close()

    real.publish(TaskMessage(task_id="t1", run_id="r1", task_name="after"))

    channel = real._ensure_channel()
    names = []
    for _ in range(2):
        method, _props, body = channel.basic_get(queue=queue, auto_ack=True)
        assert method is not None
        names.append(TaskMessage.from_json(body).task_name)
    assert names == ["before", "after"]

    channel.queue_delete(queue)
    channel.queue_delete(f"{queue}.dead")
    real.close()


@pytest.mark.skipif(not rabbitmq_available(), reason="RabbitMQ not reachable")
def test_dead_letter_queue_is_durable_and_receives_exhausted_tasks():
    from app.broker import RabbitMQBroker, TaskMessage

    queue = "test_dlq_queue"
    dlq = f"{queue}.dead"
    real = RabbitMQBroker(url=RABBIT_URL, queue=queue, dead_letter_queue=dlq)
    channel = real._ensure_channel()
    real.purge()

    real.dead_letter(
        TaskMessage(task_id="t1", run_id="r1", task_name="doomed", reason="exhausted")
    )

    method, _props, body = channel.basic_get(queue=dlq, auto_ack=True)
    assert method is not None
    parked = TaskMessage.from_json(body)
    assert parked.task_name == "doomed"
    assert parked.reason == "exhausted"

    channel.queue_delete(queue)
    channel.queue_delete(dlq)
    real.close()


def test_recovery_respects_the_retry_budget(client, broker, session, session_factory):
    """A task that keeps killing its worker still exhausts its retries."""
    run_id = _orphan_a_running_task(client, broker, session, session_factory)

    with session_factory() as s:
        task = s.query(Task).first()
        task.retry_count = task.max_retries  # already out of budget
        task.status = TASK_RUNNING
        s.commit()

        recovery.reclaim_orphaned_tasks(s, timeout=30)
        s.commit()

    run = client.get(f"/runs/{run_id}").json()
    assert run["status"] == "failed"
    assert run["tasks"][0]["status"] == "failed"
    assert len(broker.dead_letters()) == 1
