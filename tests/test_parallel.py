"""Phase 4: several workers on one run at the same time.

Three claims are made here, and each has a test that would fail without the
mechanism behind it:

1. A task is executed **exactly once**, even when two workers are handed the same
   delivery — `scheduler.claim` is a compare-and-set.
2. A fan-in is released **exactly once**, even when both branches finish in the
   same instant — `scheduler.resolve` runs under the run's row lock.
3. Branches genuinely run **at the same time** — the barriers below only release
   if the required number of workers are inside a handler simultaneously, so a
   sequential engine deadlocks them instead of passing.

Anything that needs real connection-level concurrency asks for `concurrent_db`
and is skipped on SQLite, where the suite shares a single connection.
"""

import threading
import time
import uuid

import pytest
from sqlalchemy import select

from app import executors, scheduler
from app.models import TERMINAL_RUN_STATUSES, Task
from app.worker import Worker

FAN_OUT = {
    "name": "fan_out",
    "workflow": [
        {"id": "split", "depends_on": [], "max_retries": 0},
        {"id": "x", "depends_on": ["split"], "max_retries": 0},
        {"id": "y", "depends_on": ["split"], "max_retries": 0},
        {"id": "join", "depends_on": ["x", "y"], "max_retries": 0},
    ],
}

SHARDS = [f"shard_{n}" for n in range(1, 7)]
WIDE_FAN_OUT = {
    "name": "wide_fan_out",
    "workflow": [
        {"id": "split", "depends_on": [], "max_retries": 0},
        *({"id": s, "depends_on": ["split"], "max_retries": 0} for s in SHARDS),
        {"id": "merge", "depends_on": SHARDS, "max_retries": 0},
    ],
}


@pytest.fixture()
def concurrent_db(engine):
    if engine.dialect.name != "postgresql":
        pytest.skip("needs real row locking; the SQLite suite shares one connection")
    return engine


def trigger(client, definition):
    workflow_id = client.post("/workflows", json=definition).json()["id"]
    return client.post(f"/workflows/{workflow_id}/trigger").json()["id"]


def task_named(session, run_id, name) -> Task:
    return session.scalar(
        select(Task).where(Task.run_id == uuid.UUID(run_id), Task.task_name == name)
    )


def finish(session_factory, run_id, name, worker_id="w0"):
    """Take a queued task and report it successful, as a worker would."""
    with session_factory() as session:
        task = task_named(session, run_id, name)
        assert scheduler.claim(session, task, worker_id)
        session.commit()
        scheduler.report_result(session, task, succeed=True)
        session.commit()


class WorkerPool:
    """`count` workers draining the broker concurrently until the run finishes.

    Each worker is a real `Worker` with its own session, so the only thing this
    adds over production is that the processes are threads.
    """

    def __init__(self, client, broker, session_factory, count):
        self.client = client
        self.broker = broker
        self.workers = [
            Worker(worker_id=f"parallel-{i}", session_factory=session_factory)
            for i in range(count)
        ]
        self._stop = threading.Event()
        self.errors: list[Exception] = []

    def _loop(self, worker):
        try:
            while not self._stop.is_set():
                self.broker.consume(worker.handle)
                time.sleep(0.002)
        except Exception as exc:  # a worker dying would otherwise look like a hang
            self.errors.append(exc)
            self._stop.set()

    def run_until_finished(self, run_id, timeout=30.0) -> str:
        threads = [
            threading.Thread(target=self._loop, args=(w,), daemon=True)
            for w in self.workers
        ]
        for thread in threads:
            thread.start()

        deadline = time.monotonic() + timeout
        status = "running"
        try:
            while time.monotonic() < deadline:
                status = self.client.get(f"/runs/{run_id}").json()["status"]
                if status in TERMINAL_RUN_STATUSES:
                    break
                time.sleep(0.02)
        finally:
            self._stop.set()
            for thread in threads:
                thread.join(timeout=10)

        assert not self.errors, f"worker raised: {self.errors[0]!r}"
        return status


# --- the claim is atomic ----------------------------------------------------


def test_a_second_claim_on_the_same_task_is_refused(client, session_factory):
    run_id = trigger(client, FAN_OUT)

    with session_factory() as session:
        task = task_named(session, run_id, "split")
        assert scheduler.claim(session, task, "first") is True
        assert scheduler.claim(session, task, "second") is False
        session.commit()

    with session_factory() as session:
        task = task_named(session, run_id, "split")
        assert task.status == "running"
        assert task.worker_id == "first", "the loser must not overwrite the winner"


def test_claiming_a_finished_task_is_refused(client, session_factory):
    run_id = trigger(client, FAN_OUT)
    finish(session_factory, run_id, "split")

    with session_factory() as session:
        task = task_named(session, run_id, "split")
        assert scheduler.claim(session, task, "latecomer") is False
        assert task.status == "success"


def test_a_claim_waits_for_the_holder_and_then_loses(
    client, session_factory, concurrent_db
):
    """The deterministic version of the race: hold the row lock and watch.

    Worker A claims without committing, so it holds the row lock. Worker B's
    claim blocks — proof the two are serialised rather than both reading
    `queued` — and once A commits, B's UPDATE is re-evaluated against the
    committed row, matches nothing, and it declines.
    """
    run_id = trigger(client, FAN_OUT)
    outcome: list[bool] = []
    started = threading.Event()

    def claim_from_another_connection():
        with session_factory() as session:
            task = task_named(session, run_id, "split")
            started.set()
            outcome.append(scheduler.claim(session, task, "B"))
            session.commit()

    with session_factory() as holder:
        task = task_named(holder, run_id, "split")
        assert scheduler.claim(holder, task, "A") is True  # row lock held, uncommitted

        thread = threading.Thread(target=claim_from_another_connection, daemon=True)
        thread.start()
        started.wait(timeout=5)
        time.sleep(0.3)
        assert outcome == [], "B should still be blocked on A's row lock"

        holder.commit()  # releases the lock; B re-evaluates against the new row

    thread.join(timeout=10)
    assert outcome == [False], "only one worker may claim a task"


def test_only_one_of_many_simultaneous_claims_wins(
    client, session_factory, concurrent_db
):
    run_id = trigger(client, FAN_OUT)
    contenders = 8
    barrier = threading.Barrier(contenders, timeout=10)
    won: list[str] = []
    lock = threading.Lock()

    def contend(worker_id):
        with session_factory() as session:
            task = task_named(session, run_id, "split")
            barrier.wait()  # every connection arrives at the claim together
            if scheduler.claim(session, task, worker_id):
                with lock:
                    won.append(worker_id)
            session.commit()

    threads = [
        threading.Thread(target=contend, args=(f"c{i}",), daemon=True)
        for i in range(contenders)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=15)

    assert len(won) == 1, f"{len(won)} workers claimed the same task: {won}"


def test_two_workers_racing_one_delivery_execute_it_once(
    client, broker, session_factory, concurrent_db
):
    """The end-to-end version: the same message delivered to two workers."""
    executions: list[str] = []
    lock = threading.Lock()

    @executors.register("split")
    def record(ctx):
        with lock:
            executions.append(ctx.worker_id)
        time.sleep(0.1)  # widen the window a duplicate could squeeze into

    trigger(client, FAN_OUT)
    message = broker.messages()[0]

    barrier = threading.Barrier(2, timeout=10)
    workers = [Worker(worker_id=f"racer-{i}", session_factory=session_factory) for i in (1, 2)]

    def deliver(worker):
        barrier.wait()
        worker.handle(message)

    threads = [threading.Thread(target=deliver, args=(w,), daemon=True) for w in workers]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=15)

    assert len(executions) == 1, f"task executed {len(executions)} times: {executions}"


# --- the fan-in is released exactly once ------------------------------------


@pytest.mark.parametrize("attempt", range(4))
def test_simultaneous_branch_completions_release_the_fan_in(
    client, broker, session_factory, concurrent_db, attempt
):
    """Regression: two branches finishing at once must not both miss the fan-in.

    Without the run lock in `resolve`, each worker's snapshot can exclude the
    other's uncommitted success. Both then conclude the fan-in is not runnable,
    nothing else is in flight to wake it, and the run hangs at 50% forever.
    Repeated, because the failure is a lost race rather than a certainty.
    """
    run_id = trigger(client, FAN_OUT)
    finish(session_factory, run_id, "split")

    barrier = threading.Barrier(2, timeout=10)

    def complete_branch(name):
        with session_factory() as session:
            task = task_named(session, run_id, name)
            assert scheduler.claim(session, task, f"worker-{name}")
            session.commit()
            barrier.wait()  # both report inside the same instant
            scheduler.report_result(session, task, succeed=True)
            session.commit()

    threads = [
        threading.Thread(target=complete_branch, args=(name,), daemon=True)
        for name in ("x", "y")
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=15)

    tasks = {t["task_name"]: t["status"] for t in client.get(f"/runs/{run_id}/tasks").json()}
    assert tasks["join"] == "queued", "the fan-in was never released"

    dispatched = [m.task_name for m in broker.messages()]
    assert dispatched.count("join") == 1, f"fan-in dispatched {dispatched.count('join')} times"


# --- real parallelism -------------------------------------------------------


def test_two_branches_execute_at_the_same_time(
    client, broker, session_factory, concurrent_db
):
    """A barrier that only opens if both branches are in flight together.

    A sequential engine cannot pass this: the first branch would sit in
    `barrier.wait()` waiting for a sibling that will not start until it returns.
    """
    both_running = threading.Barrier(2, timeout=15)
    executors.register("x")(lambda ctx: both_running.wait())
    executors.register("y")(lambda ctx: both_running.wait())

    run_id = trigger(client, FAN_OUT)
    pool = WorkerPool(client, broker, session_factory, count=2)

    assert pool.run_until_finished(run_id) == "completed"


def test_three_workers_share_one_wave(client, broker, session_factory, concurrent_db):
    """Six shards, three workers, a barrier that needs three at once.

    Proves the wave is spread across every worker rather than absorbed by one:
    the shards can only clear in two groups of three simultaneous executions.
    """
    all_three = threading.Barrier(3, timeout=20)
    executed: list[tuple[str, str]] = []
    lock = threading.Lock()

    def shard(ctx):
        all_three.wait()
        with lock:
            executed.append((ctx.task_name, ctx.worker_id))

    for name in SHARDS:
        executors.register(name)(shard)

    run_id = trigger(client, WIDE_FAN_OUT)
    pool = WorkerPool(client, broker, session_factory, count=3)

    assert pool.run_until_finished(run_id) == "completed"
    assert sorted(name for name, _ in executed) == sorted(SHARDS)
    assert len({worker for _, worker in executed}) == 3, "one worker took the whole wave"


def test_every_task_runs_exactly_once_under_three_workers(
    client, broker, session_factory, concurrent_db
):
    executions: list[str] = []
    lock = threading.Lock()

    def record(ctx):
        with lock:
            executions.append(ctx.task_name)

    for node in WIDE_FAN_OUT["workflow"]:
        executors.register(node["id"])(record)

    run_id = trigger(client, WIDE_FAN_OUT)
    pool = WorkerPool(client, broker, session_factory, count=3)

    assert pool.run_until_finished(run_id) == "completed"
    assert sorted(executions) == sorted(node["id"] for node in WIDE_FAN_OUT["workflow"])

    tasks = client.get(f"/runs/{run_id}/tasks").json()
    assert {t["status"] for t in tasks} == {"success"}
    assert len({t["worker_id"] for t in tasks}) > 1, "the run never left one worker"


# --- publish only what is committed -----------------------------------------


def test_dispatch_publishes_nothing_before_commit(client, broker, session_factory):
    """A message must not be visible before the row that authorises it is.

    A worker that beat the commit would read the task as still `pending`,
    decline the delivery, and strand the run — the window is one broker round
    trip, which is plenty with several idle workers.
    """
    from app.models import Workflow

    with session_factory() as session:
        workflow = Workflow(name="deferred", definition=FAN_OUT)
        session.add(workflow)
        session.flush()

        run = scheduler.create_run(session, workflow)
        scheduler.resolve(session, run.id)

        assert task_named(session, str(run.id), "split").status == "queued"
        assert broker.pending() == 0, "published before the transaction committed"

        session.commit()

    assert [m.task_name for m in broker.messages()] == ["split"]


def test_a_rolled_back_dispatch_publishes_nothing(client, broker, session_factory):
    from app.models import Workflow

    with session_factory() as session:
        workflow = Workflow(name="rolled_back", definition=FAN_OUT)
        session.add(workflow)
        session.flush()

        run = scheduler.create_run(session, workflow)
        scheduler.resolve(session, run.id)
        session.rollback()

    assert broker.pending() == 0, "a discarded dispatch must not reach the queue"
