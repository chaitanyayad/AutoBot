"""Walkthrough: register a workflow, trigger it, and let a worker drain the DAG.

Runs the API, the broker and a worker in one process against a temporary SQLite
database, so no Postgres, RabbitMQ or Docker is needed:

    python scripts/manual_test.py
    python scripts/manual_test.py examples/resume_pipeline.json

Each wave printed below is one round of the scheduler: the tasks whose
dependencies had just been satisfied, executed by the worker, releasing the next.
"""

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
os.environ["DATABASE_URL"] = "sqlite+pysqlite:///:memory:"
os.environ["BROKER_URL"] = "memory://"

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

from app import broker as broker_module  # noqa: E402
from app import config, executors  # noqa: E402
from app.broker import InMemoryBroker  # noqa: E402
from app import db as db_module  # noqa: E402
from app.db import Base, get_session  # noqa: E402
from app.main import app  # noqa: E402
from app.worker import Worker  # noqa: E402

THREE_TASK = {
    "name": "three_task_demo",
    "workflow": [
        {"id": "a", "depends_on": []},
        {"id": "b", "depends_on": ["a"]},
        {"id": "c", "depends_on": ["b"]},
    ],
}


def build(broker):
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    db_module.engine = engine
    db_module.SessionLocal = factory
    broker_module.set_broker(broker)

    def override():
        session = factory()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_session] = override
    return TestClient(app), Worker(worker_id="demo-worker", session_factory=factory)


def show(label: str, tasks: list[dict]) -> None:
    print(f"\n{label}")
    for task in sorted(tasks, key=lambda t: t["task_name"]):
        deps = ", ".join(task["depends_on"]) or "-"
        worker = f"  [{task['worker_id']}]" if task["worker_id"] else ""
        print(f"  {task['task_name']:<22} {task['status']:<9} depends_on: {deps}{worker}")


def main() -> int:
    definition = THREE_TASK
    if len(sys.argv) > 1:
        definition = json.loads(Path(sys.argv[1]).read_text())

    broker = InMemoryBroker()
    client, worker = build(broker)
    executors.set_default_duration(0.0)

    workflow_id = client.post("/workflows", json=definition).json()["id"]
    print(f"registered workflow {definition['name']} -> {workflow_id}")

    run = client.post(f"/workflows/{workflow_id}/trigger").json()
    run_id = run["id"]
    print(f"triggered run {run_id}  status={run['status']}")
    show("initial state (roots published to the queue):", run["tasks"])

    wave = 1
    while broker.pending():
        batch = [m.task_name for m in broker.messages()]
        print(f"\nwave {wave}: worker executing {batch}")
        broker.consume(worker.handle, wait=True)
        run = client.get(f"/runs/{run_id}").json()
        show(f"after wave {wave}:", run["tasks"])
        wave += 1

    run = client.get(f"/runs/{run_id}").json()
    print(f"\nrun finished: status={run['status']} completed_at={run['completed_at']}")
    return 0 if run["status"] == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
