"""Phase 1 manual test: register a workflow, trigger it, step through resolution.

Runs against an in-process app with a temporary SQLite database, so no Postgres or
RabbitMQ is needed:

    python scripts/manual_test.py
    python scripts/manual_test.py examples/resume_pipeline.json
"""

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
os.environ["DATABASE_URL"] = "sqlite+pysqlite:///:memory:"

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

from app import db as db_module  # noqa: E402
from app.db import Base, get_session  # noqa: E402
from app.main import app  # noqa: E402

THREE_TASK = {
    "name": "three_task_demo",
    "workflow": [
        {"id": "a", "depends_on": []},
        {"id": "b", "depends_on": ["a"]},
        {"id": "c", "depends_on": ["b"]},
    ],
}


def build_client() -> TestClient:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    db_module.engine = engine
    db_module.SessionLocal = factory

    def override():
        session = factory()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_session] = override
    return TestClient(app)


def show(label: str, tasks: list[dict]) -> None:
    print(f"\n{label}")
    for task in sorted(tasks, key=lambda t: t["task_name"]):
        deps = ", ".join(task["depends_on"]) or "-"
        print(f"  {task['task_name']:<22} {task['status']:<9} depends_on: {deps}")


def main() -> int:
    definition = THREE_TASK
    if len(sys.argv) > 1:
        definition = json.loads(Path(sys.argv[1]).read_text())

    client = build_client()

    workflow_id = client.post("/workflows", json=definition).json()["id"]
    print(f"registered workflow {definition['name']} -> {workflow_id}")

    run = client.post(f"/workflows/{workflow_id}/trigger").json()
    print(f"triggered run {run['id']}  status={run['status']}")
    show("initial state (roots queued):", run["tasks"])

    wave = 1
    while run["status"] == "running":
        queued = [t["task_name"] for t in run["tasks"] if t["status"] == "queued"]
        if not queued:
            print("\nno queued tasks and run is not finished — stuck")
            return 1
        print(f"\nwave {wave}: executing {queued}")
        for name in queued:
            run = client.post(f"/runs/{run['id']}/tasks/{name}/simulate").json()
        show(f"after wave {wave}:", run["tasks"])
        wave += 1

    print(f"\nrun finished: status={run['status']} completed_at={run['completed_at']}")
    return 0 if run["status"] == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
