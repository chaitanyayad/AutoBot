"""Phase 4 acceptance check: watch three workers share one fan-out DAG.

Talks to the running compose stack over HTTP — no imports from the engine, so it
measures the real system rather than a test harness:

    docker compose up -d --build
    python scripts/parallel_demo.py
    python scripts/parallel_demo.py --api http://localhost:8000 examples/fan_out.json

It registers the workflow, triggers a run, polls until it finishes, and then
reports the two things Phase 4 claims: every task ran exactly once, and the
parallel wave was spread across the workers instead of absorbed by one.

With the compose default of `TASK_DURATION=1.0`, six shards are ~6s of task time
that three workers should clear in ~2s, inside a ~4s run — one second each for
the split and the merge, which have nothing to run alongside them.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import urllib.error
import urllib.request

DEFAULT_DEFINITION = Path(__file__).resolve().parent.parent / "examples" / "fan_out.json"
TERMINAL = {"completed", "failed", "cancelled"}


def call(api: str, path: str, payload: dict | None = None) -> dict:
    request = urllib.request.Request(
        f"{api.rstrip('/')}{path}",
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={"Content-Type": "application/json"},
        method="POST" if payload is not None else "GET",
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.loads(response.read())


def wait_for_api(api: str, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            call(api, "/health")
            return
        except (urllib.error.URLError, OSError):
            time.sleep(1)
    raise SystemExit(f"no API at {api} — is `docker compose up -d` running?")


def parse_time(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def report(tasks: list[dict], elapsed: float, duration_hint: float) -> int:
    by_worker = Counter(t["worker_id"] for t in tasks)
    width = max(len(t["task_name"]) for t in tasks)

    print(f"\n{'task':<{width}}  {'status':<8}  {'worker':<14}  seconds")
    for task in sorted(tasks, key=lambda t: (t["started_at"] or "", t["task_name"])):
        started, finished = parse_time(task["started_at"]), parse_time(task["completed_at"])
        span = f"{(finished - started).total_seconds():.2f}" if started and finished else "-"
        worker = (task["worker_id"] or "-")[-14:]
        print(f"{task['task_name']:<{width}}  {task['status']:<8}  {worker:<14}  {span:>7}")

    print(f"\nworkers used: {len(by_worker)}")
    for worker, count in by_worker.most_common():
        print(f"  {worker}: {count} task(s)")

    task_seconds = sum(
        (parse_time(t["completed_at"]) - parse_time(t["started_at"])).total_seconds()
        for t in tasks
        if t["started_at"] and t["completed_at"]
    )
    print(f"\nwall clock:   {elapsed:.2f}s")
    print(f"task time:    {task_seconds:.2f}s across {len(tasks)} tasks")
    if elapsed > 0:
        concurrency = task_seconds / elapsed
        print(f"concurrency:  {concurrency:.2f} tasks executing at once, on average")
        if concurrency < 1:
            # Retry backoff and the queue round trip are wall clock but not task
            # time, so the average only reflects parallelism when work dominates.
            print("              (wall clock here is mostly waiting, not executing —"
                  " raise TASK_DURATION to see the parallelism)")

    failures = [t["task_name"] for t in tasks if t["status"] != "success"]
    if failures:
        print(f"\nFAILED: {failures}")
        return 1
    if len(by_worker) < 2:
        print("\nonly one worker took part — scale up with: docker compose up -d --scale worker=3")
        return 1
    print("\nevery task succeeded exactly once, spread across multiple workers.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("definition", nargs="?", type=Path, default=DEFAULT_DEFINITION)
    parser.add_argument("--api", default="http://localhost:8000")
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument(
        "--task-duration",
        type=float,
        default=1.0,
        help="what TASK_DURATION the workers were started with (reporting only)",
    )
    args = parser.parse_args(argv)

    definition = json.loads(args.definition.read_text())
    wait_for_api(args.api)

    workflow = call(args.api, "/workflows", definition)
    print(f"registered {definition['name']} -> {workflow['id']}")

    started = time.monotonic()
    run = call(args.api, f"/workflows/{workflow['id']}/trigger", {})
    run_id = run["id"]
    print(f"triggered run {run_id}")

    seen = ""
    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        run = call(args.api, f"/runs/{run_id}")
        if run["status"] in TERMINAL:
            break
        snapshot = " ".join(
            f"{t['task_name']}={t['status']}" for t in sorted(run["tasks"], key=lambda t: t["task_name"])
        )
        if snapshot != seen:  # only print when something actually moved
            print(f"  [{time.monotonic() - started:5.1f}s] {snapshot}")
            seen = snapshot
        time.sleep(0.25)
    else:
        print(f"run did not finish within {args.timeout}s; last status {run['status']}")
        return 1

    elapsed = time.monotonic() - started
    print(f"\nrun {run['status']} in {elapsed:.2f}s")
    return report(call(args.api, f"/runs/{run_id}/tasks"), elapsed, args.task_duration)


if __name__ == "__main__":
    sys.exit(main())
