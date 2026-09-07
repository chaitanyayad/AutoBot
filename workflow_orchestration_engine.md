# Workflow Orchestration Engine — Project Plan

## Resume Line
> *Designed a distributed workflow orchestration engine with DAG-based execution, fault-tolerant retries, and worker coordination using RabbitMQ and PostgreSQL.*

---

## What This Project Is

A mini Apache Airflow. A system that coordinates sequences of tasks and their dependencies — instead of writing one giant function that does everything sequentially, you define a workflow once and the engine handles:

- What runs first
- What can run in parallel
- What happens on failure
- When to retry
- When the workflow is complete

Think of it as a **traffic controller for jobs**.

---

## Core Concepts

### DAG (Directed Acyclic Graph)
Tasks are nodes. Dependencies are edges. No cycles allowed.

```
Resume Uploaded
        ↓
    Parse Resume
      /      \
Extract Skills  Generate Embeddings
      \      /
    Save Results
        ↓
 Send Notification
```

The engine resolves this graph and knows what can run in parallel vs what must wait.

### Workflow Definition (input JSON)
```json
{
  "name": "resume_pipeline",
  "workflow": [
    { "id": "parse_resume",         "depends_on": [] },
    { "id": "extract_skills",       "depends_on": ["parse_resume"] },
    { "id": "generate_embeddings",  "depends_on": ["parse_resume"] },
    { "id": "save_results",         "depends_on": ["extract_skills", "generate_embeddings"] },
    { "id": "send_notification",    "depends_on": ["save_results"] }
  ]
}
```

---

## Architecture

```
                +-----------+
                | Dashboard |   (React or Jinja2 — shows live task status)
                +-----------+
                      |
                      v

+----------+    +--------------+
| FastAPI  | -> |  PostgreSQL  |
+----------+    +--------------+
      |          (workflow state,
      |           task history)
      v

+----------+
| RabbitMQ |   (distributes runnable tasks to workers)
+----------+
      |
   -------
   |     |
   v     v
+------+ +------+ +------+
|  W1  | |  W2  | |  W3  |   (workers poll, execute, report back)
+------+ +------+ +------+
```

### Component Responsibilities

| Component | Role |
|-----------|------|
| **FastAPI** | Create workflows, query status, trigger runs |
| **PostgreSQL** | Source of truth for all workflow/task state |
| **RabbitMQ** | Task queue — delivers runnable tasks to available workers |
| **Workers** | Poll queue, execute tasks, update status |
| **Dashboard** | Real-time view of running workflows and task states |

---

## Database Schema

```sql
-- A workflow definition (the DAG template)
CREATE TABLE workflows (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    definition JSONB NOT NULL,          -- the full DAG JSON
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- A specific run of a workflow
CREATE TABLE workflow_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID REFERENCES workflows(id),
    status TEXT DEFAULT 'pending',      -- pending, running, completed, failed
    triggered_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

-- Individual task instances within a run
CREATE TABLE tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID REFERENCES workflow_runs(id),
    task_name TEXT NOT NULL,
    depends_on TEXT[] DEFAULT '{}',     -- array of task_names
    status TEXT DEFAULT 'pending',      -- pending, queued, running, success, failed
    retry_count INT DEFAULT 0,
    max_retries INT DEFAULT 3,
    worker_id TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message TEXT
);

-- Worker heartbeat registry
CREATE TABLE workers (
    id TEXT PRIMARY KEY,               -- worker hostname + pid
    last_heartbeat TIMESTAMPTZ,
    status TEXT DEFAULT 'idle'         -- idle, busy
);
```

---

## API Endpoints

```
POST   /workflows               → register a workflow definition
POST   /workflows/{id}/trigger  → start a new run
GET    /runs/{run_id}           → get run status + all task states
GET    /runs/{run_id}/tasks     → task-level breakdown
POST   /runs/{run_id}/cancel    → cancel a running workflow
GET    /dashboard               → all active runs (for UI)
```

---

## Scheduler Logic (core algorithm)

The **DAG resolver** runs after every task completion:

```python
def get_runnable_tasks(run_id):
    all_tasks = db.get_tasks(run_id)
    completed = {t.name for t in all_tasks if t.status == "success"}
    
    runnable = []
    for task in all_tasks:
        if task.status != "pending":
            continue
        if all(dep in completed for dep in task.depends_on):
            runnable.append(task)
    
    return runnable
```

After each task finishes → call this → push new runnable tasks to RabbitMQ queue.

---

## Worker Logic

```python
while True:
    task = rabbitmq.consume("task_queue")
    if not task:
        sleep(1)
        continue
    
    db.update_task(task.id, status="running", worker_id=self.id)
    
    try:
        execute(task)
        db.update_task(task.id, status="success")
    except Exception as e:
        if task.retry_count < task.max_retries:
            db.increment_retry(task.id)
            rabbitmq.publish("task_queue", task)   # re-queue
        else:
            db.update_task(task.id, status="failed", error=str(e))
            # optionally publish to dead letter queue
    
    trigger_scheduler(task.run_id)  # check for newly runnable tasks
```

---

## Retry Policy

| Scenario | Behavior |
|----------|----------|
| Task fails, retries remaining | Re-queued with exponential backoff |
| Task exhausts all retries | Marked `failed`, workflow halted |
| Worker dies mid-task | Heartbeat timeout → task re-queued |
| Whole workflow fails | Notify user, log to dead letter queue |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| API | FastAPI (Python) |
| Database | PostgreSQL |
| Queue | RabbitMQ (pika or aio-pika) |
| Workers | Python multiprocessing or separate containers |
| Dashboard | React or Jinja2 + HTMX for live updates |
| Scheduling | APScheduler (for time-triggered workflows) |
| Containerization | Docker Compose |

---

## Build Order (phases)

### Phase 1 — Core DAG Engine ✅
- [x] Design DB schema (workflows, workflow_runs, tasks) — `schema.sql` + `app/models.py`
- [x] FastAPI: POST /workflows, POST /trigger — `app/main.py`
- [x] DAG resolver: `get_runnable_tasks()` function — `app/dag.py` (+ cycle detection, topological sort)
- [x] Manual test: create a 3-task workflow, verify resolution order — `scripts/manual_test.py`, 28 tests passing

### Phase 2 — Worker System ✅
- [x] Single worker that consumes from RabbitMQ and executes tasks — `app/worker.py`, `app/broker.py`
- [x] Worker updates task status in DB — `claim()` → `running`, `report_result()` → `success`/`failed`
- [x] After task completes → call scheduler → publish next runnable tasks — `scheduler.dispatch()` publishes
- [x] Test full linear workflow end-to-end — 13 worker tests incl. a real-RabbitMQ round trip; 5-task pipeline verified live over Postgres + RabbitMQ

### Phase 3 — Retry + Fault Tolerance ✅
- [x] Retry counter per task — `scheduler.report_result()` re-queues while `retry_count < max_retries`
- [x] Exponential backoff on re-queue — `app/retry.py`; RabbitMQ TTL+DLX tier queues, no plugins
- [x] Dead letter queue for exhausted tasks — `broker.dead_letter()` → `task_queue.dead`
- [x] Worker heartbeat table + timeout recovery — `app/recovery.py`, swept by each worker
- Verified live: retry backoff 1.09s → 1.99s then success; exhausted task parked in the DLQ;
  worker killed mid-task and its orphan reclaimed by another worker

### Phase 4 — Parallel Execution ✅
- [x] Run 3+ workers simultaneously via Docker Compose — `Dockerfile` + `docker-compose.yml`
  (`api` + `worker` × 3 from one image; `--scale worker=N` for more)
- [x] Test a fan-out DAG (one task feeds two parallel tasks) — `examples/fan_out.json`,
  `scripts/parallel_demo.py`, and barrier-based tests that only pass if workers
  are genuinely executing at the same instant
- [x] Verify no double-execution — `scheduler.claim()` is a compare-and-set
  (`UPDATE … WHERE id = ? AND status = 'queued'`), so RabbitMQ's at-least-once
  delivery becomes exactly-once execution
- [x] Two further races that only appear with several workers, both found and fixed here:
  - `resolve()` runs under the run's row lock — otherwise sibling branches finishing
    together each miss the fan-in they jointly unblocked and the run hangs at 50%
  - publishes are deferred to after commit — otherwise a worker can consume a message
    before the row authorising it is visible, decline it, and strand the task
- [x] Recovery sweep takes orphans `FOR UPDATE SKIP LOCKED`, so two sweeping workers
  cannot burn two retry attempts on one failure
- 14 new tests in `tests/test_parallel.py` (88 total); each was checked to fail with
  its mechanism removed. Verified live: 33-node DAG over 5 containerised workers,
  every task executed exactly once, 8s of task time in 4.3s wall clock on the 6-shard demo

### Phase 5 — Scheduling ✅
- [x] Cron-style triggers: `"schedule": "0 9 * * *"` — validated at registration
  with APScheduler's own parser, same principle as DAG validation (`app/cron.py`,
  `app/schemas.py`)
- [x] APScheduler integration in FastAPI — one process-wide `BackgroundScheduler`,
  started and stopped from the app's `lifespan` (`app/main.py`)
- [x] Persist scheduled workflows, auto-trigger on schedule — `workflows.schedule`
  column; every non-null schedule is re-armed from the database on startup, and a
  newly-registered one is armed immediately, so schedules survive a restart
  without their own migration step. The job itself calls the same
  `create_run` + `resolve` pair a manual trigger does.
- 12 new tests in `tests/test_scheduling.py`; verified live against the running
  API — a bad cron expression 422s, a valid one arms an APScheduler job, and a
  workflow inserted directly into the database (no registration call) is armed
  by a fresh `cron.start()`, proving the restart path independently of the
  registration path.

### Phase 6 — Dashboard ✅
- [x] Workflow list: all runs + status — `GET /dashboard` (`app/static/`)
- [x] Run detail: DAG visualization + task states — inline SVG node-link diagram
  laid out from `GET /workflows/{id}/graph` (levels + edges), overlaid with live
  per-task status
- [x] Live updates via WebSocket or polling — `WS /ws/runs/{run_id}` polls the
  database server-side and pushes on a fixed interval until the run reaches a
  terminal status, then closes; the page falls back to polling `GET /runs/{id}`
  if the socket cannot be opened
- [x] Logs per task (stdout/stderr from worker) — the worker captures a
  handler's stdout/stderr per attempt (`app/worker.py`) into `tasks.logs`,
  served by `GET /runs/{id}/tasks/{name}/logs`
- [x] `POST /runs/{run_id}/cancel` (promised for this phase in the README) —
  stops further dispatch on a run; tasks already in flight still report back,
  since nothing here can reach into a worker process mid-execution
  (`scheduler.cancel_run`)
- 19 new tests in `tests/test_dashboard.py`, including a real WebSocket round
  trip via `TestClient.websocket_connect` and a cancel-mid-fan-out test that
  fails if the cancellation check is removed from `resolve()`. Verified live:
  registered, triggered and cancelled runs through the browser-facing API with
  a real worker on real Postgres + RabbitMQ.

### Phase 7 — Polish ✅
- [x] Docker Compose: postgres + rabbitmq + api + 3 workers + frontend — the
  dashboard needed nothing extra: it's static files under `app/static/`,
  already copied into the one image `api` and `worker` both run from
  (`Dockerfile`), served by the same FastAPI process at `GET /dashboard`
- [x] README with architecture diagram — see README's Architecture section
- [x] Example workflows: resume pipeline (existing), ML pipeline, notification
  pipeline — `examples/ml_pipeline.json`, `examples/notification_pipeline.json`,
  with matching handlers added to `examples/handlers.py` (`notification_pipeline`'s
  `send_sms` fails its first attempt on purpose, with `max_retries: 5` on that
  node, so the retry path is visible in a live run of a non-toy example)
- Also closed the two gaps carried forward from Phases 5–6, since "complete
  the project" reasonably includes not shipping with known holes:
  - **Lost dispatch messages are now recovered.** `dispatch()` and the retry
    path both stamp `tasks.dispatched_at` (the moment an attempt becomes
    claimable — "now" for a fresh dispatch, "now + backoff delay" for a
    retry, so a delayed retry is never mistaken for stuck). The existing
    heartbeat sweep now also calls `recovery.reclaim_stuck_queued_tasks`,
    which re-queues (or fails, if the retry budget is spent) anything still
    `queued` past `QUEUED_TIMEOUT` — the same `FOR UPDATE SKIP LOCKED` +
    retry-path pattern as the orphaned-`running` sweep, just keyed on
    `dispatched_at` instead of a worker's heartbeat.
  - **Cancellation is a real task status.** Added `TASK_CANCELLED` (schema +
    model CHECK constraint). `cancel_run` now bulk-updates every `pending`/
    `queued` task in the run to `cancelled` in the same statement that
    cancels the run — a `queued` task's stray message is then declined by
    `claim()`'s compare-and-set (`status = 'queued'` no longer matches), so
    it never executes even though the message still exists. A task already
    `running` is untouched, since nothing here can reach into a worker
    process mid-execution.
- 12 new/rewritten tests across `tests/test_retry.py` (lost-message recovery)
  and `tests/test_dashboard.py` (task-level cancellation, including that a
  cancelled task's stray queue message is declined rather than executed).
  Full suite: **125 tests, all passing against real Postgres + RabbitMQ**
  (111 run serviceless on SQLite alone; 10 need Postgres's real row locking,
  4 need a reachable RabbitMQ).

---

## Connection to Existing Job Tracker

This engine can **directly power** the job tracker:

```
User uploads resume
        ↓  (trigger workflow)
[parse_resume] → [extract_skills] + [generate_embeddings]  (parallel)
        ↓
[store_in_db]
        ↓
[send_notification]  (RabbitMQ → email worker)
        ↓
[update_dashboard]  (WebSocket push)
```

Both projects share: PostgreSQL, RabbitMQ, FastAPI, WebSockets. One portfolio story, not two disconnected projects.

---

## What This Demonstrates to Interviewers

- **Distributed systems** — multiple workers, coordination, no single point of failure
- **Graph algorithms** — topological sort for DAG resolution
- **Queue-based architecture** — producer/consumer patterns
- **Concurrency** — parallel task execution, race condition prevention
- **Fault tolerance** — retries, dead letters, worker recovery
- **Database design** — state machines, event history
- **System architecture** — clear separation of concerns across components

This is not a student project. This is how production systems at Netflix, Airbnb, and Uber actually work.

---

## Session Checklist (read before starting any session)

- What phase are we currently on?
- What was the last task completed?
- Any blockers from the previous session?
- What is the goal for this session?
