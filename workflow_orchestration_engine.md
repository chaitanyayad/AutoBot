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

### Phase 2 — Worker System
- [ ] Single worker that consumes from RabbitMQ and executes tasks
- [ ] Worker updates task status in DB
- [ ] After task completes → call scheduler → publish next runnable tasks
- [ ] Test full linear workflow end-to-end

### Phase 3 — Retry + Fault Tolerance
- [ ] Retry counter per task
- [ ] Exponential backoff on re-queue
- [ ] Dead letter queue for exhausted tasks
- [ ] Worker heartbeat table + timeout recovery

### Phase 4 — Parallel Execution
- [ ] Run 3+ workers simultaneously via Docker Compose
- [ ] Test a fan-out DAG (one task feeds two parallel tasks)
- [ ] Verify no double-execution (atomic task claim via DB lock or RabbitMQ ack)

### Phase 5 — Scheduling
- [ ] Cron-style triggers: `"schedule": "0 9 * * *"`
- [ ] APScheduler integration in FastAPI
- [ ] Persist scheduled workflows, auto-trigger on schedule

### Phase 6 — Dashboard
- [ ] Workflow list: all runs + status
- [ ] Run detail: DAG visualization + task states
- [ ] Live updates via WebSocket or polling
- [ ] Logs per task (stdout/stderr from worker)

### Phase 7 — Polish
- [ ] Docker Compose: postgres + rabbitmq + api + 3 workers + frontend
- [ ] README with architecture diagram
- [ ] Example workflows: resume pipeline, ML pipeline, notification pipeline

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
