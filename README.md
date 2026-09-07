# AutoBot (Workflow Orchestration Engine)

A mini Apache Airflow: define a workflow as a DAG once, and the engine works out what
runs first, what runs in parallel, and when the run is complete.

Full project plan and roadmap: [workflow_orchestration_engine.md](workflow_orchestration_engine.md)

**Status: all 7 phases complete.** The DAG engine resolves dependencies, a pool
of workers executes tasks over RabbitMQ in parallel, and failures retry with
exponential backoff before landing in a dead letter queue. Tasks orphaned by a
dead worker — or whose dispatch message was silently lost — are reclaimed, and
no task can ever be executed twice. Workflows can carry a cron schedule that
auto-triggers runs, and a live dashboard visualises every run as it happens.

---

## Architecture

```
                        ┌─────────────┐
                        │  Dashboard  │  GET /dashboard, WS /ws/runs/{id}
                        │ (app/static)│  (served by the API process itself)
                        └──────┬──────┘
                               │ HTTP + WebSocket
                               ▼
┌────────────┐   register/trigger/cancel   ┌──────────────┐
│  BackgroundScheduler │◄──────────────────►│   FastAPI    │
│  (app/cron.py,       │   cron.add_job()   │ (app/main.py)│
│   arms schedules)     │                    └──────┬───────┘
└────────────┘                                       │
                                          create_run/resolve/cancel
                                                       ▼
                                              ┌──────────────────┐
                                              │    PostgreSQL     │  workflows, workflow_runs,
                                              │ (app/scheduler.py │  tasks, workers — the one
                                              │  is the only      │  source of truth for state
                                              │  writer of state) │
                                              └─────────┬─────────┘
                                                         │ publish after commit
                                                         ▼
                                                  ┌────────────┐
                                                  │  RabbitMQ  │  task_queue (+ retry-tier
                                                  │            │  holding queues, DLQ)
                                                  └─────┬──────┘
                                          ┌──────────────┼──────────────┐
                                          ▼              ▼              ▼
                                       ┌──────┐      ┌──────┐       ┌──────┐
                                       │  W1  │      │  W2  │  ...  │  Wn  │  claim -> execute ->
                                       └──────┘      └──────┘       └──────┘  report_result -> resolve
```

Every arrow into Postgres is the same handful of functions in
[app/scheduler.py](app/scheduler.py) — the API, the cron scheduler and every
worker all drive the run/task state machine through `create_run`, `resolve`,
`dispatch`, `report_result`, `claim` and `cancel_run`, so there is exactly one
place that knows what a legal state transition looks like.

---

## What works today

- **Schema** — `workflows`, `workflow_runs`, `tasks`, `workers` ([schema.sql](schema.sql),
  mirrored by [app/models.py](app/models.py))
- **Validation** — duplicate ids, dangling dependencies, self-edges and cycles are
  rejected at registration time, so an unrunnable workflow can never be triggered
- **DAG resolver** — `get_runnable_tasks()` plus Kahn topological sort
  ([app/dag.py](app/dag.py))
- **API** — register, trigger, inspect ([app/main.py](app/main.py))
- **Workers** — consume from RabbitMQ, execute, report back, release the next wave
  ([app/worker.py](app/worker.py), [app/broker.py](app/broker.py))
- **Task handlers** — register a callable per task name ([app/executors.py](app/executors.py))
- **Retries** — exponential backoff with jitter, then a dead letter queue
  ([app/retry.py](app/retry.py))
- **Fault tolerance** — worker heartbeats, recovery of tasks orphaned by a dead
  worker, and recovery of tasks whose dispatch message was silently lost
  ([app/recovery.py](app/recovery.py))
- **Parallel execution** — a pool of workers on one run, with an atomic task claim
  and a per-run scheduler lock ([app/scheduler.py](app/scheduler.py))
- **Docker Compose** — postgres, rabbitmq, the API and three workers, one command
  ([docker-compose.yml](docker-compose.yml))
- **Scheduling** — a cron expression on a workflow auto-triggers runs, armed by
  APScheduler and re-armed from the database on every restart
  ([app/cron.py](app/cron.py))
- **Dashboard** — a live, no-build-step web UI: workflow/run lists, a DAG
  visualisation with live task status, per-task logs, and a cancel button
  ([app/static/](app/static/), `GET /dashboard`)
- **Example workflows** — resume pipeline, fan-out demo, ML pipeline and
  notification pipeline, each with matching handlers in
  [examples/handlers.py](examples/handlers.py)
- **125 tests**, all passing against Postgres + RabbitMQ (111 run serviceless
  on SQLite; 10 need Postgres's real row locking and 4 need a reachable
  RabbitMQ, so those opt out rather than fail when run without them),
  including real-RabbitMQ round trips, the delayed-retry round trip, concurrency
  tests that each fail if their mechanism is removed, and a real WebSocket round
  trip through `TestClient`

---

## Quick start

```bash
# The whole stack — postgres, rabbitmq, the API and three workers
docker compose up -d --build
python scripts/parallel_demo.py    # watch a fan-out DAG spread across them
```

Or piece by piece:

```bash
pip install -r requirements.txt

# 1. See the DAG resolve, no database or broker needed (in-memory SQLite)
python scripts/manual_test.py
python scripts/manual_test.py examples/resume_pipeline.json

# 2. Run the tests against real Postgres (the default)
cp .env.example .env
docker compose up -d postgres rabbitmq
python -m pytest -q

# 3. Run the stack by hand: API in one shell, worker in another
uvicorn app.main:app --reload      # docs at http://localhost:8000/docs
python -m app.worker               # add --task-duration 0.5 to watch it work
```

`DATABASE_URL` selects the backend, and the test suite follows it. Postgres is the
target and the default for tests, so `JSONB`, `TEXT[]` and the CHECK constraints are
exercised for real. For a quick serviceless run the models declare portable type
variants, so the same suite works on SQLite:

```bash
TEST_DATABASE_URL=sqlite+pysqlite:///:memory: python -m pytest -q
```

If a native Postgres already owns port 5432, set `POSTGRES_PORT` in `.env` (e.g.
`5433`) — compose and `DATABASE_URL` both read it.

---

## Example workflows

| File | Shape | What it exercises |
|------|-------|--------------------|
| [examples/resume_pipeline.json](examples/resume_pipeline.json) | fan-out then fan-in | the walkthrough used throughout this README |
| [examples/fan_out.json](examples/fan_out.json) | one split, six parallel shards, one merge | parallel execution across a worker pool |
| [examples/ml_pipeline.json](examples/ml_pipeline.json) | linear, fanning out at the end | a realistic multi-stage pipeline (ingest → validate → engineer_features → train → evaluate → {deploy, publish_report}) |
| [examples/notification_pipeline.json](examples/notification_pipeline.json) | fan-out then fan-in, with a per-node retry override | `send_sms` fails its first attempt on purpose (`examples/handlers.py`) and carries `max_retries: 5` so the retry path is visible in a live run |

Each has matching handlers in [examples/handlers.py](examples/handlers.py) —
run any of them the same way:

```bash
python scripts/manual_test.py examples/ml_pipeline.json         # DAG walkthrough, no services
python -m app.worker --handlers examples.handlers               # then, with the API + a worker up:
curl -X POST localhost:8000/workflows -d @examples/notification_pipeline.json
```

---

## API

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/workflows` | Register a definition (validated as a DAG), optionally with a `schedule` cron expression |
| `GET` | `/workflows` · `/workflows/{id}` | List / fetch definitions |
| `GET` | `/workflows/{id}/graph` | DAG shape for layout — execution levels + edges |
| `POST` | `/workflows/{id}/trigger` | Start a run, materialise tasks, queue the roots |
| `GET` | `/runs` · `/runs/{run_id}` | Run status, task states, `runnable_now` |
| `GET` | `/runs/{run_id}/tasks` | Task-level breakdown |
| `GET` | `/runs/{run_id}/tasks/{name}/logs` | Captured stdout/stderr from the task's latest attempt |
| `POST` | `/runs/{run_id}/cancel` | Stop dispatching further work for a run |
| `POST` | `/runs/{run_id}/tasks/{name}/simulate` | Dev only, **off by default** (`ENABLE_SIMULATE_ENDPOINT`) |
| `GET` | `/dashboard` | The live dashboard page |
| `WS` | `/ws/runs/{run_id}` | Pushes run + task state until the run finishes |
| `GET` | `/health` | Liveness |

### Example

```bash
curl -X POST localhost:8000/workflows \
     -H 'Content-Type: application/json' \
     -d @examples/resume_pipeline.json

curl -X POST localhost:8000/workflows/<id>/trigger
```

Triggering the resume pipeline creates five task rows, queues `parse_resume`, and
leaves the rest `pending`. As tasks report success the resolver releases the next
wave: `parse_resume` → `extract_skills` + `generate_embeddings` (parallel) →
`save_results` → `send_notification`.

---

## How it fits together

```
POST /workflows            validate_definition()   cycle / dangling-dep check
POST /workflows/{id}/trigger
        └─ scheduler.create_run()                  one task row per DAG node
        └─ scheduler.resolve()                     get_runnable_tasks() -> dispatch()
                                                        │
                                                        ▼
                                                   [ RabbitMQ ]
                                                        │
                    worker: claim() -> run handler -> report_result()
                                                        │
                            scheduler.resolve()  ──►  next wave, or finalize
```

The loop closes on itself: a worker finishing a task calls the same resolver the
trigger did, which publishes whatever that completion unblocked. No central polling
loop — the graph advances on completion events.

**Task handlers.** Register a callable per task name; anything unregistered logs and
succeeds, so a DAG's shape can be verified before the real work exists:

```python
from app.executors import register

@register("parse_resume")
def parse_resume(ctx):        # ctx: task_id, run_id, task_name, attempt, worker_id
    ...                       # raising marks the task failed
```

### Task state machine

```
pending ──► queued ──► running ──► success
   │           │                     └───► failed   (retry: -> queued)
   └───────────┴──────────────────────────► cancelled   (run cancelled; not from `running`)
```

A run is `completed` when every task succeeded, and `failed` once a task has failed
**and** no work is still in flight — tasks already dispatched are allowed to report
back first. Tasks downstream of a failure stay `pending` rather than executing.
Cancelling a run moves every `pending`/`queued` task straight to `cancelled` (see
Cancel, under Dashboard, below) — only a task already `running` is left alone,
since nothing here can reach into a worker process to stop it.

### Retries and fault tolerance

A failing task is re-queued with exponential backoff (`1s → 2s → 4s …`, plus jitter)
until `max_retries` is spent; only then is it `failed`, and a copy is parked on
`task_queue.dead` for inspection. Set `max_retries: 0` on a node to opt out.

Delays use no plugins: a retry is published to a per-tier holding queue whose
`x-message-ttl` expires it back onto the main queue. One queue per power-of-two
delay, so messages inside a tier expire in arrival order rather than head-of-line
blocking each other.

Workers heartbeat into the `workers` table. A worker that dies mid-task would
otherwise strand the row in `running` forever — the message was already acked, so
RabbitMQ will not redeliver it. Every worker periodically sweeps for tasks whose
worker has gone silent past `HEARTBEAT_TIMEOUT` and pushes them back through the
same retry path, so a task that repeatedly kills its worker still exhausts its
budget instead of looping.

The other way a task gets stuck needs no dead worker at all: the row commits
`queued`, and the broker publish meant to follow it — deliberately deferred
until after commit, so a worker can never see a task before the row that
authorises it (see `_publish_on_commit` in [app/scheduler.py](app/scheduler.py))
— fails anyway. No message, no worker ever claims it, and no heartbeat is
involved. The same periodic sweep also reclaims any task still `queued` past
`QUEUED_TIMEOUT` after it became claimable
(`recovery.reclaim_stuck_queued_tasks`), through the identical retry path — so
a task whose messages keep going missing still exhausts its budget instead of
sitting there forever. `QUEUED_TIMEOUT` defaults generously (5 minutes), since
a busy worker fleet can leave healthy work queued for a while with nothing
actually wrong.

### Parallel execution

Workers are interchangeable and stateless; scale them with
`docker compose up -d --scale worker=5`. `WORKER_PREFETCH=1` keeps one unacked
message per worker, so RabbitMQ spreads a wave across idle workers instead of
letting the first one buffer it.

Running several workers on one graph breaks three things that a single worker
hides. Each is fixed in [app/scheduler.py](app/scheduler.py), and each has a test
in [tests/test_parallel.py](tests/test_parallel.py) that fails when its mechanism
is removed:

| Race | What goes wrong | Fix |
|------|-----------------|-----|
| Two workers claim one task | Both read `queued`, both execute it | `claim()` is a compare-and-set: the status precondition lives in the UPDATE's `WHERE`, so exactly one worker matches a row |
| Sibling branches finish together | Neither sees the other's uncommitted success, so neither releases the fan-in and the run hangs at 50% | `resolve()` takes the run's row lock, so whoever commits last always reads the complete set |
| A message overtakes its own transaction | The worker reads the task as still `pending`, declines it, and the delivery is gone | Publishes are held until the transaction commits, making Postgres the only authority on what was dispatched |
| Two workers sweep one orphan | Both push it through the retry path, burning two attempts on one failure | The recovery sweep selects orphans `FOR UPDATE SKIP LOCKED` |

Duplicate delivery is therefore harmless by design — RabbitMQ promises
*at-least-once*, and the database is what turns that into exactly-once execution.

> Verified live on the compose stack: a 33-node DAG across 5 workers, every task
> claimed by exactly one worker (32 handler invocations for 32 default-handler
> tasks, no duplicates), with a flaky task still exhausting its backoff and
> succeeding on attempt 3.

---

## Scheduling

A workflow definition may carry a `schedule` — a five-field cron expression,
validated at registration with the same "reject at the door" principle as an
invalid DAG (`app/schemas.py` calls straight into APScheduler's own parser, so
there is exactly one place that knows what a valid expression looks like):

```bash
curl -X POST localhost:8000/workflows -H 'Content-Type: application/json' -d '{
  "name": "daily_report",
  "schedule": "0 9 * * *",
  "workflow": [{"id": "generate", "depends_on": []}]
}'
```

One `BackgroundScheduler` lives for the life of the API process (`app/cron.py`,
wired up in `main.py`'s `lifespan`). Every workflow with a non-null `schedule`
is armed from the database at startup — that is what makes a schedule survive
an API restart, rather than only existing in the process that registered it —
and a newly registered one is armed immediately rather than waiting for the
next restart. The job itself calls the same `create_run` + `resolve` pair a
manual `POST /trigger` does, so a scheduled run is indistinguishable from a
manual one once it exists.

---

## Dashboard

`GET /dashboard` serves a single static page (`app/static/`, no build step —
plain HTML/CSS/JS) that talks to the JSON API already documented above:

- **Workflow and run lists** — with a one-click trigger per workflow
- **Run detail** — an inline SVG DAG, laid out from `GET /workflows/{id}/graph`
  (execution levels + edges) and colored live from each task's status
- **Live updates** — `WS /ws/runs/{run_id}` polls the database server-side on a
  fixed interval and pushes the same `RunDetail` payload the REST API returns,
  until the run reaches a terminal status and the socket closes. There is no
  cross-process channel from a worker (possibly in another container) back to
  the API, so this is push-*transport*, poll-*backend* — honest about it rather
  than pretending to be a true event stream. The page falls back to plain
  polling if the socket cannot be opened.
- **Per-task logs** — the worker captures a handler's stdout/stderr for its
  latest attempt (`app/worker.py`, `GET /runs/{id}/tasks/{name}/logs`); a retry
  overwrites the previous attempt's capture rather than appending to it, since
  `error_message` already carries the history of *why* prior attempts failed
- **Cancel** — `POST /runs/{run_id}/cancel` moves every `pending`/`queued` task
  straight to `cancelled`. A `queued` task's message may still be sitting on
  the broker, but a worker's `claim()` is a compare-and-set gated on
  `status = 'queued'`, so it simply declines the stale delivery rather than
  running it. A task already `running` is the one exception — nothing here can
  reach into a worker process mid-execution — so it finishes and reports
  normally; it just no longer unblocks anything new, and a retryable failure on
  a cancelled run skips the retry path, since nothing would ever dispatch that
  requeue.

---

## Layout

```
app/
  config.py      environment configuration
  db.py          engine, session factory, declarative base
  models.py      workflows / workflow_runs / tasks / workers
  dag.py         validation, topological sort, get_runnable_tasks()
  scheduler.py   run lifecycle: create_run, resolve, dispatch, report_result, cancel_run
  broker.py      RabbitMQ queue (+ delayed retry, DLQ) and an in-process broker
  executors.py   task handler registry
  retry.py       exponential backoff with jitter
  recovery.py    worker heartbeats, orphan reclaim, lost-message reclaim
  worker.py      consume -> claim -> execute -> report -> resolve; captures stdout/stderr
  cron.py        APScheduler wiring: arm/re-arm cron-triggered runs
  main.py        FastAPI endpoints + the dashboard's WebSocket
  schemas.py     request/response models
  static/        dashboard.html / .css / .js — no build step
examples/handlers.py               handlers for every example workflow below
examples/resume_pipeline.json      fan-out then fan-in
examples/fan_out.json              one split, six parallel shards, one merge
examples/ml_pipeline.json          multi-stage pipeline, fans out at the end
examples/notification_pipeline.json fan-out/fan-in with a per-node retry override
scripts/manual_test.py   DAG resolution walkthrough (no services needed)
scripts/parallel_demo.py drives the live stack and reports the worker spread
tests/                   125 tests (unit + API + worker + retry/recovery + parallel +
                          scheduling + dashboard)
schema.sql               canonical Postgres DDL
Dockerfile               one image, run as either the API or a worker
docker-compose.yml       postgres + rabbitmq + api + 3 workers
```

---

## Project status

All 7 phases from [workflow_orchestration_engine.md](workflow_orchestration_engine.md)
are complete, including the two gaps Phases 5–6 had carried forward:

- **Lost dispatch messages are now recovered.** A publish that fails after its
  transaction commits used to leave a task `queued` with nothing watching it;
  the same periodic sweep that reclaims orphaned `running` tasks now also
  reclaims anything stuck `queued` past `QUEUED_TIMEOUT`
  (`recovery.reclaim_stuck_queued_tasks`).
- **Cancellation is a real task status**, not just a run-level flag — see the
  task state machine and Cancel, above.

One deliberate design tradeoff remains, not a gap: the dashboard's WebSocket is
poll-based on the server side (see Dashboard, above), since there is no
cross-process channel from a worker back to the API without adding a pub/sub
layer (Redis, or similar) purely for that purpose. Fine at this scale; called
out rather than disguised as a true event stream.
