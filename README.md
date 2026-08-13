# AutoBot (Workflow Orchestration Engine)

A mini Apache Airflow: define a workflow as a DAG once, and the engine works out what
runs first, what runs in parallel, and when the run is complete.

Full project plan and roadmap: [workflow_orchestration_engine.md](workflow_orchestration_engine.md)

**Status: Phases 1–4 complete.** The DAG engine resolves dependencies, a pool of
workers executes tasks over RabbitMQ in parallel, and failures retry with
exponential backoff before landing in a dead letter queue. Tasks orphaned by a
dead worker are reclaimed, and no task can ever be executed twice.

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
- **Fault tolerance** — worker heartbeats and recovery of tasks orphaned by a dead
  worker ([app/recovery.py](app/recovery.py))
- **Parallel execution** — a pool of workers on one run, with an atomic task claim
  and a per-run scheduler lock ([app/scheduler.py](app/scheduler.py))
- **Docker Compose** — postgres, rabbitmq, the API and three workers, one command
  ([docker-compose.yml](docker-compose.yml))
- **88 passing tests**, including real-RabbitMQ round trips, the delayed-retry
  round trip, and concurrency tests that each fail if their mechanism is removed

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

## API

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/workflows` | Register a definition (validated as a DAG) |
| `GET` | `/workflows` · `/workflows/{id}` | List / fetch definitions |
| `POST` | `/workflows/{id}/trigger` | Start a run, materialise tasks, queue the roots |
| `GET` | `/runs` · `/runs/{run_id}` | Run status, task states, `runnable_now` |
| `GET` | `/runs/{run_id}/tasks` | Task-level breakdown |
| `POST` | `/runs/{run_id}/tasks/{name}/simulate` | Dev only, **off by default** (`ENABLE_SIMULATE_ENDPOINT`) |
| `GET` | `/health` | Liveness |

`/cancel` and `/dashboard` from the plan arrive with the dashboard in Phase 6.

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
                          └──────► failed   (retry: -> queued, Phase 3)
```

A run is `completed` when every task succeeded, and `failed` once a task has failed
**and** no work is still in flight — tasks already dispatched are allowed to report
back first. Tasks downstream of a failure stay `pending` rather than executing.

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

## Layout

```
app/
  config.py      environment configuration
  db.py          engine, session factory, declarative base
  models.py      workflows / workflow_runs / tasks / workers
  dag.py         validation, topological sort, get_runnable_tasks()
  scheduler.py   run lifecycle: create_run, resolve, dispatch, report_result
  broker.py      RabbitMQ queue (+ delayed retry, DLQ) and an in-process broker
  executors.py   task handler registry
  retry.py       exponential backoff with jitter
  recovery.py    worker heartbeats + reclaiming orphaned tasks
  worker.py      consume -> claim -> execute -> report -> resolve
  main.py        FastAPI endpoints
  schemas.py     request/response models
examples/handlers.py     sample handlers (flaky, doomed, slow)
examples/fan_out.json    six parallel shards between a split and a merge
scripts/manual_test.py   DAG resolution walkthrough (no services needed)
scripts/parallel_demo.py drives the live stack and reports the worker spread
tests/                   88 tests (unit + API + worker + retry/recovery + parallel)
schema.sql               canonical Postgres DDL
Dockerfile               one image, run as either the API or a worker
docker-compose.yml       postgres + rabbitmq + api + 3 workers
```

---

## Next: Phase 5 — scheduling

1. Cron-style triggers on a workflow definition (`"schedule": "0 9 * * *"`)
2. APScheduler inside the API process
3. Persist scheduled workflows and auto-trigger them

Known gap carried forward: a publish that fails *after* its transaction commits
leaves a task `queued` with no message on the queue. It is visible in the table,
but nothing sweeps for it yet — the recovery pass only reclaims `running` tasks
whose worker died.
