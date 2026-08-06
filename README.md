# AutoBot (Workflow Orchestration Engine)

A mini Apache Airflow: define a workflow as a DAG once, and the engine works out what
runs first, what runs in parallel, and when the run is complete.

Full project plan and roadmap: [workflow_orchestration_engine.md](workflow_orchestration_engine.md)

**Status: Phases 1–3 complete.** The DAG engine resolves dependencies, real workers
execute tasks over RabbitMQ, and failures retry with exponential backoff before
landing in a dead letter queue. Tasks orphaned by a dead worker are reclaimed.
Parallel execution across multiple workers is Phase 4.

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
- **74 passing tests**, including real-RabbitMQ round trips, the delayed-retry
  round trip, and the fan-out/fan-in execution order

---

## Quick start

```bash
pip install -r requirements.txt

# 1. See the DAG resolve, no database or broker needed (in-memory SQLite)
python scripts/manual_test.py
python scripts/manual_test.py examples/resume_pipeline.json

# 2. Run the tests against real Postgres (the default)
cp .env.example .env
docker compose up -d postgres rabbitmq
python -m pytest -q

# 3. Run the stack: API in one shell, worker in another
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

`/cancel` and `/dashboard` from the plan arrive with Phases 3 and 6.

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

> Verified on both backends: 35/35 against a live Postgres 16 (the default) and
> 35/35 on SQLite.

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
scripts/manual_test.py   DAG resolution walkthrough (no services needed)
tests/                   74 tests (unit + API + worker + retry/recovery)
schema.sql               canonical Postgres DDL
docker-compose.yml       postgres + rabbitmq
```

---

## Next: Phase 4 — parallel execution

1. Run 3+ workers simultaneously via Docker Compose
2. Test a fan-out DAG across real concurrent workers
3. **Atomic task claim** — the known gap. A duplicate *delivery* is handled (the
   worker re-reads status and skips anything not `queued`), but two workers
   claiming the same task simultaneously could still both execute it. Needs
   `SELECT … FOR UPDATE SKIP LOCKED` in `Worker.handle`.
