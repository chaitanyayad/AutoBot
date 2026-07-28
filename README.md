# AutoBot (Workflow Orchestration Engine)

A mini Apache Airflow: define a workflow as a DAG once, and the engine works out what
runs first, what runs in parallel, and when the run is complete.

Full project plan and roadmap: [workflow_orchestration_engine.md](workflow_orchestration_engine.md)

**Status: Phase 1 complete — core DAG engine.** Workers, RabbitMQ and retries land in
Phases 2–3; the seams for them are already in place.

---

## What works today

- **Schema** — `workflows`, `workflow_runs`, `tasks`, `workers` ([schema.sql](schema.sql),
  mirrored by [app/models.py](app/models.py))
- **Validation** — duplicate ids, dangling dependencies, self-edges and cycles are
  rejected at registration time, so an unrunnable workflow can never be triggered
- **DAG resolver** — `get_runnable_tasks()` plus Kahn topological sort
  ([app/dag.py](app/dag.py))
- **API** — register, trigger, inspect ([app/main.py](app/main.py))
- **35 passing tests**, including the fan-out/fan-in resolution order

---

## Quick start

```bash
pip install -r requirements.txt

# 1. See the DAG resolve, no database or broker needed (in-memory SQLite)
python scripts/manual_test.py
python scripts/manual_test.py examples/resume_pipeline.json

# 2. Run the tests
python -m pytest -q

# 3. Run the API against Postgres
docker compose up -d postgres
cp .env.example .env
uvicorn app.main:app --reload      # docs at http://localhost:8000/docs
```

`DATABASE_URL` selects the backend. Postgres is the target (JSONB + `TEXT[]`); the
models declare portable type variants so the suite also runs on SQLite.

---

## API

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/workflows` | Register a definition (validated as a DAG) |
| `GET` | `/workflows` · `/workflows/{id}` | List / fetch definitions |
| `POST` | `/workflows/{id}/trigger` | Start a run, materialise tasks, queue the roots |
| `GET` | `/runs` · `/runs/{run_id}` | Run status, task states, `runnable_now` |
| `GET` | `/runs/{run_id}/tasks` | Task-level breakdown |
| `POST` | `/runs/{run_id}/tasks/{name}/simulate` | **Phase 1 only** — stands in for a worker |
| `GET` | `/health` | Liveness |

`/cancel` and `/dashboard` from the plan arrive with Phases 2 and 6.

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

task reports back  ──►  scheduler.resolve()  ──►  next wave, or finalize the run
```

`scheduler.dispatch()` currently just marks tasks `queued`. Phase 2 turns that one
function into a RabbitMQ publish and adds the worker on the other end — the resolver
and the state machine do not change.

### Task state machine

```
pending ──► queued ──► running ──► success
                          └──────► failed   (retry: -> queued, Phase 3)
```

A run is `completed` when every task succeeded, and `failed` once a task has failed
**and** no work is still in flight — tasks already dispatched are allowed to report
back first. Tasks downstream of a failure stay `pending` rather than executing.

> Tested on SQLite (the suite) and statically verified to emit `JSONB`/`TEXT[]` on
> the Postgres dialect. An end-to-end run against a live Postgres is still outstanding.

---

## Layout

```
app/
  config.py      environment configuration
  db.py          engine, session factory, declarative base
  models.py      workflows / workflow_runs / tasks / workers
  dag.py         validation, topological sort, get_runnable_tasks()
  scheduler.py   run lifecycle: create_run, resolve, dispatch, finalize
  main.py        FastAPI endpoints
  schemas.py     request/response models
scripts/manual_test.py   Phase 1 acceptance walkthrough
tests/                   35 tests (unit + API)
schema.sql               canonical Postgres DDL
docker-compose.yml       postgres + rabbitmq
```

---

## Next: Phase 2 — worker system

1. `dispatch()` publishes to RabbitMQ instead of only marking `queued`
2. Worker process consumes, executes, and reports status back
3. Worker calls `scheduler.resolve()` on completion to release the next wave
4. End-to-end test of a linear workflow across a real broker
