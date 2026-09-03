-- Canonical Postgres schema. app/models.py mirrors this, and
-- tests/test_api.py::test_model_ddl_carries_the_same_defaults_as_schema_sql
-- pins the two together so they cannot drift apart silently.
--
-- Known divergence: depends_on's DEFAULT '{}' below is Postgres array syntax and
-- has no SQLite equivalent, so the models omit it. Nothing relies on it — the
-- engine always writes depends_on explicitly.
--
-- Applied automatically by docker-compose (mounted into postgres initdb).

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- A workflow definition (the DAG template)
CREATE TABLE IF NOT EXISTS workflows (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    definition JSONB NOT NULL,
    schedule TEXT,              -- cron expression, e.g. '0 9 * * *'; NULL = manual only
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- A specific run of a workflow
CREATE TABLE IF NOT EXISTS workflow_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'pending',
    triggered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT ck_runs_status
        CHECK (status IN ('pending','running','completed','failed','cancelled'))
);

-- Individual task instances within a run
CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    task_name TEXT NOT NULL,
    depends_on TEXT[] NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending',
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    worker_id TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    logs TEXT,                  -- captured stdout/stderr from the latest attempt
    CONSTRAINT ck_tasks_status
        CHECK (status IN ('pending','queued','running','success','failed','cancelled')),
    CONSTRAINT uq_tasks_run_task_name UNIQUE (run_id, task_name)
);

CREATE INDEX IF NOT EXISTS idx_tasks_run_id ON tasks(run_id);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_runs_status ON workflow_runs(status);

-- Worker heartbeat registry (populated from Phase 2)
CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY,
    last_heartbeat TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'idle',
    CONSTRAINT ck_workers_status CHECK (status IN ('idle','busy'))
);
