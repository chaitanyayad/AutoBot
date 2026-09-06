"""Runtime configuration, read from the environment."""

import os

from dotenv import load_dotenv

load_dotenv()

# Postgres is the production target. Tests override this with a SQLite URL so the
# suite runs without a database server; the models are written to work on both.
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://orchestrator:orchestrator@localhost:5432/orchestrator",
)

# Default retry budget stamped onto every task instance at trigger time.
DEFAULT_MAX_RETRIES = int(os.getenv("DEFAULT_MAX_RETRIES", "3"))


def _flag(name: str, default: str = "") -> bool:
    return os.getenv(name, default).lower() in {"1", "true", "yes"}


SQL_ECHO = _flag("SQL_ECHO")

# --- queue ------------------------------------------------------------------

# "memory://" runs the broker in-process — used by the test suite so the full
# dispatch/consume path can be exercised without a running RabbitMQ.
BROKER_URL = os.getenv("BROKER_URL", "amqp://guest:guest@localhost:5672/")
TASK_QUEUE = os.getenv("TASK_QUEUE", "task_queue")

# How many unacked messages a worker will hold. 1 keeps distribution fair across
# workers rather than letting one greedily buffer the queue.
WORKER_PREFETCH = int(os.getenv("WORKER_PREFETCH", "1"))

DEAD_LETTER_QUEUE = os.getenv("DEAD_LETTER_QUEUE", "task_queue.dead")

# Comma-separated modules a worker imports at startup so their @register("...")
# handlers are known. Without this a standalone worker only has default_handler.
WORKER_HANDLERS = [
    m.strip() for m in os.getenv("WORKER_HANDLERS", "").split(",") if m.strip()
]

# Seconds the stand-in handler sleeps, so parallelism across workers is visible
# rather than finishing faster than the dispatch round trip. `--task-duration`
# overrides it.
TASK_DURATION = float(os.getenv("TASK_DURATION", "0.0"))

# --- retry ------------------------------------------------------------------

# delay = RETRY_BASE_DELAY * 2 ** (attempt - 1), capped, plus jitter.
RETRY_BASE_DELAY = float(os.getenv("RETRY_BASE_DELAY", "1.0"))
RETRY_MAX_DELAY = float(os.getenv("RETRY_MAX_DELAY", "60.0"))
# Fraction of the delay applied as random jitter, so a burst of tasks failing
# together does not retry in lockstep.
RETRY_JITTER = float(os.getenv("RETRY_JITTER", "0.1"))

# --- fault tolerance --------------------------------------------------------

# How often a worker updates workers.last_heartbeat.
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "5.0"))
# A worker silent for longer than this is presumed dead and its running tasks
# are reclaimed. Must be comfortably larger than HEARTBEAT_INTERVAL.
HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", "30.0"))
# How often each worker sweeps for tasks orphaned by a dead worker.
RECOVERY_INTERVAL = float(os.getenv("RECOVERY_INTERVAL", "15.0"))
# A task still `queued` this long after becoming claimable is presumed to have
# lost its message (the one gap a healthy worker fleet cannot self-heal: a
# publish that fails *after* its authorising transaction commits — see
# `_publish_on_commit` in scheduler.py). Deliberately generous, since a busy
# fleet can leave healthy work queued for a while with nothing wrong at all.
QUEUED_TIMEOUT = float(os.getenv("QUEUED_TIMEOUT", "300.0"))

# --- development --------------------------------------------------------------

# The Phase 1 stand-in for a worker. Real workers exist as of Phase 2, so this is
# off unless explicitly enabled; it mutates run state without authentication.
ENABLE_SIMULATE_ENDPOINT = _flag("ENABLE_SIMULATE_ENDPOINT")

# --- scheduling (Phase 5) ----------------------------------------------------

# Cron expressions on a workflow definition are interpreted in this timezone.
SCHEDULER_TIMEZONE = os.getenv("SCHEDULER_TIMEZONE", "UTC")
# Turned off in most tests (see conftest) so a background thread isn't started
# for every test that spins up the app; scheduling tests opt back in.
ENABLE_SCHEDULER = _flag("ENABLE_SCHEDULER", "true")

# --- dashboard (Phase 6) -----------------------------------------------------

# How often the dashboard's WebSocket re-polls a run and pushes an update.
DASHBOARD_POLL_INTERVAL = float(os.getenv("DASHBOARD_POLL_INTERVAL", "1.0"))
# Captured stdout/stderr per task attempt is truncated to this many characters
# so a runaway handler can't grow a row without bound.
MAX_LOG_CHARS = int(os.getenv("MAX_LOG_CHARS", "20000"))
