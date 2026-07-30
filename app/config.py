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

# --- development --------------------------------------------------------------

# The Phase 1 stand-in for a worker. Real workers exist as of Phase 2, so this is
# off unless explicitly enabled; it mutates run state without authentication.
ENABLE_SIMULATE_ENDPOINT = _flag("ENABLE_SIMULATE_ENDPOINT")
