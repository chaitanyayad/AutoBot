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

SQL_ECHO = os.getenv("SQL_ECHO", "").lower() in {"1", "true", "yes"}
