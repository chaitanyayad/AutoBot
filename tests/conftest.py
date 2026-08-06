"""Test fixtures: a throwaway database per test, and a TestClient bound to it.

Runs against **Postgres by default** — the database this engine actually targets,
so JSONB, TEXT[] and the CHECK constraints are exercised for real:

    docker compose up -d postgres
    python -m pytest -q

SQLite remains available for a quick serviceless run, at the cost of fidelity:

    TEST_DATABASE_URL=sqlite+pysqlite:///:memory: python -m pytest -q
"""

import os

from dotenv import load_dotenv

load_dotenv()

SQLITE_URL = "sqlite+pysqlite:///:memory:"
POSTGRES_URL = "postgresql+psycopg://orchestrator:orchestrator@localhost:5432/orchestrator"


def _test_database_url() -> str:
    """Pick a database for the suite, never the application's own.

    The fixtures TRUNCATE between tests, so pointing them at DATABASE_URL would
    destroy real data on every run. The app's URL is therefore only used to
    locate the *server*; the suite always works in a separate `<name>_test`
    database, created on demand.
    """
    explicit = os.getenv("TEST_DATABASE_URL")
    if explicit:
        return explicit

    app_url = os.getenv("DATABASE_URL") or POSTGRES_URL
    if app_url.startswith("sqlite"):
        return app_url

    base, _, name = app_url.rpartition("/")
    name = name.split("?")[0]
    return f"{base}/{name}_test" if not name.endswith("_test") else app_url


TEST_DATABASE_URL = _test_database_url()
IS_SQLITE = TEST_DATABASE_URL.startswith("sqlite")

os.environ.setdefault("DATABASE_URL", TEST_DATABASE_URL)

# Default to the in-process broker so the full publish/consume path runs without
# RabbitMQ. tests/test_worker.py opts into a real broker when one is reachable.
TEST_BROKER_URL = os.getenv("TEST_BROKER_URL", "memory://")
os.environ["BROKER_URL"] = TEST_BROKER_URL

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import create_engine, text  # noqa: E402
from sqlalchemy.exc import OperationalError  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

from app import broker as broker_module  # noqa: E402
from app import config  # noqa: E402
from app import db as db_module  # noqa: E402
from app import executors  # noqa: E402
from app.broker import InMemoryBroker  # noqa: E402
from app.db import Base, get_session  # noqa: E402
from app.main import app  # noqa: E402
from app import models  # noqa: E402,F401  (registers mappers)


TABLES = ("tasks", "workflow_runs", "workflows", "workers")


@pytest.fixture(autouse=True)
def _isolated_broker():
    """A fresh in-process broker per test, and no sleeping in the default handler."""
    broker = InMemoryBroker()
    broker_module.set_broker(broker)
    executors.set_default_duration(0.0)
    yield broker
    broker_module.set_broker(None)
    executors.clear_registry()


@pytest.fixture()
def broker(_isolated_broker):
    return _isolated_broker


@pytest.fixture(autouse=True)
def _enable_simulate_endpoint(monkeypatch):
    """The dev endpoint is off by default; the Phase 1 API tests drive it directly."""
    monkeypatch.setattr(config, "ENABLE_SIMULATE_ENDPOINT", True)


def _ensure_test_database() -> None:
    """Create the `_test` database if it does not exist yet."""
    import psycopg

    base, _, name = TEST_DATABASE_URL.rpartition("/")
    admin_dsn = f"{base}/postgres".replace("postgresql+psycopg://", "postgresql://")
    with psycopg.connect(admin_dsn, autocommit=True, connect_timeout=5) as conn:
        exists = conn.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (name,)
        ).fetchone()
        if not exists:
            conn.execute(f'CREATE DATABASE "{name}"')


@pytest.fixture(scope="session")
def _postgres_engine():
    """One engine and one schema for the whole session; tables are emptied per test."""
    try:
        _ensure_test_database()
    except Exception:
        pass  # fall through to the connection check below for a clear message

    eng = create_engine(TEST_DATABASE_URL, future=True)
    try:
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
    except OperationalError as exc:
        pytest.exit(
            f"cannot reach Postgres at {TEST_DATABASE_URL}\n"
            "start it with:  docker compose up -d postgres\n"
            "or run serviceless with:  "
            "TEST_DATABASE_URL=sqlite+pysqlite:///:memory: python -m pytest\n"
            f"({exc.orig})",
            returncode=1,
        )
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture()
def engine(request):
    if IS_SQLITE:
        eng = create_engine(
            SQLITE_URL,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,  # one shared connection => one shared in-memory db
        )
        Base.metadata.create_all(eng)
        yield eng
        eng.dispose()
        return

    eng = request.getfixturevalue("_postgres_engine")
    with eng.begin() as conn:
        conn.execute(text(f"TRUNCATE {', '.join(TABLES)} RESTART IDENTITY CASCADE"))
    yield eng


@pytest.fixture()
def session_factory(engine):
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


@pytest.fixture()
def session(session_factory):
    s = session_factory()
    yield s
    s.close()


@pytest.fixture()
def client(engine, session_factory, monkeypatch):
    # init_db() runs on startup against the module engine; point it at ours.
    monkeypatch.setattr(db_module, "engine", engine)
    monkeypatch.setattr(db_module, "SessionLocal", session_factory)

    def override_get_session():
        s = session_factory()
        try:
            yield s
        finally:
            s.close()

    app.dependency_overrides[get_session] = override_get_session
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()
