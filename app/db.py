"""Engine, session factory and declarative base."""

from collections.abc import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app import config


class Base(DeclarativeBase):
    pass


def _make_engine(url: str):
    kwargs = {"echo": config.SQL_ECHO, "future": True}
    if url.startswith("sqlite"):
        # Needed so a single in-memory database is shared across connections.
        kwargs["connect_args"] = {"check_same_thread": False}
    return create_engine(url, **kwargs)


engine = _make_engine(config.DATABASE_URL)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def get_session() -> Iterator[Session]:
    """FastAPI dependency: one session per request, rolled back on error."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def init_db() -> None:
    """Create tables if they do not exist. Real deployments use schema.sql."""
    from app import models  # noqa: F401  (registers mappers)

    Base.metadata.create_all(bind=engine)
