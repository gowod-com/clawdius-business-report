"""Database initialization and session management."""
import re
import logging
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from storage.models import Base
import config

logger = logging.getLogger(__name__)


def _resolve_db_url(url: str) -> str:
    """Convert sqlite:///relative/path to absolute path."""
    match = re.match(r"sqlite:///(.+)", url)
    if match:
        rel_path = match.group(1)
        if not Path(rel_path).is_absolute():
            abs_path = Path(config.__file__).parent / rel_path
            abs_path.parent.mkdir(parents=True, exist_ok=True)
            return f"sqlite:///{abs_path}"
    return url


_engine = None
_SessionLocal = None


def get_engine():
    global _engine
    if _engine is None:
        db_url = _resolve_db_url(config.DATABASE_URL)
        logger.info(f"Connecting to database: {db_url}")
        _engine = create_engine(db_url, connect_args={"check_same_thread": False})
        Base.metadata.create_all(_engine)
    return _engine


def get_session() -> Session:
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(bind=get_engine(), autoflush=False, autocommit=False)
    return _SessionLocal()


def init_db():
    """Ensure all tables exist."""
    engine = get_engine()
    Base.metadata.create_all(engine)
    logger.info("Database initialized.")
