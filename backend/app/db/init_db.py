from backend.app.db.base import Base
from backend.app.db.session import engine


def init_db() -> None:
    """Create tables in the configured database (useful for local dev without migrations)."""
    Base.metadata.create_all(bind=engine)
