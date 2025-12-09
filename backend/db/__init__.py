"""Compatibility shim to expose db modules at backend.db.*."""
from backend.db.base import Base
from backend.db.session import SessionLocal, engine, get_db
from backend.db import models

__all__ = ["Base", "SessionLocal", "engine", "get_db", "models"]
