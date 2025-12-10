from datetime import date

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from backend.app.db import models
from backend.app.db.base import Base
from backend.app.db.session import SessionLocal, engine
from backend.app.unicorns.queries import fetch_top50_for_date

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def init_db() -> None:
    # Ensure core tables exist (useful for fresh Render/Postgres instances).
    Base.metadata.create_all(bind=engine)
    _seed_sample_top50()


@app.get("/")
def root():
    return {"status": "ok", "message": "MLB Unicorn Engine API is running"}


def to_dict(row):
    return {
        "run_date": str(row.run_date),
        "rank": row.rank,
        "entity_type": row.entity_type,
        "entity_id": row.entity_id,
        "pattern_id": row.pattern_id,
        "metric_value": float(row.metric_value) if row.metric_value is not None else None,
        "sample_size": row.sample_size,
        "score": float(row.score) if row.score is not None else None,
        "description": row.description,
    }


@app.get("/top50/{run_date}")
def get_top50(run_date: date):
    session = SessionLocal()
    try:
        rows = fetch_top50_for_date(session, run_date)
        return [to_dict(r) for r in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        session.close()


def _seed_sample_top50() -> None:
    """Insert a demo Top 50 row so the endpoint returns data on a fresh database."""
    session = SessionLocal()
    try:
        existing = session.query(models.UnicornTop50Daily).count()
        if existing:
            return
        sample_date = date(2025, 3, 27)
        sample_row = models.UnicornTop50Daily(
            run_date=sample_date,
            rank=1,
            entity_type="player",
            entity_id=123456,
            pattern_id="DEMO-001",
            metric_value=1.234,
            sample_size=50,
            score=2.5,
            description="Sample unicorn for demo purposes.",
        )
        session.add(sample_row)
        session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()
