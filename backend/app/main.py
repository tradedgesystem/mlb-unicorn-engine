from fastapi import FastAPI

from backend.app.core.config import settings
from backend.app.core.logging import configure_logging

configure_logging(settings.log_level)

app = FastAPI(title="MLB Unicorn Engine", version="0.1.0")


@app.get("/health")
def healthcheck():
    return {"status": "ok", "env": settings.app_env}
