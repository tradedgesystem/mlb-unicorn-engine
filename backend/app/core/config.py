import os
from functools import lru_cache


class Settings:
    def __init__(self) -> None:
        self.app_env: str = os.getenv("APP_ENV", "development")
        self.database_url: str = os.getenv(
            "DATABASE_URL",
            "postgresql+psycopg2://postgres:postgres@localhost:5432/postgres",
        )
        self.alembic_config: str = os.getenv("ALEMBIC_CONFIG", "alembic.ini")
        self.uvicorn_host: str = os.getenv("UVICORN_HOST", "0.0.0.0")
        self.uvicorn_port: int = int(os.getenv("UVICORN_PORT", "8000"))
        self.log_level: str = os.getenv("LOG_LEVEL", "INFO")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
