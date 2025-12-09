FROM python:3.11-slim
WORKDIR /app

COPY pyproject.toml .
COPY backend backend

RUN pip install --no-cache-dir fastapi uvicorn psycopg2-binary SQLAlchemy pydantic pandas pybaseball

EXPOSE 8000

CMD ["uvicorn", "backend.app.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
