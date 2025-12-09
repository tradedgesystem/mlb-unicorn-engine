ENV_FILE ?=.env.example

include $(ENV_FILE)
export $(shell sed -n 's/^\([A-Za-z0-9_]*\)=.*/\1/p' $(ENV_FILE))

.PHONY: install migrate downgrade seed-patterns etl-backfill-2025 run-unicorns api test

install:
	pip install --no-cache-dir .

migrate:
	alembic upgrade head

downgrade:
	alembic downgrade -1

seed-patterns:
	python -m backend.app.unicorns.seed_patterns

etl-backfill-2025:
	python -m backend.app.etl.backfill

run-unicorns:
	python -m backend.app.unicorns.engine --date $${DATE:-$$(date +"%Y-%m-%d")}

api:
	uvicorn backend.app.main:app --reload --host $${UVICORN_HOST:-0.0.0.0} --port $${UVICORN_PORT:-8000}

test:
	pytest
