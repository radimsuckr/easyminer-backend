# Easyminer-backend

## Quickstart

1. Install [uv](https://docs.astral.sh/uv/getting-started/installation/)
2. Install [Just](https://just.systems/man/en/)
3. `uv sync`
4. `source .venv/bin/activate`
5. `cp .env.example .env`
    - Update `.env` variables if applicable
7. `docker compose up -d`
8. Launch:
    - `just dev` for API
    - `just celery` for Celery worker
    - `just fake_server` for EasyMinerCenter API mock

## Celery auto-reload

Hot reloading for Celery is done via [watchdog](https://github.com/gorakhargosh/watchdog). Install with `uv`: `uv tool install 'watchdog[watchmedo]'`.
