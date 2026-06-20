# Easyminer Backend

REST API backend for the [EasyMiner](https://easyminer.eu) data mining platform. It is orchestrated by EasyMiner Center, which provides user authentication and per-user database configuration. The backend handles CSV data ingestion, preprocessing, and association rule mining, with long-running operations offloaded to a Celery task queue.

**Stack:**

- FastAPI
- Celery
- MariaDB
- Redis
- SQLAlchemy 2.0
- Alembic

## Docker deployment

The full stack (API, Celery worker, MariaDB, Redis) can be run with Docker Compose.

1. Copy the example env file and adjust hostnames for the container network:

    ```sh
    cp .env.example .env
    ```

    In `.env`, replace `localhost` with the Docker Compose service names:

    | Variable | Local value | Docker value |
    |---|---|---|
    | `DATABASE_URL` | `…@localhost:3306/…` | `…@mariadb:3306/…` |
    | `DATABASE_URL_SYNC` | `…@localhost:3306/…` | `…@mariadb:3306/…` |
    | `CELERY_BROKER` | `redis://localhost/0` | `redis://redis/0` |
    | `CELERY_BACKEND` | `redis://localhost/1` | `redis://redis/1` |
    | `EASYMINER_CENTER_URL` | `http://localhost:8001` | `http://localhost:8001` |

2. Start all services:

    ```sh
    docker compose up -d
    ```

    Add `--build` to build the images locally instead of pulling them.

3. The API is available at `http://localhost:8000`. Database migrations run automatically on startup.

## Development setup

### Prerequisites

- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- [Just](https://just.systems/man/en/)
- [watchmedo](https://github.com/gorakhargosh/watchdog) for Celery hot-reload: `uv tool install 'watchdog[watchmedo]'`

### Setup

```sh
cp .env.example .env     # default values work as-is for local dev
uv sync                  # install dependencies
```

### Running

Start MariaDB and Redis (recommended: via Docker, but any local install works):

```sh
just docker-services
```

Then launch the services you need:

```sh
just dev          # API with hot reload and debug logging (http://localhost:8000)
just celery       # Celery worker with auto-reload via watchmedo
just fake_server  # EasyMiner Center mock — provides auth and DB config for a fake user
```

`fake_server` replaces the need for a real EasyMiner Center instance during development. It runs on port 8001, which matches the default value of `EASYMINER_CENTER_URL`.

### Tests

```sh
just test
```
