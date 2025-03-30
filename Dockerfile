FROM python:3.13-slim AS build

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY . ./

RUN uv sync


FROM build AS api

CMD ["uv", "run", "uvicorn", "easyminer.app:app", "--host", "0.0.0.0", "--port", "8000"]


FROM build AS worker

CMD ["uv", "run", "celery", "-A", "easyminer.worker", "worker", "-l", "info", "-O", "fair"]
