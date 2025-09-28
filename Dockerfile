FROM python:3.13-slim AS build

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN apt update && apt upgrade -y && apt install -y --no-install-recommends libpq5

RUN groupadd --gid 1000 easyminer && useradd -m -s /usr/sbin/nologin --uid 1000 -g easyminer easyminer

USER 1000:1000

WORKDIR /app

COPY . ./

RUN uv sync --frozen


FROM build AS api

CMD ["uv", "run", "uvicorn", "easyminer.app:app", "--host", "0.0.0.0", "--port", "8000"]


FROM build AS worker

CMD ["uv", "run", "celery", "-A", "easyminer.worker", "worker", "-l", "info", "-O", "fair"]
