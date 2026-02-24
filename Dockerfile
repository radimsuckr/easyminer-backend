FROM python:3.13-slim-trixie AS build

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt update && apt upgrade -y && apt install -y --no-install-recommends gcc git libc-dev

RUN groupadd --gid 1000 easyminer && useradd -m -s /usr/sbin/nologin --uid 1000 -g easyminer easyminer

USER 1000:1000

WORKDIR /app

COPY --chown=1000:1000 pyproject.toml uv.lock ./

RUN --mount=type=cache,target=/home/easyminer/.cache/uv,uid=1000,gid=1000 uv sync --frozen --no-install-project

COPY --chown=1000:1000 . ./

RUN --mount=type=cache,target=/home/easyminer/.cache/uv,uid=1000,gid=1000 uv sync --frozen


FROM build AS api

CMD ["uv", "run", "uvicorn", "easyminer.app:app", "--host", "0.0.0.0", "--port", "8000"]


FROM build AS worker

CMD ["uv", "run", "celery", "-A", "easyminer.worker", "worker", "-l", "info", "-O", "fair"]
