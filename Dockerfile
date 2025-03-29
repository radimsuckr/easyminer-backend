FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY . ./

RUN uv sync

CMD ["uv", "run", "uvicorn", "easyminer.app:app", "--host", "0.0.0.0", "--port", "8000"]
