docker_image_name := "easyminer-backend"

default:
	@just --list

build-docker:
	docker build -t "{{ docker_image_name }}:latest" .

[working-directory: "easyminer"]
dev:
	uv run fastapi dev

[working-directory: "easyminer"]
run:
	uv run fastapi run

[working-directory: "tools"]
fake_server:
	uv run python fake_server.py

test:
	uv run pytest

celery:
	uv run celery -A easyminer.worker worker -l INFO -O fair

reinit-db:
	docker compose kill postgres
	docker compose rm -f postgres
	docker compose up -d postgres

	rm -r easyminer/alembic/versions/*

	# check if postgres is up
	@while ! docker compose exec postgres pg_isready -U postgres; do sleep 0.25s; done

	uv run alembic upgrade head
	uv run alembic revision --autogenerate -m "init"

migrate-head:
	uv run alembic upgrade head
