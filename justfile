docker_image_name := "easyminer-backend"

[default]
list:
	@just --list

build-docker:
	docker build -t "{{ docker_image_name }}:latest" --target api .
	docker build -t "{{ docker_image_name }}-worker:latest" --target worker .

dev:
	uv run uvicorn easyminer.app:app --reload --log-level debug

run:
	uv run uvicorn easyminer.app:app --log-level info

[working-directory: "tools"]
fake_server:
	uv run python fake_server.py

test:
	uv run pytest tests/

celery:
	uv run celery -A easyminer.worker worker -l INFO -O fair

reinit-db:
	docker compose kill mariadb
	docker compose rm -f mariadb
	docker compose up -d mariadb

	rm -r easyminer/alembic/versions/* || true

	# wait for mariadb to be up
	sleep 2

	uv run alembic upgrade head
	uv run alembic revision --autogenerate -m "init"

migrate-head:
	uv run alembic upgrade head
