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
