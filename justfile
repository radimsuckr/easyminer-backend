docker_image_name := "easyminer-backend"

default:
	@just --list

build-docker:
	docker build -t "{{ docker_image_name }}:latest" .

[working-directory: "easyminer"]
dev:
	rye run fastapi dev

[working-directory: "easyminer"]
run:
	rye run fastapi run
