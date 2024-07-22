docker_image_name := "easyminer-backend"

default:
	just --list

build-docker:
	docker build -t "{{ docker_image_name }}:latest" .
