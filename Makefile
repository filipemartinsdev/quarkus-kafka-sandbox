.PHONY: up down build

build:
	podman build -t producer:latest ./producer
	podman build -t consumer:latest ./consumer

up: build
	podman play kube kube.yaml

down:
	podman play kube --down kube.yaml
