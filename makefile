DOCKER_IMAGE = airflow-2.10.4-de-challenge
DOCKER_TAG = latest

all: build run

build:
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

run:
	docker compose up

clean:
	docker compose down
	docker image rm $(DOCKER_IMAGE):$(DOCKER_TAG)
