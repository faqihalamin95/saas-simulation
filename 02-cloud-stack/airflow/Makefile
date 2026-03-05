AIRFLOW_DIR := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

.PHONY: up down restart status init logs

## Start Airflow
up:
	docker compose -f $(AIRFLOW_DIR)/docker-compose.yml up -d
	@echo "Airflow running at http://localhost:8080"

## Stop Airflow
down:
	docker compose -f $(AIRFLOW_DIR)/docker-compose.yml down

## Restart Airflow
restart:
	docker compose -f $(AIRFLOW_DIR)/docker-compose.yml down
	docker compose -f $(AIRFLOW_DIR)/docker-compose.yml up -d

## First time setup — init DB and create admin user
init:
	docker compose -f $(AIRFLOW_DIR)/docker-compose.yml up airflow-init
	docker compose -f $(AIRFLOW_DIR)/docker-compose.yml up -d
	@echo "Airflow initialized. Open http://localhost:8080 (admin/admin)"

## Check running containers
status:
	docker compose -f $(AIRFLOW_DIR)/docker-compose.yml ps

## Tail logs
logs:
	docker compose -f $(AIRFLOW_DIR)/docker-compose.yml logs -f