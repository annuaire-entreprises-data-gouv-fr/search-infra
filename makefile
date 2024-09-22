# Makefile for Docker Compose operations

# Variables
COMPOSE_FILE := docker-compose.yml

# Default target
.PHONY: help
help:
	@echo "Available commands:"
	@echo "  make up      - Start Docker Compose services"
	@echo "  make down    - Stop Docker Compose services"
	@echo "  make logs    - View logs of Docker Compose services"

# Start Docker Compose services
.PHONY: up
up:
	docker-compose -f $(COMPOSE_FILE) up -d --build

# Stop Docker Compose services
.PHONY: down
down:
	docker-compose -f $(COMPOSE_FILE) down -v --remove-orphans

# View logs of Docker Compose services
.PHONY: logs
logs:
	docker-compose -f $(COMPOSE_FILE) logs -f

# Reload Docker Compose services
.PHONY: reload
reload:
	make down
	make up
