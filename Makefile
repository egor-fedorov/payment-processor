COMPOSE := docker compose
PYTHON := python3

.PHONY: help up down restart build logs ps clean \
	migrate shell-api shell-postgres install compile

help:
	@printf "Available targets:\n"
	@printf "  make up            Start the full stack in Docker Compose\n"
	@printf "  make down          Stop the stack\n"
	@printf "  make restart       Recreate the stack\n"
	@printf "  make build         Rebuild images\n"
	@printf "  make logs          Tail compose logs\n"
	@printf "  make ps            Show compose services status\n"
	@printf "  make clean         Stop the stack and remove volumes\n"
	@printf "  make migrate       Run Alembic migrations in compose\n"
	@printf "  make shell-api     Open a shell in the api container\n"
	@printf "  make shell-postgres Open psql in the postgres container\n"
	@printf "  make install       Install Python dependencies locally\n"
	@printf "  make compile       Run a quick Python syntax check\n"

up:
	$(COMPOSE) up --build

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) down
	$(COMPOSE) up --build

build:
	$(COMPOSE) build

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

clean:
	$(COMPOSE) down -v

migrate:
	$(COMPOSE) run --rm migrate

shell-api:
	$(COMPOSE) exec api sh

shell-postgres:
	$(COMPOSE) exec postgres psql -U payment -d payments

install:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install .

compile:
	$(PYTHON) -m compileall app alembic
