COMPOSE := docker compose
UV := uv
PYTHON := $(UV) run --locked python
TEST_COMPOSE := $(COMPOSE) -p payment-processor-tests -f docker-compose.test.yml

.PHONY: help install up down build logs ps migrate format lint typecheck architecture check test test-integration test-containers test-all test-down benchmark

help:
	@printf '%s\n' \
	  'make install          Install locked runtime and development dependencies' \
	  'make up               Build and start the application stack' \
	  'make down             Stop the application stack, preserving volumes' \
	  'make format           Apply Ruff lint fixes and formatting' \
	  'make check            Check lockfile, lint, formatting, types, architecture and unit tests' \
	  'make architecture     Check module boundaries and dependency cycles' \
	  'make test-integration Run tests with isolated PostgreSQL/RabbitMQ namespaces' \
	  'make test-containers  Test restarts, persistence and worker SIGKILL recovery' \
	  'make test-all         Run all tests with a 90% branch coverage gate' \
	  'make test-down        Stop the dedicated test containers' \
	  'make benchmark        Compare 1 and 2 consumers (build image first)'

install:
	$(UV) sync --locked

up:
	$(COMPOSE) up --build -d --wait

down:
	$(COMPOSE) down

build:
	docker build -t payment-processor:local .

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

migrate:
	$(COMPOSE) run --rm migrate

format:
	$(UV) run --locked ruff check --fix .
	$(UV) run --locked ruff format .

lint:
	$(UV) run --locked ruff check .
	$(UV) run --locked ruff format --check .

typecheck:
	$(UV) run --locked mypy

architecture:
	$(UV) run --locked lint-imports

check:
	$(UV) lock --check
	$(MAKE) lint typecheck architecture test

test:
	$(PYTHON) -m pytest -m 'not integration'

test-integration:
	$(TEST_COMPOSE) up -d --wait
	RUN_INTEGRATION=1 $(PYTHON) -m pytest -m 'not containers'

test-containers:
	$(TEST_COMPOSE) up -d --wait
	RUN_INTEGRATION=1 RUN_CONTAINER_TESTS=1 $(PYTHON) -m pytest -m containers

test-all:
	$(TEST_COMPOSE) up -d --wait
	RUN_INTEGRATION=1 RUN_CONTAINER_TESTS=1 $(PYTHON) -m pytest --cov=app --cov-report=term-missing --cov-report=xml

test-down:
	$(TEST_COMPOSE) down

benchmark:
	$(PYTHON) benchmarks/run.py
