import asyncio
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import psycopg
import pytest
from psycopg import sql
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.config import Settings
from app.messaging.broker import create_broker
from app.messaging.dispatcher import OutboxDispatcher
from app.messaging.topology import DLQ_QUEUE, PAYMENTS_QUEUE, declare_topology


@pytest.fixture(scope="session")
def settings():
    run_id = f"payment_test_{uuid4().hex}"
    db = make_url(
        os.getenv(
            "TEST_DATABASE_URL",
            "postgresql+asyncpg://payment_test:payment_test@127.0.0.1:15433/payment_test",
        )
    )
    rabbit = make_url(os.getenv("TEST_RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:15673/"))
    return Settings(
        _env_file=None,
        api_key="test-api-key",
        database_url=db.set(database=run_id).render_as_string(hide_password=False),
        rabbitmq_url=rabbit.set(database=run_id).render_as_string(hide_password=False),
        payment_delay_min_seconds=0,
        payment_delay_max_seconds=0,
        outbox_poll_interval_seconds=0.05,
    )


@pytest.fixture(scope="session")
def migrated_database(settings):
    if os.getenv("RUN_INTEGRATION") != "1":
        pytest.skip("Set RUN_INTEGRATION=1 and start docker-compose.test.yml")
    target = make_url(settings.database_url)
    admin = target.set(drivername="postgresql", database="postgres").render_as_string(
        hide_password=False
    )
    with psycopg.connect(admin, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target.database)))
    try:
        env = {**os.environ, "DATABASE_URL": settings.database_url}
        subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"], env=env, check=True)
        subprocess.run([sys.executable, "-m", "alembic", "check"], env=env, check=True)
        yield
    finally:
        with psycopg.connect(admin, autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(target.database))
            )


@pytest.fixture
async def session_factory(migrated_database, settings):
    engine = create_async_engine(settings.database_url, pool_pre_ping=True)
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE payments, outbox"))
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


@pytest.fixture(scope="session")
def rabbit_namespace(settings):
    if os.getenv("RUN_INTEGRATION") != "1":
        pytest.skip("Set RUN_INTEGRATION=1")
    url = make_url(settings.rabbitmq_url)
    management = os.getenv("TEST_RABBITMQ_MANAGEMENT_URL", "http://127.0.0.1:15674")
    with httpx.Client(base_url=management, auth=(url.username, url.password), timeout=20) as client:
        client.put(f"/api/vhosts/{url.database}", json={}).raise_for_status()
        try:
            client.put(
                f"/api/permissions/{url.database}/{url.username}",
                json={"configure": ".*", "write": ".*", "read": ".*"},
            ).raise_for_status()
            yield
        finally:
            client.delete(f"/api/vhosts/{url.database}").raise_for_status()


@pytest.fixture
async def rabbit(rabbit_namespace, settings):
    broker = create_broker(settings)
    await broker.start()
    await declare_topology(broker)
    for queue in (PAYMENTS_QUEUE, DLQ_QUEUE):
        actual = await broker.declare_queue(queue)
        await actual.purge()
    try:
        yield broker
    finally:
        await broker.stop()


@pytest.fixture
async def relay(session_factory, rabbit, settings):
    stop = asyncio.Event()
    task = asyncio.create_task(OutboxDispatcher(session_factory, rabbit, settings).run(stop))
    try:
        yield
    finally:
        stop.set()
        await asyncio.wait_for(task, 5)


class FakeClock:
    def __init__(self):
        self.value = datetime.now(UTC)

    def now(self):
        return self.value

    def advance(self, seconds):
        self.value += timedelta(seconds=seconds)


@pytest.fixture
def clock():
    return FakeClock()
