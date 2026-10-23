import os
import subprocess
import sys

import pytest

if os.getenv("RUN_INTEGRATION") == "1":
    os.environ["DATABASE_URL"] = os.getenv(
        "TEST_DATABASE_URL",
        "postgresql+asyncpg://payment_test:payment_test@127.0.0.1:15433/payment_test",
    )
    os.environ["RABBITMQ_URL"] = os.getenv(
        "TEST_RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:15673/"
    )
    os.environ["PAYMENT_DELAY_MIN_SECONDS"] = "0"
    os.environ["PAYMENT_DELAY_MAX_SECONDS"] = "0"


@pytest.fixture(scope="session")
def migrated_database():
    if os.getenv("RUN_INTEGRATION") != "1":
        pytest.skip("Set RUN_INTEGRATION=1 and start docker-compose.test.yml")
    from sqlalchemy.engine import make_url

    assert make_url(os.environ["DATABASE_URL"]).database.endswith("_test"), (
        "Use a dedicated *_test database"
    )
    subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"], check=True)


@pytest.fixture
async def session_factory(migrated_database):
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    engine = create_async_engine(os.environ["DATABASE_URL"])
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE payments, outbox"))
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


@pytest.fixture
async def rabbit(migrated_database):
    from app.core.config import get_settings
    from app.messaging.broker import create_broker
    from app.messaging.topology import DLQ_QUEUE, PAYMENTS_QUEUE, declare_topology, retry_queue

    broker = create_broker(get_settings())
    await broker.start()
    await declare_topology(broker, get_settings())
    queues = [
        PAYMENTS_QUEUE,
        DLQ_QUEUE,
        *[retry_queue(i, get_settings()) for i in range(1, get_settings().retry_max_attempts)],
    ]
    for queue in queues:
        actual = await broker.declare_queue(queue)
        await actual.purge()
    yield broker
    await broker.stop()
