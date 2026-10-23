import asyncio
import json
import os
from time import monotonic

import pytest
from sqlalchemy import select

from app.api.schemas import PaymentCreateRequest
from app.db.models import OutboxMessage
from app.messaging.broker import create_broker
from app.messaging.dispatcher import OutboxDispatcher
from app.messaging.topology import PAYMENTS_QUEUE
from app.services.payments import PaymentService

pytestmark = [
    pytest.mark.integration,
    pytest.mark.containers,
    pytest.mark.skipif(os.getenv("RUN_CONTAINER_TESTS") != "1", reason="Set RUN_CONTAINER_TESTS=1"),
]


async def compose(*args):
    process = await asyncio.create_subprocess_exec(
        "docker",
        "compose",
        "-p",
        "payment-processor-tests",
        "-f",
        "docker-compose.test.yml",
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    output, _ = await process.communicate()
    assert process.returncode == 0, output.decode()


async def wait_until(check, deadline_seconds=30):
    deadline = monotonic() + deadline_seconds
    while monotonic() < deadline:
        if await check():
            return
        await asyncio.sleep(0.1)
    raise AssertionError("Recovery timed out")


async def test_rabbitmq_recreation_keeps_persistent_messages(rabbit):
    await rabbit.publish({"persistence_probe": True}, queue=PAYMENTS_QUEUE, persist=True)
    await rabbit.stop()
    await compose("up", "-d", "--force-recreate", "--wait", "rabbitmq")
    recovered = create_broker()
    await recovered.start()
    try:
        queue = await recovered.declare_queue(PAYMENTS_QUEUE)
        message = await queue.get(fail=False)
        assert message is not None
        assert json.loads(message.body) == {"persistence_probe": True}
        await message.ack()
    finally:
        await recovered.stop()


@pytest.mark.parametrize("service", ["postgres", "rabbitmq"])
async def test_dispatcher_recovers_after_dependency_restart(service, session_factory, rabbit):
    async with session_factory() as session:
        await PaymentService(session).create_payment(
            PaymentCreateRequest(
                amount="1.00",
                currency="EUR",
                description="recovery",
                webhook_url="https://example.com/hook",
            ),
            "recovery",
        )
    dispatcher = OutboxDispatcher(session_factory, rabbit)
    dispatcher._poll_interval = 0.1
    stop = asyncio.Event()
    await compose("stop", service)
    task = asyncio.create_task(dispatcher.run(stop))
    try:

        async def failed():
            return dispatcher.last_error is not None

        await wait_until(failed)
        await compose("up", "-d", "--wait", service)

        async def published():
            async with session_factory() as session:
                return (await session.scalar(select(OutboxMessage))).published_at is not None

        await wait_until(published)
        assert not task.done()
        assert dispatcher.last_error is None
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=5)
        await compose("up", "-d", "--wait", service)
