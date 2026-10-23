import asyncio
import json
import os
from time import monotonic

import pytest
from sqlalchemy import select

from app.api.schemas import PaymentCreateRequest
from app.db.models import OutboxMessage
from app.messaging.broker import create_broker
from app.messaging.topology import PAYMENTS_QUEUE
from app.services.payments import PaymentService
from tests.factories import create_dispatcher

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


async def test_rabbitmq_recreation_keeps_persistent_messages(rabbit, settings):
    await rabbit.publish({"persistence_probe": True}, queue=PAYMENTS_QUEUE, persist=True)
    await rabbit.stop()
    await compose("up", "-d", "--force-recreate", "--wait", "rabbitmq")
    recovered = create_broker(settings)
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
async def test_dispatcher_recovers_after_dependency_restart(
    service, session_factory, rabbit, settings
):
    async with session_factory() as session:
        await PaymentService(session).create_payment(
            PaymentCreateRequest(
                amount="1.00",
                currency="EUR",
                description="recovery",
                webhook_url="https://example.com/hook",
            ).to_command(),
            "recovery",
        )
    dispatcher = create_dispatcher(session_factory, rabbit, settings)
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


async def test_killed_worker_recovers_from_durable_reservation(
    session_factory, rabbit, settings, relay, tmp_path
):
    import sys

    from app.db.models import Payment
    from app.domain import PaymentStatus

    # A pending gateway call leaves a durable reservation. SIGKILL skips every
    # application cleanup hook, exercising recovery rather than graceful stop.
    env = {
        **os.environ,
        **{
            key.upper(): str(value).lower() if isinstance(value, bool) else str(value)
            for key, value in settings.model_dump().items()
        },
    }
    env.update(
        PAYMENT_DELAY_MIN_SECONDS="3",
        PAYMENT_DELAY_MAX_SECONDS="3",
        WEBHOOK_TIMEOUT_SECONDS="0.1",
        WEBHOOK_LEASE_SECONDS="4",
        RETRY_MAX_ATTEMPTS="1",
    )
    async with session_factory() as session:
        payment = await PaymentService(session).create_payment(
            PaymentCreateRequest(
                amount="1.00",
                currency="USD",
                description="crash probe",
                webhook_url="http://127.0.0.1:1/hook",
            ).to_command(),
            "crash-probe",
        )
    log = (tmp_path / "worker.log").open("wb")
    worker = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "app.consumer", env=env, stdout=log, stderr=log
    )
    replacement = None
    try:

        async def reserved():
            async with session_factory() as session:
                row = await session.get(Payment, payment.id)
                return row.webhook_lease_token is not None and row.status == PaymentStatus.PENDING

        await wait_until(reserved)
        worker.kill()
        await worker.wait()
        # Remove the original broker delivery by consuming its redelivery. The
        # scheduled recovery event must independently drive the next worker.
        queue = await rabbit.declare_queue(PAYMENTS_QUEUE)
        redelivered = []

        async def returned():
            message = await queue.get(fail=False)
            if message is not None:
                redelivered.append(message)
                return True
            return False

        await wait_until(returned, deadline_seconds=2)
        assert json.loads(redelivered[0].body)["recovery_token"] is None
        await redelivered[0].ack()
        replacement = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "app.consumer", env=env, stdout=log, stderr=log
        )

        async def exhausted():
            async with session_factory() as session:
                row = await session.get(Payment, payment.id)
                return row.webhook_exhausted_at is not None

        await wait_until(exhausted)
        async with session_factory() as session:
            row = await session.get(Payment, payment.id)
            assert row.status != PaymentStatus.PENDING
            assert row.webhook_attempts == 1
    finally:
        for process in (worker, replacement):
            if process is not None and process.returncode is None:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), 10)
                except TimeoutError:
                    process.kill()
                    await process.wait()
        log.close()
