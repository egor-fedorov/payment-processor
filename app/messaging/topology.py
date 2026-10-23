from faststream.rabbit import RabbitQueue

from app.core.config import get_settings

PAYMENTS_QUEUE_NAME = "payments.new"
DLQ_QUEUE_NAME = "payments.dlq"
PAYMENTS_QUEUE = RabbitQueue(PAYMENTS_QUEUE_NAME, durable=True)
DLQ_QUEUE = RabbitQueue(DLQ_QUEUE_NAME, durable=True)


def retry_queue(attempt: int) -> RabbitQueue:
    settings = get_settings()
    delay_ms = settings.retry_base_delay_seconds * 2 ** (attempt - 1) * 1000
    return RabbitQueue(
        f"payments.retry.{delay_ms}ms",
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": PAYMENTS_QUEUE_NAME,
            "x-dead-letter-strategy": "at-least-once",
            "x-overflow": "reject-publish",
        },
    )


async def declare_topology(broker) -> None:
    await broker.declare_queue(PAYMENTS_QUEUE)
    await broker.declare_queue(DLQ_QUEUE)
    for attempt in range(1, get_settings().retry_max_attempts):
        await broker.declare_queue(retry_queue(attempt))
