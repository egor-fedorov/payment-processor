from faststream.rabbit import RabbitQueue


PAYMENTS_QUEUE_NAME = "payments.new"
RETRY_QUEUE_NAME = "payments.retry"
DLQ_QUEUE_NAME = "payments.dlq"

PAYMENTS_QUEUE = RabbitQueue(PAYMENTS_QUEUE_NAME, durable=True)
RETRY_QUEUE = RabbitQueue(
    RETRY_QUEUE_NAME,
    durable=True,
    arguments={
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": PAYMENTS_QUEUE_NAME,
    },
)
DLQ_QUEUE = RabbitQueue(DLQ_QUEUE_NAME, durable=True)


async def declare_topology(broker) -> None:
    await broker.declare_queue(PAYMENTS_QUEUE)
    await broker.declare_queue(RETRY_QUEUE)
    await broker.declare_queue(DLQ_QUEUE)
