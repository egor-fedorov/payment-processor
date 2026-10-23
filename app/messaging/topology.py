from collections.abc import Awaitable, Callable

from faststream import AckPolicy
from faststream.rabbit import RabbitBroker, RabbitQueue

PAYMENTS_QUEUE = RabbitQueue("payments.new", durable=True)
DLQ_QUEUE = RabbitQueue("payments.dlq", durable=True)


async def declare_topology(broker: RabbitBroker) -> None:
    await broker.declare_queue(PAYMENTS_QUEUE)
    await broker.declare_queue(DLQ_QUEUE)


def subscribe_payments(broker: RabbitBroker, handler: Callable[[bytes], Awaitable[None]]) -> None:
    broker.subscriber(
        PAYMENTS_QUEUE, ack_policy=AckPolicy.NACK_ON_ERROR, decoder=lambda message: message.body
    )(handler)
