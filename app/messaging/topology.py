from faststream.rabbit import RabbitBroker, RabbitQueue

PAYMENTS_QUEUE = RabbitQueue("payments.new", durable=True)
DLQ_QUEUE = RabbitQueue("payments.dlq", durable=True)


async def declare_topology(broker: RabbitBroker) -> None:
    await broker.declare_queue(PAYMENTS_QUEUE)
    await broker.declare_queue(DLQ_QUEUE)
