from faststream.rabbit import RabbitBroker

from app.contracts import Publication


class RabbitEventPublisher:
    def __init__(self, broker: RabbitBroker, timeout_seconds: float) -> None:
        self._broker = broker
        self._timeout = timeout_seconds

    async def publish(self, item: Publication) -> None:
        await self._broker.publish(
            item.payload,
            queue=item.destination,
            correlation_id=str(item.aggregate_id),
            message_id=str(item.id),
            persist=True,
            mandatory=True,
            timeout=self._timeout,
        )
