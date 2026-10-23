from faststream.rabbit import RabbitBroker
from faststream.rabbit.schemas import Channel

from app.core.config import Settings


def create_broker(settings: Settings) -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(
            prefetch_count=settings.consumer_concurrency,
            publisher_confirms=True,
            on_return_raises=True,
        ),
    )
