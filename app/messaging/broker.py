from faststream.rabbit import RabbitBroker
from faststream.rabbit.schemas import Channel

from app.core.config import get_settings


def create_broker() -> RabbitBroker:
    settings = get_settings()
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(
            prefetch_count=1,
            publisher_confirms=True,
            on_return_raises=True,
        ),
    )
