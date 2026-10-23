"""Test composition roots with replaceable external effects."""

from app.core.clock import SystemClock
from app.db.outbox import OutboxRepository
from app.messaging.broker import create_broker
from app.messaging.consumer import Consumer
from app.messaging.dispatcher import OutboxDispatcher
from app.messaging.publisher import RabbitEventPublisher
from app.messaging.topology import subscribe_payments
from app.services.gateway import SimulatedGateway
from app.services.payments import PaymentProcessingService
from app.services.rejections import RejectedMessageService


def create_processor(sessions, settings, *, gateway=None, clock=None):
    return PaymentProcessingService(
        sessions,
        settings,
        gateway=gateway if gateway is not None else SimulatedGateway(settings),
        clock=clock if clock is not None else SystemClock(),
    )


def create_dispatcher(sessions, broker, settings, *, clock=None):
    return OutboxDispatcher(
        OutboxRepository(sessions, settings),
        RabbitEventPublisher(broker, settings.outbox_publish_timeout_seconds),
        clock=clock if clock is not None else SystemClock(),
        poll_interval_seconds=settings.outbox_poll_interval_seconds,
        publish_timeout_seconds=settings.outbox_publish_timeout_seconds,
    )


def create_consumer(sessions, settings, sender, *, processor=None):
    broker = create_broker(settings)
    consumer = Consumer(
        processor if processor is not None else create_processor(sessions, settings),
        sender,
        RejectedMessageService(sessions, SystemClock()),
    )
    subscribe_payments(broker, consumer.consume_payment)
    return broker, consumer
