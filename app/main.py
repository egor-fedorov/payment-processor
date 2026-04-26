import asyncio
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI

from app.api.routes.payments import router as payments_router
from app.core.config import get_settings
from app.db.session import SessionFactory, dispose_engine
from app.messaging.broker import create_broker
from app.messaging.dispatcher import OutboxDispatcher
from app.messaging.topology import declare_topology


settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    broker = create_broker()
    await broker.start()
    await declare_topology(broker)

    stop_event = asyncio.Event()
    dispatcher = OutboxDispatcher(SessionFactory, broker)
    dispatcher_task = asyncio.create_task(dispatcher.run(stop_event))

    app.state.broker = broker
    app.state.outbox_dispatcher = dispatcher_task

    try:
        yield
    finally:
        stop_event.set()
        try:
            await asyncio.wait_for(dispatcher_task, timeout=5)
        except asyncio.TimeoutError:
            dispatcher_task.cancel()
            with suppress(asyncio.CancelledError):
                await dispatcher_task

        await broker.stop()
        await dispose_engine()


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.include_router(payments_router, prefix=settings.api_v1_prefix)
