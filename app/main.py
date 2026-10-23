import asyncio
import logging
from contextlib import asynccontextmanager, suppress

from fastapi import Depends, FastAPI, HTTPException, Request

from app.api.deps import verify_api_key
from app.api.routes.payments import router as payments_router
from app.core.config import get_settings
from app.db.session import SessionFactory, dispose_engine
from app.messaging.broker import create_broker
from app.messaging.dispatcher import OutboxDispatcher
from app.messaging.topology import declare_topology


settings = get_settings()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    broker = create_broker()
    stop_event = asyncio.Event()
    dispatcher_task = None
    try:
        await broker.start()
        await declare_topology(broker)
        dispatcher = OutboxDispatcher(SessionFactory, broker)
        dispatcher_task = asyncio.create_task(dispatcher.run(stop_event))
        app.state.broker = broker
        app.state.outbox_dispatcher = dispatcher_task
        app.state.dispatcher = dispatcher
        yield
    finally:
        stop_event.set()
        try:
            if dispatcher_task is not None:
                try:
                    await asyncio.wait_for(dispatcher_task, timeout=5)
                except asyncio.TimeoutError:
                    dispatcher_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await dispatcher_task
                except Exception:
                    logger.exception("Outbox dispatcher terminated unexpectedly")
        finally:
            try:
                await broker.stop()
            finally:
                await dispose_engine()


app = FastAPI(
    title=settings.app_name,
    lifespan=lifespan,
    docs_url="/docs" if settings.docs_enabled else None,
    redoc_url="/redoc" if settings.docs_enabled else None,
    openapi_url="/openapi.json" if settings.docs_enabled else None,
)
app.include_router(payments_router, prefix=settings.api_v1_prefix)


@app.get("/health/ready", dependencies=[Depends(verify_api_key)])
async def readiness(request: Request) -> dict[str, str]:
    task = getattr(request.app.state, "outbox_dispatcher", None)
    dispatcher = getattr(request.app.state, "dispatcher", None)
    if task is None or task.done() or dispatcher is None or dispatcher.last_error:
        raise HTTPException(status_code=503, detail="Outbox dispatcher unavailable")
    return {"status": "ready"}
