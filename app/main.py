import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from fastapi import Depends, FastAPI, HTTPException, Request

from app.api.deps import verify_api_key
from app.api.routes.payments import router as payments_router
from app.core.config import Settings, get_settings
from app.db.session import create_database
from app.messaging.broker import create_broker
from app.messaging.dispatcher import OutboxDispatcher
from app.messaging.topology import declare_topology

logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        database = create_database(settings)
        broker = create_broker(settings)
        stop = asyncio.Event()
        task = None
        try:
            await broker.start()
            await declare_topology(broker, settings)
            dispatcher = OutboxDispatcher(database.sessions, broker, settings)
            task = asyncio.create_task(dispatcher.run(stop))
            app.state.database = database
            app.state.dispatcher = dispatcher
            app.state.outbox_dispatcher = task
            yield
        finally:
            stop.set()
            try:
                if task is not None:
                    try:
                        await asyncio.wait_for(task, timeout=5)
                    except TimeoutError:
                        task.cancel()
                        with suppress(asyncio.CancelledError):
                            await task
                    except Exception:
                        logger.exception("Outbox dispatcher terminated unexpectedly")
            finally:
                try:
                    await broker.stop()
                finally:
                    await database.close()

    application = FastAPI(
        title=settings.app_name,
        lifespan=lifespan,
        docs_url="/docs" if settings.docs_enabled else None,
        redoc_url="/redoc" if settings.docs_enabled else None,
        openapi_url="/openapi.json" if settings.docs_enabled else None,
    )
    application.state.settings = settings
    application.include_router(payments_router, prefix=settings.api_v1_prefix)

    @application.get("/health/ready", dependencies=[Depends(verify_api_key)])
    async def readiness(request: Request) -> dict[str, str]:
        task = getattr(request.app.state, "outbox_dispatcher", None)
        dispatcher = getattr(request.app.state, "dispatcher", None)
        if task is None or task.done() or dispatcher is None or dispatcher.last_error:
            raise HTTPException(status_code=503, detail="Outbox dispatcher unavailable")
        return {"status": "ready"}

    return application


app = create_app()
