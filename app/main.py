import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Request
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.api.deps import verify_api_key
from app.api.routes.payments import router as payments_router
from app.core.config import Settings, get_settings
from app.db.session import create_database


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        database = create_database(settings)
        app.state.database = database
        try:
            yield
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

    @application.get("/health/live")
    async def liveness() -> dict[str, str]:
        return {"status": "alive"}

    @application.get("/health/ready", dependencies=[Depends(verify_api_key)])
    async def readiness(request: Request) -> dict[str, str]:
        try:
            async with asyncio.timeout(2), request.app.state.database.sessions() as session:
                await session.execute(text("SELECT 1"))
        except (SQLAlchemyError, TimeoutError) as exc:
            raise HTTPException(status_code=503, detail="Database unavailable") from exc
        return {"status": "ready"}

    return application


app = create_app()
