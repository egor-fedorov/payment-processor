import secrets
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.db.session import Database


def request_settings(request: Request) -> Settings:
    settings: Settings = request.app.state.settings
    return settings


async def verify_api_key(
    settings: Annotated[Settings, Depends(request_settings)],
    x_api_key: Annotated[str, Header(alias="X-API-Key")],
) -> None:
    if not secrets.compare_digest(x_api_key.encode(), settings.api_key.encode()):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    database: Database = request.app.state.database
    async with database.sessions() as session:
        yield session
