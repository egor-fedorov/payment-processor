import secrets
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request, status

from app.core.config import Settings


def request_settings(request: Request) -> Settings:
    settings: Settings = request.app.state.settings
    return settings


async def verify_api_key(
    settings: Annotated[Settings, Depends(request_settings)],
    x_api_key: Annotated[str, Header(alias="X-API-Key")],
) -> None:
    if not secrets.compare_digest(x_api_key.encode(), settings.api_key.encode()):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
