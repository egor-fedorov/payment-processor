from dataclasses import dataclass

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import Settings


@dataclass(frozen=True)
class Database:
    engine: AsyncEngine
    sessions: async_sessionmaker[AsyncSession]

    async def close(self) -> None:
        await self.engine.dispose()


def create_database(settings: Settings) -> Database:
    engine = create_async_engine(
        settings.database_url,
        pool_pre_ping=True,
        hide_parameters=True,
        pool_size=settings.database_pool_size,
        max_overflow=0,
        pool_timeout=settings.database_pool_timeout_seconds,
    )
    return Database(engine, async_sessionmaker(engine, expire_on_commit=False))
