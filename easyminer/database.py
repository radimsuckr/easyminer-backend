import contextlib
import logging
from collections.abc import AsyncGenerator, AsyncIterator, Generator
from typing import Any, final

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from easyminer.center_client import get_center_client
from easyminer.config import settings
from easyminer.schemas.center import DatabaseConfig
from easyminer.schemas.data import DbType

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


@final
class DatabaseSessionManager:
    def __init__(self, host: str, engine_kwargs: dict[str, Any] | None = None):
        if engine_kwargs is None:
            engine_kwargs = {}

        self._engine = create_async_engine(host, **engine_kwargs)
        self._sessionmaker = async_sessionmaker(self._engine)

    async def close(self):
        if self._engine is None:
            raise Exception("DatabaseSessionManager is not initialized")
        await self._engine.dispose()

        self._engine = None
        self._sessionmaker = None

    @contextlib.asynccontextmanager
    async def connect(self) -> AsyncIterator[AsyncConnection]:
        if self._engine is None:
            raise Exception("DatabaseSessionManager is not initialized")

        async with self._engine.begin() as connection:
            try:
                yield connection
            except Exception:
                await connection.rollback()
                raise

    @contextlib.asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        if self._sessionmaker is None:
            raise Exception("DatabaseSessionManager is not initialized")

        session = self._sessionmaker()
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


sessionmanager = DatabaseSessionManager(settings.database_url, {"echo": settings.echo_sql})


async def get_db_session() -> AsyncGenerator[AsyncSession]:
    async with sessionmanager.session() as session:
        yield session


# Cache for database engines to avoid creating new engines for each request
_engine_cache: dict[str, AsyncEngine] = {}


async def get_database_config(api_key: str, db_type: DbType) -> DatabaseConfig:
    client = get_center_client()
    db_config = await client.get_database_config(api_key, db_type)
    return db_config


def get_db_url_from_config(db_config: DatabaseConfig, sync: bool = True) -> str:
    if sync:
        return db_config.get_sync_url()
    return db_config.get_async_url()


async def get_user_db_session(api_key: str, db_type: DbType) -> AsyncGenerator[AsyncSession]:
    db_config = await get_database_config(api_key, db_type)
    db_url = db_config.get_async_url()

    if db_url not in _engine_cache:
        logger.info(f"Creating new database engine for {db_config.server}:{db_config.port}/{db_config.database}")

        # Run migrations on first connection
        from easyminer.migrations import run_migrations

        await run_migrations(db_config.get_sync_url())

        _engine_cache[db_url] = create_async_engine(db_url, echo=settings.echo_sql, pool_pre_ping=True)

    engine = _engine_cache[db_url]
    session_maker = async_sessionmaker(engine)
    session = session_maker()
    try:
        yield session
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


@contextlib.contextmanager
def get_sync_db_session(db_url: str | None = None) -> Generator[Session]:
    url = db_url or settings.database_url_sync
    sync_engine = create_engine(url, echo=settings.echo_sql, pool_pre_ping=True)
    session = sessionmaker(sync_engine)()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        sync_engine.dispose()


async def close_all_engines():
    for url, engine in _engine_cache.items():
        logger.info(f"Closing database engine for {url}")
        await engine.dispose()
    _engine_cache.clear()
