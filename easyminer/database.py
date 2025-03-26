import contextlib
from collections.abc import AsyncGenerator, AsyncIterator, Generator
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from easyminer.config import settings


class Base(DeclarativeBase):
    pass


class DatabaseSessionManager:
    def __init__(self, host: str, engine_kwargs: dict[str, Any] = {}):
        self._engine = create_async_engine(host, **engine_kwargs)
        self._sessionmaker = async_sessionmaker(autocommit=False, bind=self._engine)

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


sessionmanager = DatabaseSessionManager(
    settings.database_url, {"echo": settings.echo_sql}
)


async def get_db_session() -> AsyncGenerator[AsyncSession]:
    async with sessionmanager.session() as session:
        yield session


@contextlib.contextmanager
def get_sync_db_session() -> Generator[Session]:
    sync_engine = create_engine(
        settings.database_url_sync,
        echo=settings.echo_sql,
    )
    session = sessionmaker(sync_engine, expire_on_commit=False)()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
