import asyncio
import contextlib
import logging
from collections.abc import AsyncGenerator, Generator
from datetime import datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from easyminer.center_client import get_center_client
from easyminer.config import settings
from easyminer.schemas.center import DatabaseConfig
from easyminer.schemas.data import DbType

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class UserSession:
    def __init__(self, api_key: str, db_config: DatabaseConfig, engine: AsyncEngine):
        self.api_key: str = api_key
        self.db_config: DatabaseConfig = db_config
        self.engine: AsyncEngine = engine
        self.last_activity: datetime = datetime.now()
        self.created_at: datetime = datetime.now()

    def touch(self):
        """Update last activity time."""
        self.last_activity = datetime.now()

    def is_expired(self, ttl_seconds: int) -> bool:
        return datetime.now() - self.last_activity > timedelta(seconds=ttl_seconds)


class UserSessionManager:
    """
    Manages per-user database sessions with activity-based expiry.
    Mimics Scala's UserService actor lifecycle (5-minute sliding window).
    """

    def __init__(self, ttl_seconds: int = settings.user_session_ttl):
        self._sessions: dict[str, UserSession] = {}
        self._lock: asyncio.Lock = asyncio.Lock()
        self.ttl_seconds: int = ttl_seconds

    async def get_or_create_session(self, api_key: str) -> UserSession:
        async with self._lock:
            if api_key in self._sessions:
                session = self._sessions[api_key]
                if not session.is_expired(self.ttl_seconds):
                    session.touch()  # Extend TTL
                    logger.debug("Reusing cached session for user (activity extended)")
                    return session
                else:
                    logger.info("Session expired for user, disposing engine")
                    await session.engine.dispose()
                    del self._sessions[api_key]

        logger.info("Creating new session for user")

        try:
            client = get_center_client()
            db_config = await client.get_database_config(api_key, DbType.limited)
            logger.debug(
                f"Got database config from EM Center: {db_config.server}:{db_config.port}/{db_config.database}"
            )

            from easyminer.migrations import run_migrations

            run_migrations(db_config.get_sync_url())
            logger.debug("Migrations completed, creating engine")

            engine = create_async_engine(
                db_config.get_async_url(), echo=settings.echo_sql, pool_pre_ping=True, pool_size=5, max_overflow=10
            )
            logger.debug("Engine created successfully")

            session = UserSession(api_key, db_config, engine)

            async with self._lock:
                self._sessions[api_key] = session

            logger.info("Session created and cached for user")
            return session
        except Exception as e:
            logger.error(f"Failed to create user session: {e}", exc_info=True)
            raise

    async def cleanup_expired_sessions(self):
        while True:
            await asyncio.sleep(60)  # Check every minute
            async with self._lock:
                expired_keys = [key for key, session in self._sessions.items() if session.is_expired(self.ttl_seconds)]

            for key in expired_keys:
                session = self._sessions[key]
                logger.info("Cleaning up expired session for user")
                await session.engine.dispose()
                async with self._lock:
                    del self._sessions[key]

    async def close_all(self):
        """Close all sessions."""
        async with self._lock:
            sessions_to_close = list(self._sessions.values())
            self._sessions.clear()

        for session in sessions_to_close:
            await session.engine.dispose()


_session_manager: UserSessionManager | None = None


def get_session_manager() -> UserSessionManager:
    global _session_manager
    if _session_manager is None:
        _session_manager = UserSessionManager(ttl_seconds=settings.user_session_ttl)
    return _session_manager


async def get_user_db_session(api_key: str) -> AsyncGenerator[AsyncSession]:
    logger.debug("Getting user DB session for api_key")

    try:
        session_manager = get_session_manager()
        user_session = await session_manager.get_or_create_session(api_key)
        logger.debug("Got user session from manager")

        session_maker = async_sessionmaker(user_session.engine)
        session = session_maker()
        logger.debug("Created database session")

        try:
            yield session
        except Exception as e:
            logger.error(f"Error during session use: {e}", exc_info=True)
            await session.rollback()
            raise
        finally:
            await session.close()
            logger.debug("Database session closed")
    except Exception as e:
        logger.error(f"Failed to get user DB session: {e}", exc_info=True)
        raise


@contextlib.contextmanager
def get_sync_db_session(db_url: str) -> Generator[Session]:
    sync_engine = create_engine(db_url, echo=settings.echo_sql, pool_pre_ping=True)
    session = sessionmaker(sync_engine)()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        sync_engine.dispose()
