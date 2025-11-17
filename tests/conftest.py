from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
import uvloop
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from easyminer.app import app
from easyminer.database import Base
from easyminer.dependencies import get_authenticated_db_session

BASE_URL: str = "http://test"


@pytest.fixture(scope="session")
def event_loop():
    loop = uvloop.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def async_engine() -> AsyncGenerator[AsyncEngine]:
    # Use in-memory SQLite for tests
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)

    # Create all tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    # Drop all tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    await engine.dispose()


@pytest_asyncio.fixture
async def async_db_session(async_engine: AsyncEngine) -> AsyncGenerator[AsyncSession]:
    async_session_maker = async_sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)
    async with async_session_maker() as session:
        yield session


@pytest_asyncio.fixture
async def async_client(async_db_session: AsyncSession) -> AsyncGenerator[AsyncClient]:
    async def override_get_authenticated_db_session():
        yield async_db_session

    app.dependency_overrides[get_authenticated_db_session] = override_get_authenticated_db_session

    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as client:
        yield client

    app.dependency_overrides.clear()
