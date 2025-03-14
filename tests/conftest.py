from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from easyminer.app import app
from easyminer.database import Base
from easyminer.models.upload import Upload


@pytest_asyncio.fixture(scope="function")
async def engine():
    """Create a SQLAlchemy engine for testing."""
    # Use in-memory SQLite database for testing
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)

    # Create all tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    # Drop all tables after tests
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture(scope="function")
async def db_session_factory(engine):
    """Create a SQLAlchemy session factory."""
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


@pytest_asyncio.fixture(scope="function")
async def db_session(db_session_factory) -> AsyncGenerator[AsyncSession]:
    """Create a SQLAlchemy session for each test that gets rolled back after the test."""
    async with db_session_factory() as session:
        yield session
        # The session will be automatically rolled back and closed after the test


@pytest_asyncio.fixture
async def test_upload(db_session: AsyncSession) -> Upload:
    """Create a test upload for testing."""
    upload = Upload(
        uuid="test-upload-uuid",
        name="Test Upload",
        media_type="csv",
        db_type="limited",
        separator=",",
        encoding="utf-8",
        quotes_char='"',
        escape_char="\\",
        locale="en_US",
        compression="",
        format="csv",
    )
    db_session.add(upload)
    await db_session.commit()
    await db_session.refresh(upload)
    return upload


@pytest.fixture
def client():
    """Create a FastAPI TestClient for testing."""
    # Create test client
    test_client = TestClient(app)

    yield test_client
