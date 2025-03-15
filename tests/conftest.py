from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from easyminer.app import app
from easyminer.database import Base, get_db_session

# Import all models to ensure they're registered with Base.metadata
from easyminer.models import *  # noqa: F401,F403
from easyminer.models.upload import Upload

# Global variables for test database
test_engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
TestSessionLocal = async_sessionmaker(
    bind=test_engine, class_=AsyncSession, expire_on_commit=False
)


@pytest_asyncio.fixture(scope="function")
async def setup_db():
    """Set up the database for testing."""
    # Create all tables
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield

    # Drop all tables after tests
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture(scope="function")
async def db_session(setup_db) -> AsyncGenerator[AsyncSession]:
    """Create a SQLAlchemy session for each test that gets rolled back after the test."""
    async with TestSessionLocal() as session:
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


# Override the database dependency
async def override_get_db_session():
    """Override the database session for testing."""
    async with TestSessionLocal() as session:
        yield session


# Override the dependency in the app
app.dependency_overrides[get_db_session] = override_get_db_session


@pytest.fixture
def client(setup_db):
    """Create a FastAPI TestClient for testing."""
    # Create test client
    test_client = TestClient(app)

    yield test_client


@pytest.fixture
def test_csv_data():
    """Create test CSV data for uploads."""
    return "name,age,score\nAlice,30,85\nBob,25,92\nCharlie,35,78"
