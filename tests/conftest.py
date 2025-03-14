from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from easyminer.api.dependencies.auth import get_current_user
from easyminer.app import app
from easyminer.database import Base
from easyminer.models import User
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
async def test_user(db_session: AsyncSession) -> User:
    """Create a test user for testing."""
    user = User(
        id=1,  # Explicitly set ID to 1 for consistency
        username="testuser",
        slug="testuser",
        email="test@example.com",
        first_name="Test",
        last_name="User",
        hashed_password="hashed_password",
        is_superuser=False,
    )
    db_session.add(user)
    await db_session.commit()
    await db_session.refresh(user)
    return user


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
def client(test_user: User):
    """Create a FastAPI TestClient for testing with pre-authenticated user."""
    # Set up app overrides
    original_dependencies = app.dependency_overrides.copy()

    # Override the auth dependency to use our test user
    async def get_test_user():
        return test_user

    app.dependency_overrides[get_current_user] = get_test_user

    # Create test client with default auth headers
    test_client = TestClient(app)
    test_client.headers.update({"Authorization": "Bearer test_token"})

    yield test_client

    # Restore original dependencies
    app.dependency_overrides = original_dependencies


@pytest.fixture(scope="function")
def override_dependencies(test_user: User):
    """
    Modern fixture to override FastAPI dependencies for testing.
    This is similar to the client fixture but without creating a client.
    """
    # Save the original dependencies
    original_dependencies = app.dependency_overrides.copy()

    # Override with test dependencies
    async def get_test_user():
        return test_user

    app.dependency_overrides[get_current_user] = get_test_user

    # Yield to the test
    yield

    # Restore original dependencies
    app.dependency_overrides = original_dependencies
