import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from easyminer.api.dependencies.auth import get_current_user
from easyminer.api.dependencies.db import get_db_session
from easyminer.app import app
from easyminer.database import Base
from easyminer.models import User

# Test configuration
TEST_DB_URL = "sqlite+aiosqlite:///:memory:"

# Create test client
client = TestClient(app)

# Test user for authentication
TEST_USER = User(
    id=1,
    username="testuser",
    email="test@example.com",
    slug="testuser",
    first_name="Test",
    last_name="User",
    hashed_password="hashed_pwd",
    is_superuser=False,
)


# Setup and teardown for test database
@pytest_asyncio.fixture(scope="function")
async def test_db():
    # Create test engine and session
    engine = create_async_engine(TEST_DB_URL)
    TestingSessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=engine, class_=AsyncSession
    )

    # Create all tables in the test database
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Create a test session
    async with TestingSessionLocal() as session:
        yield session

    # Drop all tables after the test
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture(scope="function")
def override_dependencies(test_db):
    # Override the get_db_session dependency
    async def get_test_db():
        yield test_db

    # Override the get_current_user dependency
    async def get_test_user():
        return TEST_USER

    # Apply the overrides
    app.dependency_overrides[get_db_session] = get_test_db
    app.dependency_overrides[get_current_user] = get_test_user

    yield

    # Remove the overrides after the test
    app.dependency_overrides = {}
