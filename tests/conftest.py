from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from easyminer.app import app
from easyminer.database import Base, get_db_session

# Import all models to ensure they're registered with Base.metadata
from easyminer.models import *  # noqa: F401,F403

# Database connection parameters
DB_HOST = "localhost"
DB_PORT = 5432
DB_USER = "easyminer"
DB_PASSWORD = "easyminer"
DB_NAME = "postgres"  # Connect to default database initially
TEST_DB_NAME = "easyminer_test"


# Create a fixture that manages the test database
@pytest.fixture(scope="session")
def database_url():
    """Create and drop the test database using SQLAlchemy primitives."""
    # Connection string to default database for creating/dropping test database
    admin_url = f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    # Create a synchronous engine for database administration
    admin_engine = create_engine(admin_url)

    # Create the test database
    try:
        with admin_engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            # Drop test database if it exists
            conn.execute(text(f"DROP DATABASE IF EXISTS {TEST_DB_NAME}"))
            # Create fresh test database
            conn.execute(text(f"CREATE DATABASE {TEST_DB_NAME}"))
    except Exception as e:
        pytest.fail(f"Failed to create test database: {e}")

    # Return the URL for the test database
    test_db_url = f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{TEST_DB_NAME}"
    yield test_db_url

    try:
        with admin_engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            # Terminate any connections to the test database
            conn.execute(
                text(f"""
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = '{TEST_DB_NAME}'
                AND pid <> pg_backend_pid()
            """)
            )
            # Drop the test database
            conn.execute(text(f"DROP DATABASE IF EXISTS {TEST_DB_NAME}"))
    except Exception as e:
        print(f"Warning: Failed to drop test database: {e}")

    finally:
        # Dispose of the admin engine
        admin_engine.dispose()


# Create a fixture that provides database connection URI
@pytest.fixture(scope="session")
def db_url(database_url):
    """Return the database URL for the test database."""
    return database_url


# Create test engine based on db_url fixture
@pytest.fixture(scope="function")
def test_engine(db_url):
    """Create an async SQLAlchemy engine for testing."""
    engine = create_async_engine(db_url)
    yield engine
    # The engine will be closed automatically after the test


# Create test session factory
@pytest.fixture(scope="function")
def test_session_local(test_engine):
    """Create a session factory for the test database."""
    return async_sessionmaker(test_engine)


@pytest_asyncio.fixture(scope="function")
async def setup_db(test_engine):
    """Set up the database schema for testing."""
    # Create all tables
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    # Drop all tables after tests
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture(scope="function")
async def async_db_session(setup_db, test_session_local) -> AsyncGenerator[AsyncSession]:
    """Create a SQLAlchemy session for each test that gets rolled back after the test."""
    async with test_session_local() as session:
        yield session
        # The session will be automatically rolled back and closed after the test


# Override database dependency fixture
@pytest.fixture(scope="function")
def override_get_db(test_session_local):
    """Return a fixture that overrides the get_db_session dependency."""

    async def _override_get_db_session():
        async with test_session_local() as session:
            yield session

    return _override_get_db_session


@pytest.fixture
def client(setup_db, override_get_db):
    """Create a FastAPI TestClient for testing."""
    # Override the dependency in the app
    app.dependency_overrides[get_db_session] = override_get_db

    # Create test client
    test_client = TestClient(app)

    yield test_client

    # Clean up
    app.dependency_overrides.clear()
