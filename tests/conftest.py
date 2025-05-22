from collections.abc import AsyncGenerator, Generator

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from easyminer.app import app
from easyminer.database import Base, get_db_session, get_sync_db_session

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

    # Return the URLs for the test database
    async_db_url = f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{TEST_DB_NAME}"
    sync_db_url = f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{TEST_DB_NAME}"

    yield {"async": async_db_url, "sync": sync_db_url}

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


# Create fixtures that provide database connection URIs
@pytest.fixture(scope="session")
def async_db_url(database_url):
    """Return the async database URL for the test database."""
    return database_url["async"]


@pytest.fixture(scope="session")
def sync_db_url(database_url):
    """Return the sync database URL for the test database."""
    return database_url["sync"]


# Create test engines based on db_url fixtures
@pytest.fixture(scope="function")
def async_test_engine(async_db_url):
    """Create an async SQLAlchemy engine for testing."""
    engine = create_async_engine(async_db_url)
    yield engine
    # The engine will be closed automatically after the test


@pytest.fixture(scope="function")
def sync_test_engine(sync_db_url):
    """Create a sync SQLAlchemy engine for testing."""
    engine = create_engine(sync_db_url)
    yield engine
    # The engine will be closed automatically after the test


# Create test session factories
@pytest.fixture(scope="function")
def async_session_local(async_test_engine):
    """Create an async session factory for the test database."""
    return async_sessionmaker(async_test_engine, expire_on_commit=False)


@pytest.fixture(scope="function")
def sync_session_local(sync_test_engine):
    """Create a sync session factory for the test database."""
    return sessionmaker(sync_test_engine, expire_on_commit=False)


@pytest_asyncio.fixture(scope="function")
async def setup_db(async_test_engine):
    """Set up the database schema for testing using async engine."""
    # Create all tables
    async with async_test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    # Drop all tables after tests
    async with async_test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture(scope="function")
async def async_db_session(setup_db, async_session_local) -> AsyncGenerator[AsyncSession]:
    """Create an async SQLAlchemy session for each test that gets rolled back after the test."""
    async with async_session_local() as session:
        yield session
        # The session will be automatically rolled back and closed after the test


@pytest.fixture(scope="function")
def sync_db_session(setup_db, sync_session_local) -> Generator[Session]:
    """Create a sync SQLAlchemy session for each test that gets rolled back after the test."""
    with sync_session_local() as session:
        yield session
        # The session will be automatically rolled back and closed after the test


# Override database dependency fixtures
@pytest.fixture(scope="function")
def override_get_async_db(async_session_local):
    """Return a fixture that overrides the get_db_session dependency."""

    async def _override_get_db_session():
        async with async_session_local() as session:
            yield session

    return _override_get_db_session


@pytest.fixture(scope="function")
def override_get_sync_db(sync_session_local):
    """Return a fixture that overrides the get_sync_db_session dependency."""

    def _override_get_sync_db_session():
        with sync_session_local() as session:
            yield session

    return _override_get_sync_db_session


@pytest.fixture
def client(setup_db, override_get_async_db, override_get_sync_db):
    """Create a FastAPI TestClient for testing with both async and sync DB sessions."""
    # Override the dependencies in the app
    app.dependency_overrides[get_db_session] = override_get_async_db
    app.dependency_overrides[get_sync_db_session] = override_get_sync_db

    # Create test client
    test_client = TestClient(app)

    yield test_client

    # Clean up
    app.dependency_overrides.clear()


# For backward compatibility
test_engine = async_test_engine
test_session_local = async_session_local
override_get_db = override_get_async_db

