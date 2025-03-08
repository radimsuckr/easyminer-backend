from uuid import UUID

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from sqlalchemy import text
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


# Override dependencies for testing
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


# Test for start_upload endpoint
@pytest.mark.asyncio
async def test_start_upload(override_dependencies, test_db):
    # Upload settings
    settings = {
        "name": "test_upload",
        "media_type": "csv",
        "db_type": "limited",
        "separator": ",",
        "encoding": "utf-8",
        "quotes_char": '"',
        "escape_char": "\\",
        "locale": "en_US",
        "compression": "none",
        "format": "csv",
    }

    # Make request to start_upload endpoint
    response = client.post(
        "/api/v1/upload/start",
        json=settings,
        headers={"Authorization": "Bearer test_token"},
    )

    # Check response
    assert response.status_code == 200

    # Verify that the upload was created in the database
    upload_id = response.json()
    assert UUID(upload_id)  # Verify it's a valid UUID

    # Query the database to check if the upload was created
    result = await test_db.execute(
        text(f"SELECT * FROM upload WHERE uuid = '{upload_id}'")
    )
    upload = result.fetchone()
    assert upload is not None
    assert upload.name == "test_upload"

    # Return the upload_id for use in subsequent tests
    return upload_id


# Test for upload_chunk endpoint
@pytest.mark.asyncio
async def test_upload_chunk(override_dependencies, test_db):
    # First create an upload
    upload_id = await test_start_upload(override_dependencies, test_db)

    # Create a test chunk
    test_data = b"col1,col2,col3\nval1,val2,val3\nval4,val5,val6"

    # Upload the chunk
    response = client.post(
        f"/api/v1/upload/{upload_id}",
        headers={
            "Authorization": "Bearer test_token",
            "Content-Type": "application/octet-stream",
        },
        content=test_data,
    )

    # Check response
    assert response.status_code == 202

    # Verify that a data source was created
    result = await test_db.execute(
        text("SELECT * FROM data_source WHERE user_id = 1 ORDER BY id DESC LIMIT 1")
    )
    data_source = result.fetchone()
    assert data_source is not None
    assert data_source.size_bytes == len(test_data)

    # Instead of checking for files directly, just verify the response and data source
    # The DiskStorage saves files in ../../var/data which may not exist in the test environment

    return upload_id, data_source.id


# Test for complete upload flow (start + chunks + empty chunk)
@pytest.mark.asyncio
async def test_complete_upload_flow(override_dependencies, test_db):
    # First create an upload
    upload_id = await test_start_upload(override_dependencies, test_db)

    # Create and upload test chunks
    chunks = [
        b"col1,col2,col3\n",
        b"val1,val2,val3\n",
        b"val4,val5,val6\n",
        b"",  # Empty chunk signifies end of upload
    ]

    data_source_id = None

    # Upload each chunk
    for chunk in chunks:
        response = client.post(
            f"/api/v1/upload/{upload_id}",
            headers={
                "Authorization": "Bearer test_token",
                "Content-Type": "application/octet-stream",
            },
            content=chunk,
        )
        assert response.status_code == 202

    # After upload is complete, verify data source was created with correct size
    total_size = sum(len(chunk) for chunk in chunks)

    result = await test_db.execute(
        text("SELECT * FROM data_source WHERE user_id = 1 ORDER BY id DESC LIMIT 1")
    )
    data_source = result.fetchone()
    assert data_source is not None
    assert data_source.size_bytes == total_size
    data_source_id = data_source.id

    # Skip file checks since files are saved to ../../var/data by DiskStorage

    return upload_id, data_source_id
