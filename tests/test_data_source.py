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
from easyminer.models import DataSource, User

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


# Test listing data sources
@pytest.mark.asyncio
async def test_list_data_sources(override_dependencies, test_db):
    # Create a test data source directly
    data_source = DataSource(
        name="Test Data Source",
        type="csv",
        user_id=TEST_USER.id,
        size_bytes=1000,
        row_count=10,
    )

    test_db.add(data_source)
    await test_db.commit()
    await test_db.refresh(data_source)

    # Add the data source to the user's data_sources
    TEST_USER.data_sources = [data_source]

    # Make request to list data sources
    response = client.get(
        "/api/v1/sources", headers={"Authorization": "Bearer test_token"}
    )

    # Check response
    assert response.status_code == 200

    # Verify the response contains the test data source
    data_sources = response.json()
    assert len(data_sources) == 1
    assert data_sources[0]["name"] == "Test Data Source"
    assert data_sources[0]["id"] == data_source.id


# Test creating a data source
@pytest.mark.asyncio
async def test_create_data_source(override_dependencies, test_db):
    # Data source data
    data = {
        "name": "New Data Source",
        "type": "csv",
        "size_bytes": 2000,
        "row_count": 20,
    }

    # Make request to create data source
    response = client.post(
        "/api/v1/sources", json=data, headers={"Authorization": "Bearer test_token"}
    )

    # Check response
    assert response.status_code == 200

    # Verify the response contains the new data source
    data_source = response.json()
    assert data_source["name"] == "New Data Source"
    assert data_source["type"] == "csv"
    assert data_source["size_bytes"] == 2000
    assert data_source["row_count"] == 20

    # Verify the data source was created in the database
    result = await test_db.execute(
        text(f"SELECT * FROM data_source WHERE id = {data_source['id']}")
    )
    db_data_source = result.fetchone()
    assert db_data_source is not None
    assert db_data_source.name == "New Data Source"


# Test getting a specific data source
@pytest.mark.asyncio
async def test_get_data_source(override_dependencies, test_db):
    # Create a test data source directly
    data_source = DataSource(
        name="Test Data Source",
        type="csv",
        user_id=TEST_USER.id,
        size_bytes=1000,
        row_count=10,
    )

    test_db.add(data_source)
    await test_db.commit()
    await test_db.refresh(data_source)

    # Make request to get data source
    response = client.get(
        f"/api/v1/sources/{data_source.id}",
        headers={"Authorization": "Bearer test_token"},
    )

    # Check response
    assert response.status_code == 200

    # Verify the response contains the test data source
    data_source_response = response.json()
    assert data_source_response["name"] == "Test Data Source"
    assert data_source_response["id"] == data_source.id


# Test renaming a data source
@pytest.mark.asyncio
async def test_rename_data_source(override_dependencies, test_db):
    # Create a test data source directly
    data_source = DataSource(
        name="Test Data Source",
        type="csv",
        user_id=TEST_USER.id,
        size_bytes=1000,
        row_count=10,
    )

    test_db.add(data_source)
    await test_db.commit()
    await test_db.refresh(data_source)

    # New name
    new_name = "Renamed Data Source"

    # Make request to rename data source
    response = client.put(
        f"/api/v1/sources/{data_source.id}/name",
        json=new_name,
        headers={"Authorization": "Bearer test_token"},
    )

    # Check response
    assert response.status_code == 200

    # Verify the response contains the renamed data source
    data_source_response = response.json()
    assert data_source_response["name"] == "Renamed Data Source"

    # Verify the data source was renamed in the database
    result = await test_db.execute(
        text(f"SELECT * FROM data_source WHERE id = {data_source.id}")
    )
    db_data_source = result.fetchone()
    assert db_data_source is not None
    assert db_data_source.name == "Renamed Data Source"


# Test deleting a data source
@pytest.mark.asyncio
async def test_delete_data_source(override_dependencies, test_db):
    # Create a test data source directly
    data_source = DataSource(
        name="Test Data Source",
        type="csv",
        user_id=TEST_USER.id,
        size_bytes=1000,
        row_count=10,
    )

    test_db.add(data_source)
    await test_db.commit()
    await test_db.refresh(data_source)

    # Make request to delete data source
    response = client.delete(
        f"/api/v1/sources/{data_source.id}",
        headers={"Authorization": "Bearer test_token"},
    )

    # Check response
    assert response.status_code == 204

    # Verify the data source was deleted from the database
    result = await test_db.execute(
        text(f"SELECT * FROM data_source WHERE id = {data_source.id}")
    )
    db_data_source = result.fetchone()
    assert db_data_source is None
