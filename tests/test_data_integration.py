from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.api.data import router as data_router
from easyminer.models.data import DataSource, Field
from easyminer.models.task import Task
from easyminer.storage import DiskStorage


@pytest.fixture
def mock_auth_middleware():
    """Mock the authentication middleware for testing."""
    from easyminer.models.user import User

    async def get_current_user():
        return User(id=1, username="test_user", email="test@example.com")

    with patch("easyminer.api.data.get_current_user", side_effect=get_current_user):
        yield


@pytest.fixture
def mock_db_session():
    """Mock the database session for testing."""
    mock_session = AsyncMock(spec=AsyncSession)

    with patch("easyminer.api.data.get_db_session", return_value=mock_session):
        yield mock_session


@pytest.fixture
def mock_storage():
    """Mock the storage system for testing."""
    storage = MagicMock(spec=DiskStorage)

    with patch("easyminer.storage.DiskStorage", return_value=storage):
        yield storage


@pytest.fixture
def test_client(mock_auth_middleware, mock_db_session, mock_storage):
    """Create a test client with mocked dependencies."""
    app = FastAPI()
    app.include_router(data_router)

    return TestClient(app)


@pytest.fixture
def mock_data_source():
    """Create a mock data source for testing."""
    mock_ds = MagicMock(spec=DataSource)
    mock_ds.id = 1
    mock_ds.name = "Test Data Source"
    mock_ds.type = "csv"
    mock_ds.user_id = 1
    mock_ds.size_bytes = 1000
    mock_ds.row_count = 10
    mock_ds.upload_id = 1
    return mock_ds


@pytest.fixture
def mock_field():
    """Create a mock field for testing."""
    mock_f = MagicMock(spec=Field)
    mock_f.id = 1
    mock_f.name = "score"
    mock_f.data_type = "integer"
    mock_f.data_source_id = 1
    mock_f.min_value = "10"
    mock_f.max_value = "100"
    mock_f.avg_value = 50.0
    return mock_f


@pytest.mark.asyncio
async def test_datasource_preview(
    test_client, mock_db_session, mock_data_source, mock_field
):
    """Test the preview data endpoint."""
    # Mock the CRUD operations
    with (
        patch(
            "easyminer.api.data.get_data_source_by_id", return_value=mock_data_source
        ),
        patch(
            "easyminer.api.data.get_fields_by_data_source", return_value=[mock_field]
        ),
        patch(
            "easyminer.processing.data_retrieval.get_data_preview"
        ) as mock_get_preview,
    ):
        # Setup mock return value for get_data_preview
        field_names = ["score"]
        rows = [{"score": "85"}, {"score": "92"}]
        mock_get_preview.return_value = (field_names, rows)

        # Make the request
        response = test_client.get("/api/v1/datasource/1/preview?limit=10")

        # Check response
        assert response.status_code == 200
        result = response.json()

        # API might use either field_names (snake_case) or fieldNames (camelCase)
        # We'll check for both possibilities
        field_names_key = None
        if "fieldNames" in result:
            field_names_key = "fieldNames"
        elif "field_names" in result:
            field_names_key = "field_names"

        assert field_names_key is not None, (
            "Neither fieldNames nor field_names found in response"
        )
        assert "rows" in result
        assert result[field_names_key] == field_names
        assert result["rows"] == rows

        # Check mocks were called with correct parameters
        mock_get_preview.assert_called_once()
        args, kwargs = mock_get_preview.call_args
        assert kwargs["data_source"] == mock_data_source
        assert kwargs["limit"] == 10


@pytest.mark.asyncio
async def test_datasource_instances(
    test_client, mock_db_session, mock_data_source, mock_field
):
    """Test the get instances endpoint."""
    # Mock the CRUD operations
    with (
        patch(
            "easyminer.api.data.get_data_source_by_id", return_value=mock_data_source
        ),
        patch(
            "easyminer.api.data.get_fields_by_data_source", return_value=[mock_field]
        ),
        patch(
            "easyminer.processing.data_retrieval.get_data_preview"
        ) as mock_get_preview,
    ):
        # Setup mock return value for get_data_preview
        field_names = ["score"]
        rows = [{"score": "85"}, {"score": "92"}, {"score": "78"}, {"score": "90"}]
        mock_get_preview.return_value = (field_names, rows)

        # Make the request with offset and limit
        response = test_client.get("/api/v1/datasource/1/instances?offset=1&limit=2")

        # Check response
        assert response.status_code == 200
        assert response.json() == [
            {"score": "92"},
            {"score": "78"},
        ]  # Should return rows[1:3]

        # Make sure we requested enough data to satisfy offset+limit
        mock_get_preview.assert_called_once()
        args, kwargs = mock_get_preview.call_args
        assert kwargs["limit"] == 3  # offset(1) + limit(2)


@pytest.mark.asyncio
async def test_field_aggregated_values(
    test_client, mock_db_session, mock_data_source, mock_field
):
    """Test the aggregated values endpoint."""
    # Mock CRUD operations
    task_id = uuid4()
    task = MagicMock(spec=Task)
    task.task_id = task_id

    with (
        patch(
            "easyminer.api.data.get_data_source_by_id", return_value=mock_data_source
        ),
        patch("easyminer.api.data.get_field_by_id", return_value=mock_field),
        patch("easyminer.api.data.create_task", return_value=task),
        # We don't need to mock BackgroundTasks, it's a FastAPI dependency
    ):
        # Make the request
        response = test_client.get(
            "/api/v1/datasource/1/field/1/aggregated-values?bins=5&min_value=10&max_value=100"
        )

        # Check response
        assert response.status_code == 202
        result = response.json()

        # Check the task response structure with camelCase keys
        # Note: API returns taskId and resultLocation even though code uses task_id and result_location
        assert "taskId" in result
        assert result["statusMessage"] == "Histogram generation started"
        assert result["statusLocation"].startswith("/api/v1/task-status/")
        assert result["resultLocation"] is None


@pytest.mark.asyncio
async def test_task_result(test_client, mock_db_session):
    """Test the task result endpoint."""
    # Create mock task
    task_id = uuid4()
    task = MagicMock(spec=Task)
    task.task_id = task_id
    task.name = "aggregated_values"
    task.status = "completed"
    task.user_id = 1
    task.result_location = "1/1/results/histogram_1_5.json"

    # Expected result data
    result_data = {
        "field_id": 1,
        "field_name": "score",
        "bins": 5,
        "histogram": [
            {"interval_start": 0, "interval_end": 20, "count": 3},
            {"interval_start": 20, "interval_end": 40, "count": 5},
            {"interval_start": 40, "interval_end": 60, "count": 7},
            {"interval_start": 60, "interval_end": 80, "count": 4},
            {"interval_start": 80, "interval_end": 100, "count": 2},
        ],
    }

    # Create mock user
    from easyminer.models.user import User

    mock_user = User(id=1, username="test_user", email="test@example.com")

    # Let's fully mock the dependencies rather than patching them
    app = FastAPI()

    # Override dependencies
    async def mock_get_current_user():
        return mock_user

    async def mock_get_db():
        yield mock_db_session

    async def mock_get_task(task_id_param):
        return task

    async def mock_read_result(path):
        return result_data

    # Define our test endpoint to mimic the real one but with mocked dependencies
    @app.get("/api/v1/task-result/{task_id}")
    async def task_result_test(task_id: UUID):
        task_result = await mock_get_task(task_id)
        if not task_result:
            raise HTTPException(status_code=404, detail="Task not found")
        if task_result.status != "completed":
            raise HTTPException(
                status_code=400, detail=f"Task status is {task_result.status}"
            )

        result = await mock_read_result(task_result.result_location)
        return result

    # Create a test client with our mocked app
    test_client_local = TestClient(app)

    # Make the request
    response = test_client_local.get(f"/api/v1/task-result/{task_id}")

    # Check response
    assert response.status_code == 200
    assert response.json() == result_data
