import uuid

import pytest
from fastapi import status
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.models.task import Task, TaskStatusEnum

from .conftest import BASE_URL


@pytest.mark.asyncio
async def test_task_not_found(async_client: AsyncClient):
    task_id = uuid.uuid4()
    response = await async_client.get(f"/api/v1/task-status/{task_id}")
    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json()["message"] == "Task not found"


@pytest.mark.asyncio
async def test_task_pending_returns_200(async_client: AsyncClient, async_db_session: AsyncSession):
    """Test that pending task returns 200 with statusLocation."""
    task_id = uuid.uuid4()
    task = Task(task_id=task_id, name="test_task", status=TaskStatusEnum.pending, status_message="Task is pending")
    async_db_session.add(task)
    await async_db_session.commit()

    response = await async_client.get(f"/api/v1/task-status/{task_id}")
    assert response.status_code == status.HTTP_200_OK

    data = response.json()
    assert data["taskId"] == str(task_id)
    assert data["taskName"] == "test_task"
    assert data["statusMessage"] == "Task is pending"
    assert data["statusLocation"] == f"{BASE_URL}/api/v1/task-status/{task_id}"
    assert "resultLocation" not in data or data.get("resultLocation") is None
    assert "Location" not in response.headers


@pytest.mark.asyncio
async def test_task_scheduled_returns_200(async_client: AsyncClient, async_db_session: AsyncSession):
    """Test that scheduled task returns 200 with statusLocation."""
    task_id = uuid.uuid4()
    task = Task(
        task_id=task_id,
        name="test_task",
        status=TaskStatusEnum.scheduled,
    )
    async_db_session.add(task)
    await async_db_session.commit()

    response = await async_client.get(f"/api/v1/task-status/{task_id}")
    assert response.status_code == status.HTTP_200_OK

    data = response.json()
    assert data["taskId"] == str(task_id)
    assert data["taskName"] == "test_task"
    assert data["statusMessage"] == "Task is scheduled"
    assert data["statusLocation"] == f"{BASE_URL}/api/v1/task-status/{task_id}"
    assert "resultLocation" not in data or data.get("resultLocation") is None


@pytest.mark.asyncio
async def test_task_started_returns_200(async_client: AsyncClient, async_db_session: AsyncSession):
    """Test that started task returns 200 with statusLocation."""
    task_id = uuid.uuid4()
    task = Task(
        task_id=task_id,
        name="test_task",
        status=TaskStatusEnum.started,
        status_message="Processing data",
    )
    async_db_session.add(task)
    await async_db_session.commit()

    response = await async_client.get(f"/api/v1/task-status/{task_id}")
    assert response.status_code == status.HTTP_200_OK

    data = response.json()
    assert data["taskId"] == str(task_id)
    assert data["taskName"] == "test_task"
    assert data["statusMessage"] == "Processing data"
    assert data["statusLocation"] == f"{BASE_URL}/api/v1/task-status/{task_id}"
    assert "resultLocation" not in data or data.get("resultLocation") is None


@pytest.mark.asyncio
async def test_task_success_returns_201_with_location(async_client: AsyncClient, async_db_session: AsyncSession):
    """Test that successful task returns 201 with Location header and resultLocation."""
    task_id = uuid.uuid4()
    task = Task(
        task_id=task_id,
        name="test_task",
        status=TaskStatusEnum.success,
        status_message="Task completed",
    )
    async_db_session.add(task)
    await async_db_session.commit()

    response = await async_client.get(f"/api/v1/task-status/{task_id}")
    assert response.status_code == status.HTTP_201_CREATED

    assert "Location" in response.headers
    assert response.headers["Location"] == f"{BASE_URL}/api/v1/task-result/{task_id}"

    data = response.json()
    assert data["taskId"] == str(task_id)
    assert data["taskName"] == "test_task"
    assert data["statusMessage"] == "Task completed"
    assert data["resultLocation"] == f"{BASE_URL}/api/v1/task-result/{task_id}"
    assert "statusLocation" not in data or data.get("statusLocation") is None


@pytest.mark.asyncio
async def test_task_success_without_message_returns_201(async_client: AsyncClient, async_db_session: AsyncSession):
    """Test that successful task without message returns default message."""
    task_id = uuid.uuid4()
    task = Task(
        task_id=task_id,
        name="test_task",
        status=TaskStatusEnum.success,
    )
    async_db_session.add(task)
    await async_db_session.commit()

    response = await async_client.get(f"/api/v1/task-status/{task_id}")
    assert response.status_code == status.HTTP_201_CREATED

    data = response.json()
    assert data["statusMessage"] == "Task completed successfully"


@pytest.mark.asyncio
async def test_task_failure_returns_400(async_client: AsyncClient, async_db_session: AsyncSession):
    """Test that failed task returns 400 with error message."""
    task_id = uuid.uuid4()
    task = Task(
        task_id=task_id,
        name="test_task",
        status=TaskStatusEnum.failure,
        status_message="Database connection failed",
    )
    async_db_session.add(task)
    await async_db_session.commit()

    response = await async_client.get(f"/api/v1/task-status/{task_id}")
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert response.json()["message"] == "Database connection failed"


@pytest.mark.asyncio
async def test_task_failure_without_message_returns_400(async_client: AsyncClient, async_db_session: AsyncSession):
    """Test that failed task without message returns default error."""
    task_id = uuid.uuid4()
    task = Task(
        task_id=task_id,
        name="test_task",
        status=TaskStatusEnum.failure,
    )
    async_db_session.add(task)
    await async_db_session.commit()

    response = await async_client.get(f"/api/v1/task-status/{task_id}")
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert response.json()["message"] == "Task failed"


@pytest.mark.asyncio
async def test_response_schema_required_fields(async_client: AsyncClient, async_db_session: AsyncSession):
    """Test that response always contains required fields: taskId and taskName."""
    task_id = uuid.uuid4()
    task = Task(task_id=task_id, name="test_task", status=TaskStatusEnum.pending)
    async_db_session.add(task)
    await async_db_session.commit()

    response = await async_client.get(f"/api/v1/task-status/{task_id}")
    assert response.status_code == status.HTTP_200_OK

    data = response.json()
    assert "taskId" in data
    assert "taskName" in data
    assert isinstance(data["taskId"], str)
    assert isinstance(data["taskName"], str)


@pytest.mark.asyncio
async def test_response_schema_optional_fields_conditional(async_client: AsyncClient, async_db_session: AsyncSession):
    """Test that optional fields appear conditionally based on task status."""
    task_id_pending = uuid.uuid4()
    task_pending = Task(task_id=task_id_pending, name="pending_task", status=TaskStatusEnum.pending)
    async_db_session.add(task_pending)

    task_id_success = uuid.uuid4()
    task_success = Task(task_id=task_id_success, name="success_task", status=TaskStatusEnum.success)
    async_db_session.add(task_success)
    await async_db_session.commit()

    response_pending = await async_client.get(f"/api/v1/task-status/{task_id_pending}")
    data_pending = response_pending.json()
    assert "statusLocation" in data_pending
    assert data_pending.get("resultLocation") is None

    response_success = await async_client.get(f"/api/v1/task-status/{task_id_success}")
    data_success = response_success.json()
    assert "resultLocation" in data_success
    assert data_success.get("statusLocation") is None
