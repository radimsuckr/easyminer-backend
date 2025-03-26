import uuid
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.crud.aio.task import (
    create_task,
    get_task_by_id,
    update_task_status,
)
from easyminer.models.data import DataSource
from easyminer.models.task import Task, TaskStatusEnum


@pytest.mark.asyncio
async def test_create_task(db_session: AsyncSession):
    """Test creating a new task with SQLite."""
    # Generate a random UUID for the task
    task_id = UUID(str(uuid.uuid4()))

    # Create a new task
    task = await create_task(
        db_session=db_session,
        task_id=task_id,
        name="test_task",
        data_source_id=42,
    )

    # Check that the task was created correctly
    assert task.id is not None
    assert task.task_id == task_id
    assert task.name == "test_task"
    assert task.status == TaskStatusEnum.pending
    assert task.status_message == "Task created and waiting to start"
    assert task.data_source_id == 42
    assert task.result_location is None

    # Check that the task is in the database
    result = await db_session.execute(select(Task).where(Task.task_id == task_id))
    db_task = result.scalar_one()
    assert db_task.id == task.id
    assert db_task.task_id == task_id
    assert db_task.name == "test_task"
    assert db_task.status == TaskStatusEnum.pending
    assert db_task.data_source_id == 42


@pytest.mark.asyncio
async def test_get_task_by_id(db_session: AsyncSession, test_data_source: DataSource):
    """Test getting a task by ID successfully."""
    # Create a data source
    data_source = test_data_source

    # Create a UUID for the task
    task_id = UUID("12345678-1234-5678-1234-567812345678")

    # Create a task
    created_task = await create_task(
        db_session=db_session,
        task_id=task_id,
        name="get_task_test",
        data_source_id=data_source.id,
    )

    # Get the task by ID
    task = await get_task_by_id(db_session, task_id)
    assert task is not None
    assert task.id == created_task.id
    assert task.task_id == task_id
    assert task.name == "get_task_test"
    assert task.status == TaskStatusEnum.pending


@pytest.mark.asyncio
async def test_get_task_by_id_nonexistent(db_session: AsyncSession):
    """Test getting a non-existent task returns None."""
    # Test getting non-existent task
    non_existent_id = UUID("99999999-9999-9999-9999-999999999999")
    task = await get_task_by_id(db_session, non_existent_id)
    assert task is None


@pytest.mark.asyncio
async def test_update_task_status(
    db_session: AsyncSession, test_data_source: DataSource
):
    """Test updating a task status."""
    # Create a data source
    data_source = test_data_source

    # Create a UUID for the task
    task_id = UUID("87654321-4321-8765-4321-876543210987")

    # Create a task
    _ = await create_task(
        db_session=db_session,
        task_id=task_id,
        name="update_status_test",
        data_source_id=data_source.id,
    )

    # Update the task status
    updated_task = await update_task_status(
        db_session,
        task_id,
        TaskStatusEnum.started,
        "Task is currently processing",
        None,
    )
    assert updated_task is not None
    assert updated_task.task_id == task_id
    assert updated_task.status == TaskStatusEnum.started
    assert updated_task.status_message == "Task is currently processing"
    assert updated_task.result_location is None

    # Check that the status was updated in the database
    result = await db_session.execute(select(Task).where(Task.task_id == task_id))
    db_task = result.scalar_one()
    assert db_task.status == TaskStatusEnum.started
    assert db_task.status_message == "Task is currently processing"

    # Update with result location
    updated_task = await update_task_status(
        db_session,
        task_id,
        TaskStatusEnum.success,
        "Task completed successfully",
        "/path/to/result.json",
    )
    assert updated_task is not None
    assert updated_task.status == TaskStatusEnum.success
    assert updated_task.status_message == "Task completed successfully"
    assert updated_task.result_location == "/path/to/result.json"

    # Verify the update in the database
    result = await db_session.execute(select(Task).where(Task.task_id == task_id))
    db_task = result.scalar_one()
    assert db_task.status == TaskStatusEnum.success
    assert db_task.result_location == "/path/to/result.json"


@pytest.mark.asyncio
async def test_update_task_status_nonexistent(db_session: AsyncSession):
    """Test updating a non-existent task."""
    # Try to update a non-existent task
    non_existent_id = UUID("99999999-9999-9999-9999-999999999999")
    updated_task = await update_task_status(
        db_session,
        non_existent_id,
        TaskStatusEnum.success,
        "This should not update anything",
    )
    assert updated_task is None

    # Verify no task was created
    result = await db_session.execute(
        select(Task).where(Task.task_id == non_existent_id)
    )
    db_task = result.scalar_one_or_none()
    assert db_task is None
