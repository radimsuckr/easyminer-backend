import logging
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.models.task import Task

logger = logging.getLogger(__name__)


async def create_task(
    db_session: AsyncSession,
    task_id: UUID,
    name: str,
    user_id: int,
    data_source_id: int | None = None,
) -> Task:
    """Create a new task entry in the database."""
    logger.info(
        f"Creating task: {task_id}, {name}, user={user_id}, source={data_source_id}"
    )
    try:
        task = Task(
            task_id=task_id,
            name=name,
            status="pending",
            status_message="Task created and waiting to start",
            user_id=user_id,
            data_source_id=data_source_id,
        )
        db_session.add(task)
        await db_session.commit()
        await db_session.refresh(task)
        logger.info(f"Task created successfully: {task.id}")
        return task
    except Exception as e:
        logger.error(f"Error creating task: {str(e)}")
        raise


async def get_task_by_id(db_session: AsyncSession, task_id: UUID) -> Task | None:
    """Get a task by its UUID."""
    result = await db_session.execute(select(Task).where(Task.task_id == task_id))
    return result.scalar_one_or_none()


async def update_task_status(
    db_session: AsyncSession,
    task_id: UUID,
    status: str,
    status_message: str | None = None,
    result_location: str | None = None,
) -> Task | None:
    """Update the status of a task."""
    task = await get_task_by_id(db_session, task_id)
    if task:
        task.status = status
        if status_message:
            task.status_message = status_message
        if result_location:
            task.result_location = result_location
        await db_session.commit()
        await db_session.refresh(task)
    return task
