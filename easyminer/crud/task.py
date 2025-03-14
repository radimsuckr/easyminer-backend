import logging
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.models.task import Task

logger = logging.getLogger(__name__)


async def create_task(
    db_session: AsyncSession,
    task_id: UUID,
    name: str,
    data_source_id: int,
) -> Task:
    """Create a new task."""
    logger.info(f"Creating task: {task_id}, {name}, source={data_source_id}")
    try:
        task = Task(
            task_id=task_id,
            name=name,
            status="pending",
            status_message="Task created and waiting to start",
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


async def get_task_by_id(db: AsyncSession, task_id: UUID) -> Task | None:
    """Get a task by ID."""
    result = await db.execute(select(Task).where(Task.task_id == task_id))
    return result.scalars().first()


async def update_task_status(
    db: AsyncSession,
    task_id: UUID,
    status: str,
    status_message: str | None = None,
    result_location: str | None = None,
) -> Task | None:
    """Update a task status."""
    update_values = {"status": status}
    if status_message is not None:
        update_values["status_message"] = status_message
    if result_location is not None:
        update_values["result_location"] = result_location

    await db.execute(
        update(Task).where(Task.task_id == task_id).values(**update_values)
    )
    await db.commit()
    return await get_task_by_id(db, task_id)
