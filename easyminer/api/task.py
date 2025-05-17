import logging
from typing import Annotated
from uuid import UUID

import celery.result
from fastapi import APIRouter, Depends, HTTPException, Path, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.config import API_V1_PREFIX
from easyminer.database import get_db_session
from easyminer.models import Task
from easyminer.schemas.task import TaskStatus

router = APIRouter(prefix=API_V1_PREFIX, tags=["Tasks"])


@router.get(
    "/task-status/{task_id}",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_404_NOT_FOUND: {},
    },
)
async def get_task_status(
    db: Annotated[AsyncSession, Depends(get_db_session)], request: Request, task_id: Annotated[UUID, Path()]
) -> TaskStatus:
    task = await db.get(Task, task_id)
    if not task:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")

    return TaskStatus(
        task_id=task.task_id,
        task_name=task.name,
        status_message=task.status_message if task.status_message else task.status.name,
        status_location=request.url_for("get_task_status", task_id=task.task_id).path,
        result_location=request.url_for("get_task_result", task_id=task.task_id).path,
    )


@router.get("/task-result/{task_id}", status_code=status.HTTP_200_OK)
async def get_task_result(task_id: Annotated[UUID, Path()]):
    logger = logging.getLogger(__name__)

    ares = celery.result.AsyncResult(str(task_id))
    if not ares.ready():
        raise HTTPException(status_code=status.HTTP_202_ACCEPTED, detail="Task not ready")
    try:
        result = ares.result
    except BaseException as e:
        logger.error(f"Error getting task result: {e}")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task result not found")
    return result
