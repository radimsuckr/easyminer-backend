import logging
from typing import Annotated
from uuid import UUID

import celery.result
from fastapi import APIRouter, HTTPException, Path, Request, Response, status
from sqlalchemy import select

from easyminer.config import API_V1_PREFIX
from easyminer.dependencies import AuthenticatedSession
from easyminer.models.task import Task, TaskStatusEnum
from easyminer.schemas.task import TaskStatus

router = APIRouter(prefix=API_V1_PREFIX, tags=["Tasks"])


@router.get(
    "/task-status/{task_id}",
    responses={
        status.HTTP_200_OK: {"description": "Task is still in progress"},
        status.HTTP_201_CREATED: {"description": "Task has been completed successfully"},
        status.HTTP_400_BAD_REQUEST: {"description": "Task failed with a known exception"},
        status.HTTP_404_NOT_FOUND: {"description": "Task not found"},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Task failed with a fatal error"},
    },
)
async def get_task_status(
    db: AuthenticatedSession, request: Request, response: Response, task_id: Annotated[UUID, Path()]
) -> TaskStatus:
    task = await db.scalar(select(Task).where(Task.task_id == task_id))
    if not task:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")

    match task.status:
        case TaskStatusEnum.success:
            response.status_code = status.HTTP_201_CREATED
            result_url = str(request.url_for("get_task_result", task_id=task.task_id))
            response.headers["Location"] = result_url
            return TaskStatus(
                task_id=task.task_id,
                task_name=task.name,
                status_message=task.status_message if task.status_message else "Task completed successfully",
                result_location=result_url,
            )
        case TaskStatusEnum.failure:
            error_message = task.status_message or "Task failed"
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error_message)
        case TaskStatusEnum.pending | TaskStatusEnum.scheduled | TaskStatusEnum.started:
            response.status_code = status.HTTP_200_OK
            return TaskStatus(
                task_id=task.task_id,
                task_name=task.name,
                status_message=task.status_message if task.status_message else f"Task is {task.status.name}",
                status_location=str(request.url_for("get_task_status", task_id=task.task_id)),
            )


@router.get("/task-result/{task_id}", status_code=status.HTTP_200_OK)
async def get_task_result(task_id: Annotated[UUID, Path()]):
    logger = logging.getLogger(__name__)

    ares: celery.result.AsyncResult[str] = celery.result.AsyncResult(str(task_id))
    if not ares.ready():
        raise HTTPException(status_code=status.HTTP_202_ACCEPTED, detail="Task is still processing")
    try:
        result = ares.result
    except BaseException as e:
        logger.error(f"Error getting task result: {e}")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task result not found")
    return result
