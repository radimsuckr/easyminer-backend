from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.config import API_V1_PREFIX
from easyminer.crud.aio.task import get_task_by_id
from easyminer.database import get_db_session
from easyminer.models import TaskStatusEnum
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
    request: Request,
    task_id: Annotated[UUID, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> TaskStatus:
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Task not found"
        )

    return TaskStatus(
        task_id=task.task_id,
        task_name=task.name,
        status_message=task.status_message if task.status_message else task.status.name,
        status_location=request.url_for("get_task_status", task_id=task.task_id).path,
        result_location=request.url_for("get_task_result", task_id=task.task_id).path,
    )


@router.get("/task-result/{task_id}", status_code=status.HTTP_200_OK)
async def get_task_result(
    task_id: Annotated[UUID, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    task = await get_task_by_id(db, task_id, eager=True)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Task not found"
        )

    # Check if the task is completed
    if task.status != TaskStatusEnum.success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Task is not completed yet (current status: {task.status})",
        )

    if not task.result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task result not found",
        )

    return task.result.value
