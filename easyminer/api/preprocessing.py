from typing import Annotated
from uuid import UUID

from fastapi import (
    APIRouter,
    Body,
    Depends,
    Form,
    HTTPException,
    Path,
    Query,
    Request,
    status,
)
from sqlalchemy import exists, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.config import API_V1_PREFIX
from easyminer.database import get_db_session
from easyminer.models.dataset import Dataset
from easyminer.schemas.preprocessing import (
    AttributeRead,
    AttributeValueRead,
    DatasetRead,
    TaskStatus,
)
from easyminer.tasks import create_dataset

router = APIRouter(prefix=API_V1_PREFIX, tags=["Preprocessing"])


@router.get("/dataset", response_model=list[DatasetRead])
async def list_datasets(db: Annotated[AsyncSession, Depends(get_db_session)]):
    """Display a list of all datasets within the user data space."""
    stmt = select(Dataset)
    datasets = (await db.execute(stmt)).scalars().all()
    return [DatasetRead.model_validate(dataset) for dataset in datasets]


@router.post("/dataset", response_model=TaskStatus, status_code=status.HTTP_202_ACCEPTED)
async def create_dataset_api(
    request: Request,
    dataSource: Annotated[int, Form()],
    name: Annotated[str, Form()],
):
    """Create a task for the dataset creation from a data source."""
    task = create_dataset.delay(dataSource, name)
    if task:
        return TaskStatus(
            task_id=UUID(task.task_id),
            task_name="create_dataset",
            status_message="Task created successfully",
            status_location=request.url_for("get_task_status", task_id=task.task_id).path,
            result_location=request.url_for("get_task_result", task_id=task.task_id).path,
        )
    else:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


@router.get(
    "/dataset/{id}",
    response_model=DatasetRead,
    responses={
        status.HTTP_404_NOT_FOUND: {},
    },
)
async def get_dataset(
    id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Get detail information about a dataset."""
    stmt = select(Dataset).where(Dataset.id == id)
    dataset = (await db.execute(stmt)).scalars().first()
    if dataset:
        return DatasetRead.model_validate(dataset)
    else:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)


@router.delete("/dataset/{id}")
async def delete_dataset(
    id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Delete this dataset."""
    # TODO: Implement this endpoint
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)


@router.put(
    "/dataset/{id}",
    responses={
        status.HTTP_204_NO_CONTENT: {},
        status.HTTP_404_NOT_FOUND: {},
    },
    status_code=status.HTTP_204_NO_CONTENT,
)
async def rename_dataset(
    id: Annotated[int, Path()],
    name: Annotated[str, Body()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> None:
    """Rename this dataset."""
    stmt = select(exists(Dataset).where(Dataset.id == id))
    result = await db.execute(stmt)
    if not result.scalar_one():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    stmt = update(Dataset).where(Dataset.id == id).values(name=name)
    _ = await db.execute(stmt)
    await db.commit()


# Attribute endpoints
@router.get("/dataset/{dataset_id}/attribute", response_model=list[AttributeRead])
async def list_attributes(
    dataset_id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Display a list of all attributes/columns for a specific dataset."""
    # TODO: Implement this endpoint
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)


@router.post(
    "/dataset/{dataset_id}/attribute",
    response_model=TaskStatus,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_attribute(
    dataset_id: Annotated[int, Path()],
    body: Annotated[str, Body()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Create a task for the attribute creation from a data source field."""
    # TODO: Implement this endpoint
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)


@router.get("/dataset/{dataset_id}/attribute/{attribute_id}", response_model=AttributeRead)
async def get_attribute(
    dataset_id: Annotated[int, Path()],
    attribute_id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Get detail information about an attribute of a specific dataset."""
    # TODO: Implement this endpoint
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)


@router.delete("/dataset/{dataset_id}/attribute/{attribute_id}")
async def delete_attribute(
    dataset_id: Annotated[int, Path()],
    attribute_id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Delete this attribute of a specific dataset."""
    # TODO: Implement this endpoint
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)


@router.put("/dataset/{dataset_id}/attribute/{attribute_id}")
async def rename_attribute(
    dataset_id: Annotated[int, Path()],
    attribute_id: Annotated[int, Path()],
    name: Annotated[str, Body()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Rename this attribute of a specific dataset."""
    # TODO: Implement this endpoint
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)


@router.get(
    "/dataset/{dataset_id}/attribute/{attribute_id}/values",
    response_model=list[AttributeValueRead],
)
async def list_values(
    dataset_id: Annotated[int, Path()],
    attribute_id: Annotated[int, Path()],
    offset: Annotated[int, Query(ge=0)],
    limit: Annotated[int, Query(gt=0, le=1000)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Display a list of all unique values for a specific attribute and dataset."""
    # TODO: Implement this endpoint
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)
