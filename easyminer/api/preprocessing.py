from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Body, Depends, Form, HTTPException, Path, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from easyminer.api.task import router as task_router
from easyminer.config import API_V1_PREFIX
from easyminer.database import get_db_session
from easyminer.models.preprocessing import Attribute, Dataset, DatasetValue
from easyminer.schemas.preprocessing import (
    AttributeRead,
    AttributeValueRead,
    DatasetRead,
    TaskStatus,
)
from easyminer.tasks.create_attribute import create_attributes
from easyminer.tasks.create_dataset import create_dataset

router = APIRouter(prefix=API_V1_PREFIX, tags=["Preprocessing"])


@router.get("/dataset")
async def list_datasets(
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> list[DatasetRead]:
    """Display a list of all datasets within the user data space."""
    datasets = (await db.execute(select(Dataset).options(joinedload(Dataset.data_source)))).scalars().all()
    return [DatasetRead.model_validate(dataset) for dataset in datasets]


@router.post("/dataset", response_model=TaskStatus, status_code=status.HTTP_202_ACCEPTED)
async def create_dataset_api(dataSource: Annotated[int, Form()], name: Annotated[str, Form()]):
    """Create a task for the dataset creation from a data source."""
    task = create_dataset.delay(dataSource, name)
    if task:
        return TaskStatus(
            task_id=UUID(task.task_id),
            task_name="create_dataset",
            status_message="Task created successfully",
            status_location=task_router.url_path_for("get_task_status", task_id=task.task_id),
            result_location=task_router.url_path_for("get_task_result", task_id=task.task_id),
        )
    else:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


@router.get(
    "/dataset/{id}",
    response_model=DatasetRead,
    responses={status.HTTP_404_NOT_FOUND: {}},
)
async def get_dataset(db: Annotated[AsyncSession, Depends(get_db_session)], id: Annotated[int, Path()]):
    """Get detail information about a dataset."""
    dataset = await db.get(Dataset, id)
    if dataset:
        return DatasetRead.model_validate(dataset)
    else:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)


@router.delete("/dataset/{id}")
async def delete_dataset(db: Annotated[AsyncSession, Depends(get_db_session)], id: Annotated[int, Path()]):
    """Delete this dataset."""
    dataset = await db.get(Dataset, id)
    if not dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    await db.delete(dataset)
    await db.commit()


@router.put(
    "/dataset/{id}",
    responses={status.HTTP_204_NO_CONTENT: {}, status.HTTP_404_NOT_FOUND: {}},
    status_code=status.HTTP_204_NO_CONTENT,
)
async def rename_dataset(
    db: Annotated[AsyncSession, Depends(get_db_session)],
    id: Annotated[int, Path()],
    name: Annotated[str, Body(examples=["A New Exciting Name"], media_type="text/plain")],
) -> None:
    """Rename this dataset."""
    dataset = await db.get(Dataset, id)
    if not dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    dataset.name = name
    await db.commit()


@router.get("/dataset/{dataset_id}/attribute")
async def list_attributes(
    db: Annotated[AsyncSession, Depends(get_db_session)],
    dataset_id: Annotated[int, Path()],
) -> list[AttributeRead]:
    """Display a list of all attributes/columns for a specific dataset."""
    dataset = await db.get(Dataset, dataset_id, options=[joinedload(Dataset.attributes)])
    if not dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    return [AttributeRead.model_validate(attribute) for attribute in dataset.attributes]


@router.post("/dataset/{dataset_id}/attribute", status_code=status.HTTP_202_ACCEPTED)
async def create_attribute(
    db: Annotated[AsyncSession, Depends(get_db_session)],
    dataset_id: Annotated[int, Path()],
    body: Annotated[str, Body(media_type="application/xml")],
) -> TaskStatus:
    """Create a task for the attribute creation from a data source field."""
    dataset = await db.get(Dataset, dataset_id)
    if not dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    task = create_attributes.delay(dataset_id, body)
    if task:
        return TaskStatus(
            task_id=UUID(task.task_id),
            task_name="create_attribute",
            status_message="Task created successfully",
            status_location=task_router.url_path_for("get_task_status", task_id=task.task_id),
            result_location=task_router.url_path_for("get_task_result", task_id=task.task_id),
        )
    else:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


@router.delete("/dataset/{dataset_id}/attribute/{attribute_id}")
async def delete_attribute(
    db: Annotated[AsyncSession, Depends(get_db_session)],
    dataset_id: Annotated[int, Path()],
    attribute_id: Annotated[int, Path()],
) -> None:
    """Delete this attribute of a specific dataset."""
    dataset = await db.get(Dataset, dataset_id, options=[joinedload(Dataset.attributes)])
    if not dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    attribute = await db.scalar(
        select(Attribute).where(Attribute.id == attribute_id).where(Attribute.dataset_id == dataset_id)
    )
    if not attribute:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Attribute not found")

    await db.delete(attribute)
    await db.commit()


@router.get("/dataset/{dataset_id}/attribute/{attribute_id}")
async def get_attribute(
    db: Annotated[AsyncSession, Depends(get_db_session)],
    dataset_id: Annotated[int, Path()],
    attribute_id: Annotated[int, Path()],
) -> AttributeRead:
    """Get detail information about an attribute of a specific dataset."""
    dataset = await db.get(Dataset, dataset_id, options=[joinedload(Dataset.attributes)])
    if not dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    attribute = await db.scalar(
        select(Attribute).where(Attribute.id == attribute_id).where(Attribute.dataset_id == dataset_id)
    )
    if not attribute:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Attribute not found")

    return AttributeRead.model_validate(attribute)


@router.put("/dataset/{dataset_id}/attribute/{attribute_id}")
async def rename_attribute(
    db: Annotated[AsyncSession, Depends(get_db_session)],
    dataset_id: Annotated[int, Path()],
    attribute_id: Annotated[int, Path()],
    name: Annotated[str, Body(media_type="text/plain", examples=["A New Exciting Name"])],
):
    """Rename this attribute of a specific dataset."""
    dataset = await db.get(Dataset, dataset_id, options=[joinedload(Dataset.attributes)])
    if not dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    attribute = await db.scalar(
        select(Attribute).where(Attribute.id == attribute_id).where(Attribute.dataset_id == dataset_id)
    )
    if not attribute:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Attribute not found")

    attribute.name = name
    await db.commit()


@router.get("/dataset/{dataset_id}/attribute/{attribute_id}/values")
async def list_values(
    db: Annotated[AsyncSession, Depends(get_db_session)],
    dataset_id: Annotated[int, Path()],
    attribute_id: Annotated[int, Path()],
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(gt=0, le=1000)] = 100,
) -> list[AttributeValueRead]:
    """Display a list of all unique values for a specific attribute and dataset."""
    dataset = await db.get(Dataset, dataset_id)
    if not dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    attribute = await db.scalar(
        select(Attribute).where(Attribute.id == attribute_id).where(Attribute.dataset_id == dataset_id)
    )
    if not attribute:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Attribute not found")

    values = (
        (
            await db.execute(
                select(DatasetValue).where(DatasetValue.attribute_id == attribute.id).offset(offset).limit(limit)
            )
        )
        .scalars()
        .all()
    )
    return [AttributeValueRead.model_validate(value) for value in values]
