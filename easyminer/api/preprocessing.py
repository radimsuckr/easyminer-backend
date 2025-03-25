from typing import Annotated
from uuid import UUID

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Body,
    Depends,
    HTTPException,
    Path,
    Query,
    Request,
    status,
)
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.config import API_V1_PREFIX
from easyminer.database import get_db_session
from easyminer.schemas.preprocessing import (
    AttributeRead,
    AttributeValueRead,
    DatasetRead,
    TaskResult,
    TaskStatus,
)

router = APIRouter(prefix=API_V1_PREFIX, tags=["Preprocessing API"])


# Dataset endpoints
@router.get("/dataset", response_model=list[DatasetRead])
async def list_datasets(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Display a list of all datasets within the user data space."""
    # TODO: Implement this endpoint
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)


@router.post(
    "/dataset", response_model=TaskStatus, status_code=status.HTTP_202_ACCEPTED
)
async def create_dataset(
    request: Request,
    dataSource: Annotated[int, Body()],
    name: Annotated[str, Body()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    background_tasks: BackgroundTasks,
):
    """Create a task for the dataset creation from a data source."""
    # TODO: Implement this endpoint
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)


@router.get("/dataset/{id}", response_model=DatasetRead)
async def get_dataset(
    id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Get detail information about a dataset."""
    # TODO: Implement this endpoint
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)


@router.delete("/dataset/{id}")
async def delete_dataset(
    id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Delete this dataset."""
    # TODO: Implement this endpoint
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)


@router.put("/dataset/{id}")
async def rename_dataset(
    id: Annotated[int, Path()],
    name: Annotated[str, Body()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Rename this dataset."""
    # TODO: Implement this endpoint
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)


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
    background_tasks: BackgroundTasks,
):
    """Create a task for the attribute creation from a data source field."""
    # TODO: Implement this endpoint
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)


@router.get(
    "/dataset/{dataset_id}/attribute/{attribute_id}", response_model=AttributeRead
)
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


# Value endpoints
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
