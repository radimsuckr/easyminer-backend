from enum import Enum
from typing import Annotated, Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, UploadFile, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.api.dependencies.auth import get_current_user
from easyminer.api.dependencies.db import get_db_session
from easyminer.models import DataSource, Field as FieldModel, Upload, User
from easyminer.schemas.data import DataSourceCreate, DataSourceRead, UploadSettings

router = APIRouter(prefix="/api/v1/data", tags=["Data"])


class MediaType(str, Enum):
    csv = "csv"
    rdf = "rdf"


class DbType(str, Enum):
    limited = "limited"
    unlimited = "unlimited"


class CompressionType(str, Enum):
    zip = "zip"
    gzip = "gzip"
    bzip2 = "bzip2"


class RdfFormat(str, Enum):
    nt = "nt"
    nq = "nq"
    ttl = "ttl"


class Stats(BaseModel):
    min: float
    max: float
    avg: float


class Value(BaseModel):
    id: int
    frequency: int
    value: str | float | None


class TaskStatus(BaseModel):
    task_id: UUID
    task_name: str
    status_message: Optional[str] = None
    status_location: Optional[str] = None
    result_location: Optional[str] = None


@router.post("/upload/start", response_model=UUID)
async def start_upload(
    settings: UploadSettings,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Start a new upload process."""
    upload_id = uuid4()
    upload = Upload(
        uuid=str(upload_id),
        name=settings.name,
        media_type=settings.media_type,
        db_type=settings.db_type,
        separator=settings.separator,
        encoding=settings.encoding,
        quotes_char=settings.quotes_char,
        escape_char=settings.escape_char,
        locale=settings.locale,
        compression=settings.compression,
        format=settings.format,
    )
    db.add(upload)
    await db.commit()
    return upload_id


@router.post("/upload/{upload_id}", status_code=status.HTTP_202_ACCEPTED)
async def upload_chunk(
    upload_id: UUID,
    chunk: UploadFile,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Upload a chunk of data."""
    # TODO: Implement chunk processing
    pass


@router.post("/upload/preview/start", response_model=UUID)
async def start_preview_upload(
    settings: UploadSettings,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    max_lines: Annotated[int, Query(gt=0)] = 100,
):
    """Start a preview upload process."""
    upload_id = uuid4()
    upload = Upload(
        uuid=str(upload_id),
        name=f"preview_{settings.name}",
        media_type=settings.media_type,
        db_type=settings.db_type,
        separator=settings.separator,
        encoding=settings.encoding,
        quotes_char=settings.quotes_char,
        escape_char=settings.escape_char,
        locale=settings.locale,
        compression=settings.compression,
        format=settings.format,
        preview_max_lines=max_lines,
    )
    db.add(upload)
    await db.commit()
    return upload_id


@router.post("/upload/preview/{upload_id}", status_code=status.HTTP_202_ACCEPTED)
async def upload_preview_chunk(
    upload_id: UUID,
    chunk: UploadFile,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Upload a chunk of preview data."""
    # TODO: Implement preview chunk processing
    pass


@router.get("/sources", response_model=list[DataSourceRead])
async def list_data_sources(
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """List all data sources for the current user."""
    return user.data_sources


@router.post("/sources", response_model=DataSourceRead)
async def create_data_source(
    data: DataSourceCreate,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Create a new data source."""
    data_source = DataSource(**data.model_dump(), user_id=user.id)
    db.add(data_source)
    await db.commit()
    await db.refresh(data_source)
    return data_source


@router.get("/sources/{source_id}", response_model=DataSourceRead)
async def get_data_source(
    source_id: int,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Get a specific data source."""
    data_source = await db.get(DataSource, source_id)
    if not data_source or data_source.user_id != user.id:
        raise HTTPException(status_code=404, detail="Data source not found")
    return data_source


@router.delete("/sources/{source_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_data_source(
    source_id: int,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Delete a data source."""
    data_source = await db.get(DataSource, source_id)
    if not data_source or data_source.user_id != user.id:
        raise HTTPException(status_code=404, detail="Data source not found")
    await db.delete(data_source)
    await db.commit()


@router.put("/sources/{source_id}/name")
async def rename_data_source(
    source_id: int,
    new_name: Annotated[str, Body()],
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Rename a data source."""
    data_source = await db.get(DataSource, source_id)
    if not data_source or data_source.user_id != user.id:
        raise HTTPException(status_code=404, detail="Data source not found")
    data_source.name = new_name
    await db.commit()
    await db.refresh(data_source)
    return data_source


@router.get("/sources/{source_id}/instances")
async def get_instances(
    source_id: int,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    field_ids: Annotated[list[int] | None, Query()] = None,
):
    """Get instances from a data source."""
    data_source = await db.get(DataSource, source_id)
    if not data_source or data_source.user_id != user.id:
        raise HTTPException(status_code=404, detail="Data source not found")
    # TODO: Implement instance retrieval
    pass


@router.get("/sources/{source_id}/fields/{field_id}/stats", response_model=Stats)
async def get_field_stats(
    source_id: int,
    field_id: int,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Get statistics for a field."""
    data_source = await db.get(DataSource, source_id)
    if not data_source or data_source.user_id != user.id:
        raise HTTPException(status_code=404, detail="Data source not found")
    field = await db.get(FieldModel, field_id)
    if not field or field.data_source_id != source_id:
        raise HTTPException(status_code=404, detail="Field not found")
    # TODO: Implement field statistics calculation
    pass


@router.get("/sources/{source_id}/fields/{field_id}/values", response_model=list[Value])
async def get_field_values(
    source_id: int,
    field_id: int,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
):
    """Get values for a field."""
    data_source = await db.get(DataSource, source_id)
    if not data_source or data_source.user_id != user.id:
        raise HTTPException(status_code=404, detail="Data source not found")
    field = await db.get(FieldModel, field_id)
    if not field or field.data_source_id != source_id:
        raise HTTPException(status_code=404, detail="Field not found")
    # TODO: Implement field value retrieval
    pass


@router.get("/sources/{source_id}/fields/{field_id}/aggregated-values", response_model=TaskStatus)
async def get_aggregated_values(
    source_id: int,
    field_id: int,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    bins: Annotated[int, Query(ge=2, le=1000)] = 10,
    min_value: Annotated[float | None, Query()] = None,
    max_value: Annotated[float | None, Query()] = None,
    min_inclusive: Annotated[bool, Query()] = True,
    max_inclusive: Annotated[bool, Query()] = True,
):
    """Get aggregated values for a field."""
    data_source = await db.get(DataSource, source_id)
    if not data_source or data_source.user_id != user.id:
        raise HTTPException(status_code=404, detail="Data source not found")
    field = await db.get(FieldModel, field_id)
    if not field or field.data_source_id != source_id:
        raise HTTPException(status_code=404, detail="Field not found")
    # TODO: Implement aggregated value calculation
    task_id = uuid4()
    return TaskStatus(
        task_id=task_id,
        task_name="aggregate_field_values",
        status_message="Task started",
    )


@router.get("/tasks/{task_id}", response_model=TaskStatus)
async def get_task_status(
    task_id: UUID,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Get the status of a task."""
    # TODO: Implement task status retrieval
    pass


@router.get("/tasks/{task_id}/result")
async def get_task_result(
    task_id: UUID,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Get the result of a task."""
    # TODO: Implement task result retrieval
    pass

