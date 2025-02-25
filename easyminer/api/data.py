from typing import Annotated
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.api.dependencies.auth import get_current_user
from easyminer.api.dependencies.db import get_db_session
from easyminer.models import DataSource, Upload, User
from easyminer.schemas.data import DataSourceCreate, DataSourceRead, UploadSettings

router = APIRouter(prefix="/api/v1/data", tags=["Data"])


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

