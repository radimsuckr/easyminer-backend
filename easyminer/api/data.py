import logging
from datetime import datetime
from enum import Enum
from pathlib import Path as PathLib
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import (
    APIRouter,
    Body,
    Depends,
    HTTPException,
    Path,
    Query,
    Request,
    Response,
    status,
)
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.api.dependencies.auth import get_current_user
from easyminer.api.dependencies.db import get_db_session
from easyminer.models import DataSource, Upload, User
from easyminer.models import Field as FieldModel
from easyminer.schemas.data import DataSourceCreate, DataSourceRead, UploadSettings
from easyminer.storage import DiskStorage

# Maximum chunk size for preview uploads (100KB)
MAX_PREVIEW_CHUNK_SIZE = 100 * 1024

router = APIRouter(prefix="/api/v1", tags=["Upload"])


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
    status_message: str | None = None
    status_location: str | None = None
    result_location: str | None = None


class PreviewUpload(BaseModel):
    maxLines: int
    compression: CompressionType | None = None


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
    request: Request,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Upload a chunk of data."""
    storage = DiskStorage(PathLib("../../var/data"))

    try:
        # Get the upload record by UUID
        result = await db.execute(select(Upload).where(Upload.uuid == str(upload_id)))
        upload = result.scalar_one_or_none()

        if not upload:
            raise HTTPException(status_code=404, detail="Upload not found")

        # Read raw body data
        chunk = await request.body()
        if len(chunk) > 1024 * 1024:  # 1MB limit as per Swagger spec
            raise HTTPException(
                status_code=413, detail="Chunk too large. Maximum size is 1MB"
            )

        # Check if data source exists for this upload
        ds_result = await db.execute(
            select(DataSource).where(DataSource.upload_id == upload.id)
        )
        data_source = ds_result.scalar_one_or_none()

        # Create a data source if it doesn't exist
        if not data_source:
            new_data_source = DataSource(
                name=upload.name,
                type=upload.media_type,
                user_id=user.id,
                upload_id=upload.id,
                size_bytes=len(chunk),
            )
            db.add(new_data_source)
            await db.commit()

            # Get the newly created data source ID by directly accessing the attribute
            data_source_id = new_data_source.id
        else:
            # Update data source size with explicit SQL
            data_source.size_bytes += len(chunk)
            await db.commit()

            # Use existing data source ID
            await db.refresh(data_source)
            await db.refresh(user)
            data_source_id = data_source.id

        try:
            storage.save(
                PathLib(
                    f"{user.id}/{data_source_id}/chunks/{datetime.now().strftime('%Y%m%d%H%M%S%f')}.chunk"
                ),
                chunk,
            )
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Error saving chunk: {str(e)}",
            )

        # If this is the last chunk (empty chunk), process the data
        if len(chunk) == 0:
            # TODO: Implement data processing
            # For now, we'll just log that processing would happen here
            logging.info(
                f"Upload complete for data source {data_source_id}. Processing would start here."
            )

            # In a real implementation, we would:
            # 1. Start a background task to process all chunks
            # 2. Parse the data according to the upload format (CSV, RDF, etc.)
            # 3. Insert the data into appropriate database tables
            # 4. Update the data source status when complete

        return Response(status_code=status.HTTP_202_ACCEPTED)

    except Exception as e:
        # Ensure we rollback on any error
        try:
            await db.rollback()
        except:
            pass  # Ignore rollback errors

        logging.error(f"Error in upload_chunk: {str(e)}", exc_info=True)

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing chunk: {str(e)}",
        )


@router.post(
    "/upload/preview/start", response_model=str, response_model_exclude_none=True
)
async def start_preview_upload(
    settings: PreviewUpload,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Start a preview upload process."""
    upload_id = uuid4()
    upload = Upload(
        uuid=str(upload_id),
        name=f"preview_{upload_id}",  # Using UUID as name for preview uploads
        media_type="csv",  # Default to CSV for preview
        db_type="limited",
        separator=",",  # Default separator
        encoding="utf-8",  # Default encoding
        quotes_char='"',
        escape_char="\\",
        locale="en_US",
        compression=settings.compression.value if settings.compression else None,
        format="csv",
        preview_max_lines=settings.maxLines,
    )
    db.add(upload)
    await db.commit()
    return str(upload_id)


@router.post("/upload/preview/{upload_id}")
async def upload_preview_chunk(
    upload_id: UUID,
    request: Request,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Upload a chunk of preview data."""
    storage = DiskStorage(PathLib("../../var/data"))

    try:
        # Get the upload record by UUID
        result = await db.execute(select(Upload).where(Upload.uuid == str(upload_id)))
        upload = result.scalar_one_or_none()

        if not upload:
            raise HTTPException(status_code=404, detail="Upload not found")

        # Check if this is a preview upload
        if not upload.name.startswith("preview_"):
            raise HTTPException(status_code=400, detail="Not a preview upload")

        # Read raw body data
        chunk = await request.body()
        if len(chunk) > MAX_PREVIEW_CHUNK_SIZE:
            raise HTTPException(
                status_code=413,
                detail=f"Chunk too large. Maximum size is {MAX_PREVIEW_CHUNK_SIZE} bytes",
            )

        # Check if data source exists for this upload
        ds_result = await db.execute(
            select(DataSource).where(DataSource.upload_id == upload.id)
        )
        data_source = ds_result.scalar_one_or_none()

        # Create a data source if it doesn't exist
        if not data_source:
            new_data_source = DataSource(
                name=upload.name.replace("preview_", ""),
                type=upload.media_type,
                user_id=user.id,
                upload_id=upload.id,
                size_bytes=len(chunk),
            )
            db.add(new_data_source)
            await db.commit()

            # Get the newly created data source ID
            data_source_id = new_data_source.id
        else:
            # Update data source size directly
            data_source.size_bytes += len(chunk)
            await db.commit()

            await db.refresh(data_source)
            await db.refresh(user)
            # Use existing data source ID
            data_source_id = data_source.id

        try:
            storage.save(
                PathLib(
                    f"{user.id}/{data_source_id}/preview_chunks/{datetime.now().strftime('%Y%m%d%H%M%S%f')}.chunk"
                ),
                chunk,
            )
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Error saving preview chunk: {str(e)}",
            )

        # If this is the last chunk (empty chunk), process the preview data
        if len(chunk) == 0:
            # TODO: Implement preview data processing
            # For now, we'll just log that processing would happen here
            logging.info(
                f"Preview upload complete for data source {data_source_id}. Processing would start here."
            )

            # In a real implementation, we would:
            # 1. Start a background task to process all chunks
            # 2. Parse the data according to the upload format (CSV, RDF, etc.)
            # 3. Generate a preview of the data (limited number of rows)
            # 4. Return the preview data to the client

            # For now, just return 202 Accepted
            # In the future, this should return 200 OK with the preview data
            return Response(status_code=status.HTTP_202_ACCEPTED)

        # Return 202 Accepted for intermediate chunks
        return Response(status_code=status.HTTP_202_ACCEPTED)

    except Exception as e:
        # Ensure we rollback on any error
        try:
            await db.rollback()
        except:
            pass  # Ignore rollback errors

        # Log the error for debugging
        logging.error(f"Error in upload_preview_chunk: {str(e)}", exc_info=True)

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing preview chunk: {str(e)}",
        )


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
    source_id: Annotated[int, Path(gt=0)],
    field_id: Annotated[int, Path(gt=0)],
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(gt=0, le=100)] = 20,
):
    """Get values for a field."""
    # Implementation would go here
    return []


@router.get(
    "/sources/{source_id}/fields/{field_id}/values/aggregated",
    response_model=list[Value],
)
async def get_aggregated_values(
    source_id: Annotated[int, Path(gt=0)],
    field_id: Annotated[int, Path(gt=0)],
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    bins: Annotated[int, Query(gt=0, le=100)] = 10,
    min_value: float | None = None,
    max_value: float | None = None,
):
    """Get aggregated values for a field."""
    # Implementation would go here
    return []


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
