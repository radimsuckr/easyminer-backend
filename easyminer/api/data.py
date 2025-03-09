import csv
import io
import logging
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID, uuid4

from fastapi import (
    APIRouter,
    Body,
    Depends,
    HTTPException,
    Query,
    Request,
    Response,
    status,
)
from fastapi import (
    Path as FastAPIPath,
)
from pydantic import BaseModel
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.api.dependencies.auth import get_current_user
from easyminer.api.dependencies.db import get_db_session
from easyminer.models import DataSource, Field, Upload, User
from easyminer.models import Field as FieldModel
from easyminer.processing import CsvProcessor
from easyminer.schemas.data import (
    DataSourceCreate,
    DataSourceRead,
    FieldRead,
    UploadSettings,
)
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
    upload_id: str,
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> Response:
    """Upload a chunk of data for an upload.

    Args:
        upload_id: The UUID of the upload.
        request: The request object containing the chunk data.
        db: Database session.
        user: Current authenticated user.

    Returns:
        Response with 202 Accepted status code.

    Raises:
        HTTPException: If the upload is not found, the user doesn't have access,
            or there's an error processing the chunk.
    """
    storage = DiskStorage(Path("../../var/data"))

    try:
        # Read the chunk data from the request body
        chunk = await request.body()

        # Retrieve the upload by UUID with all attributes
        upload_result = await db.execute(select(Upload).where(Upload.uuid == upload_id))
        upload_record = upload_result.scalar_one_or_none()

        if not upload_record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Upload not found"
            )

        # Get upload attributes we'll need later
        upload_id_value = upload_record.id
        upload_name = upload_record.name
        upload_media_type = upload_record.media_type

        # Get upload settings
        encoding = "utf-8"
        separator = ","
        quote_char = '"'

        # Try to get these values from the database
        try:
            if hasattr(upload_record, "encoding") and upload_record.encoding:
                encoding = upload_record.encoding
            if hasattr(upload_record, "separator") and upload_record.separator:
                separator = upload_record.separator
            if hasattr(upload_record, "quotes_char") and upload_record.quotes_char:
                quote_char = upload_record.quotes_char
        except Exception:
            # Use defaults if there's an error
            pass

        # Get the associated data source
        ds_result = await db.execute(
            select(DataSource).where(DataSource.upload_id == upload_id_value)
        )
        data_source_record = ds_result.scalar_one_or_none()

        # Create a data source if it doesn't exist
        if not data_source_record:
            data_source_record = DataSource(
                name=upload_name,
                type=upload_media_type,
                user_id=user.id,
                upload_id=upload_id_value,
                size_bytes=len(chunk),
            )
            db.add(data_source_record)
            await db.commit()
            await db.refresh(data_source_record)
        else:
            # Check permissions
            if data_source_record.user_id != user.id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You don't have access to this upload",
                )

            # Update data source size
            data_source_record.size_bytes += len(chunk)

        data_source_id = data_source_record.id

        # For empty chunks (signaling end of upload), don't write to disk
        try:
            if len(chunk) > 0:
                # Use the storage service to save the chunk
                chunk_path = Path(
                    f"{user.id}/{data_source_id}/chunks/{datetime.now().strftime('%Y%m%d%H%M%S%f')}.chunk"
                )
                storage.save(chunk_path, chunk)
                await db.commit()
            else:
                # Update the data source when upload is complete
                # Row count will be updated by processor
                await db.commit()
        except Exception as e:
            await db.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error saving chunk: {str(e)}",
            )

        # If this is the last chunk (empty chunk), process the data
        if len(chunk) == 0:
            # Process the uploaded chunks
            logging.info(
                f"Upload complete for data source {data_source_id}. Processing data..."
            )

            # Process the CSV data
            if upload_media_type == "csv":
                try:
                    # Create processor with settings
                    processor = CsvProcessor(
                        data_source_record,
                        db,
                        data_source_id,
                        encoding=encoding,
                        separator=separator,
                        quote_char=quote_char,
                    )
                    storage_dir = Path(
                        f"../../var/data/{user.id}/{data_source_id}/chunks"
                    )
                    await processor.process_chunks(storage_dir)
                except Exception as e:
                    logging.error(f"Error processing CSV: {str(e)}", exc_info=True)
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=f"Error processing CSV data: {str(e)}",
                    )
            else:
                logging.warning(f"Unsupported media type: {upload_media_type}")

        # Return 202 Accepted
        return Response(status_code=status.HTTP_202_ACCEPTED)

    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        # Ensure we rollback on any error
        try:
            await db.rollback()
        except Exception:
            pass  # Ignore rollback errors

        logging.error(f"Error in upload_chunk: {str(e)}", exc_info=True)

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing chunk: {str(e)}",
        )


@router.post(
    "/upload/start/preview", response_model=str, response_model_exclude_none=True
)
async def start_preview_upload(
    settings: PreviewUpload,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Start a new preview upload process."""
    # Create a new upload with preview flag
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


@router.post("/upload/{upload_id}/preview", status_code=status.HTTP_202_ACCEPTED)
async def upload_preview_chunk(
    upload_id: str,
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db_session)],
    user: Annotated[User, Depends(get_current_user)],
    max_lines: Annotated[int, Query(gt=0, le=1000)] = 100,
) -> Response:
    """Upload a chunk of preview data.

    Args:
        upload_id: The UUID of the upload
        request: Request containing the chunk data
        db: Database session
        user: Current authenticated user
        max_lines: Maximum number of lines to process for preview

    Returns:
        Response with 202 Accepted status code
    """
    storage = DiskStorage(Path("../../var/data"))

    try:
        # Retrieve the upload by UUID
        upload_result = await db.execute(select(Upload).where(Upload.uuid == upload_id))
        upload_record = upload_result.scalar_one_or_none()

        if not upload_record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Upload not found"
            )

        # Read raw body data
        chunk = await request.body()
        if len(chunk) > MAX_PREVIEW_CHUNK_SIZE:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"Chunk too large. Maximum size is {MAX_PREVIEW_CHUNK_SIZE} bytes",
            )

        # Get the associated data source
        ds_result = await db.execute(
            select(DataSource).where(DataSource.upload_id == upload_record.id)
        )
        data_source_record = ds_result.scalar_one_or_none()

        # Create a data source if it doesn't exist
        if not data_source_record:
            data_source_record = DataSource(
                name=upload_record.name,
                type=upload_record.media_type,
                user_id=user.id,
                upload_id=upload_record.id,
                size_bytes=len(chunk),
            )
            db.add(data_source_record)
            await db.commit()
            await db.refresh(data_source_record)
            data_source_id = data_source_record.id
        else:
            # Check permissions
            if data_source_record.user_id != user.id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You don't have access to this upload",
                )
            data_source_id = data_source_record.id

        # Store the chunk using DiskStorage
        try:
            storage.save(
                Path(
                    f"{user.id}/{data_source_id}/preview_chunks/{datetime.now().strftime('%Y%m%d%H%M%S%f')}.chunk"
                ),
                chunk,
            )
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Error saving chunk: {str(e)}",
            )

        # Process the preview data
        if upload_record.media_type == "csv":
            try:
                # Get upload settings
                encoding = "utf-8"
                separator = ","
                quote_char = '"'

                # Try to get these values from the database
                try:
                    if hasattr(upload_record, "encoding") and upload_record.encoding:
                        encoding = upload_record.encoding
                    if hasattr(upload_record, "separator") and upload_record.separator:
                        separator = upload_record.separator
                    if (
                        hasattr(upload_record, "quotes_char")
                        and upload_record.quotes_char
                    ):
                        quote_char = upload_record.quotes_char
                except Exception:
                    # Use defaults if there's an error
                    pass

                # Process the preview CSV data
                processor = CsvProcessor(
                    data_source_record,
                    db,
                    data_source_id,
                    encoding=encoding,
                    separator=separator,
                    quote_char=quote_char,
                )
                storage_dir = Path(
                    f"../../var/data/{user.id}/{data_source_id}/preview_chunks"
                )
                await processor.process_chunks(storage_dir)

                # Update the data source with the max_lines limit for preview
                if max_lines and data_source_record.row_count > max_lines:
                    # For preview, limit the number of rows to max_lines
                    data_source_record.row_count = max_lines
                    await db.commit()

            except Exception as e:
                logging.error(f"Error processing preview CSV: {str(e)}", exc_info=True)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Error processing preview CSV data: {str(e)}",
                )

        # Return 202 Accepted
        return Response(status_code=status.HTTP_202_ACCEPTED)

    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        # Ensure we rollback on any error
        try:
            await db.rollback()
        except Exception:
            pass  # Ignore rollback errors

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


@router.get("/sources/{source_id}/preview")
async def preview_data_source(
    source_id: int,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
):
    """Get a preview of data from a data source.

    Args:
        source_id: The ID of the data source
        user: Current authenticated user
        db: Database session
        limit: Maximum number of rows to return

    Returns:
        Preview data including field names and values
    """
    # Get the data source
    data_source = await db.get(DataSource, source_id)
    if not data_source or data_source.user_id != user.id:
        raise HTTPException(status_code=404, detail="Data source not found")

    # Get fields for the data source
    result = await db.execute(
        select(FieldModel)
        .where(FieldModel.data_source_id == source_id)
        .order_by(FieldModel.index)
    )
    fields = result.scalars().all()

    if not fields:
        return {"field_names": [], "rows": []}

    # For this preview implementation, we'll return field names and sample data
    field_names = [field.name for field in fields]

    # In a real implementation, you would retrieve actual data from your storage system
    # For now, we'll return placeholder data with appropriate types
    preview_rows = []
    for i in range(min(limit, 10)):
        # Create a row as a dictionary of field_name -> sample value
        row_data: dict[str, Any] = {}
        for field in fields:
            if field.data_type == "integer":
                row_data[field.name] = i * 10
            elif field.data_type == "float":
                row_data[field.name] = i * 10.5
            else:
                row_data[field.name] = f"Sample value {i}"
        preview_rows.append(row_data)

    return {"field_names": field_names, "rows": preview_rows}


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
) -> list[dict[str, Any]]:
    """Get instances from a data source."""
    # Check if data source exists and belongs to the user
    data_source = await db.get(DataSource, source_id)
    if not data_source or data_source.user_id != user.id:
        raise HTTPException(status_code=404, detail="Data source not found")

    # If field_ids are provided, verify they exist and belong to this data source
    fields_to_include: list[Field] = []
    if field_ids:
        # Query for fields that belong to this data source and match the provided IDs
        query = (
            select(Field)
            .where(and_(Field.data_source_id == source_id, Field.id.in_(field_ids)))
            .order_by(Field.index)
        )
        result = await db.execute(query)
        fields_to_include = list(result.scalars().all())

        # If some field IDs don't exist, return an error
        if len(fields_to_include) != len(field_ids):
            raise HTTPException(
                status_code=400,
                detail="One or more requested fields do not exist in this data source",
            )
    else:
        # If no field_ids are provided, include all fields
        query = (
            select(Field).where(Field.data_source_id == source_id).order_by(Field.index)
        )
        result = await db.execute(query)
        fields_to_include = list(result.scalars().all())

    # Get the file path for the data source from the upload
    if not data_source.upload_id:
        # If there's no upload associated with this data source, return empty list
        return []

    # Get the upload associated with this data source
    upload = await db.get(Upload, data_source.upload_id)
    if not upload:
        return []

    # Path to the data file
    storage_dir = Path(f"uploads/{upload.uuid}")

    try:
        # Find all chunk files
        chunks = list(storage_dir.glob("*.chunk"))
        if not chunks:
            return []

        # Combine chunks
        combined_data = b""
        for chunk_file in chunks:
            try:
                combined_data += chunk_file.read_bytes()
            except Exception:
                continue

        if not combined_data:
            return []

        # Safe CSV parameters
        csv_params = {
            "delimiter": ",",  # Default delimiter
            "quotechar": '"',  # Default quote character
        }

        # Use upload parameters if available
        encoding = "utf-8"  # Default encoding

        # Safe attribute access with defaults
        if upload.separator is not None:
            csv_params["delimiter"] = upload.separator

        if hasattr(upload, "quotes_char") and upload.quotes_char is not None:
            csv_params["quotechar"] = upload.quotes_char

        if upload.encoding is not None:
            encoding = upload.encoding

        # Decode the data with fallback
        try:
            text = combined_data.decode(encoding)
        except UnicodeDecodeError:
            text = combined_data.decode("utf-8", errors="replace")

        # Parse the CSV safely
        reader = csv.reader(io.StringIO(text), **csv_params)
        csv_rows = list(reader)

        if not csv_rows:
            return []

        # Get the actual data rows with pagination
        start_row = offset + 1  # +1 because we skip the header
        end_row = min(start_row + limit, len(csv_rows))

        if start_row >= len(csv_rows):
            return []

        # Get field indices
        field_indices = [field.index for field in fields_to_include]
        field_names = [field.name for field in fields_to_include]

        # Extract the requested instances
        instances = []
        for row_idx in range(start_row, end_row):
            row = csv_rows[row_idx]
            instance = {}

            for i, field_idx in enumerate(field_indices):
                field_name = field_names[i]

                if field_idx < len(row):
                    instance[field_name] = row[field_idx]
                else:
                    instance[field_name] = None

            instances.append(instance)

        return instances

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error retrieving instances: {str(e)}"
        )


@router.get("/sources/{source_id}/fields", response_model=list[FieldRead])
async def list_fields(
    source_id: int,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """List all fields for a data source.

    Args:
        source_id: The ID of the data source
        user: Current authenticated user
        db: Database session

    Returns:
        List of fields for the data source
    """
    # Get the data source to validate access
    data_source = await db.get(DataSource, source_id)
    if not data_source or data_source.user_id != user.id:
        raise HTTPException(status_code=404, detail="Data source not found")

    # Get fields for the data source
    result = await db.execute(
        select(FieldModel)
        .where(FieldModel.data_source_id == source_id)
        .order_by(FieldModel.index)
    )
    fields = result.scalars().all()

    return fields


@router.get("/sources/{source_id}/fields/{field_id}", response_model=FieldRead)
async def get_field(
    source_id: int,
    field_id: int,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Get metadata for a specific field.

    Args:
        source_id: The ID of the data source
        field_id: The ID of the field
        user: Current authenticated user
        db: Database session

    Returns:
        Field metadata
    """
    # Get the data source to validate access
    data_source = await db.get(DataSource, source_id)
    if not data_source or data_source.user_id != user.id:
        raise HTTPException(status_code=404, detail="Data source not found")

    # Get the field
    field = await db.get(FieldModel, field_id)
    if not field or field.data_source_id != source_id:
        raise HTTPException(status_code=404, detail="Field not found")

    return field


@router.get("/sources/{source_id}/fields/{field_id}/stats", response_model=Stats)
async def get_field_stats(
    source_id: int,
    field_id: int,
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    """Get statistical information about a field."""
    # Check if data source exists and belongs to the user
    data_source = await db.get(DataSource, source_id)
    if not data_source or data_source.user_id != user.id:
        raise HTTPException(status_code=404, detail="Data source not found")

    # Check if field exists and belongs to the data source
    field = await db.get(Field, field_id)
    if not field or field.data_source_id != source_id:
        raise HTTPException(status_code=404, detail="Field not found")

    # Check if field is numeric
    if field.data_type not in ["integer", "float", "numeric"]:
        raise HTTPException(
            status_code=400, detail="Statistics are only available for numeric fields"
        )

    # Check if statistics are available
    if field.min_value is None or field.max_value is None or field.avg_value is None:
        raise HTTPException(
            status_code=404, detail="Statistics not available for this field"
        )

    # Create and return Stats object
    return Stats(
        min=float(field.min_value), max=float(field.max_value), avg=field.avg_value
    )


@router.get("/sources/{source_id}/fields/{field_id}/values", response_model=list[Value])
async def get_field_values(
    source_id: Annotated[int, FastAPIPath(gt=0)],
    field_id: Annotated[int, FastAPIPath(gt=0)],
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(gt=0, le=100)] = 20,
):
    """Get values for a specific field."""
    # First check data source
    data_source = await db.get(DataSource, source_id)
    if not data_source or data_source.user_id != user.id:
        raise HTTPException(status_code=404, detail="Data source not found")

    # Then check field
    field = await db.get(FieldModel, field_id)
    if not field or field.data_source_id != source_id:
        raise HTTPException(status_code=404, detail="Field not found")

    # TODO: Implement field value retrieval
    return []


@router.get("/sources/{source_id}/export")
async def export_data_source(
    source_id: Annotated[int, FastAPIPath(gt=0)],
    user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    format: Annotated[str, Query()] = "csv",
):
    """Export data from a data source.

    Args:
        source_id: The ID of the data source to export
        user: Current authenticated user
        db: Database session
        format: Export format (csv, json, etc.)

    Returns:
        Downloadable file with the exported data
    """
    # Get the data source
    data_source = await db.get(DataSource, source_id)
    if not data_source or data_source.user_id != user.id:
        raise HTTPException(status_code=404, detail="Data source not found")

    # For now, only support CSV export
    if format.lower() != "csv":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Export format '{format}' not supported. Only 'csv' is currently supported.",
        )

    # Start an export task
    task_id = uuid4()

    # Return task ID - the actual export will be handled asynchronously
    return {
        "task_id": task_id,
        "task_name": "export_data",
        "status_message": "Export task started",
        "status_location": f"/api/v1/tasks/{task_id}",
    }


@router.get(
    "/sources/{source_id}/fields/{field_id}/values/aggregated",
    response_model=list[Value],
)
async def get_aggregated_values(
    source_id: Annotated[int, FastAPIPath(gt=0)],
    field_id: Annotated[int, FastAPIPath(gt=0)],
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
