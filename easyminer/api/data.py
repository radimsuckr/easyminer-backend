import logging
import pathlib as pl
from datetime import datetime
from typing import Annotated, Any
from uuid import UUID, uuid4

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Body,
    Depends,
    HTTPException,
    Path,
    Query,
    Request,
    Response,
    status,
)
from sqlalchemy import Update
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.config import API_V1_PREFIX
from easyminer.crud.aio.data_source import (
    create_data_source,
    delete_data_source,
    get_data_source_by_id,
    get_data_source_by_preview_upload_id,
    get_data_source_by_upload_id,
    get_data_sources,
    update_data_source_name,
    update_data_source_size,
)
from easyminer.crud.aio.field import (
    get_field_by_id,
    get_field_stats,
    get_fields_by_data_source,
    get_fields_by_ids,
)
from easyminer.crud.aio.task import create_task, get_task_by_id, update_task_status
from easyminer.crud.aio.upload import (
    create_chunk,
    create_preview_upload,
    create_upload,
    get_preview_upload_by_uuid,
    get_upload_by_id,
    get_upload_by_uuid,
)
from easyminer.database import get_db_session, sessionmanager
from easyminer.models import Field
from easyminer.models.data import DataSource, FieldType
from easyminer.models.task import TaskStatusEnum
from easyminer.processing.csv_utils import extract_field_values_from_csv
from easyminer.processing.data_retrieval import (
    generate_histogram_for_field,
    get_data_preview,
    read_task_result,
)
from easyminer.schemas.data import (
    DataSourceCreate,
    DataSourceRead,
    FieldRead,
    PreviewResponse,
    PreviewUploadSchema,
    Stats,
    UploadSettings,
)
from easyminer.schemas.field_values import Value
from easyminer.schemas.task import TaskResult, TaskStatus
from easyminer.storage import DiskStorage
from easyminer.tasks import process_csv

# Maximum chunk size for preview uploads (100KB)
MAX_PREVIEW_CHUNK_SIZE = 100 * 1024

router = APIRouter(prefix=API_V1_PREFIX, tags=["Data API"])

logger = logging.getLogger(__name__)


@router.post("/upload/start")
async def start_upload(
    settings: UploadSettings,
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> UUID:
    """Start a new upload process."""
    if settings.media_type != "csv":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only CSV uploads are supported",
        )

    upload = await create_upload(db, settings)
    return UUID(upload.uuid)


@router.post("/upload/preview/start")
async def upload_preview_start(
    settings: PreviewUploadSchema,
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> UUID:
    upload = await create_preview_upload(db, settings)
    return upload.uuid


@router.post(
    "/upload/{upload_id}",
    responses={
        status.HTTP_200_OK: {"description": "Upload successful and closed"},
        status.HTTP_202_ACCEPTED: {},
        status.HTTP_403_FORBIDDEN: {"description": "Upload already closed"},
        status.HTTP_429_TOO_MANY_REQUESTS: {"description": "Uploading chunks too fast"},
    },
)
async def upload_chunk(
    db: Annotated[AsyncSession, Depends(get_db_session)],
    upload_id: str,
    body: str = "",
) -> Response:
    # TODO: add is_finished toggle that will be set to True when the last empty chunk is uploaded. Do this also for preview.
    """Upload a chunk of data for an upload."""
    storage = DiskStorage(pl.Path("../../var/data"))

    try:
        # Read the chunk data from the request body
        chunk = body

        # Retrieve the upload by UUID
        upload_record = await get_upload_by_uuid(db, upload_id)

        if not upload_record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Upload not found"
            )

        if upload_record.data_source.is_finished:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="Upload already closed"
            )

        # Get upload attributes we'll need later
        upload_id_value = upload_record.id
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
        data_source_record = await get_data_source_by_upload_id(db, upload_id_value)
        if not data_source_record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found"
            )

        # Store data source ID before updating
        data_source_id = data_source_record.id

        # Update data source size
        updated_ds = await update_data_source_size(db, data_source_id, len(chunk))
        if updated_ds:
            data_source_record = updated_ds

        # For empty chunks (signaling end of upload), don't write to disk
        if len(chunk) > 0:
            # Use the storage service to save the chunk
            chunk_path = pl.Path(
                f"{data_source_id}/chunks/{datetime.now().strftime('%Y%m%d%H%M%S%f')}.chunk"
            )
            _, path = storage.save(chunk_path, bytes(chunk, encoding))
            _ = await create_chunk(db, upload_id_value, str(path))
        # If this is the last chunk (empty chunk), process the data
        elif len(chunk) == 0:
            logger.info(
                f"Upload complete for data source {data_source_id}. Processing data..."
            )
            _ = await db.execute(
                Update(DataSource)
                .where(DataSource.id == data_source_id)
                .values(is_finished=True)
            )
            await db.commit()
            _ = process_csv.delay(
                data_source_id=data_source_id,
                upload_media_type=upload_media_type,
                encoding=encoding,
                separator=separator,
                quote_char=quote_char,
            )
            return Response(status_code=status.HTTP_200_OK)

        return Response(status_code=status.HTTP_202_ACCEPTED)
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
    "/upload/preview/{upload_id}",
    responses={
        status.HTTP_200_OK: {"description": "Upload successful and closed"},
        status.HTTP_202_ACCEPTED: {},
        status.HTTP_403_FORBIDDEN: {"description": "Upload already closed"},
        status.HTTP_429_TOO_MANY_REQUESTS: {"description": "Uploading chunks too fast"},
    },
)
async def upload_preview_chunk(
    db: Annotated[AsyncSession, Depends(get_db_session)],
    upload_id: UUID,
    body: str = "",
) -> Response:
    # TODO: check that the upload is not bigger than the set max_lines.
    """Upload a chunk of data for an upload."""
    storage = DiskStorage(pl.Path("../../var/data"))

    try:
        # Read the chunk data from the request body
        chunk = body

        # Retrieve the upload by UUID
        upload_record = await get_preview_upload_by_uuid(db, upload_id)

        if not upload_record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Upload not found"
            )

        if upload_record.data_source.is_finished:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="Upload already closed"
            )

        # Get upload attributes we'll need later
        upload_id_value = upload_record.id

        # Get upload settings
        encoding = "utf-8"
        separator = ","
        quote_char = '"'

        # Get the associated data source
        data_source_record = await get_data_source_by_preview_upload_id(
            db, upload_id_value
        )

        # Store data source ID before updating
        data_source_id = data_source_record.id

        # Update data source size
        updated_ds = await update_data_source_size(db, data_source_id, len(chunk))
        if updated_ds:
            data_source_record = updated_ds

        # For empty chunks (signaling end of upload), don't write to disk
        if len(chunk) > 0:
            try:
                # Use the storage service to save the chunk
                chunk_path = pl.Path(
                    f"{data_source_id}/chunks/{datetime.now().strftime('%Y%m%d%H%M%S%f')}.chunk"
                )
                _, path = storage.save(chunk_path, bytes(chunk, "utf-8"))
                _ = await create_chunk(db, upload_id_value, str(path))
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Error saving chunk: {str(e)}",
                )
        # If this is the last chunk (empty chunk), process the data
        elif len(chunk) == 0:
            logger.info(
                f"Upload complete for data source {data_source_id}. Processing data..."
            )
            _ = await db.execute(
                Update(DataSource)
                .where(DataSource.id == data_source_id)
                .values(is_finished=True)
            )
            await db.commit()
            _ = process_csv.delay(
                data_source_id=data_source_id,
                upload_media_type="csv",
                encoding=encoding,
                separator=separator,
                quote_char=quote_char,
            )
            return Response(status_code=status.HTTP_200_OK)

        # Return 202 Accepted
        return Response(status_code=status.HTTP_202_ACCEPTED)

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


@router.get("/datasource")
async def list_data_sources(
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> list[DataSourceRead]:
    """List all data sources for the current user."""
    data_sources = await get_data_sources(db, eager=True)
    return [DataSourceRead.model_validate(ds) for ds in data_sources]


@router.post("/datasource")
async def create_datasource(
    data: DataSourceCreate,
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> DataSourceRead:
    """Create a new data source."""
    data_source = await create_data_source(
        db_session=db,
        name=data.name,
        type=data.type,
        size_bytes=data.size_bytes if hasattr(data, "size_bytes") else 0,
        row_count=data.row_count if hasattr(data, "row_count") else 0,
    )
    return DataSourceRead.model_validate(data_source)


@router.get("/datasource/{id}")
async def get_data_source_api(
    id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> DataSourceRead:
    """Get a specific data source."""
    data_source = await get_data_source_by_id(db, id, eager=True)
    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found"
        )
    return DataSourceRead.model_validate(data_source)


@router.get("/datasource/{id}/preview")
async def preview_data_source(
    id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PreviewResponse:
    """Get a preview of data from a data source."""
    # Get the data source
    data_source = await get_data_source_by_id(db, id)
    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found"
        )

    # Get fields for the data source
    fields = await get_fields_by_data_source(db, id)

    if not fields:
        return PreviewResponse(field_names=[], rows=[])

    # Retrieve actual data from storage
    try:
        field_names, rows = await get_data_preview(
            db=db, data_source=data_source, limit=limit
        )

        return PreviewResponse(field_names=field_names, rows=rows)
    except Exception as e:
        logging.error(f"Error retrieving preview data: {str(e)}", exc_info=True)

        # Return empty result if we can't get the actual data
        return PreviewResponse(field_names=[field.name for field in fields], rows=[])


@router.delete(
    "/datasource/{id}",
    status_code=status.HTTP_200_OK,  # TODO: This should return 204 to be Restful and also have better OAPI UI
)
async def delete_data_source_api(
    id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> None:
    """Delete a data source."""
    success = await delete_data_source(db, id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found"
        )


@router.put(
    "/datasource/{id}",
    status_code=status.HTTP_200_OK,  # TODO: This should return 204 to be Restful and also have better OAPI UI
)
async def rename_data_source(
    id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    name: Annotated[str, Body(media_type="text/plain; charset=UTF-8")],
) -> None:
    """Rename a data source."""
    data_source = await update_data_source_name(db, id, name)
    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found"
        )


@router.get("/datasource/{id}/instances")
async def get_instances(
    id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    field_ids: Annotated[list[int] | None, Query()] = None,
) -> list[dict[Any, Any]]:
    """Get instances from a data source."""
    # Check if data source exists
    data_source = await get_data_source_by_id(db, id)
    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found"
        )

    # If field_ids are provided, verify they exist and belong to this data source
    fields_to_include: list[Field] = []
    if field_ids:
        # Query for fields that belong to this data source and match the provided IDs
        fields_to_include = list(await get_fields_by_ids(db, field_ids, id))

        # If some field IDs don't exist, return an error
        if len(fields_to_include) != len(field_ids):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="One or more requested fields do not exist in this data source",
            )
    else:
        # If no field_ids are provided, include all fields
        fields_to_include = list(await get_fields_by_data_source(db, id))

    try:
        # Use the same preview data function, but apply offset and limit

        field_names, rows = await get_data_preview(
            db=db,
            data_source=data_source,
            limit=offset + limit,  # Fetch enough rows to apply offset
            field_ids=field_ids,
        )

        # Apply offset manually (the preview function always starts from the beginning)
        if offset >= len(rows):
            return []

        # Convert rows to Instance objects
        instances = [row for row in rows[offset : offset + limit]]

        # Return the paginated subset
        return instances

    except FileNotFoundError:
        # No data found
        return []
    except Exception as e:
        logger.error(f"Error retrieving instances: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving instances: {str(e)}",
        )


@router.get("/datasource/{id}/field")
async def get_fields_api(
    id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> list[FieldRead]:
    """List all fields for a data source.

    Args:
        id: The ID of the data source
        db: Database session

    Returns:
        List of fields for the data source
    """
    # Get the data source to validate access
    data_source = await get_data_source_by_id(db, id)
    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found"
        )

    # Get fields for the data source
    fields = await get_fields_by_data_source(db, id)

    return [FieldRead.model_validate(field) for field in fields]


@router.get("/datasource/{id}/field/{field_id}")
async def get_field_api(
    id: Annotated[int, Path()],
    field_id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> FieldRead:
    """Get metadata for a specific field.

    Args:
        id: The ID of the data source
        fieldId: The ID of the field
        db: Database session

    Returns:
        Field metadata
    """
    # Get the data source to validate access
    data_source = await get_data_source_by_id(db, id)
    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found"
        )

    # Get the field
    field = await get_field_by_id(db, field_id, id)
    if not field:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Field not found"
        )

    return FieldRead.model_validate(field)


@router.get("/datasource/{id}/field/{field_id}/stats")
async def get_field_stats_api(
    id: Annotated[int, Path()],
    field_id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> Stats:
    """Get statistical information about a field."""
    # Check if data source exists and belongs to the user
    data_source = await get_data_source_by_id(db, id)
    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found"
        )

    # Check if field exists and belongs to the data source
    field = await get_field_by_id(db, field_id, id)
    if not field:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Field not found"
        )

    # Check if field is numeric
    if field.data_type != FieldType.numeric:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Statistics are only available for numeric fields",
        )

    # Check if statistics are available
    stats = await get_field_stats(db, field.id)
    if not stats:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Statistics not available for this field",
        )
    if stats.min_value is None or stats.max_value is None or stats.avg_value is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Statistics not available for this field",
        )

    # Create and return Stats object
    return Stats(min=stats.min_value, max=stats.max_value, avg=stats.avg_value)


@router.get("/datasource/{id}/field/{field_id}/values")
async def get_field_values(
    id: Annotated[int, Path()],
    field_id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(gt=0, le=100)] = 20,
) -> list[Value]:
    """Get values for a specific field.

    Args:
        id: The ID of the data source
        fieldId: The ID of the field
        db: Database session
        offset: Pagination offset
        limit: Number of values to return

    Returns:
        List of unique values with their frequencies

    Raises:
        HTTPException: If the data source or field is not found
    """
    # First check data source
    data_source = await get_data_source_by_id(db, id, eager=True)
    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found"
        )

    # Then check field
    field = await get_field_by_id(db, field_id, id)
    if not field:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Field not found"
        )

    try:
        # Initialize storage and settings
        storage = DiskStorage(pl.Path("../../var/data"))
        storage_dir = pl.Path(f"{id}/chunks")
        encoding = "utf-8"
        separator = ","
        quote_char = '"'

        # Get encoding and CSV settings from upload if available
        upload = await get_upload_by_id(db, data_source.upload.id)

        if upload:
            if hasattr(upload, "encoding") and upload.encoding:
                encoding = upload.encoding
            if hasattr(upload, "separator") and upload.separator:
                separator = upload.separator
            if hasattr(upload, "quotes_char") and upload.quotes_char:
                quote_char = upload.quotes_char

        # Find all chunk files
        chunk_paths = list(storage_dir.glob("*.chunk"))
        if not chunk_paths:
            return []  # No data available

        # Process each chunk file and aggregate the results
        all_values = []
        for chunk_path in sorted(chunk_paths):
            try:
                # Read the chunk file
                chunk_full_path = storage._root / chunk_path
                if not chunk_full_path.exists():
                    continue

                chunk_data = chunk_full_path.read_bytes()
                if not chunk_data:
                    continue

                # Decode the data
                try:
                    text_data = chunk_data.decode(encoding)
                except UnicodeDecodeError:
                    text_data = chunk_data.decode("utf-8", errors="replace")

                # Process this chunk using our utility function
                chunk_values = extract_field_values_from_csv(
                    text_data, field, encoding, separator, quote_char
                )

                # Add to our collection
                all_values.extend(chunk_values)

            except Exception as e:
                logging.error(f"Error processing chunk {chunk_path}: {str(e)}")
                continue

        # Consolidate values (sum up frequencies for the same values)
        value_map: dict[Any, int] = {}
        for val in all_values:
            if val.value in value_map:
                value_map[val.value] += val.frequency
            else:
                value_map[val.value] = val.frequency

        # Create final result list
        result = []
        for i, (value, frequency) in enumerate(
            sorted(value_map.items(), key=lambda x: x[1], reverse=True)
        ):
            result.append(Value(id=i, value=value, frequency=frequency))

        # Apply pagination
        return result[offset : offset + limit]

    except Exception as e:
        logging.error(f"Error retrieving field values: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving field values: {str(e)}",
        )


@router.get(
    "/datasource/{id}/field/{field_id}/aggregated-values",
    status_code=status.HTTP_202_ACCEPTED,
)
async def get_aggregated_values(
    request: Request,
    id: Annotated[int, Path()],
    field_id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    bins: Annotated[int, Query(gt=0, le=1000)] = 10,
    min_value: float | None = None,
    max_value: float | None = None,
    min_inclusive: bool = True,
    max_inclusive: bool = True,
    background_tasks: BackgroundTasks = BackgroundTasks(),
) -> TaskStatus:
    """Get aggregated values for a field.

    This operation creates a task for generating a histogram of a numeric field
    with values aggregated into intervals by number of bins.

    Args:
        id: The ID of the data source
        fieldId: The ID of the field
        db: Database session
        bins: Number of bins (2-1000)
        min_value: Minimum value (optional)
        max_value: Maximum value (optional)
        min_inclusive: Whether the minimum value is inclusive (default: True)
        max_inclusive: Whether the maximum value is inclusive (default: True)
        background_tasks: Background tasks manager

    Returns:
        TaskStatus: Information about the created task
    """
    # Check if data source exists
    data_source = await get_data_source_by_id(db, id)
    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found"
        )

    # Check if field exists and belongs to the data source
    field = await get_field_by_id(db, field_id, id)
    if not field:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Field not found"
        )

    # Check if field is numeric
    if field.data_type not in ["numeric", "integer", "float"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Field is not numeric"
        )

    # Create a task ID
    task_id = uuid4()

    # Store the task in the database
    await create_task(
        db_session=db,
        task_id=task_id,
        name="aggregated_values",
        data_source_id=id,
    )

    # Start a background task to process the histogram data
    async def process_histogram_task():
        try:
            # Create a new DB session for background task

            async with sessionmanager.session() as session:
                # Get the task
                task_record = await get_task_by_id(session, task_id)
                if not task_record:
                    logger.error(f"Task {task_id} not found")
                    return

                # Update task status to in_progress
                _ = await update_task_status(
                    session,
                    task_id,
                    TaskStatusEnum.started,
                    "Generating histogram data",
                )

                # Get data source and field again in this session
                data_source_record = await get_data_source_by_id(session, id)
                field_record = await get_field_by_id(session, field_id, id)

                if not data_source_record or not field_record:
                    _ = await update_task_status(
                        session,
                        task_id,
                        TaskStatusEnum.failure,
                        "Data source or field no longer exists",
                    )
                    return

                try:
                    # Generate histogram
                    _, result_path = await generate_histogram_for_field(
                        db=session,
                        field=field_record,
                        data_source=data_source_record,
                        bins=bins,
                        min_value=min_value,
                        max_value=max_value,
                        min_inclusive=min_inclusive,
                        max_inclusive=max_inclusive,
                    )

                    # Update task status
                    _ = await update_task_status(
                        session,
                        task_id,
                        TaskStatusEnum.success,
                        "Histogram generation completed",
                        result_location=result_path,
                    )

                except Exception as e:
                    logger.error(f"Error generating histogram: {str(e)}", exc_info=True)
                    _ = await update_task_status(
                        session,
                        task_id,
                        TaskStatusEnum.failure,
                        f"Error generating histogram: {str(e)}",
                    )
        except Exception as e:
            logger.error(f"Background task error: {str(e)}", exc_info=True)

    # Add task to background_tasks
    background_tasks.add_task(process_histogram_task)

    # Return task ID and status location
    return TaskStatus(
        task_id=task_id,
        task_name="aggregated_values",
        status=TaskStatusEnum.pending,
        status_message="Histogram generation started",
        status_location=request.url_for("get_task_status", task_id=task_id).path,
        result_location=None,
    )


@router.get("/task-status/{task_id}", name="get_task_status")
async def get_task_status(
    request: Request,
    task_id: Annotated[UUID, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> TaskStatus:
    """Get status of a task."""
    # Look up the task by ID
    task = await get_task_by_id(db, task_id)

    # Check if the task exists
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Task not found"
        )

    return TaskStatus(
        task_id=task.task_id,
        task_name=task.name,
        status=task.status,
        status_message=task.status_message,
        status_location=request.url_for("get_task_status", task_id=task.task_id).path,
        result_location=task.result_location if task.result_location else None,
    )


@router.get("/task-result/{task_id}")
async def get_task_result(
    task_id: Annotated[UUID, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> TaskResult:
    """Get the result of a completed task."""
    # Look up the task by ID
    task = await get_task_by_id(db, task_id)

    # Check if the task exists
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

    # Check if there's a result location
    if not task.result_location:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No result available for this task",
        )

    # Return the actual result
    try:
        # Read the task result
        result_data = await read_task_result(task.result_location)
        return TaskResult(
            message="Task result retrieved successfully",
            result_location=task.result_location,
            result=result_data,
        )
    except FileNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Result file not found",
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error reading result file: {str(e)}",
        )
