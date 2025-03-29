import logging
import pathlib as pl
from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import (
    APIRouter,
    Body,
    Depends,
    HTTPException,
    Path,
    Query,
    Response,
    status,
)
from sqlalchemy import Update, update
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.config import API_V1_PREFIX
from easyminer.crud.aio.data_source import (
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
    get_fields_by_data_source,
    get_fields_by_ids,
)
from easyminer.crud.aio.upload import (
    create_chunk,
    create_preview_upload,
    create_upload,
    get_preview_upload_by_uuid,
    get_upload_by_uuid,
)
from easyminer.database import get_db_session
from easyminer.models import Field
from easyminer.models.data import DataSource, FieldType
from easyminer.processing.data_retrieval import (
    get_data_preview,
)
from easyminer.schemas.data import (
    DataSourceRead,
    FieldRead,
    PreviewUploadSchema,
    UploadSettings,
)
from easyminer.storage import DiskStorage
from easyminer.tasks import process_csv

# Maximum chunk size for preview uploads (100KB)
MAX_PREVIEW_CHUNK_SIZE = 100 * 1024

router = APIRouter(prefix=API_V1_PREFIX, tags=["Data"])

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


@router.put(
    "/datasource/{id}",
    status_code=status.HTTP_200_OK,  # TODO: This should return 204 to be Restful and also have better OAPI UI
)
async def rename_data_source(
    id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    name: Annotated[str, Body(example="A New Exciting Name", media_type="text/plain")],
) -> None:
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


@router.delete("/datasource/{id}/field/{field_id}")
async def delete_field_api(
    id: Annotated[int, Path()],
    field_id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    # Get the data source to validate access
    data_source = await get_data_source_by_id(db, id)
    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found"
        )

    # Get the field to validate access
    field = await get_field_by_id(db, field_id, id)
    if not field:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Field not found"
        )

    # Delete the field
    await db.delete(field)


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


@router.put("/datasource/{id}/field/{field_id}")
async def rename_field(
    id: Annotated[int, Path()],
    field_id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    name: Annotated[str, Body(example="New Field Name", media_type="text/plain")],
):  # Response status code should be 204
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

    # Rename the field
    _ = await db.execute(update(Field).where(Field.id == field.id).values(name=name))


@router.put(
    "/datasource/{id}/field/{field_id}/change-type",
    description="Toggle between nominal and numeric field types",
)
async def toggle_field_type(
    id: Annotated[int, Path()],
    field_id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):  # Response status code should be 204
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

    # Toggle the field type
    new_type = (
        FieldType.nominal if field.data_type == FieldType.numeric else FieldType.numeric
    )
    _ = await db.execute(
        update(Field).where(Field.id == field.id).values(field_type=new_type)
    )
