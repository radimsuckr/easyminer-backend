import csv
import io
import logging
import pathlib
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from typing import Annotated
from uuid import UUID

from fastapi import (
    APIRouter,
    Body,
    HTTPException,
    Path,
    Query,
    Request,
    status,
)
from fastapi.responses import PlainTextResponse
from sqlalchemy import delete, distinct, func, select
from sqlalchemy.orm import joinedload

from easyminer.config import API_V1_PREFIX
from easyminer.crud.aio.data import create_chunk, create_preview_upload, create_upload
from easyminer.decompress import (
    CompressionError,
    decompress_bzip2,
    decompress_gzip,
    decompress_zip,
    extract_first_n_lines,
)
from easyminer.dependencies import ApiKey, AuthenticatedSession, get_database_config
from easyminer.models.data import (
    DataSource,
    DataSourceInstance,
    Field,
    FieldNumericDetail,
    PreviewUpload,
    Upload,
    UploadState,
)
from easyminer.schemas.data import (
    AggregatedInstance,
    AggregatedInstanceValue,
    CompressionType,
    DataSourceRead,
    DbType,
    FieldRead,
    FieldStatsSchema,
    FieldType,
    FieldValueSchema,
    PreviewUploadSchema,
    StartUploadSchema,
    UploadResponseSchema,
)
from easyminer.schemas.error import StructuredHTTPException
from easyminer.schemas.task import TaskStatus
from easyminer.storage import DiskStorage
from easyminer.tasks.aggregate_field_values import aggregate_field_values
from easyminer.tasks.calculate_field_numeric_detail import (
    calculate_field_numeric_detail,
)
from easyminer.tasks.process_chunk import process_chunk

MAX_FULL_UPLOAD_CHUNK_SIZE = 500 * 1024 * 1.05  # 500KB plus 5% overhead
MAX_PREVIEW_UPLOAD_CHUNK_SIZE = 100 * 1024 * 1.05  # 100KB plus 5% overhead

router = APIRouter(prefix=API_V1_PREFIX, tags=["Data"])

logger = logging.getLogger(__name__)

_csv_upload_example = """a,b,c
1,2,3
4,5,6
7,x,9"""


@router.post("/upload/start")
async def start_upload(db: AuthenticatedSession, settings: StartUploadSchema) -> PlainTextResponse:
    if settings.media_type != "csv":
        raise StructuredHTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            error="InvalidMediaType",
            message="Only 'csv' media type is supported",
            details={"receivedType": settings.media_type.value},
        )

    if all(dt is None for dt in settings.data_types):
        raise StructuredHTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            error="AllColumnsSkipped",
            message="At least one column must have a type (not null)",
            details={"providedDataTypes": settings.data_types},
        )

    upload_uuid = await create_upload(db, settings)
    await db.commit()
    return PlainTextResponse(content=str(upload_uuid))


@router.post("/upload/preview/start")
async def start_preview_upload(db: AuthenticatedSession, settings: PreviewUploadSchema) -> PlainTextResponse:
    if settings.media_type != "csv":
        raise StructuredHTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            error="InvalidMediaType",
            message="Only 'csv' media type is supported",
            details={"receivedType": settings.media_type.value},
        )

    upload_uuid = await create_preview_upload(db, settings)
    await db.commit()
    return PlainTextResponse(content=str(upload_uuid))


@router.post(
    "/upload/{upload_id}",
    response_model=None,
    responses={
        status.HTTP_200_OK: {
            "description": "Upload successful and closed",
            "model": UploadResponseSchema,
        },
        status.HTTP_202_ACCEPTED: {"description": "Chunk accepted"},
        status.HTTP_403_FORBIDDEN: {"description": "Upload already closed"},
        status.HTTP_429_TOO_MANY_REQUESTS: {"description": "Uploading chunks too fast"},
    },
)
async def upload_chunk(
    db: AuthenticatedSession,
    api_key: ApiKey,
    upload_id: Annotated[UUID, Path()],
    content: Annotated[
        str,
        Body(examples=[_csv_upload_example], media_type="text/plain"),  # NOTE: text/csv could possibly be used here
    ] = "",
) -> UploadResponseSchema | PlainTextResponse:
    if len(content) > MAX_FULL_UPLOAD_CHUNK_SIZE:
        raise StructuredHTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            error="ChunkTooLarge",
            message="Chunk size exceeds 500KB limit",
            details={"maxSize": int(MAX_FULL_UPLOAD_CHUNK_SIZE), "receivedSize": len(content)},
        )

    upload = await db.scalar(
        select(Upload)
        .where(Upload.uuid == upload_id)
        .options(joinedload(Upload.data_source), joinedload(Upload.null_values), joinedload(Upload.data_types))
    )
    if not upload:
        raise StructuredHTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            error="UploadNotFound",
            message="Upload session not found or expired",
            details={"uploadId": str(upload_id)},
        )

    if upload.state == UploadState.finished:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Upload already closed")

    if upload.state == UploadState.locked:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Upload is locked, try again later")

    # Get database config to pass to Celery tasks
    db_config = await get_database_config(api_key, DbType.limited)
    db_url = db_config.get_sync_url()

    if upload.state == UploadState.ready and len(content) == 0:
        upload.state = UploadState.finished
        result = UploadResponseSchema(
            id=upload.id,
            name=upload.name,
            type=upload.media_type,
            size=upload.data_source.size,
        )

        field_ids = (
            await db.scalars(
                select(Field.id).where(Field.data_source_id == upload.data_source.id).order_by(Field.index)
            )
        ).all()
        await db.commit()

        for field_id in field_ids:
            _ = calculate_field_numeric_detail.apply_async(
                kwargs={"field_id": field_id, "db_url": db_url}, headers={"db_url": db_url}
            )
        return result

    if len(content) == 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Empty chunk")

    if upload.state == UploadState.initialized:
        reader = csv.reader(
            io.StringIO(content),
            delimiter=upload.separator,
            quotechar=upload.quotes_char,
            escapechar=upload.escape_char,
        )
        try:
            header = next(reader)
            column_count = len(header)
        except StopIteration:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot read header from first chunk")

        data_types_list = [dt.value for dt in sorted(upload.data_types, key=lambda x: x.index)]

        if len(data_types_list) != column_count:
            raise StructuredHTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                error="DataTypesMismatch",
                message="Number of columns must match number of data types set for the upload",
                details={
                    "expectedColumnsCount": column_count,
                    "providedDataTypesCount": len(data_types_list),
                    "columns": header,
                    "providedDataTypes": data_types_list,
                },
            )

    # Lock the upload
    original_state = upload.state
    logging.info("Locking the upload")
    upload.state = UploadState.locked
    await db.flush()
    logging.info("Locked")

    chunk_datetime = datetime.now()
    chunk_path = pathlib.Path(f"{upload_id}/chunks/{chunk_datetime.strftime('%Y%m%d%H%M%S%f')}.chunk")
    storage = DiskStorage()
    _, saved_path = storage.save(chunk_path, content.encode("utf-8"))
    chunk_id = await create_chunk(db, upload.id, chunk_datetime, str(saved_path))
    logger.info("Chunk %s created for upload %s", chunk_id, upload_id)

    separator = upload.separator
    quote_char = upload.quotes_char
    escape_char = upload.escape_char
    encoding = upload.encoding
    null_values_list = [nv.value for nv in upload.null_values]
    data_types_list = [dt.value for dt in sorted(upload.data_types, key=lambda x: x.index)]

    await db.commit()

    _ = process_chunk.apply_async(
        args=(chunk_id, original_state),
        kwargs={
            "separator": separator,
            "quote_char": quote_char,
            "escape_char": escape_char,
            "encoding": encoding,
            "null_values": null_values_list,
            "data_types": data_types_list,
            "db_url": db_url,
        },
        headers={"db_url": db_url},
    )
    return PlainTextResponse(content="", status_code=status.HTTP_202_ACCEPTED)


@router.post(
    "/upload/preview/{upload_id}",
    responses={
        status.HTTP_200_OK: {
            "description": "Preview complete - returns first N lines as plain text CSV",
            "content": {"text/plain": {"example": _csv_upload_example}},
        },
        status.HTTP_400_BAD_REQUEST: {"description": "Empty data or invalid request"},
        status.HTTP_404_NOT_FOUND: {"description": "Preview upload session not found"},
        status.HTTP_413_REQUEST_ENTITY_TOO_LARGE: {"description": "Compressed data exceeds limit"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Decompression or processing error"},
    },
)
async def upload_preview_chunk(
    db: AuthenticatedSession,
    upload_id: Annotated[UUID, Path()],
    content: Annotated[bytes, Body(media_type="text/plain")] = b"",
) -> PlainTextResponse:
    if len(content) == 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No data provided")

    if len(content) > MAX_PREVIEW_UPLOAD_CHUNK_SIZE:
        raise StructuredHTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            error="PayloadTooLarge",
            message="Preview data exceeds 100KB limit",
            details={"maxSize": int(MAX_PREVIEW_UPLOAD_CHUNK_SIZE), "receivedSize": len(content)},
        )

    preview_upload = await db.scalar(select(PreviewUpload).where(PreviewUpload.uuid == upload_id))
    if not preview_upload:
        raise StructuredHTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            error="UploadNotFound",
            message="Upload session not found or expired",
            details={"uploadId": str(upload_id)},
        )

    try:
        logger.info(f"Processing preview upload {upload_id} with {len(content)} bytes")

        match preview_upload.compression:
            case CompressionType.zip:
                logger.info("Decompressing ZIP content")
                csv_text = decompress_zip(content)
            case CompressionType.gzip:
                logger.info("Decompressing GZIP content")
                csv_text = decompress_gzip(content)
            case CompressionType.bzip2:
                logger.info("Decompressing BZIP2 content")
                csv_text = decompress_bzip2(content)
            case CompressionType.none:
                logger.info("No compression, decoding content directly")
                csv_text = content.decode("utf-8", errors="replace")

        logger.info(f"Loaded to {len(csv_text)} characters")

        result_text = extract_first_n_lines(csv_text, preview_upload.max_lines)
        lines_count = len(result_text.split("\n"))
        logger.info(f"Extracted {lines_count} lines for preview")

        await db.delete(preview_upload)
        await db.commit()
        logger.info(f"Deleted preview upload session {upload_id}")

        return PlainTextResponse(content=result_text, status_code=status.HTTP_200_OK)
    except CompressionError as e:
        logger.error(f"Decompression error for preview {upload_id}: {e}")
        await db.delete(preview_upload)
        await db.commit()
        raise StructuredHTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            error="InvalidArchive",
            message=f"Failed to decompress: {str(e)}",
            details={"compression": preview_upload.compression.value},
        )
    except Exception as e:
        logger.error(f"Unexpected error processing preview {upload_id}: {e}", exc_info=True)
        await db.delete(preview_upload)
        await db.commit()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Processing failed: {str(e)}")


@router.get("/datasource")
async def list_data_sources(
    db: AuthenticatedSession,
) -> list[DataSourceRead]:
    """List all data sources."""
    data_sources = (await db.execute(select(DataSource).options(joinedload(DataSource.upload)))).scalars().all()
    return [DataSourceRead.model_validate(ds) for ds in data_sources]


@router.delete(
    "/datasource/{id}",
    status_code=status.HTTP_200_OK,  # TODO: This should return 204 to be Restful and also have better OAPI UI
    responses={
        status.HTTP_404_NOT_FOUND: {},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {},
    },
)
async def delete_data_source(db: AuthenticatedSession, id: Annotated[int, Path()]) -> None:
    """Delete a data source."""
    result = await db.execute(delete(DataSource).where(DataSource.id == id))
    if result.rowcount != 1:
        raise StructuredHTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            error="DataSourceNotFound",
            message="Data source not found",
            details={"dataSourceId": id},
        )


@router.get(
    "/datasource/{id}",
    responses={
        status.HTTP_404_NOT_FOUND: {},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {},
    },
)
async def get_data_source(db: AuthenticatedSession, id: Annotated[int, Path()]) -> DataSourceRead:
    """Get a specific data source."""
    data_source = await db.get(DataSource, id, options=[joinedload(DataSource.upload)])
    if not data_source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found")
    return DataSourceRead.model_validate(data_source)


@router.put(
    "/datasource/{id}",
    status_code=status.HTTP_200_OK,  # TODO: This should return 204 to be Restful and also have better OAPI UI
    responses={
        status.HTTP_404_NOT_FOUND: {},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {},
    },
)
async def rename_data_source(
    db: AuthenticatedSession,
    id: Annotated[int, Path()],
    name: Annotated[str, Body(examples=["A New Exciting Name"], media_type="text/plain")],
) -> None:
    data_source = await db.get(DataSource, id)
    if not data_source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found")
    data_source.name = name
    await db.commit()


@router.get(
    "/datasource/{id}/instances",
    status_code=status.HTTP_200_OK,
    responses={
        status.HTTP_400_BAD_REQUEST: {},
        status.HTTP_404_NOT_FOUND: {},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {},
    },
)
async def get_instances(
    db: AuthenticatedSession,
    id: Annotated[int, Path()],
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    field_ids: Annotated[list[int] | None, Query()] = None,
) -> list[AggregatedInstance]:
    data_source = await db.get(DataSource, id, options=[joinedload(DataSource.upload)])
    if not data_source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found")
    if data_source.upload.state != UploadState.finished:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Data source upload is not finished processing"
        )

    # Check if data source has instances
    instances_count = (
        await db.execute(
            select(func.count(distinct(DataSourceInstance.row_id)))
            .select_from(DataSourceInstance)
            .where(DataSourceInstance.data_source_id == id)
        )
    ).scalar_one()
    if instances_count == 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Data source has no instances")

    # First, get the row_ids for pagination
    row_ids_stmt = (
        select(distinct(DataSourceInstance.row_id))
        .where(DataSourceInstance.data_source_id == id)
        .order_by(DataSourceInstance.row_id)
        .limit(limit)
        .offset(offset)
    )

    if field_ids:
        # If filtering by field_ids, we need to ensure the row has at least one of those fields
        row_ids_stmt = (
            select(distinct(DataSourceInstance.row_id))
            .select_from(DataSourceInstance)
            .join(Field, DataSourceInstance.field_id == Field.id)
            .where(DataSourceInstance.data_source_id == id, Field.index.in_(field_ids))
            .order_by(DataSourceInstance.row_id)
            .limit(limit)
            .offset(offset)
        )

    row_ids_result = await db.execute(row_ids_stmt)
    row_ids: list[int] = [row[0] for row in row_ids_result.all()]

    if not row_ids:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No instances found")

    # Now get all the data for these specific rows in a single query
    data_stmt = (
        select(
            DataSourceInstance.row_id,
            Field.index.label("field_index"),
            DataSourceInstance.value_nominal,
            DataSourceInstance.value_numeric,
        )
        .select_from(DataSourceInstance)
        .join(Field, DataSourceInstance.field_id == Field.id)
        .where(
            DataSourceInstance.data_source_id == id,
            DataSourceInstance.row_id.in_(row_ids),
        )
        .order_by(DataSourceInstance.row_id, Field.index)
    )

    if field_ids:
        data_stmt = data_stmt.where(Field.index.in_(field_ids))

    data_result = await db.execute(data_stmt)
    data_rows = data_result.all()

    # Group the results by row_id
    instances_data: dict[int, list[AggregatedInstanceValue]] = defaultdict(list)
    for row in data_rows:
        # Use numeric value if it exists, otherwise use nominal value
        if row.value_numeric is not None:
            value = float(row.value_numeric)
        else:
            value = row.value_nominal

        instances_data[row.row_id].append(AggregatedInstanceValue(field=row.field_index, value=value))

    # Build the response maintaining the order of row_ids
    response: list[AggregatedInstance] = []
    for row_id in row_ids:
        if row_id in instances_data:
            response.append(AggregatedInstance(id=row_id, values=instances_data[row_id]))

    return response


@router.get(
    "/datasource/{id}/field",
    responses={
        status.HTTP_404_NOT_FOUND: {},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {},
    },
)
async def get_fields(db: AuthenticatedSession, id: Annotated[int, Path()]) -> list[FieldRead]:
    """List all fields for a data source."""
    data_source = await db.get(
        DataSource,
        id,
        options=[
            joinedload(DataSource.fields).joinedload(Field.numeric_detail),
            joinedload(DataSource.upload),
        ],
    )
    if not data_source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found")
    if data_source.upload.state != UploadState.finished:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Data source upload is not finished processing"
        )
    return [FieldRead.model_validate(field) for field in data_source.fields]


@router.delete(
    "/datasource/{id}/field/{field_id}",
    responses={
        status.HTTP_404_NOT_FOUND: {},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {},
    },
)
async def delete_field(
    db: AuthenticatedSession,
    id: Annotated[int, Path()],
    field_id: Annotated[int, Path()],
):
    data_source = await db.get(DataSource, id)
    if not data_source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found")

    field = await db.get(Field, field_id)
    if not field or field.data_source_id != id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Field not found")

    await db.delete(field)
    await db.commit()


@router.get(
    "/datasource/{id}/field/{field_id}",
    responses={
        status.HTTP_404_NOT_FOUND: {},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {},
    },
)
async def get_field(
    id: Annotated[int, Path()],
    field_id: Annotated[int, Path()],
    db: AuthenticatedSession,
) -> FieldRead:
    """Get metadata for a specific field."""
    data_source = await db.get(DataSource, id)
    if not data_source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found")

    field = await db.get(Field, field_id, options=[joinedload(Field.numeric_detail)])
    if not field or field.data_source_id != id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Field not found")

    return FieldRead.model_validate(field)


@router.put(
    "/datasource/{id}/field/{field_id}",
    # NOTE: Response status code should be 204
    responses={
        status.HTTP_404_NOT_FOUND: {},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {},
    },
)
async def rename_field(
    db: AuthenticatedSession,
    id: Annotated[int, Path()],
    field_id: Annotated[int, Path()],
    name: Annotated[str, Body(examples=["New Field Name"], media_type="text/plain")],
):
    data_source = await db.get(DataSource, id)
    if not data_source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found")

    field = await db.get(Field, field_id)
    if not field or field.data_source_id != id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Field not found")

    field.name = name
    await db.commit()


@router.put(
    "/datasource/{id}/field/{field_id}/change-type",
    description="Toggle between nominal and numeric field types",
    # Response status code should be 204
    responses={
        status.HTTP_404_NOT_FOUND: {},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {},
    },
)
async def toggle_field_type(
    db: AuthenticatedSession,
    id: Annotated[int, Path()],
    field_id: Annotated[int, Path()],
):
    data_source = await db.get(DataSource, id)
    if not data_source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found")

    field = await db.get(Field, field_id)
    if not field or field.data_source_id != id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Field not found")

    new_type = FieldType.nominal if field.data_type == FieldType.numeric else FieldType.numeric
    field.data_type = new_type
    await db.commit()


@router.get(
    "/datasource/{id}/field/{field_id}/stats",
    status_code=status.HTTP_200_OK,
    responses={
        status.HTTP_400_BAD_REQUEST: {},
        status.HTTP_404_NOT_FOUND: {},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {},
    },
)
async def get_field_stats(
    id: Annotated[int, Path()],
    field_id: Annotated[int, Path()],
    db: AuthenticatedSession,
) -> FieldStatsSchema:
    data_source = (await db.execute(select(DataSource).where(DataSource.id == id))).scalar_one_or_none()
    if not data_source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found")

    field = (
        await db.execute(select(Field).where(Field.data_source_id == id, Field.id == field_id))
    ).scalar_one_or_none()
    if not field:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Field not found")
    if field.data_type != FieldType.numeric:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Field is not of numeric type")

    field_numerical_details = (
        await db.execute(select(FieldNumericDetail).where(FieldNumericDetail.id == field_id))
    ).scalar_one_or_none()
    if not field_numerical_details:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Field numeric details not found")

    return FieldStatsSchema(
        id=field.id,
        min=float(field_numerical_details.min_value),
        max=float(field_numerical_details.max_value),
        avg=float(field_numerical_details.avg_value),
    )


@router.get(
    "/datasource/{id}/field/{field_id}/values",
    status_code=status.HTTP_200_OK,
    responses={
        status.HTTP_404_NOT_FOUND: {},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {},
    },
)
async def get_field_values(
    db: AuthenticatedSession,
    id: Annotated[int, Path()],
    field_id: Annotated[int, Path()],
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=0, le=1000)] = 100,
) -> list[FieldValueSchema]:
    data_source = await db.get(DataSource, id)
    if not data_source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found")

    field = await db.get(Field, field_id)
    if not field or field.data_source_id != id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Field not found")

    # NOTE: possibly rewrite to pre-save values in db
    instances = (
        (
            await db.execute(
                select(
                    DataSourceInstance.field_id,
                    DataSourceInstance.value_nominal,
                    DataSourceInstance.value_numeric,
                    func.count(DataSourceInstance.id).label("frequency"),
                )
                .where(DataSourceInstance.field_id == field.id)
                .group_by(
                    DataSourceInstance.field_id,
                    DataSourceInstance.value_nominal,
                    DataSourceInstance.value_numeric,
                )
                .limit(limit)
                .offset(offset)
            )
        )
        .tuples()
        .all()
    )
    if not instances:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No field values found")

    result: list[FieldValueSchema] = []
    for instance in instances:
        if field.data_type == FieldType.numeric:
            result.append(FieldValueSchema(id=instance[0], value=float(instance[2]) or 0, frequency=instance[3]))
        else:
            result.append(FieldValueSchema(id=instance[0], value=instance[1] or "", frequency=instance[3]))
    return result


@router.get(
    "/datasource/{id}/field/{field_id}/aggregated-values",
    summary="Create a task for getting a histogram of a numeric field where values are aggregated to intervals by number of bins.",
    description="There is one required query parameter 'bins'. This value means number of bins in an output histogram (maximum is 1000). You can specify min and max borders. This operation is processed asynchronously due to its complexity - it returns 202 Accepted and a location header with URL where all information about the task status are placed (see the background tasks section).",
    status_code=status.HTTP_200_OK,
    responses={
        status.HTTP_200_OK: {
            "links": {
                "TaskStatus": {
                    "operationId": "get_task_status",
                    "parameters": {"task_id": "{task_id}"},
                    "description": "Get task status",
                },
                "TaskResult": {
                    "operationId": "get_task_result",
                    "parameters": {"task_id": "{task_id}"},
                    "description": "Get task result",
                },
            },
            "model": TaskStatus,
        },
        status.HTTP_400_BAD_REQUEST: {},
        status.HTTP_404_NOT_FOUND: {},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {},
    },
)
async def get_aggregated_values(
    db: AuthenticatedSession,
    api_key: ApiKey,
    request: Request,
    id: Annotated[int, Path()],
    field_id: Annotated[int, Path()],
    bins: Annotated[int, Query(ge=1, le=1000)] = 10,
    min: Annotated[Decimal | None, Query()] = None,
    max: Annotated[Decimal | None, Query()] = None,
    min_inclusive: Annotated[bool, Query()] = True,
    max_inclusive: Annotated[bool, Query()] = True,
) -> TaskStatus:
    data_source = (await db.execute(select(DataSource).where(DataSource.id == id))).scalar_one_or_none()
    if not data_source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found")

    field = (
        await db.execute(select(Field).where(Field.data_source_id == id, Field.id == field_id))
    ).scalar_one_or_none()
    if not field:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Field not found")

    if field.data_type != FieldType.numeric:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Field is not of numeric type")

    # Select min and max values if not provided
    if min is None:
        field_min = (
            await db.execute(select(FieldNumericDetail.min_value).where(FieldNumericDetail.id == field_id))
        ).scalar_one_or_none()
        if field_min is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Couldn't find field min value. Please provide it in the request",
            )
        min = field_min
    if max is None:
        field_max = (
            await db.execute(select(FieldNumericDetail.max_value).where(FieldNumericDetail.id == field_id))
        ).scalar_one_or_none()
        if field_max is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Couldn't find field max value. Please provide it in the request",
            )
        max = field_max

    # Get database config to pass to Celery task
    db_config = await get_database_config(api_key, DbType.limited)
    db_url = db_config.get_sync_url()

    task = aggregate_field_values.apply_async(
        kwargs={
            "data_source_id": data_source.id,
            "field_id": field.id,
            "bins": bins,
            "min": min,
            "max": max,
            "min_inclusive": min_inclusive,
            "max_inclusive": max_inclusive,
            "db_url": db_url,
        },
        headers={"db_url": db_url},
    )

    return TaskStatus(
        task_id=UUID(task.task_id),
        task_name="get_aggregated_values",
        status_message="Task created successfully",
        status_location=request.url_for("get_task_status", task_id=task.task_id).path,
        result_location=request.url_for("get_task_result", task_id=task.task_id).path,
    )
