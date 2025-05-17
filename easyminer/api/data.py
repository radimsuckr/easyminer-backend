import itertools
import logging
import pathlib
from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import (
    APIRouter,
    Body,
    Depends,
    HTTPException,
    Path,
    Query,
    Request,
    status,
)
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from easyminer.config import API_V1_PREFIX
from easyminer.crud.aio.chunk import create_chunk
from easyminer.crud.aio.data_source import (
    delete_data_source,
    get_data_source_by_id,
    get_data_sources,
    update_data_source_name,
)
from easyminer.crud.aio.field import (
    get_field_by_id,
    get_fields_by_data_source,
)
from easyminer.crud.aio.upload import (
    create_preview_upload,
    create_upload,
)
from easyminer.database import get_db_session
from easyminer.models import (
    DataSource,
    Field,
    FieldNumericDetail,
    FieldType,
    Instance,
    Upload,
)
from easyminer.schemas.data import (
    AggregatedInstance,
    AggregatedInstanceValue,
    DataSourceRead,
    FieldRead,
    FieldStatsSchema,
    FieldValueSchema,
    PreviewUploadSchema,
    StartUploadSchema,
    UploadResponseSchema,
)
from easyminer.schemas.task import TaskStatus
from easyminer.storage import DiskStorage
from easyminer.tasks.process_chunk import process_chunk

# Maximum chunk size for preview uploads (1MB)
MAX_CHUNK_SIZE = 1000 * 1024

router = APIRouter(prefix=API_V1_PREFIX, tags=["Data"])

logger = logging.getLogger(__name__)

_csv_upload_example = """a,b,c
1,2,3
4,5,6
7,x,9"""


@router.post("/upload/start")
async def start_upload(db: Annotated[AsyncSession, Depends(get_db_session)], settings: StartUploadSchema) -> UUID:
    """Start a new upload process."""
    if settings.media_type != "csv":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only CSV uploads are supported",
        )

    upload_uuid = await create_upload(db, settings)
    await db.commit()
    return upload_uuid


@router.post("/upload/preview/start")
async def start_preview_upload(
    db: Annotated[AsyncSession, Depends(get_db_session)], settings: PreviewUploadSchema
) -> UUID:
    upload = await create_preview_upload(db, settings)
    return upload.uuid


@router.post(
    "/upload/{upload_id}",
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
    db: Annotated[AsyncSession, Depends(get_db_session)],
    upload_id: Annotated[UUID, Path()],
    content: Annotated[
        str, Body(example=_csv_upload_example, media_type="text/plain")
    ] = "",  # NOTE: text/csv could possibly be used here
):
    if len(content) > MAX_CHUNK_SIZE:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Chunk size exceeds {MAX_CHUNK_SIZE} bytes",
        )

    upload = await db.scalar(select(Upload).where(Upload.uuid == upload_id))
    if not upload:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Upload not found")

    chunk_datetime = datetime.now()
    chunk_path = pathlib.Path(f"{upload_id}/chunks/{chunk_datetime.strftime('%Y%m%d%H%M%S%f')}.chunk")
    storage = DiskStorage()
    _, saved_path = storage.save(chunk_path, content.encode("utf-8"))
    chunk_id = await create_chunk(db, upload.id, chunk_datetime, str(saved_path))
    logger.info("Chunk %s created for upload %s", chunk_id, upload_id)

    await db.commit()

    result = process_chunk.delay(chunk_id)
    return result.get(timeout=10)


@router.post(
    "/upload/preview/{upload_id}",
    responses={
        status.HTTP_200_OK: {"description": "Upload successful and closed"},
        status.HTTP_202_ACCEPTED: {"description": "Chunk accepted"},
        status.HTTP_403_FORBIDDEN: {"description": "Upload already closed"},
        status.HTTP_429_TOO_MANY_REQUESTS: {"description": "Uploading chunks too fast"},
    },
)
async def upload_preview_chunk(
    db: Annotated[AsyncSession, Depends(get_db_session)],
    upload_id: Annotated[UUID, Path()],
    chunk: Annotated[str, Body(example=_csv_upload_example, media_type="text/plain")] = "",
):
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)


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
    responses={
        status.HTTP_404_NOT_FOUND: {},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {},
    },
)
async def delete_data_source_api(
    id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> None:
    """Delete a data source."""
    success = await delete_data_source(db, id)
    if not success:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found")


@router.get(
    "/datasource/{id}",
    responses={
        status.HTTP_404_NOT_FOUND: {},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {},
    },
)
async def get_data_source_api(
    id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> DataSourceRead:
    """Get a specific data source."""
    data_source = await get_data_source_by_id(db, id, eager=True)
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
    id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    name: Annotated[str, Body(example="A New Exciting Name", media_type="text/plain")],
) -> None:
    data_source = await update_data_source_name(db, id, name)
    if not data_source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found")


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
    db: Annotated[AsyncSession, Depends(get_db_session)],
    id: Annotated[int, Path()],
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    field_ids: Annotated[list[int] | None, Query()] = None,
) -> list[AggregatedInstance]:
    query = (
        select(DataSource)
        .where(DataSource.id == id)
        .options(
            joinedload(DataSource.instances),
        )
    )
    data_source = (await db.execute(query)).unique().scalar_one_or_none()
    if not data_source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found")
    if not data_source.is_finished:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Data source is not finished processing",
        )
    if not data_source.instances:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Data source has no instances",
        )

    fields_count = (
        await db.execute(select(func.count()).select_from(Field).where(Field.data_source_id == id))
    ).scalar_one()

    stmt = (
        select(Instance).limit(fields_count * limit).offset(fields_count * offset).options(joinedload(Instance.field))
    )
    if field_ids:
        stmt = stmt.where(Field.id.in_(field_ids))
    instances = (await db.execute(stmt)).scalars().all()
    if not instances:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No instances found")

    # the response of AggregatedInstance list must be grouped by row_id
    response: list[AggregatedInstance] = []
    for key, group in itertools.groupby(instances, lambda x: x.row_id):
        group = list(group)
        response.append(
            AggregatedInstance(
                id=key,
                values=[
                    AggregatedInstanceValue(field=instance.field_id, value=instance.value_nominal)
                    if instance.field.data_type == FieldType.nominal
                    else AggregatedInstanceValue(field=instance.field_id, value=instance.value_numeric)
                    for instance in group
                ],
            )
        )

    return response


@router.get(
    "/datasource/{id}/field",
    responses={
        status.HTTP_404_NOT_FOUND: {},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {},
    },
)
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
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found")
    if not data_source.is_finished:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Data source is not finished processing")

    # Get fields for the data source
    fields = await get_fields_by_data_source(db, id)

    return [FieldRead.model_validate(field) for field in fields]


@router.delete(
    "/datasource/{id}/field/{field_id}",
    responses={
        status.HTTP_404_NOT_FOUND: {},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {},
    },
)
async def delete_field_api(
    id: Annotated[int, Path()],
    field_id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):
    # Get the data source to validate access
    data_source = await get_data_source_by_id(db, id)
    if not data_source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found")

    # Get the field to validate access
    field = await get_field_by_id(db, field_id, id)
    if not field:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Field not found")

    # Delete the field
    await db.delete(field)
    await db.commit()


@router.get(
    "/datasource/{id}/field/{field_id}",
    responses={
        status.HTTP_404_NOT_FOUND: {},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {},
    },
)
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
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found")

    # Get the field
    field = await get_field_by_id(db, field_id, id)
    if not field:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Field not found")

    return FieldRead.model_validate(field)


@router.put(
    "/datasource/{id}/field/{field_id}",
    responses={
        status.HTTP_404_NOT_FOUND: {},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {},
    },
)
async def rename_field(
    id: Annotated[int, Path()],
    field_id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    name: Annotated[str, Body(example="New Field Name", media_type="text/plain")],
):  # Response status code should be 204
    # Get the data source to validate access
    data_source = await get_data_source_by_id(db, id)
    if not data_source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found")

    # Get the field
    field = await get_field_by_id(db, field_id, id)
    if not field:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Field not found")

    # Rename the field
    _ = await db.execute(update(Field).where(Field.id == field.id).values(name=name))
    await db.commit()


@router.put(
    "/datasource/{id}/field/{field_id}/change-type",
    description="Toggle between nominal and numeric field types",
    responses={
        status.HTTP_404_NOT_FOUND: {},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {},
    },
)
async def toggle_field_type(
    id: Annotated[int, Path()],
    field_id: Annotated[int, Path()],
    db: Annotated[AsyncSession, Depends(get_db_session)],
):  # Response status code should be 204
    # Get the data source to validate access
    data_source = await get_data_source_by_id(db, id)
    if not data_source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found")

    # Get the field
    field = await get_field_by_id(db, field_id, id)
    if not field:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Field not found")

    # Toggle the field type
    new_type = FieldType.nominal if field.data_type == FieldType.numeric else FieldType.numeric
    _ = await db.execute(update(Field).where(Field.id == field.id).values(data_type=new_type))
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
    db: Annotated[AsyncSession, Depends(get_db_session)],
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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Field is not of numeric type",
        )

    field_numerical_details = (
        await db.execute(select(FieldNumericDetail).where(FieldNumericDetail.id == field_id))
    ).scalar_one_or_none()
    if not field_numerical_details:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Field details not found")

    return FieldStatsSchema(
        id=field.id,
        min=field_numerical_details.min_value,
        max=field_numerical_details.max_value,
        avg=field_numerical_details.avg_value,
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
    db: Annotated[AsyncSession, Depends(get_db_session)],
    id: Annotated[int, Path()],
    field_id: Annotated[int, Path()],
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=0, le=1000)] = 100,
) -> list[FieldValueSchema]:
    data_source = (await db.execute(select(DataSource).where(DataSource.id == id))).scalar_one_or_none()
    if not data_source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found")

    field = (
        await db.execute(select(Field).where(Field.data_source_id == id, Field.id == field_id))
    ).scalar_one_or_none()
    if not field:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Field not found")

    # NOTE: possibly rewrite to pre-save values in db
    instances = (
        (
            await db.execute(
                select(
                    Instance.field_id,
                    Instance.value_nominal,
                    Instance.value_numeric,
                    func.count(Instance.id).label("frequency"),
                )
                .where(Instance.field_id == field.id)
                .group_by(Instance.field_id, Instance.value_nominal, Instance.value_numeric)
                .limit(limit)
                .offset(offset)
            )
        )
        .tuples()
        .all()
    )
    if not instances:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No values found")

    result: list[FieldValueSchema] = []
    for instance in instances:
        if field.data_type == FieldType.numeric:
            result.append(
                FieldValueSchema(id=instance.field_id, value=instance.value_numeric, frequency=instance.frequency)
            )
        else:
            result.append(
                FieldValueSchema(id=instance.field_id, value=instance.value_nominal, frequency=instance.frequency)
            )
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
    db: Annotated[AsyncSession, Depends(get_db_session)],
    request: Request,
    id: Annotated[int, Path()],
    field_id: Annotated[int, Path()],
    bins: Annotated[int, Query(ge=1, le=1000)] = 10,
    min: Annotated[float | None, Query()] = None,
    max: Annotated[float | None, Query()] = None,
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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Field is not of numeric type",
        )

    # Select min and max values if not provided
    if min is None:
        field_min = (
            await db.execute(select(FieldNumericDetail.min_value).where(FieldNumericDetail.id == field_id))
        ).scalar_one_or_none()
        if field_min is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Field min value not found",
            )
        min = field_min
    if max is None:
        field_max = (
            await db.execute(select(FieldNumericDetail.max_value).where(FieldNumericDetail.id == field_id))
        ).scalar_one_or_none()
        if field_max is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Field max value not found",
            )
        max = field_max

    # Create the task
    # task = aggregate_field_values.delay(
    #     data_source_id=data_source.id,
    #     field_id=field.id,
    #     bins=bins,
    #     min=min,
    #     max=max,
    #     min_inclusive=min_inclusive,
    #     max_inclusive=max_inclusive,
    # )

    return TaskStatus(
        task_id=UUID(task.task_id),
        task_name="get_aggregated_values",
        status_message="Task created successfully",
        status_location=request.url_for("get_task_status", task_id=task.task_id).path,
        result_location=request.url_for("get_task_result", task_id=task.task_id).path,
    )
