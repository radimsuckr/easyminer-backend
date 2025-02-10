from enum import Enum
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Body, HTTPException, Path, Query
from pydantic import BaseModel
from pydantic import Field as PField

router = APIRouter()


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


class Error(BaseModel):
    code: int
    name: str
    message: str


class DataSource(BaseModel):
    id: int
    name: str = PField(max_length=255)
    type: DbType
    size: int


class Field(BaseModel):
    id: int
    dataSource: int
    name: str = PField(max_length=255)
    type: str
    uniqueValuesSize: int
    support: int


class Value(BaseModel):
    id: int
    frequency: int
    value: str | float | None


class Stats(BaseModel):
    id: int
    min: float
    max: float
    avg: float


class Interval(BaseModel):
    from_: float = PField(alias="from")
    to: float
    fromInclusive: bool
    toInclusive: bool
    frequency: int


class Upload(BaseModel):
    name: str = PField(max_length=255)
    mediaType: MediaType
    dbType: DbType
    separator: str | None = PField(default=None, min_length=1, max_length=1)
    encoding: str | None = None
    quotesChar: str | None = PField(default=None, min_length=1, max_length=1)
    escapeChar: str | None = PField(default=None, min_length=1, max_length=1)
    locale: str | None = None
    compression: CompressionType | None = None
    nullValues: list[str] | None = None
    dataTypes: list[str] | None = None
    format: RdfFormat | None = None


class PreviewUpload(BaseModel):
    maxLines: int = PField(gt=0)
    compression: CompressionType | None = None


class TaskStatus(BaseModel):
    taskId: UUID
    taskName: str
    statusMessage: str | None = None
    statusLocation: str | None = None
    resultLocation: str | None = None


@router.post("/api/v1/upload/start", response_model=str)
async def start_upload(upload: Upload):
    raise HTTPException(status_code=HTTPStatus.NOT_IMPLEMENTED)


@router.post(
    "/api/v1/upload/{id}",
    responses={200: {"model": DataSource}, 202: {"model": None}, 429: {"model": None}},
)
async def upload_chunk(id: UUID, chunk: Annotated[str | None, Body()] = None):
    raise HTTPException(status_code=HTTPStatus.NOT_IMPLEMENTED)


@router.post("/api/v1/upload/preview/start", response_model=str)
async def start_preview_upload(preview_upload: PreviewUpload):
    raise HTTPException(status_code=HTTPStatus.NOT_IMPLEMENTED)


@router.post(
    "/api/v1/upload/preview/{id}", responses={200: {"model": str}, 202: {"model": None}}
)
async def upload_preview_chunk(id: UUID, chunk: Annotated[str | None, Body()] = None):
    raise HTTPException(status_code=HTTPStatus.NOT_IMPLEMENTED)


@router.get("/api/v1/datasource", response_model=list[DataSource])
async def list_datasources():
    raise HTTPException(status_code=HTTPStatus.NOT_IMPLEMENTED)


@router.get("/api/v1/datasource/{id}", response_model=DataSource)
async def get_datasource(id: Annotated[int | None, Path()]):
    raise HTTPException(status_code=HTTPStatus.NOT_IMPLEMENTED)


@router.delete("/api/v1/datasource/{id}")
async def delete_datasource(id: Annotated[int | None, Path()]):
    raise HTTPException(status_code=HTTPStatus.NOT_IMPLEMENTED)


@router.put("/api/v1/datasource/{id}")
async def rename_datasource(
    id: Annotated[int | None, Path()],
    new_name: Annotated[str | None, Body()] = None,
):
    raise HTTPException(status_code=HTTPStatus.NOT_IMPLEMENTED)


@router.get("/api/v1/datasource/{id}/instances")
async def get_instances(
    id: Annotated[int | None, Path()],
    offset: Annotated[int | None, Query(ge=0)] = None,
    limit: Annotated[int | None, Query(ge=1, le=1000)] = None,
    field: Annotated[list[int] | None, Query()] = None,
):
    raise HTTPException(status_code=HTTPStatus.NOT_IMPLEMENTED)


@router.get("/api/v1/datasource/{dataSourceId}/field", response_model=list[Field])
async def list_fields(dataSourceId: Annotated[int | None, Path()]):
    raise HTTPException(status_code=HTTPStatus.NOT_IMPLEMENTED)


@router.get("/api/v1/datasource/{dataSourceId}/field/{fieldId}", response_model=Field)
async def get_field(
    dataSourceId: Annotated[int | None, Path()], fieldId: Annotated[int | None, Path()]
):
    raise HTTPException(status_code=HTTPStatus.NOT_IMPLEMENTED)


@router.delete("/api/v1/datasource/{dataSourceId}/field/{fieldId}")
async def delete_field(
    dataSourceId: Annotated[int | None, Path()],
    fieldId: Annotated[int | None, Path()],
):
    raise HTTPException(status_code=HTTPStatus.NOT_IMPLEMENTED)


@router.put("/api/v1/datasource/{dataSourceId}/field/{fieldId}")
async def rename_field(
    dataSourceId: Annotated[int | None, Path()],
    fieldId: Annotated[int | None, Path()],
    new_name: Annotated[str | None, Body()] = None,
):
    raise HTTPException(status_code=HTTPStatus.NOT_IMPLEMENTED)


@router.put("/api/v1/datasource/{dataSourceId}/field/{fieldId}/change-type")
async def change_field_type(
    dataSourceId: Annotated[int | None, Path()],
    fieldId: Annotated[int | None, Path()],
):
    raise HTTPException(status_code=HTTPStatus.NOT_IMPLEMENTED)


@router.get(
    "/api/v1/datasource/{dataSourceId}/field/{fieldId}/stats", response_model=Stats
)
async def get_field_stats(
    dataSourceId: Annotated[int | None, Path()],
    fieldId: Annotated[int | None, Path()],
):
    raise HTTPException(status_code=HTTPStatus.NOT_IMPLEMENTED)


@router.get(
    "/api/v1/datasource/{dataSourceId}/field/{fieldId}/values",
    response_model=list[Value],
)
async def get_field_values(
    dataSourceId: Annotated[int | None, Path()],
    fieldId: Annotated[int | None, Path()],
    offset: Annotated[int | None, Query(ge=0)] = None,
    limit: Annotated[int | None, Query(ge=1, le=1000)] = None,
):
    raise HTTPException(status_code=HTTPStatus.NOT_IMPLEMENTED)


@router.get(
    "/api/v1/datasource/{dataSourceId}/field/{fieldId}/aggregated-values",
    response_model=TaskStatus,
)
async def get_aggregated_values(
    dataSourceId: Annotated[int | None, Path()],
    fieldId: Annotated[int | None, Path()],
    bins: Annotated[int | None, Query(ge=2, le=1000)] = None,
    min: Annotated[float | None, Query()] = None,
    max: Annotated[float | None, Query()] = None,
    minInclusive: Annotated[bool | None, Query()] = True,
    maxInclusive: Annotated[bool | None, Query()] = True,
):
    raise HTTPException(status_code=HTTPStatus.NOT_IMPLEMENTED)


@router.get("/api/v1/task-status/{taskId}", response_model=TaskStatus)
async def get_task_status(taskId: Annotated[UUID | None, Path()]):
    raise HTTPException(status_code=HTTPStatus.NOT_IMPLEMENTED)


@router.get("/api/v1/task-result/{taskId}")
async def get_task_result(taskId: Annotated[UUID | None, Path()]):
    raise HTTPException(status_code=HTTPStatus.NOT_IMPLEMENTED)
