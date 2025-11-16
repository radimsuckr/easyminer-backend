from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any
from uuid import UUID

from pydantic import ConfigDict, Field, model_validator

from easyminer.schemas import BaseSchema

if TYPE_CHECKING:
    from easyminer.models.data import DataSource


class FieldType(str, Enum):
    nominal = "nominal"
    numeric = "numeric"


class UploadFormat(str, Enum):
    nq = "nq"
    nt = "nt"
    ttl = "ttl"


class CompressionType(str, Enum):
    zip = "zip"
    gzip = "gzip"
    bzip2 = "bzip2"
    none = ""


class MediaType(str, Enum):
    csv = "csv"


class Locale(str, Enum):
    en = "en"


class DbType(str, Enum):
    limited = "limited"


class PreviewUploadSchema(BaseSchema):
    max_lines: int = Field(20, ge=1, le=1000, description="Maximum number of lines to preview")
    compression: CompressionType = Field(CompressionType.none, description="Compression type")
    media_type: MediaType = Field(MediaType.csv, description="Media type")


class PreviewResponse(BaseSchema):
    field_names: list[str] = Field(..., description="List of field names")
    rows: list[dict[str, Any]] = Field(..., description="List of rows with field values")


class StartUploadSchema(BaseSchema):
    name: str = Field("upload", description="Name of the file")
    media_type: MediaType = Field(MediaType.csv, description="Media type")
    db_type: DbType = Field(DbType.limited, description="Database type")
    separator: str = Field(",", description="CSV separator character")
    encoding: str = Field("utf-8", description="File encoding")
    quotes_char: str = Field('"', description="Quote character")
    escape_char: str = Field("\\", description="Escape character")
    locale: Locale = Field(Locale.en, description="Locale for number formatting")
    null_values: list[str] = Field([], description="List of null value representations")
    data_types: list["FieldType"] = Field([], description="List of data types")


class DataSourceBase(BaseSchema):
    name: str = Field("Super data source", description="Name of the data source")
    type: DbType = Field(DbType.limited, description="Type of the data source")


class DataSourceCreate(DataSourceBase):
    size: int = Field(0, description="Size of the data source")


class DataSourceRead(DataSourceBase):
    id: int = Field(1, description="ID of the data source")
    created_at: datetime = Field(datetime(2025, 1, 1, 10, 10, 10), description="Creation date of the data source")
    updated_at: datetime = Field(
        datetime(2025, 1, 1, 10, 20, 30),
        description="Last update date of the data source",
    )
    size: int = Field(100, description="Size of the data source")
    upload_id: UUID = Field(
        UUID("397aab84-43c0-4cfc-9c9f-54d2585ce9ac"),
        description="UUID from either Upload or PreviewUpload relationship",
    )

    model_config: ConfigDict = ConfigDict(from_attributes=True)

    @model_validator(mode="before")
    @classmethod
    def set_upload_id(cls, data: "DataSource") -> "DataSource":
        if hasattr(data, "upload") and data.upload:
            setattr(data, "upload_id", data.upload.uuid)
        elif hasattr(data, "preview_upload") and data.preview_upload:
            setattr(data, "upload_id", data.preview_upload.uuid)
        else:
            raise ValueError("No upload or preview_upload relationship found")
        return data


class FieldBase(BaseSchema):
    name: str = Field(..., description="Name of the field")
    data_type: str = Field(..., description="Data type of the field")


class FieldCreate(FieldBase):
    pass


class FieldRead(FieldBase):
    id: int
    data_source_id: int
    unique_count: int | None = None
    support: int | None = None
    min_value: float | None = None
    max_value: float | None = None
    avg_value: float | None = None

    model_config: ConfigDict = ConfigDict(from_attributes=True)


class FieldStatsSchema(BaseSchema):
    id: int = Field(..., description="ID of the field")
    min: float = Field(..., description="Minimum value of the field")
    max: float = Field(..., description="Maximum value of the field")
    avg: float = Field(..., description="Average value of the field")


class FieldValueSchema(BaseSchema):
    id: int = Field(..., description="ID of the field")
    value: str | float = Field(..., description="Value of the field")
    frequency: int = Field(..., description="Frequency of the value")


class UploadResponseSchema(BaseSchema):
    id: int = Field(1, description="ID of the data source")
    name: str = Field("upload", description="Name of the data source")
    type: DbType = Field(DbType.limited, description="Database type")
    size: int = Field(100, description="Number of instances (rows) in the data source")


class AggregatedInstanceValue(BaseSchema):
    field: int = Field(description="Field ID")
    value: str | float | None = Field(description="Value can be string for nominal or number for numeric fields")


class AggregatedInstance(BaseSchema):
    id: int = Field(description="Row number (1-based)")
    values: list[AggregatedInstanceValue] = Field(description="List of field-value pairs")


class Value(BaseSchema):
    id: int
    value: Any = None
    frequency: int
