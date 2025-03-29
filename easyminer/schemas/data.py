from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import ConfigDict, Field, model_validator

from easyminer.schemas import BaseSchema


class CompressionType(str, Enum):
    zip = "zip"
    gzip = "gzip"
    bzip2 = "bzip2"


class MediaType(str, Enum):
    csv = "csv"


class DbType(str, Enum):
    limited = "limited"


class PreviewUploadSchema(BaseSchema):
    """Settings for preview upload."""

    max_lines: int = Field(..., description="Maximum number of lines to preview")
    compression: CompressionType | None = Field(
        ..., description="Compression type (none, gzip, etc.)"
    )


class PreviewResponse(BaseSchema):
    """Response model for the data source preview endpoint."""

    field_names: list[str] = Field(..., description="List of field names")
    rows: list[dict[str, Any]] = Field(
        ..., description="List of rows with field values"
    )


class UploadSettings(BaseSchema):
    """Settings for file upload."""

    name: str = Field("upload", description="Name of the file")
    media_type: MediaType = Field(MediaType.csv, description="Media type")
    db_type: DbType = Field(DbType.limited, description="Database type")
    separator: str = Field(",", description="CSV separator character")
    encoding: str = Field("utf-8", description="File encoding")
    quotes_char: str = Field('"', description="Quote character")
    escape_char: str = Field("\\", description="Escape character")
    locale: str = Field("en_US", description="Locale for number formatting")
    compression: CompressionType | None = Field(
        None, description="Compression type (none, gzip, etc.)"
    )


class DataSourceBase(BaseSchema):
    """Base schema for data source."""

    name: str = Field(..., description="Name of the data source")
    type: str = Field(..., description="Type of the data source (e.g., csv, mysql)")


class DataSourceCreate(DataSourceBase):
    """Schema for creating a data source."""

    size_bytes: int = Field(0, description="Size of the data source in bytes")
    row_count: int = Field(0, description="Number of rows in the data source")


class DataSourceRead(DataSourceBase):
    """Schema for reading a data source."""

    id: int
    created_at: datetime
    updated_at: datetime
    row_count: int
    size_bytes: int
    upload_id: UUID | None = Field(
        None, description="UUID from either Upload or PreviewUpload relationship"
    )
    model_config = ConfigDict(from_attributes=True, json_encoders={UUID: str})

    @model_validator(mode="before")
    @classmethod
    def set_upload_id(cls, data: Any) -> Any:
        """Set the upload_id from either Upload or PreviewUpload relationship."""
        try:
            if hasattr(data, "upload") and data.upload:
                # Upload.uuid is a string, convert it to UUID
                data.upload_id = UUID(data.upload.uuid)
            elif hasattr(data, "preview_upload") and data.preview_upload:
                # PreviewUpload.uuid is already a UUID
                data.upload_id = data.preview_upload.uuid
        except Exception:
            # If we can't access the relationships, just keep upload_id as None
            pass
        return data


class FieldBase(BaseSchema):
    """Base schema for field."""

    name: str = Field(..., description="Name of the field")
    data_type: str = Field(..., description="Data type of the field")


class FieldCreate(FieldBase):
    """Schema for creating a field."""

    pass


class FieldRead(FieldBase):
    """Schema for reading a field."""

    id: int
    data_source_id: int
    unique_count: int | None = None
    support: int | None = None
    min_value: float | None = None
    max_value: float | None = None
    avg_value: float | None = None
    model_config = ConfigDict(from_attributes=True)


class Stats(BaseSchema):
    min: float
    max: float
    avg: float
