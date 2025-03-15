from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import ConfigDict, Field

from easyminer.schemas import BaseSchema


class CompressionType(str, Enum):
    """Compression type for file uploads."""

    zip = "zip"
    gzip = "gzip"
    bzip2 = "bzip2"


class PreviewUpload(BaseSchema):
    """Settings for preview upload."""

    max_lines: int = Field(..., description="Maximum number of lines to preview")
    compression: CompressionType | None = Field(
        None, description="Compression type (none, gzip, etc.)"
    )


class PreviewResponse(BaseSchema):
    """Response model for the data source preview endpoint."""

    field_names: list[str] = Field(..., description="List of field names")
    rows: list[dict[str, Any]] = Field(
        ..., description="List of rows with field values"
    )


class Instance(BaseSchema):
    """A single instance (row) from a data source."""

    values: dict[str, Any] = Field(..., description="Field values for this instance")


class InstanceList(BaseSchema):
    """List of instances from a data source."""

    instances: list[Instance] = Field(..., description="List of instances")


class UploadSettings(BaseSchema):
    """Settings for file upload."""

    name: str = Field(..., description="Name of the file")
    media_type: str = Field(..., description="Media type (e.g., text/csv)")
    db_type: str = Field(..., description="Database type (e.g., mysql)")
    separator: str = Field(",", description="CSV separator character")
    encoding: str = Field("utf-8", description="File encoding")
    quotes_char: str = Field('"', description="Quote character")
    escape_char: str = Field("\\", description="Escape character")
    locale: str = Field("en_US", description="Locale for number formatting")
    compression: str = Field("none", description="Compression type (none, gzip, etc.)")
    format: str = Field("csv", description="File format")


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
    user_id: int
    upload_id: int | None = None
    model_config = ConfigDict(from_attributes=True)


class FieldBase(BaseSchema):
    """Base schema for field."""

    name: str = Field(..., description="Name of the field")
    data_type: str = Field(..., description="Data type of the field")
    index: int = Field(..., description="Position in the data source")


class FieldCreate(FieldBase):
    """Schema for creating a field."""

    pass


class FieldRead(FieldBase):
    """Schema for reading a field."""

    id: int
    data_source_id: int
    unique_values_count: int | None = None
    missing_values_count: int | None = None
    min_value: str | None = None
    max_value: str | None = None
    avg_value: float | None = None
    std_value: float | None = None
    model_config = ConfigDict(from_attributes=True)
