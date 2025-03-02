from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class PreviewUpload(BaseModel):
    """Settings for preview upload."""
    maxLines: int = Field(..., description="Maximum number of lines to preview")
    compression: Optional[str] = Field(None, description="Compression type (none, gzip, etc.)")


class UploadSettings(BaseModel):
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


class DataSourceBase(BaseModel):
    """Base schema for data source."""
    name: str = Field(..., description="Name of the data source")
    type: str = Field(..., description="Type of the data source (e.g., csv, mysql)")


class DataSourceCreate(DataSourceBase):
    """Schema for creating a data source."""
    pass


class DataSourceRead(DataSourceBase):
    """Schema for reading a data source."""
    id: int
    created_at: datetime
    updated_at: datetime
    row_count: int
    size_bytes: int
    user_id: int
    upload_id: Optional[int] = None

    class Config:
        from_attributes = True


class FieldBase(BaseModel):
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
    unique_values_count: Optional[int] = None
    missing_values_count: Optional[int] = None
    min_value: Optional[str] = None
    max_value: Optional[str] = None
    avg_value: Optional[float] = None
    std_value: Optional[float] = None

    class Config:
        from_attributes = True
