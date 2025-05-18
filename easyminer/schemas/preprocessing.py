from typing import Any
from uuid import UUID

from pydantic import ConfigDict, Field

from easyminer.schemas import BaseSchema
from easyminer.schemas.data import DbType


class Error(BaseSchema):
    """Error schema."""

    code: int
    name: str
    message: str


class DatasetCreate(BaseSchema):
    """Schema for creating a dataset."""

    data_source: int
    name: str


class DatasetRead(BaseSchema):
    """Schema for reading a dataset."""

    id: int
    name: str
    data_source_id: int
    type: DbType
    size: int
    is_active: bool

    model_config: ConfigDict = ConfigDict(from_attributes=True)


class DatasetUpdate(BaseSchema):
    """Schema for updating a dataset."""

    name: str


class DatasetSchema(BaseSchema):
    id: int = Field(..., description="Dataset ID")
    name: str = Field(..., description="Dataset name")
    size: int = Field(..., description="Dataset size")
    type: DbType = Field(..., description="Database type")
    data_source_id: int = Field(..., description="Data source ID")


class AttributeBase(BaseSchema):
    """Base schema for attribute."""

    name: str


class AttributeCreate(BaseSchema):
    """Schema for creating an attribute."""

    field: int
    name: str


class AttributeRead(BaseSchema):
    """Schema for reading an attribute."""

    id: int
    dataset: int
    field: int
    name: str
    unique_values_size: int


class AttributeUpdate(BaseSchema):
    """Schema for updating an attribute."""

    name: str


class AttributeValueRead(BaseSchema):
    """Schema for reading an attribute value."""

    id: int
    frequency: int
    value: str | None = None


class TaskStatus(BaseSchema):
    """Schema for task status."""

    task_id: UUID
    task_name: str
    status_message: str | None = None
    status_location: str | None = None
    result_location: str | None = None


class TaskResult(BaseSchema):
    """Schema for task result."""

    task_id: UUID
    result: dict[str, Any] = Field(default_factory=dict)
