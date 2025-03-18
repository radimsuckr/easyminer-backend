from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import Field

from easyminer.schemas import BaseSchema


class Error(BaseSchema):
    """Error schema."""

    code: int
    name: str
    message: str


class DatasetType(str, Enum):
    """Dataset type enum."""

    LIMITED = "limited"
    UNLIMITED = "unlimited"


class DatasetBase(BaseSchema):
    """Base schema for dataset."""

    name: str
    type: DatasetType


class DatasetCreate(BaseSchema):
    """Schema for creating a dataset."""

    data_source: int
    name: str


class DatasetRead(BaseSchema):
    """Schema for reading a dataset."""

    id: int
    name: str
    data_source: int
    type: DatasetType
    size: int


class DatasetUpdate(BaseSchema):
    """Schema for updating a dataset."""

    name: str


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
