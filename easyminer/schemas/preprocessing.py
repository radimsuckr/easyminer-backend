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
    """Schema for reading a dataset.

    Model uses 'data_source' and 'active' but we expose as
    'data_source_id' and 'is_active' for API compatibility.
    """

    id: int
    name: str
    data_source_id: int = Field(validation_alias="data_source")
    type: DbType
    size: int
    is_active: bool = Field(validation_alias="active")

    model_config: ConfigDict = ConfigDict(from_attributes=True, populate_by_name=True)


class DatasetUpdate(BaseSchema):
    """Schema for updating a dataset."""

    name: str


class DatasetSchema(BaseSchema):
    id: int = Field(..., description="Dataset ID")
    name: str = Field(..., description="Dataset name")
    size: int = Field(..., description="Dataset size")
    type: DbType = Field(..., description="Database type")
    data_source_id: int = Field(..., description="Data source ID")


class AttributeCreate(BaseSchema):
    """Schema for creating an attribute."""

    field: int
    name: str


class AttributeRead(BaseSchema):
    """Schema for reading an attribute.

    Model uses 'dataset' and 'field' but we expose as
    'dataset_id' and 'field_id' for API compatibility.
    """

    id: int
    name: str
    dataset_id: int = Field(validation_alias="dataset")
    field_id: int = Field(validation_alias="field")
    unique_values_size: int

    model_config: ConfigDict = ConfigDict(from_attributes=True, populate_by_name=True)


class AttributeValueRead(BaseSchema):
    """Schema for reading an attribute value."""

    id: int
    frequency: int
    value: str | None = None

    model_config: ConfigDict = ConfigDict(from_attributes=True)


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
