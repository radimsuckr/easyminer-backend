"""Schema definitions for tasks."""

from typing import Any
from uuid import UUID

from pydantic import ConfigDict, Field

from easyminer.schemas import BaseSchema


class TaskStatus(BaseSchema):
    """Schema for task status."""

    task_id: UUID
    task_name: str
    status: str
    status_message: str | None = None
    status_location: str | None = None
    result_location: str | None = None


class TaskResult(BaseSchema):
    """Schema for task result."""

    message: str
    resultLocation: str = Field(..., alias="result_location")
    result: dict[str, Any]

    model_config = ConfigDict(populate_by_name=True)
