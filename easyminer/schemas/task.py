from uuid import UUID

from easyminer.schemas import BaseSchema


class TaskStatus(BaseSchema):
    """Schema for task status."""

    task_id: UUID
    task_name: str
    status_message: str | None = None
    status_location: str | None = None
    result_location: str | None = None
