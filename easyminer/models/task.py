import enum
from uuid import UUID

from sqlalchemy import Enum, String
from sqlalchemy.orm import Mapped, mapped_column

from easyminer.database import Base


class TaskStatusEnum(enum.Enum):
    pending = "pending"
    scheduled = "scheduled"
    started = "started"
    success = "success"
    failure = "failure"


class Task(Base):
    """Task model for background processing tasks."""

    __tablename__: str = "task"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    task_id: Mapped[UUID] = mapped_column(unique=True, index=True)
    name: Mapped[str] = mapped_column(String(100))
    status: Mapped[TaskStatusEnum] = mapped_column(Enum(TaskStatusEnum), default=TaskStatusEnum.pending)
    status_message: Mapped[str | None] = mapped_column(String(255), nullable=True)
