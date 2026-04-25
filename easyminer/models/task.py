import enum
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import Enum, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from easyminer.database import Base

if TYPE_CHECKING:
    from easyminer.models.data import DataSource


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

    data_source_id: Mapped[int | None] = mapped_column(ForeignKey("data_source.id", ondelete="CASCADE"), nullable=True)
    data_source: Mapped["DataSource"] = relationship("DataSource", back_populates="tasks")
