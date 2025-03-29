import enum
from uuid import UUID

from sqlalchemy import JSON, Enum, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

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

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    task_id: Mapped[UUID] = mapped_column(unique=True, index=True)
    name: Mapped[str] = mapped_column(String(100))
    status: Mapped[TaskStatusEnum] = mapped_column(
        Enum(TaskStatusEnum), default=TaskStatusEnum.pending
    )
    status_message: Mapped[str | None] = mapped_column(String(255), nullable=True)
    data_source_id: Mapped[int] = mapped_column(
        ForeignKey("data_source.id"), nullable=True
    )
    data_source = relationship("DataSource", back_populates="tasks")

    result: Mapped["TaskResult"] = relationship(
        back_populates="task", cascade="all, delete-orphan"
    )


class TaskResult(Base):
    """Task result model for storing task results."""

    __tablename__: str = "task_result"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    task_id: Mapped[int] = mapped_column(ForeignKey("task.id"))
    value: Mapped[JSON] = mapped_column(JSON())

    task = relationship("Task", back_populates="result")
