from uuid import UUID

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from easyminer.database import Base


class Task(Base):
    """Task model for background processing tasks."""

    __tablename__ = "task"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    task_id: Mapped[UUID] = mapped_column(unique=True, index=True)
    name: Mapped[str] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(50), default="pending")
    status_message: Mapped[str | None] = mapped_column(String(255), nullable=True)
    result_location: Mapped[str | None] = mapped_column(String(255), nullable=True)
    data_source_id: Mapped[int] = mapped_column(
        ForeignKey("data_source.id"), nullable=True
    )
    data_source = relationship("DataSource", back_populates="tasks")
