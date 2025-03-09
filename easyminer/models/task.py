from datetime import datetime
from uuid import UUID

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from easyminer.database import Base


class Task(Base):
    """Represents a background task in the system."""

    __tablename__ = "task"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    task_id: Mapped[UUID] = mapped_column(unique=True, index=True)
    name: Mapped[str] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(
        String(50)
    )  # 'pending', 'in_progress', 'completed', 'failed'
    status_message: Mapped[str | None] = mapped_column(String(255), nullable=True)
    result_location: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # User who created the task
    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    user = relationship("User", back_populates="tasks")

    # Data source related to the task (may be null for some task types)
    data_source_id: Mapped[int | None] = mapped_column(
        ForeignKey("data_source.id"), nullable=True
    )
    data_source = relationship("DataSource", back_populates="tasks")
