from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from easyminer.database import Base

if TYPE_CHECKING:
    from easyminer.models.task import Task

# Import Task as a string to avoid circular imports
# Task will be resolved at runtime, not at import time


class DataSource(Base):
    """Represents a data source in the system."""

    __tablename__ = "data_source"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    type: Mapped[str] = mapped_column(String(50))  # e.g., 'csv', 'mysql', etc.
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.utcnow, onupdate=datetime.utcnow
    )
    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    upload_id: Mapped[int | None] = mapped_column(
        ForeignKey("upload.id"), nullable=True
    )
    row_count: Mapped[int] = mapped_column(default=0)
    size_bytes: Mapped[int] = mapped_column(default=0)

    # Relationships
    fields: Mapped[list["Field"]] = relationship(
        back_populates="data_source", cascade="all, delete-orphan"
    )
    user = relationship("User", back_populates="data_sources")
    upload = relationship("Upload", back_populates="data_source")
    tasks: Mapped[list["Task"]] = relationship(
        back_populates="data_source", cascade="all, delete-orphan"
    )


class Field(Base):
    """Represents a field/column in a data source."""

    __tablename__ = "field"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    data_type: Mapped[str] = mapped_column(
        String(50)
    )  # e.g., 'string', 'integer', 'float', etc.
    data_source_id: Mapped[int] = mapped_column(ForeignKey("data_source.id"))
    index: Mapped[int] = mapped_column()  # Position in the data source
    unique_values_count: Mapped[int | None] = mapped_column(nullable=True)
    missing_values_count: Mapped[int | None] = mapped_column(nullable=True)
    min_value: Mapped[str | None] = mapped_column(String(255), nullable=True)
    max_value: Mapped[str | None] = mapped_column(String(255), nullable=True)
    avg_value: Mapped[float | None] = mapped_column(nullable=True)
    std_value: Mapped[float | None] = mapped_column(nullable=True)

    # Relationships
    data_source: Mapped[DataSource] = relationship(back_populates="fields")
