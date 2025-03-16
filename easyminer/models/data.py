from datetime import UTC, datetime

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from easyminer.database import Base
from easyminer.models.task import Task


class DataSource(Base):
    """Data source model representing a data set."""

    __tablename__ = "data_source"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    type: Mapped[str] = mapped_column(String(50))
    created_at: Mapped[datetime] = mapped_column(default=datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now(UTC), onupdate=datetime.now(UTC)
    )
    upload_id: Mapped[int | None] = mapped_column(ForeignKey("upload.id"))
    row_count: Mapped[int] = mapped_column(default=0)
    size_bytes: Mapped[int] = mapped_column(default=0)

    # Relationships
    fields: Mapped[list["Field"]] = relationship(
        back_populates="data_source", cascade="all, delete-orphan"
    )
    upload = relationship("Upload", back_populates="data_source")
    tasks: Mapped[list["Task"]] = relationship(
        back_populates="data_source", cascade="all, delete-orphan"
    )


class Field(Base):
    """Field model representing a column in a data source."""

    __tablename__ = "field"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    data_type: Mapped[str] = mapped_column(String(50))
    data_source_id: Mapped[int] = mapped_column(ForeignKey("data_source.id"))
    index: Mapped[int] = mapped_column()  # Position in the data source
    min_value: Mapped[str | None] = mapped_column(String(255), nullable=True)
    max_value: Mapped[str | None] = mapped_column(String(255), nullable=True)
    avg_value: Mapped[float | None] = mapped_column(nullable=True)
    unique_count: Mapped[int] = mapped_column(default=0)
    has_nulls: Mapped[bool] = mapped_column(default=False)

    data_source: Mapped[DataSource] = relationship(
        "DataSource", back_populates="fields"
    )
