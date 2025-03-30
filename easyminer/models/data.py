import enum
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import Double, Enum, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from easyminer.database import Base
from easyminer.schemas.data import DbType

if TYPE_CHECKING:
    from easyminer.models.task import Task
    from easyminer.models.upload import PreviewUpload, Upload


class DataSource(Base):
    """Data source model representing a data set."""

    __tablename__: str = "data_source"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255))
    type: Mapped[DbType] = mapped_column(Enum(DbType), nullable=False)
    created_at: Mapped[datetime] = mapped_column(default=datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now(UTC), onupdate=datetime.now(UTC)
    )
    row_count: Mapped[int] = mapped_column(default=0)
    size_bytes: Mapped[int] = mapped_column(default=0)
    is_finished: Mapped[bool] = mapped_column(default=False)

    # Relationships
    fields: Mapped[list["Field"]] = relationship(
        "Field", back_populates="data_source", cascade="all, delete-orphan"
    )
    upload: Mapped["Upload"] = relationship(
        "Upload",
        back_populates="data_source",
        cascade="all, delete-orphan",
        single_parent=True,
        uselist=False,  # This makes it a one-to-one relationship
    )
    preview_upload: Mapped["PreviewUpload"] = relationship(
        "PreviewUpload",
        back_populates="data_source",
        cascade="all, delete-orphan",
        single_parent=True,
        uselist=False,  # This makes it a one-to-one relationship
    )
    tasks: Mapped[list["Task"]] = relationship(
        "Task", back_populates="data_source", cascade="all, delete-orphan"
    )


class FieldType(str, enum.Enum):
    nominal = "nominal"
    numeric = "numeric"


class Field(Base):
    """Field model representing a column in a data source."""

    __tablename__: str = "field"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255))
    data_type: Mapped["FieldType"] = mapped_column(Enum(FieldType))
    unique_count: Mapped[int] = mapped_column(Integer())
    support: Mapped[int] = mapped_column(Integer())

    data_source_id: Mapped[int] = mapped_column(
        ForeignKey("data_source.id", ondelete="CASCADE")
    )
    data_source: Mapped["DataSource"] = relationship(
        "DataSource", back_populates="fields"
    )


class FieldNumericDetails(Base):
    """Field details model for numeric fields."""

    __tablename__: str = "field_numeric_details"

    id: Mapped[int] = mapped_column(primary_key=True)
    min_value: Mapped[float] = mapped_column(Double())
    max_value: Mapped[float] = mapped_column(Double())
    avg_value: Mapped[float] = mapped_column(Double())
