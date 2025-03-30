from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import Double, Enum, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from easyminer.database import Base
from easyminer.schemas.data import DbType, FieldType

if TYPE_CHECKING:
    from easyminer.models import PreviewUpload, Task, Upload


class DataSource(Base):
    """Data source model representing a data set."""

    __tablename__: str = "data_source"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255))
    type: Mapped[DbType] = mapped_column(Enum(DbType), nullable=False)
    size: Mapped[int] = mapped_column(default=0)
    created_at: Mapped[datetime] = mapped_column(default=datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now(UTC), onupdate=datetime.now(UTC)
    )
    is_finished: Mapped[bool] = mapped_column(default=False)

    # Relationships
    fields: Mapped[list["Field"]] = relationship(
        "Field", back_populates="data_source", cascade="all, delete-orphan"
    )
    instances: Mapped[list["Instance"]] = relationship(
        "Instance", back_populates="data_source", cascade="all, delete-orphan"
    )
    values: Mapped[list["Value"]] = relationship(
        "Value", back_populates="data_source", cascade="all, delete-orphan"
    )
    upload: Mapped["Upload"] = relationship(
        "Upload",
        back_populates="data_source",
        cascade="all, delete-orphan",
        single_parent=True,
        uselist=False,
    )
    preview_upload: Mapped["PreviewUpload"] = relationship(
        "PreviewUpload",
        back_populates="data_source",
        cascade="all, delete-orphan",
        single_parent=True,
        uselist=False,
    )
    tasks: Mapped[list["Task"]] = relationship(
        "Task", back_populates="data_source", cascade="all, delete-orphan"
    )


class Field(Base):
    """Field model representing a column in a data source."""

    __tablename__: str = "field"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255))
    data_type: Mapped["FieldType"] = mapped_column(Enum(FieldType))

    # Statistics
    unique_values_size_nominal: Mapped[int] = mapped_column(default=0)
    unique_values_size_numeric: Mapped[int] = mapped_column(default=0)
    support_nominal: Mapped[int] = mapped_column(default=0)
    support_numeric: Mapped[int] = mapped_column(default=0)

    data_source_id: Mapped[int] = mapped_column(
        ForeignKey("data_source.id", ondelete="CASCADE")
    )
    data_source: Mapped["DataSource"] = relationship(
        "DataSource", back_populates="fields"
    )


class FieldNumericDetail(Base):
    """Additional statistics for numeric fields."""

    __tablename__: str = "field_numeric_detail"

    id: Mapped[int] = mapped_column(
        ForeignKey("field.id", ondelete="CASCADE"), primary_key=True
    )
    min_value: Mapped[float] = mapped_column(Double)
    max_value: Mapped[float] = mapped_column(Double)
    avg_value: Mapped[float] = mapped_column(Double)


class Instance(Base):
    """Represents a single cell in the data source."""

    __tablename__: str = "data_source_instance"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    row_id: Mapped[int] = mapped_column(Integer)  # Row number in the original data
    value_nominal: Mapped[str | None] = mapped_column(String(255), nullable=True)
    value_numeric: Mapped[float | None] = mapped_column(Double, nullable=True)

    data_source_id: Mapped[int] = mapped_column(
        ForeignKey("data_source.id", ondelete="CASCADE")
    )
    data_source: Mapped["DataSource"] = relationship(
        "DataSource", back_populates="instances"
    )
    field_id: Mapped[int] = mapped_column(ForeignKey("field.id", ondelete="CASCADE"))
    field: Mapped["Field"] = relationship("Field")


class Value(Base):
    """Represents unique values and their frequencies."""

    __tablename__: str = "data_source_value"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    value_nominal: Mapped[str | None] = mapped_column(String(255), nullable=True)
    value_numeric: Mapped[float | None] = mapped_column(Double, nullable=True)
    frequency: Mapped[int] = mapped_column(Integer)

    data_source_id: Mapped[int] = mapped_column(
        ForeignKey("data_source.id", ondelete="CASCADE")
    )
    data_source: Mapped["DataSource"] = relationship(
        "DataSource", back_populates="values"
    )
    field_id: Mapped[int] = mapped_column(ForeignKey("field.id", ondelete="CASCADE"))
    field: Mapped["Field"] = relationship("Field")
