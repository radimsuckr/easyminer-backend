import enum
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, final
from uuid import UUID as pyUUID

from sqlalchemy import DECIMAL, UUID, DateTime, Enum, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from easyminer.database import Base
from easyminer.schemas.data import CompressionType, DbType, FieldType, Locale, MediaType

if TYPE_CHECKING:
    from easyminer.models.preprocessing import Attribute, Dataset
    from easyminer.models.task import Task


@final
class UploadState(int, enum.Enum):
    initialized = 0
    locked = 1
    ready = 2
    finished = 3


class Upload(Base):
    __tablename__: str = "upload"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    uuid: Mapped[pyUUID] = mapped_column(UUID())
    name: Mapped[str] = mapped_column(String(100))
    media_type: Mapped["MediaType"] = mapped_column(Enum(MediaType))
    db_type: Mapped["DbType"] = mapped_column(Enum(DbType))
    separator: Mapped[str] = mapped_column(String(1))
    encoding: Mapped[str] = mapped_column(String(40))
    quotes_char: Mapped[str] = mapped_column(String(1))
    escape_char: Mapped[str] = mapped_column(String(1))
    locale: Mapped[Locale] = mapped_column(Enum(Locale))

    state: Mapped[UploadState] = mapped_column(Enum(UploadState), default=UploadState.initialized)
    last_change_at: Mapped[datetime] = mapped_column(DateTime(), default=datetime.now(UTC), onupdate=datetime.now(UTC))

    data_source: Mapped["DataSource"] = relationship(back_populates="upload")
    chunks: Mapped[list["Chunk"]] = relationship("Chunk", back_populates="upload", cascade="all, delete-orphan")
    null_values: Mapped[list["NullValue"]] = relationship(
        "NullValue", back_populates="upload", cascade="all, delete-orphan"
    )
    data_types: Mapped[list["DataType"]] = relationship(
        "DataType", back_populates="upload", cascade="all, delete-orphan"
    )


class NullValue(Base):
    __tablename__: str = "null_value"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    value: Mapped[str] = mapped_column(String(255))

    upload_id: Mapped[int] = mapped_column(ForeignKey("upload.id", ondelete="CASCADE"))
    upload: Mapped["Upload"] = relationship("Upload", back_populates="null_values")


class DataType(Base):
    __tablename__: str = "data_type"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    index: Mapped[int] = mapped_column(Integer, nullable=False)
    value: Mapped[FieldType | None] = mapped_column(Enum(FieldType), nullable=True)

    upload_id: Mapped[int] = mapped_column(ForeignKey("upload.id", ondelete="CASCADE"))
    upload: Mapped["Upload"] = relationship("Upload", back_populates="data_types")


class PreviewUpload(Base):
    __tablename__: str = "preview_upload"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    uuid: Mapped[pyUUID] = mapped_column(UUID(), unique=True)
    max_lines: Mapped[int] = mapped_column(Integer())
    compression: Mapped[CompressionType] = mapped_column(Enum(CompressionType))
    media_type: Mapped["MediaType"] = mapped_column(Enum(MediaType))


class Chunk(Base):
    __tablename__: str = "chunk"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    uploaded_at: Mapped[datetime] = mapped_column(DateTime())
    path: Mapped[str] = mapped_column(String(255))

    upload_id: Mapped[int] = mapped_column(ForeignKey("upload.id", ondelete="CASCADE"))
    upload: Mapped["Upload"] = relationship(back_populates="chunks")


class DataSource(Base):
    """Data source model representing a data set."""

    __tablename__: str = "data_source"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    type: Mapped[DbType] = mapped_column(Enum(DbType), nullable=False)
    size: Mapped[int] = mapped_column(default=0)
    created_at: Mapped[datetime] = mapped_column(default=datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(default=datetime.now(UTC), onupdate=datetime.now(UTC))

    # Relationships
    fields: Mapped[list["Field"]] = relationship(back_populates="data_source", cascade="all, delete-orphan")
    instances: Mapped[list["DataSourceInstance"]] = relationship(
        back_populates="data_source", cascade="all, delete-orphan"
    )
    values: Mapped[list["DataSourceValue"]] = relationship(back_populates="data_source", cascade="all, delete-orphan")
    upload_id: Mapped[int] = mapped_column(ForeignKey("upload.id", ondelete="CASCADE"))
    upload: Mapped["Upload"] = relationship(
        back_populates="data_source", cascade="all, delete-orphan", single_parent=True
    )
    tasks: Mapped[list["Task"]] = relationship(back_populates="data_source", cascade="all, delete-orphan")
    datasets: Mapped[list["Dataset"]] = relationship(back_populates="data_source", cascade="all, delete-orphan")


class Field(Base):
    """Field model representing a column in a data source."""

    __tablename__: str = "field"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    data_type: Mapped["FieldType"] = mapped_column(Enum(FieldType))
    index: Mapped[int] = mapped_column(Integer, nullable=False)

    # Statistics
    unique_values_size_nominal: Mapped[int] = mapped_column(default=0)
    unique_values_size_numeric: Mapped[int] = mapped_column(default=0)
    support_nominal: Mapped[int] = mapped_column(default=0)
    support_numeric: Mapped[int] = mapped_column(default=0)

    data_source_id: Mapped[int] = mapped_column(ForeignKey("data_source.id", ondelete="CASCADE"))
    data_source: Mapped["DataSource"] = relationship("DataSource", back_populates="fields")
    attributes: Mapped[list["Attribute"]] = relationship(back_populates="field", cascade="all, delete-orphan")
    numeric_detail: Mapped["FieldNumericDetail | None"] = relationship(
        "FieldNumericDetail", back_populates="field", uselist=False
    )

    @property
    def unique_count(self) -> int | None:
        """Return unique count based on field type."""
        if self.data_type == FieldType.numeric:
            return self.unique_values_size_numeric if self.unique_values_size_numeric > 0 else None
        return self.unique_values_size_nominal if self.unique_values_size_nominal > 0 else None

    @property
    def support(self) -> int | None:
        """Return support based on field type."""
        if self.data_type == FieldType.numeric:
            return self.support_numeric if self.support_numeric > 0 else None
        return self.support_nominal if self.support_nominal > 0 else None

    @property
    def min_value(self) -> float | None:
        """Return minimum value for numeric fields."""
        if self.data_type == FieldType.numeric and self.numeric_detail:
            return float(self.numeric_detail.min_value)
        return None

    @property
    def max_value(self) -> float | None:
        """Return maximum value for numeric fields."""
        if self.data_type == FieldType.numeric and self.numeric_detail:
            return float(self.numeric_detail.max_value)
        return None

    @property
    def avg_value(self) -> float | None:
        """Return average value for numeric fields."""
        if self.data_type == FieldType.numeric and self.numeric_detail:
            return float(self.numeric_detail.avg_value)
        return None


class FieldNumericDetail(Base):
    """Additional statistics for numeric fields."""

    __tablename__: str = "field_numeric_detail"

    id: Mapped[int] = mapped_column(ForeignKey("field.id", ondelete="CASCADE"), primary_key=True, index=True)
    min_value: Mapped[Decimal] = mapped_column(DECIMAL)
    max_value: Mapped[Decimal] = mapped_column(DECIMAL)
    avg_value: Mapped[Decimal] = mapped_column(DECIMAL)

    field: Mapped["Field"] = relationship("Field", back_populates="numeric_detail")


class DataSourceInstance(Base):
    """Represents a single cell in the data source."""

    __tablename__: str = "data_source_instance"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    row_id: Mapped[int] = mapped_column(Integer)  # Row number in the original data
    col_id: Mapped[int] = mapped_column(Integer)  # Column number in the original data
    value_nominal: Mapped[str | None] = mapped_column(String(255), nullable=True)
    value_numeric: Mapped[Decimal | None] = mapped_column(DECIMAL, nullable=True)

    data_source_id: Mapped[int] = mapped_column(ForeignKey("data_source.id", ondelete="CASCADE"))
    data_source: Mapped["DataSource"] = relationship("DataSource", back_populates="instances")
    field_id: Mapped[int] = mapped_column(ForeignKey("field.id", ondelete="CASCADE"))
    field: Mapped["Field"] = relationship("Field")


class DataSourceValue(Base):
    """Represents unique values and their frequencies."""

    __tablename__: str = "data_source_value"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    value_nominal: Mapped[str | None] = mapped_column(String(255), nullable=True)
    value_numeric: Mapped[Decimal | None] = mapped_column(DECIMAL, nullable=True)
    frequency: Mapped[int] = mapped_column(Integer)

    data_source_id: Mapped[int] = mapped_column(ForeignKey("data_source.id", ondelete="CASCADE"))
    data_source: Mapped["DataSource"] = relationship("DataSource", back_populates="values")
    field_id: Mapped[int] = mapped_column(ForeignKey("field.id", ondelete="CASCADE"))
    field: Mapped["Field"] = relationship("Field")
