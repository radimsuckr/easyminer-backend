import enum
from datetime import UTC, datetime

from sqlalchemy import Double, Enum, ForeignKey, String
from sqlalchemy.orm import Mapped, Relationship, mapped_column, relationship

from easyminer.database import Base
from easyminer.models.task import Task
from easyminer.models.upload import Upload


class DataSource(Base):
    """Data source model representing a data set."""

    __tablename__: str = "data_source"

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
    upload: Relationship[Upload] = relationship("Upload", back_populates="data_source")
    tasks: Mapped[list["Task"]] = relationship(
        back_populates="data_source", cascade="all, delete-orphan"
    )


class FieldType(str, enum.Enum):
    nominal = "nominal"
    numeric = "numeric"


class Field(Base):
    """Field model representing a column in a data source."""

    __tablename__: str = "field"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    data_type: Mapped[FieldType] = mapped_column(Enum(FieldType))
    data_source_id: Mapped[int] = mapped_column(ForeignKey("data_source.id"))
    unique_count: Mapped[int] = mapped_column()
    support: Mapped[int] = mapped_column()

    data_source: Mapped[DataSource] = relationship(
        "DataSource", back_populates="fields"
    )


class FieldNumericDetails(Base):
    """Field details model for numeric fields."""

    __tablename__: str = "field_numeric_details"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    min_value: Mapped[float] = mapped_column(Double())
    max_value: Mapped[float] = mapped_column(Double())
    avg_value: Mapped[float] = mapped_column(Double())
