from collections.abc import Sequence
from datetime import UTC, datetime
from uuid import UUID as pyUUID

from sqlalchemy import (
    UUID,
    Column,
    Constraint,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, Relationship, mapped_column, relationship

from easyminer.database import Base

# from easyminer.models.data import DataSource
from easyminer.schemas.data import CompressionType, DbType, MediaType


def create_association_table(name: str, target_table: str) -> Table:
    """Create a many-to-many association table."""
    return Table(
        f"{name}_table",
        Base.metadata,
        Column("id", Integer, primary_key=True, autoincrement=True, index=True),
        Column("upload_id", Integer, ForeignKey("upload.id")),
        Column(f"{name}_id", Integer, ForeignKey(f"{target_table}.id")),
    )


UploadNullValueTable = create_association_table(
    "upload_null_value", "upload_null_value"
)
UploadDataTypeTable = create_association_table("upload_data_type", "upload_data_types")


class UploadNullValue(Base):
    __tablename__: str = "upload_null_value"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    value: Mapped[str] = mapped_column(String(100))
    uploads = relationship(
        "Upload", secondary=UploadNullValueTable, back_populates="null_values"
    )


class UploadDataType(Base):
    __tablename__: str = "upload_data_types"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    name: Mapped[str] = mapped_column(String(100))
    uploads = relationship(
        "Upload", secondary=UploadDataTypeTable, back_populates="data_types"
    )


class Upload(Base):
    __tablename__: str = "upload"
    __table_args__: Sequence[Constraint] = (UniqueConstraint("data_source_id"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    uuid: Mapped[str] = mapped_column(String(36))
    name: Mapped[str] = mapped_column(String(100))
    media_type: Mapped["MediaType"] = mapped_column(Enum(MediaType))
    db_type: Mapped["DbType"] = mapped_column(Enum(DbType))
    separator: Mapped[str] = mapped_column(String(1))
    encoding: Mapped[str] = mapped_column(String(40))
    quotes_char: Mapped[str] = mapped_column(String(1))
    escape_char: Mapped[str] = mapped_column(String(1))
    locale: Mapped[str] = mapped_column(String(20))
    compression: Mapped["CompressionType | None"] = mapped_column(
        Enum(CompressionType), nullable=True
    )
    null_values: Relationship[list["UploadNullValue"]] = relationship(
        "UploadNullValue", secondary=UploadNullValueTable, back_populates="uploads"
    )
    data_types: Relationship[list["UploadDataType"]] = relationship(
        "UploadDataType", secondary=UploadDataTypeTable, back_populates="uploads"
    )
    format: Mapped[str] = mapped_column(String(20))
    preview_max_lines: Mapped[int | None] = mapped_column(Integer, nullable=True)

    data_source_id: Mapped[int] = mapped_column(ForeignKey("data_source.id"))
    data_source: Mapped["DataSource"] = relationship(
        "DataSource", back_populates="upload", single_parent=True
    )

    chunks: Relationship[list["Chunk"]] = relationship(
        "Chunk", back_populates="upload", order_by="Chunk.uploaded_at"
    )


class PreviewUpload(Base):
    __tablename__: str = "preview_upload"
    __table_args__: Sequence[Constraint] = (UniqueConstraint("data_source_id"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    uuid: Mapped[pyUUID] = mapped_column(UUID(), unique=True)
    max_lines: Mapped[int] = mapped_column(Integer())
    compression: Mapped[CompressionType | None] = mapped_column(Enum(CompressionType))

    data_source_id: Mapped[int] = mapped_column(ForeignKey("data_source.id"))
    data_source: Mapped["DataSource"] = relationship(
        "DataSource", back_populates="preview_upload", single_parent=True
    )


class Chunk(Base):
    __tablename__: str = "chunk"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    upload_id: Mapped[int] = mapped_column(ForeignKey("upload.id"))
    uploaded_at: Mapped[datetime] = mapped_column(DateTime(), default=datetime.now)
    path: Mapped[str] = mapped_column(String(255))

    upload: Relationship["Upload"] = relationship("Upload", back_populates="chunks")
