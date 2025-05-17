from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID as pyUUID

from sqlalchemy import (
    ARRAY,
    UUID,
    Constraint,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from easyminer.database import Base
from easyminer.schemas.data import CompressionType, DbType, FieldType, MediaType

if TYPE_CHECKING:
    from easyminer.models import DataSource


class Upload(Base):
    __tablename__: str = "upload"
    __table_args__: Sequence[Constraint] = (UniqueConstraint("data_source_id"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    uuid: Mapped[pyUUID] = mapped_column(UUID())
    name: Mapped[str] = mapped_column(String(100))
    media_type: Mapped["MediaType"] = mapped_column(Enum(MediaType))
    db_type: Mapped["DbType"] = mapped_column(Enum(DbType))
    separator: Mapped[str] = mapped_column(String(1))
    encoding: Mapped[str] = mapped_column(String(40))
    quotes_char: Mapped[str] = mapped_column(String(1))
    escape_char: Mapped[str] = mapped_column(String(1))
    locale: Mapped[str] = mapped_column(String(20))
    compression: Mapped["CompressionType | None"] = mapped_column(Enum(CompressionType), nullable=True)
    preview_max_lines: Mapped[int | None] = mapped_column(Integer(), nullable=True)
    null_values: Mapped[list[str]] = mapped_column(ARRAY(String(255)))
    data_types: Mapped[list[FieldType]] = mapped_column(ARRAY(Enum(FieldType)))

    data_source_id: Mapped[int] = mapped_column(ForeignKey("data_source.id", ondelete="CASCADE"))
    data_source: Mapped["DataSource"] = relationship("DataSource", back_populates="upload")
    chunks: Mapped[list["Chunk"]] = relationship(
        "Chunk",
        back_populates="upload",
        order_by="Chunk.uploaded_at",
        cascade="all, delete-orphan",
    )


class PreviewUpload(Base):
    __tablename__: str = "preview_upload"
    __table_args__: Sequence[Constraint] = (UniqueConstraint("data_source_id"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    uuid: Mapped[pyUUID] = mapped_column(UUID(), unique=True)
    max_lines: Mapped[int] = mapped_column(Integer())
    compression: Mapped[CompressionType | None] = mapped_column(Enum(CompressionType))
    media_type: Mapped["MediaType"] = mapped_column(Enum(MediaType))

    data_source_id: Mapped[int] = mapped_column(ForeignKey("data_source.id", ondelete="CASCADE"))
    data_source: Mapped["DataSource"] = relationship("DataSource", back_populates="preview_upload", single_parent=True)


class Chunk(Base):
    __tablename__: str = "chunk"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    uploaded_at: Mapped[datetime] = mapped_column(DateTime(), default=datetime.now)
    path: Mapped[str] = mapped_column(String(255))

    upload_id: Mapped[int] = mapped_column(ForeignKey("upload.id", ondelete="CASCADE"))
    upload: Mapped["Upload"] = relationship("Upload", back_populates="chunks")
