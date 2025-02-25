from sqlalchemy import Column, ForeignKey, Integer, String, Table
from sqlalchemy.orm import Mapped, mapped_column, relationship

from easyminer.database import Base


def create_association_table(name: str, target_table: str) -> Table:
    """Create a many-to-many association table."""
    return Table(
        f"{name}_table",
        Base.metadata,
        Column("id", Integer, primary_key=True, autoincrement=True, index=True),
        Column("upload_id", Integer, ForeignKey("upload.id")),
        Column(f"{name}_id", Integer, ForeignKey(f"{target_table}.id")),
    )


UploadNullValueTable = create_association_table("upload_null_value", "upload_null_value")
UploadDataTypeTable = create_association_table("upload_data_type", "upload_data_types")


class UploadNullValue(Base):
    __tablename__: str = "upload_null_value"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    value: Mapped[str] = mapped_column(String(100))
    uploads = relationship("Upload", secondary=UploadNullValueTable, back_populates="null_values")


class UploadDataType(Base):
    __tablename__: str = "upload_data_types"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    name: Mapped[str] = mapped_column(String(100))
    uploads = relationship("Upload", secondary=UploadDataTypeTable, back_populates="data_types")


class Upload(Base):
    __tablename__: str = "upload"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    uuid: Mapped[str] = mapped_column(String(36))
    name: Mapped[str] = mapped_column(String(100))
    media_type: Mapped[str] = mapped_column(String(20))
    db_type: Mapped[str] = mapped_column(String(20))
    separator: Mapped[str] = mapped_column(String(1))
    encoding: Mapped[str] = mapped_column(String(40))
    quotes_char: Mapped[str] = mapped_column(String(1))
    escape_char: Mapped[str] = mapped_column(String(1))
    locale: Mapped[str] = mapped_column(String(20))
    compression: Mapped[str] = mapped_column(String(20))
    null_values = relationship("UploadNullValue", secondary=UploadNullValueTable, back_populates="uploads")
    data_types = relationship("UploadDataType", secondary=UploadDataTypeTable, back_populates="uploads")
    format: Mapped[str] = mapped_column(String(20))
    
    # Relationship to DataSource
    data_source = relationship("DataSource", back_populates="upload", uselist=False)
