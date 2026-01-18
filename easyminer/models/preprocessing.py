from typing import TYPE_CHECKING

from sqlalchemy import Enum, ForeignKey, Integer, PrimaryKeyConstraint, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from easyminer.database import Base
from easyminer.schemas.data import DbType

if TYPE_CHECKING:
    from easyminer.models.data import DataSource, Field


class Dataset(Base):
    """Dataset model for preprocessed data.

    Preprocessed instance data is stored in dynamic tables:
    - dataset_{id} for instance data
    - pp_value_{id} for unique values
    These are created/dropped via easyminer.models.dynamic_tables.
    """

    __tablename__: str = "dataset"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    type: Mapped["DbType"] = mapped_column(Enum(DbType), default=DbType.limited, nullable=False)
    size: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    active: Mapped[bool] = mapped_column(default=False, nullable=False)

    # Renamed from data_source_id for Scala compatibility
    data_source: Mapped[int] = mapped_column(ForeignKey("data_source.id", ondelete="CASCADE"), nullable=False)
    data_source_rel: Mapped["DataSource"] = relationship("DataSource", back_populates="datasets")
    attributes: Mapped[list["Attribute"]] = relationship(back_populates="dataset_rel", cascade="all, delete-orphan")


class Attribute(Base):
    """Attribute model for preprocessed dataset columns.

    Uses composite primary key (id, dataset) for Scala compatibility.
    Table renamed from dataset_attribute to attribute for Scala compatibility.
    Note: autoincrement not used because SQLite doesn't support it with composite keys.
    IDs are assigned manually based on max(id)+1 for the dataset.
    """

    __tablename__: str = "attribute"
    __table_args__ = (PrimaryKeyConstraint("id", "dataset"),)

    id: Mapped[int] = mapped_column()
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    unique_values_size: Mapped[int] = mapped_column(default=0, nullable=False)
    active: Mapped[bool] = mapped_column(default=False, nullable=False)

    # Renamed from dataset_id and field_id for Scala compatibility
    dataset: Mapped[int] = mapped_column(ForeignKey("dataset.id", ondelete="CASCADE"), nullable=False)
    dataset_rel: Mapped["Dataset"] = relationship(back_populates="attributes")
    field: Mapped[int] = mapped_column(ForeignKey("field.id", ondelete="CASCADE"), nullable=False)
    field_rel: Mapped["Field"] = relationship(back_populates="attributes")
