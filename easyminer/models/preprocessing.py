from typing import TYPE_CHECKING

from sqlalchemy import Constraint, Enum, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from easyminer.database import Base
from easyminer.schemas.data import DbType

if TYPE_CHECKING:
    from easyminer.models.data import DataSource, Field


class Dataset(Base):
    __tablename__: str = "dataset"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    type: Mapped["DbType"] = mapped_column(Enum(DbType), default=DbType.limited, nullable=False)
    size: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    is_active: Mapped[bool] = mapped_column(
        default=True,  # Because we do not create the tables on-demand as opposed to the Scala implementation, we can activate the dataset right away
        nullable=False,
    )

    data_source_id: Mapped[int] = mapped_column(ForeignKey("data_source.id", ondelete="CASCADE"), nullable=False)
    data_source: Mapped["DataSource"] = relationship("DataSource", back_populates="datasets")
    attributes: Mapped[list["Attribute"]] = relationship(back_populates="dataset", cascade="all, delete-orphan")


class Attribute(Base):
    __tablename__: str = "dataset_attribute"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    unique_values_size: Mapped[int] = mapped_column(default=0, nullable=False)
    is_active: Mapped[bool] = mapped_column(default=False, nullable=False)

    dataset_id: Mapped[int] = mapped_column(ForeignKey("dataset.id", ondelete="CASCADE"), nullable=False)
    dataset: Mapped["Dataset"] = relationship(back_populates="attributes")
    field_id: Mapped[int] = mapped_column(ForeignKey("field.id", ondelete="CASCADE"), nullable=False)
    field: Mapped["Field"] = relationship(back_populates="attributes")
    instances: Mapped[list["DatasetInstance"]] = relationship(back_populates="attribute", cascade="all, delete-orphan")
    values: Mapped[list["DatasetValue"]] = relationship(back_populates="attribute", cascade="all, delete-orphan")


class DatasetInstance(Base):
    __tablename__: str = "dataset_instance"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    tx_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    value_id: Mapped[int] = mapped_column(ForeignKey("dataset_value.id", ondelete="CASCADE"), nullable=False)
    value: Mapped["DatasetValue"] = relationship(back_populates="instances")
    attribute_id: Mapped[int] = mapped_column(ForeignKey("dataset_attribute.id", ondelete="CASCADE"), nullable=False)
    attribute: Mapped["Attribute"] = relationship(back_populates="instances")


class DatasetValue(Base):
    __tablename__: str = "dataset_value"
    __table_args__: tuple[Constraint] = (UniqueConstraint("value", "attribute_id"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    value: Mapped[str] = mapped_column(String(255), nullable=False)
    frequency: Mapped[int] = mapped_column(Integer, nullable=False)

    attribute_id: Mapped[int] = mapped_column(ForeignKey("dataset_attribute.id", ondelete="CASCADE"), nullable=False)
    attribute: Mapped["Attribute"] = relationship(back_populates="values")
    instances: Mapped["DatasetInstance"] = relationship(back_populates="value")
