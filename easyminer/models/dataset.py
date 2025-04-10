from sqlalchemy import Boolean, Enum, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from easyminer.database import Base
from easyminer.models.data import DataSource
from easyminer.schemas.data import DbType


class Dataset(Base):
    __tablename__: str = "dataset"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    type: Mapped[DbType] = mapped_column(Enum(DbType), nullable=False)
    size: Mapped[int] = mapped_column(Integer(), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean(), default=False, nullable=False)

    data_source_id: Mapped[int] = mapped_column(
        ForeignKey("data_source.id", ondelete="CASCADE"),
        nullable=False,
    )
    data_source: Mapped["DataSource"] = relationship("DataSource", back_populates="datasets")
