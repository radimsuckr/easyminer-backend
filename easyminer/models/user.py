from sqlalchemy import Boolean, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from easyminer.database import Base
from easyminer.models.data import DataSource
from easyminer.models.task import Task


class User(Base):
    __tablename__: str = "user"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    username: Mapped[str] = mapped_column(String(100), index=True, unique=True)
    slug: Mapped[str] = mapped_column(String(100), index=True, unique=True)
    email: Mapped[str] = mapped_column(String(100), index=True, unique=True)
    first_name: Mapped[str] = mapped_column(String(100))
    last_name: Mapped[str] = mapped_column(String(100))
    hashed_password: Mapped[str] = mapped_column(String(100))
    is_superuser: Mapped[bool] = mapped_column(Boolean, default=False)

    # Relationships
    data_sources: Mapped[list[DataSource]] = relationship(back_populates="user")
    tasks: Mapped[list[Task]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
