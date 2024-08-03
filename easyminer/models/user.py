from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from easyminer.database import Base


class User(Base):
    __tablename__: str = "user"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    username: Mapped[str] = mapped_column(String(100), index=True, unique=True)
    slug: Mapped[str] = mapped_column(String(100), index=True, unique=True)
    email: Mapped[str] = mapped_column(String(100), index=True, unique=True)
    first_name: Mapped[str] = mapped_column(String(100), default=False)
    last_name: Mapped[str] = mapped_column(String(100), default=False)
    hashed_password: Mapped[str] = mapped_column(String(100), default=False)
    is_superuser: Mapped[bool] = mapped_column(String(100), default=False)
