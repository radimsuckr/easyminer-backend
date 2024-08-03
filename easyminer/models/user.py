from sqlalchemy.orm import Mapped, mapped_column

from easyminer.database import Base


class User(Base):
    __tablename__: str = "user"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    username: Mapped[str] = mapped_column(index=True, unique=True)
    slug: Mapped[str] = mapped_column(index=True, unique=True)
    email: Mapped[str] = mapped_column(index=True, unique=True)
    first_name: Mapped[str] = mapped_column(default=False)
    last_name: Mapped[str] = mapped_column(default=False)
    hashed_password: Mapped[str] = mapped_column(default=False)
    is_superuser: Mapped[bool] = mapped_column(default=False)
