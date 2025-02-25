from collections.abc import Sequence
from http import HTTPStatus

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.models.user import User


async def get_users(db_session: AsyncSession) -> Sequence[User]:
    user = (await db_session.scalars(select(User))).all()
    return user


async def get_user(db_session: AsyncSession, user_id: int) -> User | None:
    user = (await db_session.scalars(select(User).where(User.id == user_id))).first()
    if not user:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="User not found")
    return user


async def get_user_by_email(db_session: AsyncSession, email: str) -> User | None:
    return (await db_session.scalars(select(User).where(User.email == email))).first()
