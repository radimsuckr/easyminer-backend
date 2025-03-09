from typing import Annotated

from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.api.dependencies.db import get_db_session
from easyminer.models import User


async def get_current_user(
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> User:
    """Get the current authenticated user."""
    # For now, we'll return a default user since we have a static password auth
    # In a real app, we would decode a JWT token and get the user from the database
    result = await db.execute(select(User).where(User.is_superuser))
    user = result.scalar_one_or_none()

    if not user:
        # Create a default superuser if none exists
        user = User(
            username="admin",
            slug="admin",
            email="admin@example.com",
            first_name="Admin",
            last_name="User",
            hashed_password="not_used",  # We're using static token auth for now
            is_superuser=True,
        )
        db.add(user)
        await db.commit()
        await db.refresh(user)

    return user
