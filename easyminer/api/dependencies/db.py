from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.database import sessionmanager


async def get_db_session() -> AsyncGenerator[AsyncSession]:
    """Get a database session."""
    async with sessionmanager.session() as session:
        yield session

