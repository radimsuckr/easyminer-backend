from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.models import DataSource


async def get_data_source_by_id(
    db_session: AsyncSession, id: int, user_id: int
) -> DataSource | None:
    """Get a data source by ID if it belongs to the user."""
    data_source = await db_session.get(DataSource, id)
    if not data_source or data_source.user_id != user_id:
        return None
    return data_source


async def get_data_sources_by_user(
    db_session: AsyncSession, user_id: int
) -> Sequence[DataSource]:
    """Get all data sources for a user."""
    result = await db_session.execute(
        select(DataSource).where(DataSource.user_id == user_id)
    )
    return result.scalars().all()


async def create_data_source(
    db_session: AsyncSession,
    name: str,
    type: str,
    user_id: int,
    upload_id: int | None = None,
    size_bytes: int = 0,
    row_count: int = 0,
) -> DataSource:
    """Create a new data source."""
    data_source = DataSource(
        name=name,
        type=type,
        user_id=user_id,
        upload_id=upload_id,
        size_bytes=size_bytes,
        row_count=row_count,
    )
    db_session.add(data_source)
    await db_session.commit()
    await db_session.refresh(data_source)
    return data_source


async def get_data_source_by_upload_id(
    db_session: AsyncSession, upload_id: int
) -> DataSource | None:
    """Get a data source by its upload ID."""
    result = await db_session.execute(
        select(DataSource).where(DataSource.upload_id == upload_id)
    )
    return result.scalar_one_or_none()


async def update_data_source_name(
    db_session: AsyncSession, id: int, user_id: int, name: str
) -> DataSource | None:
    """Rename a data source."""
    data_source = await get_data_source_by_id(db_session, id, user_id)
    if not data_source:
        return None

    data_source.name = name
    await db_session.commit()
    return data_source


async def update_data_source_size(
    db_session: AsyncSession, data_source_id: int, additional_bytes: int
) -> DataSource | None:
    """Update the size of a data source by adding bytes."""
    data_source = await db_session.get(DataSource, data_source_id)
    if not data_source:
        return None

    data_source.size_bytes += additional_bytes
    await db_session.commit()
    return data_source


async def delete_data_source(db_session: AsyncSession, id: int, user_id: int) -> bool:
    """Delete a data source if it belongs to the user."""
    data_source = await get_data_source_by_id(db_session, id, user_id)
    if not data_source:
        return False

    await db_session.delete(data_source)
    await db_session.commit()
    return True
