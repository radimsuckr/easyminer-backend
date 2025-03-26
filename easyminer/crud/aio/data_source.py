from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from easyminer.models.data import DataSource
from easyminer.models.upload import PreviewUpload, Upload


async def get_data_source_by_id(db: AsyncSession, id: int) -> DataSource | None:
    """Get a data source by ID."""
    result = await db.execute(
        select(DataSource)
        .options(joinedload(DataSource.upload), joinedload(DataSource.preview_upload))
        .where(DataSource.id == id)
    )
    return result.scalars().first()


async def get_data_sources_by_user(db: AsyncSession) -> list[DataSource]:
    """Get all data sources."""
    result = await db.execute(
        select(DataSource).options(
            joinedload(DataSource.upload), joinedload(DataSource.preview_upload)
        )
    )
    return list(result.scalars().all())


async def create_data_source(
    db_session: AsyncSession,
    name: str,
    type: str,
    size_bytes: int = 0,
    row_count: int = 0,
    upload_id: int | None = None,
) -> DataSource:
    """Create a new data source."""
    data_source = DataSource(
        name=name,
        type=type,
        size_bytes=size_bytes,
        row_count=row_count,
        upload_id=upload_id,
    )
    db_session.add(data_source)
    await db_session.commit()
    await db_session.refresh(data_source)
    return data_source


async def get_data_source_by_upload_id(db: AsyncSession, upload_id: int) -> DataSource:
    """Get a data source by upload ID."""
    upload = (
        await db.execute(select(Upload).where(Upload.id == upload_id))
    ).scalar_one()
    result = await db.execute(
        select(DataSource).where(DataSource.id == upload.data_source_id)
    )
    return result.scalar_one()


async def get_data_source_by_preview_upload_id(db: AsyncSession, id: int) -> DataSource:
    """Get a data source by upload ID."""
    upload = (
        await db.execute(select(PreviewUpload).where(PreviewUpload.id == id))
    ).scalar_one()
    result = await db.execute(
        select(DataSource).where(DataSource.id == upload.data_source_id)
    )
    return result.scalar_one()


async def update_data_source_name(
    db: AsyncSession, id: int, name: str
) -> DataSource | None:
    """Update a data source name."""
    await db.execute(update(DataSource).where(DataSource.id == id).values(name=name))
    await db.commit()
    return await get_data_source_by_id(db, id)


async def update_data_source_size(
    db: AsyncSession, id: int, size_bytes: int
) -> DataSource | None:
    """Update a data source size in bytes."""
    await db.execute(
        update(DataSource)
        .where(DataSource.id == id)
        .values(size_bytes=DataSource.size_bytes + size_bytes)
    )
    await db.commit()
    return await get_data_source_by_id(db, id)


async def delete_data_source(db: AsyncSession, id: int) -> bool:
    """Delete a data source."""
    result = await db.execute(delete(DataSource).where(DataSource.id == id))
    await db.commit()
    return result.rowcount > 0
