from collections.abc import Sequence

from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from easyminer.models.data import DataSource
from easyminer.models.upload import PreviewUpload, Upload


async def get_data_source_by_id(
    db: AsyncSession, id: int, eager: bool = False
) -> DataSource | None:
    """Get a data source by ID."""
    query = select(DataSource).where(DataSource.id == id)
    if eager:
        query = query.options(
            joinedload(DataSource.upload), joinedload(DataSource.preview_upload)
        )
    result = await db.execute(query)
    return result.scalars().first()


async def get_data_sources(
    db: AsyncSession, eager: bool = False
) -> Sequence[DataSource]:
    """Get all data sources."""
    query = select(DataSource)
    if eager:
        query = query.options(
            joinedload(DataSource.upload), joinedload(DataSource.preview_upload)
        )
    result = await db.execute(query)
    return result.scalars().all()


async def create_data_source(
    db_session: AsyncSession,
    name: str,
    type: str,
    size_bytes: int = 0,
    row_count: int = 0,
) -> DataSource:
    """Create a new data source."""
    query = (
        insert(DataSource)
        .values(
            name=name,
            type=type,
            size_bytes=size_bytes,
            row_count=row_count,
        )
        .returning(DataSource.id)
    )
    id = (await db_session.execute(query)).scalar_one()
    return (
        await db_session.execute(
            select(DataSource)
            .options(
                joinedload(DataSource.upload), joinedload(DataSource.preview_upload)
            )
            .where(DataSource.id == id)
        )
    ).scalar_one()


async def get_data_source_by_upload_id(
    db: AsyncSession, upload_id: int, eager: bool = False
) -> DataSource | None:
    """Get a data source by upload ID."""
    upload = (
        await db.execute(select(Upload).where(Upload.id == upload_id))
    ).scalar_one_or_none()
    if not upload:
        return None

    query = select(DataSource).where(DataSource.id == upload.data_source_id)
    if eager:
        query = query.options(
            joinedload(DataSource.upload), joinedload(DataSource.preview_upload)
        )
    result = await db.execute(query)
    return result.scalar_one_or_none()


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
    datasource = await db.execute(
        update(DataSource)
        .where(DataSource.id == id)
        .values(name=name)
        .returning(DataSource)
    )
    await db.commit()
    return datasource.scalar_one_or_none()


async def update_data_source_size(
    db: AsyncSession, id: int, size_bytes: int
) -> DataSource | None:
    """Update a data source size in bytes."""
    datasource = await db.execute(
        update(DataSource)
        .where(DataSource.id == id)
        .values(size_bytes=DataSource.size_bytes + size_bytes)
        .returning(DataSource)
    )
    await db.commit()
    return datasource.scalar_one_or_none()


async def delete_data_source(db: AsyncSession, id: int) -> bool:
    """Delete a data source."""
    result = await db.execute(delete(DataSource).where(DataSource.id == id))
    await db.commit()
    return result.rowcount > 0
