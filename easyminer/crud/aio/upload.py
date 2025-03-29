from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from easyminer.models.data import DataSource
from easyminer.models.upload import Chunk, PreviewUpload, Upload
from easyminer.schemas.data import PreviewUploadSchema, UploadSettings


async def create_upload(db_session: AsyncSession, settings: UploadSettings) -> Upload:
    """Create a new upload entry in the database with settings."""
    upload_id = uuid4()
    upload = Upload(
        uuid=str(upload_id),
        name=settings.name,
        media_type=settings.media_type,
        db_type=settings.db_type,
        separator=settings.separator,
        encoding=settings.encoding,
        quotes_char=settings.quotes_char,
        escape_char=settings.escape_char,
        locale=settings.locale,
        compression=settings.compression,
    )
    db_session.add(upload)
    data_source = DataSource(
        name=upload.name,
        type=upload.db_type,
        upload=upload,
        size_bytes=0,
    )
    db_session.add(data_source)
    await db_session.commit()
    await db_session.refresh(upload)
    return upload


async def create_preview_upload(
    db_session: AsyncSession, settings: PreviewUploadSchema
) -> PreviewUpload:
    """Create a new upload entry in the database with settings."""
    upload_id = uuid4()
    upload = PreviewUpload(
        uuid=upload_id,
        max_lines=settings.max_lines,
        compression=settings.compression,
    )
    db_session.add(upload)
    data_source = DataSource(
        name=f"preview-{upload_id}",
        type="limited",
        preview_upload=upload,
    )
    db_session.add(data_source)
    await db_session.commit()
    await db_session.refresh(upload)
    return upload


async def get_upload_by_uuid(
    db_session: AsyncSession, upload_uuid: str
) -> Upload | None:
    """Get an upload by its UUID."""
    result = await db_session.execute(
        select(Upload)
        .options(joinedload(Upload.data_source))
        .where(Upload.uuid == upload_uuid)
    )
    return result.scalar_one_or_none()


async def get_preview_upload_by_uuid(
    db_session: AsyncSession,
    uuid: UUID,
) -> PreviewUpload | None:
    """Get an upload by its UUID."""
    result = await db_session.execute(
        select(PreviewUpload)
        .options(joinedload(PreviewUpload.data_source))
        .where(PreviewUpload.uuid == uuid)
    )
    return result.scalar_one_or_none()


async def get_upload_by_id(db_session: AsyncSession, upload_id: int) -> Upload | None:
    """Get an upload by its ID."""
    return await db_session.get(Upload, upload_id)


async def create_chunk(db_session: AsyncSession, upload_id: int, path: str) -> None:
    chunk = Chunk(upload_id=upload_id, path=path)
    db_session.add(chunk)
    await db_session.commit()
