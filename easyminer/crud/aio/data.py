from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.models.data import (
    Chunk,
    DataSource,
    DataType,
    NullValue,
    PreviewUpload,
    Upload,
)
from easyminer.schemas.data import FieldType, PreviewUploadSchema, StartUploadSchema


async def create_upload(db: AsyncSession, settings: StartUploadSchema) -> UUID:
    if len(settings.data_types) == 0:
        raise ValueError("data_types cannot be empty")
    if len(settings.data_types) > len(FieldType):
        raise ValueError(f"data_types cannot have more than {len(FieldType)} values, got {len(settings.data_types)}")

    upload = Upload(
        uuid=uuid4(),
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
    db.add(upload)

    for null_value in settings.null_values:
        db.add(NullValue(value=null_value, upload=upload))
    for data_type in settings.data_types:
        db.add(DataType(value=data_type, upload=upload))

    data_source = DataSource(name=upload.name, type=settings.db_type, upload=upload)
    db.add(data_source)

    await db.flush()
    return data_source.upload.uuid


async def create_preview_upload(db: AsyncSession, settings: PreviewUploadSchema) -> UUID:
    upload_id = uuid4()
    upload = PreviewUpload(uuid=upload_id, max_lines=settings.max_lines, compression=settings.compression)
    db.add(upload)

    data_source = DataSource(name=f"preview-{upload_id}", type="limited", preview_upload=upload)
    db.add(data_source)

    await db.flush()
    return upload.uuid


async def create_chunk(db: AsyncSession, upload_id: int, uploaded_at: datetime, path: str) -> int:
    chunk = Chunk(upload_id=upload_id, uploaded_at=uploaded_at, path=path)
    db.add(chunk)
    await db.flush()
    return chunk.id
