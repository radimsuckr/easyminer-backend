import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.crud.aio.data import create_upload
from easyminer.models.data import Upload
from easyminer.schemas.data import DbType, FieldType, MediaType, StartUploadSchema, UploadFormat


@pytest.mark.asyncio
async def test_create_upload_succeeds(async_db_session: AsyncSession):
    """Test creating an upload with valid settings."""

    settings = StartUploadSchema(
        name="test_upload",
        db_type=DbType.limited,
        compression=None,
        media_type=MediaType.csv,
        separator=",",
        encoding="utf-8",
        quotes_char='"',
        escape_char="\\",
        locale="en_US",
        null_values=["NULL"],
        data_types=[FieldType.nominal],
        format=UploadFormat.nq,
    )

    upload_uuid = await create_upload(async_db_session, settings)
    uploads_count = (
        await async_db_session.execute(select(func.count()).select_from(Upload).where(Upload.uuid == upload_uuid))
    ).scalar_one()

    assert upload_uuid is not None
    assert uploads_count == 1
