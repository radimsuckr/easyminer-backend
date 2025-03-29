import uuid
from unittest.mock import patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.crud.aio.upload import (
    create_upload,
    get_upload_by_id,
    get_upload_by_uuid,
)
from easyminer.models.upload import Upload
from easyminer.schemas.data import DbType, MediaType, UploadSettings


@pytest.mark.asyncio
async def test_create_upload(db_session: AsyncSession):
    """Test creating a new upload with SQLite."""
    # Create upload settings
    settings = UploadSettings(
        name="Test Upload",
        media_type=MediaType.csv,
        db_type=DbType.limited,
        separator=",",
        encoding="utf-8",
        quotes_char='"',
        escape_char="\\",
        locale="en_US",
        compression=None,
    )

    # Mock the UUID generation for deterministic testing
    test_uuid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    with patch("easyminer.crud.aio.upload.uuid4", return_value=test_uuid):
        # Create a new upload
        upload = await create_upload(db_session=db_session, settings=settings)

        # Check that the upload was created correctly
        assert upload.id is not None
        assert upload.uuid == str(test_uuid)
        assert upload.name == "Test Upload"
        assert upload.media_type == "csv"
        assert upload.db_type == "limited"
        assert upload.separator == ","
        assert upload.encoding == "utf-8"
        assert upload.quotes_char == '"'
        assert upload.escape_char == "\\"
        assert upload.locale == "en_US"
        assert upload.compression is None

        # Check that the upload is in the database
        result = await db_session.execute(select(Upload).where(Upload.id == upload.id))
        db_upload = result.scalar_one()
        assert db_upload.id == upload.id
        assert db_upload.uuid == str(test_uuid)
        assert db_upload.name == "Test Upload"


@pytest.mark.asyncio
async def test_get_upload_by_uuid(db_session: AsyncSession, test_upload: Upload):
    """Test getting an upload by UUID successfully."""
    # Create a new upload with a known UUID
    upload = test_upload

    # Get the upload by UUID
    retrieved_upload = await get_upload_by_uuid(db_session, upload.uuid)
    assert retrieved_upload is not None
    assert retrieved_upload.id == upload.id
    assert retrieved_upload.uuid == upload.uuid
    assert retrieved_upload.name == "Test Upload"


@pytest.mark.asyncio
async def test_get_upload_by_uuid_nonexistent(db_session: AsyncSession):
    """Test getting a non-existent upload by UUID returns None."""
    # Test getting non-existent upload
    non_existent = await get_upload_by_uuid(db_session, "non-existent-uuid")
    assert non_existent is None


@pytest.mark.asyncio
async def test_get_upload_by_id(db_session: AsyncSession, test_upload: Upload):
    """Test getting an upload by ID successfully."""
    # Create a new upload
    upload = test_upload

    retrieved_upload = await get_upload_by_id(db_session, upload.id)
    assert retrieved_upload is not None
    assert retrieved_upload.id == upload.id
    assert retrieved_upload.uuid == upload.uuid
    assert retrieved_upload.name == "Test Upload"


@pytest.mark.asyncio
async def test_get_upload_by_id_nonexistent(db_session: AsyncSession):
    """Test getting a non-existent upload by ID returns None."""
    # Test getting non-existent upload
    non_existent = await get_upload_by_id(db_session, 999)
    assert non_existent is None
