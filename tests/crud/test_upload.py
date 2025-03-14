import uuid
from unittest.mock import patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.crud.upload import (
    create_preview_upload,
    create_upload,
    get_upload_by_id,
    get_upload_by_uuid,
)
from easyminer.models.upload import Upload
from easyminer.schemas.data import UploadSettings


@pytest.mark.asyncio
async def test_create_upload(db_session: AsyncSession):
    """Test creating a new upload with SQLite."""
    # Create upload settings
    settings = UploadSettings(
        name="Test Upload",
        media_type="csv",
        db_type="limited",
        separator=",",
        encoding="utf-8",
        quotes_char='"',
        escape_char="\\",
        locale="en_US",
        compression="",
        format="csv",
    )

    # Mock the UUID generation for deterministic testing
    test_uuid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    with patch("easyminer.crud.upload.uuid4", return_value=test_uuid):
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
        assert upload.compression == ""
        assert upload.format == "csv"

        # Check that the upload is in the database
        result = await db_session.execute(select(Upload).where(Upload.id == upload.id))
        db_upload = result.scalar_one()
        assert db_upload.id == upload.id
        assert db_upload.uuid == str(test_uuid)
        assert db_upload.name == "Test Upload"


@pytest.mark.asyncio
async def test_create_preview_upload(db_session: AsyncSession):
    """Test creating a new preview upload with SQLite."""
    # Mock the UUID generation for deterministic testing
    test_uuid = uuid.UUID("87654321-4321-8765-4321-876543210987")
    with patch("easyminer.crud.upload.uuid4", return_value=test_uuid):
        # Create a new preview upload
        upload = await create_preview_upload(
            db_session=db_session,
            max_lines=100,
            compression="gzip",
        )

        # Check that the preview upload was created correctly
        assert upload.id is not None
        assert upload.uuid == str(test_uuid)
        assert upload.name == f"preview_{test_uuid}"
        assert upload.media_type == "csv"
        assert upload.db_type == "limited"
        assert upload.separator == ","
        assert upload.encoding == "utf-8"
        assert upload.quotes_char == '"'
        assert upload.escape_char == "\\"
        assert upload.locale == "en_US"
        assert upload.compression == "gzip"
        assert upload.format == "csv"
        assert upload.preview_max_lines == 100

        # Check that the upload is in the database
        result = await db_session.execute(select(Upload).where(Upload.id == upload.id))
        db_upload = result.scalar_one()
        assert db_upload.id == upload.id
        assert db_upload.uuid == str(test_uuid)
        assert db_upload.name == f"preview_{test_uuid}"
        assert db_upload.preview_max_lines == 100


@pytest.mark.asyncio
async def test_get_upload_by_uuid(db_session: AsyncSession):
    """Test getting an upload by UUID successfully."""
    # Create a new upload with a known UUID
    test_uuid = "test-uuid-123"
    upload = Upload(
        uuid=test_uuid,
        name="Test Upload",
        media_type="csv",
        db_type="limited",
        separator=",",
        encoding="utf-8",
        quotes_char='"',
        escape_char="\\",
        locale="en_US",
        compression="",
        format="csv",
    )
    db_session.add(upload)
    await db_session.commit()
    await db_session.refresh(upload)

    # Get the upload by UUID
    retrieved_upload = await get_upload_by_uuid(db_session, test_uuid)
    assert retrieved_upload is not None
    assert retrieved_upload.id == upload.id
    assert retrieved_upload.uuid == test_uuid
    assert retrieved_upload.name == "Test Upload"


@pytest.mark.asyncio
async def test_get_upload_by_uuid_nonexistent(db_session: AsyncSession):
    """Test getting a non-existent upload by UUID returns None."""
    # Test getting non-existent upload
    non_existent = await get_upload_by_uuid(db_session, "non-existent-uuid")
    assert non_existent is None


@pytest.mark.asyncio
async def test_get_upload_by_id(db_session: AsyncSession):
    """Test getting an upload by ID successfully."""
    # Create a new upload
    upload = Upload(
        uuid="test-uuid-456",
        name="Test Upload",
        media_type="csv",
        db_type="limited",
        separator=",",
        encoding="utf-8",
        quotes_char='"',
        escape_char="\\",
        locale="en_US",
        compression="",
        format="csv",
    )
    db_session.add(upload)
    await db_session.commit()
    await db_session.refresh(upload)

    # Get the upload by ID
    retrieved_upload = await get_upload_by_id(db_session, upload.id)
    assert retrieved_upload is not None
    assert retrieved_upload.id == upload.id
    assert retrieved_upload.uuid == "test-uuid-456"
    assert retrieved_upload.name == "Test Upload"


@pytest.mark.asyncio
async def test_get_upload_by_id_nonexistent(db_session: AsyncSession):
    """Test getting a non-existent upload by ID returns None."""
    # Test getting non-existent upload
    non_existent = await get_upload_by_id(db_session, 999)
    assert non_existent is None
