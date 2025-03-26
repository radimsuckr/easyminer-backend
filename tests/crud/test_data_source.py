import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.crud.aio.data_source import (
    create_data_source,
    delete_data_source,
    get_data_source_by_id,
    get_data_source_by_upload_id,
    update_data_source_name,
    update_data_source_size,
)
from easyminer.models import DataSource
from easyminer.models.upload import Upload


@pytest.mark.asyncio
async def test_create_data_source(
    db_session: AsyncSession, test_data_source: DataSource
):
    """Test creating a new data source with SQLite."""
    # Create a new data source
    data_source = test_data_source

    # Check that the data source was created correctly
    assert data_source.id is not None
    assert data_source.name == "Test Data Source"
    assert data_source.type == "csv"
    assert data_source.upload is None
    assert data_source.preview_upload is None
    assert data_source.size_bytes == 1000
    assert data_source.row_count == 20

    # Check that the data source is in the database
    result = await db_session.execute(
        select(DataSource).where(DataSource.id == data_source.id)
    )
    db_data_source = result.scalar_one()
    assert db_data_source.id == data_source.id
    assert db_data_source.name == "Test Data Source"


@pytest.mark.asyncio
async def test_get_data_source_by_id(
    db_session: AsyncSession, test_data_source: DataSource
):
    """Test getting a data source by ID."""
    # Create a new data source
    created_ds = test_data_source

    # Get the data source by ID
    data_source = await get_data_source_by_id(db_session, created_ds.id)
    assert data_source is not None
    assert data_source.id == created_ds.id
    assert data_source.name == "Test Data Source"

    # Test getting non-existent data source
    data_source = await get_data_source_by_id(db_session, 999)
    assert data_source is None


@pytest.mark.asyncio
async def test_get_data_source_by_upload_id(
    db_session: AsyncSession, test_upload: Upload
):
    """Test getting a data source by upload ID."""
    # Create a new data source with upload_id
    upload = test_upload

    # Get the data source by upload ID
    data_source = await get_data_source_by_upload_id(db_session, upload.id, eager=True)
    assert data_source is not None
    assert data_source.upload is not None
    assert data_source.name == "Test Upload"

    # Test getting data source with wrong upload_id
    data_source = await get_data_source_by_upload_id(db_session, 999)
    assert data_source is None


@pytest.mark.asyncio
async def test_update_data_source_name(
    db_session: AsyncSession, test_data_source: DataSource
):
    """Test updating a data source name."""
    # Create a new data source
    data_source = test_data_source

    # Update the name
    updated_ds = await update_data_source_name(db_session, data_source.id, "New Name")
    assert updated_ds is not None
    assert updated_ds.id == data_source.id
    assert updated_ds.name == "New Name"

    # Check that the name was updated in the database
    result = await db_session.execute(
        select(DataSource).where(DataSource.id == data_source.id)
    )
    db_data_source = result.scalar_one()
    assert db_data_source.name == "New Name"

    # Test updating non-existent data source
    updated_ds = await update_data_source_name(db_session, 999, "Non-existent")
    assert updated_ds is None


@pytest.mark.asyncio
async def test_update_data_source_size(
    db_session: AsyncSession, test_data_source: DataSource
):
    """Test updating a data source size."""
    # Create a new data source with initial size
    data_source = test_data_source

    # Update the size
    updated_ds = await update_data_source_size(db_session, data_source.id, 500)
    assert updated_ds is not None
    assert updated_ds.id == data_source.id
    assert updated_ds.size_bytes == 1500  # 1000 + 500

    # Check that the size was updated in the database
    result = await db_session.execute(
        select(DataSource).where(DataSource.id == data_source.id)
    )
    db_data_source = result.scalar_one()
    assert db_data_source.size_bytes == 1500

    # Test updating non-existent data source
    updated_ds = await update_data_source_size(db_session, 999, 500)
    assert updated_ds is None


@pytest.mark.asyncio
async def test_delete_data_source(
    db_session: AsyncSession, test_data_source: DataSource
):
    """Test deleting a data source successfully."""
    # Create a new data source
    data_source = test_data_source

    # Verify it exists
    result = await db_session.execute(
        select(DataSource).where(DataSource.id == data_source.id)
    )
    assert result.scalar_one() is not None

    # Delete the data source
    success = await delete_data_source(db_session, data_source.id)
    assert success is True

    # Verify it was deleted
    result = await db_session.execute(
        select(DataSource).where(DataSource.id == data_source.id)
    )
    assert result.scalar_one_or_none() is None


@pytest.mark.asyncio
async def test_delete_data_source_nonexistent(db_session: AsyncSession):
    """Test deleting a non-existent data source fails."""
    # Try to delete a non-existent data source
    success = await delete_data_source(db_session, 999)
    assert success is False
