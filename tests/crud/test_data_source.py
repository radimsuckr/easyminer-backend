import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.crud.data_source import (
    create_data_source,
    delete_data_source,
    get_data_source_by_id,
    get_data_source_by_upload_id,
    update_data_source_name,
    update_data_source_size,
)
from easyminer.models import DataSource


@pytest.mark.asyncio
async def test_create_data_source(db_session: AsyncSession):
    """Test creating a new data source with SQLite."""
    # Create a new data source
    data_source = await create_data_source(
        db_session=db_session,
        name="Test Data Source",
        type="csv",
        upload_id=1,
        size_bytes=1000,
        row_count=20,
    )

    # Check that the data source was created correctly
    assert data_source.id is not None
    assert data_source.name == "Test Data Source"
    assert data_source.type == "csv"
    assert data_source.upload_id == 1
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
async def test_get_data_source_by_id(db_session: AsyncSession):
    """Test getting a data source by ID."""
    # Create a new data source
    created_ds = await create_data_source(
        db_session=db_session,
        name="Test Data Source",
        type="csv",
    )

    # Get the data source by ID
    data_source = await get_data_source_by_id(db_session, created_ds.id)
    assert data_source is not None
    assert data_source.id == created_ds.id
    assert data_source.name == "Test Data Source"

    # Test getting non-existent data source
    data_source = await get_data_source_by_id(db_session, 999)
    assert data_source is None


@pytest.mark.asyncio
async def test_get_data_source_by_upload_id(db_session: AsyncSession):
    """Test getting a data source by upload ID."""
    # Create a new data source with upload_id
    await create_data_source(
        db_session=db_session,
        name="Test Data Source",
        type="csv",
        upload_id=42,
    )

    # Get the data source by upload ID
    data_source = await get_data_source_by_upload_id(db_session, 42)
    assert data_source is not None
    assert data_source.upload_id == 42
    assert data_source.name == "Test Data Source"

    # Test getting data source with wrong upload_id
    data_source = await get_data_source_by_upload_id(db_session, 999)
    assert data_source is None


@pytest.mark.asyncio
async def test_update_data_source_name(db_session: AsyncSession):
    """Test updating a data source name."""
    # Create a new data source
    data_source = await create_data_source(
        db_session=db_session,
        name="Original Name",
        type="csv",
    )

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
async def test_update_data_source_size(db_session: AsyncSession):
    """Test updating a data source size."""
    # Create a new data source with initial size
    data_source = await create_data_source(
        db_session=db_session,
        name="Test Data Source",
        type="csv",
        size_bytes=1000,
    )

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
async def test_delete_data_source(db_session: AsyncSession):
    """Test deleting a data source successfully."""
    # Create a new data source
    data_source = await create_data_source(
        db_session=db_session,
        name="Test Data Source",
        type="csv",
    )

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
