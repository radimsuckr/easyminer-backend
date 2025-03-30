import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.crud.aio.data_source import create_data_source
from easyminer.crud.aio.field import (
    create_field,
    get_field_by_id,
    get_fields_by_data_source,
    get_fields_by_ids,
    update_field_stats,
)
from easyminer.models import Field, FieldNumericDetail, FieldType


@pytest.mark.asyncio
async def test_create_field(db_session: AsyncSession):
    # First create a data source
    data_source = await create_data_source(
        db_session=db_session,
        name="Test Data Source",
        type="csv",
    )

    # Create a new field
    field = await create_field(
        db_session=db_session,
        name="Test Field",
        data_source_id=data_source.id,
        data_type=FieldType.nominal,
        unique_count=10,
        support=5,
    )

    # Check that the field was created correctly
    assert field.id is not None
    assert field.name == "Test Field"
    assert field.data_source_id == data_source.id
    assert field.data_type == FieldType.nominal
    assert field.unique_count == 10
    assert field.support == 5

    # Check that the field is in the database
    result = await db_session.execute(select(Field).where(Field.id == field.id))
    db_field = result.scalar_one()
    assert db_field.id == field.id
    assert db_field.name == "Test Field"
    assert db_field.data_source_id == data_source.id
    assert db_field.data_type == FieldType.nominal
    assert db_field.unique_count == 10
    assert db_field.support == 5


@pytest.mark.asyncio
async def test_get_field_by_id(db_session: AsyncSession):
    """Test getting a field by ID successfully."""
    # First create a data source
    data_source = await create_data_source(
        db_session=db_session,
        name="Test Data Source",
        type="csv",
    )

    # Create a field
    created_field = await create_field(
        db_session=db_session,
        name="Test Field",
        data_source_id=data_source.id,
        data_type=FieldType.nominal,
        unique_count=10,
        support=5,
    )

    # Get the field by ID
    field = await get_field_by_id(db_session, created_field.id, data_source.id)
    assert field is not None
    assert field.id == created_field.id
    assert field.name == "Test Field"
    assert field.data_source_id == data_source.id
    assert field.data_type == FieldType.nominal
    assert field.unique_count == 10
    assert field.support == 5


@pytest.mark.asyncio
async def test_get_field_by_id_wrong_data_source(db_session: AsyncSession):
    """Test getting a field with wrong data source ID returns None."""
    # First create a data source
    data_source = await create_data_source(
        db_session=db_session,
        name="Test Data Source",
        type="csv",
    )

    # Create a field
    created_field = await create_field(
        db_session=db_session,
        name="Test Field",
        data_source_id=data_source.id,
        data_type=FieldType.nominal,
        unique_count=10,
        support=5,
    )

    # Test getting field with wrong data_source_id
    field = await get_field_by_id(db_session, created_field.id, 999)
    assert field is None


@pytest.mark.asyncio
async def test_get_field_by_id_nonexistent(db_session: AsyncSession):
    """Test getting a non-existent field returns None."""
    # Create a data source
    data_source = await create_data_source(
        db_session=db_session,
        name="Test Data Source",
        type="csv",
    )

    # Test getting non-existent field
    field = await get_field_by_id(db_session, 999, data_source.id)
    assert field is None


@pytest.mark.asyncio
async def test_get_fields_by_data_source(db_session: AsyncSession):
    """Test getting all fields for a data source."""
    # Create a data source
    data_source = await create_data_source(
        db_session=db_session,
        name="Test Data Source",
        type="csv",
    )

    # Create another data source
    other_data_source = await create_data_source(
        db_session=db_session,
        name="Other Data Source",
        type="csv",
    )

    # Create fields for both data sources
    await create_field(
        db_session=db_session,
        name="Field 1",
        data_source_id=data_source.id,
        data_type=FieldType.nominal,
        unique_count=10,
        support=5,
    )
    await create_field(
        db_session=db_session,
        name="Field 2",
        data_source_id=data_source.id,
        data_type=FieldType.nominal,
        unique_count=10,
        support=5,
    )
    await create_field(
        db_session=db_session,
        name="Other Field",
        data_source_id=other_data_source.id,
        data_type=FieldType.nominal,
        unique_count=10,
        support=5,
    )

    # Get fields for the first data source
    fields = await get_fields_by_data_source(db_session, data_source.id)
    assert len(fields) == 2
    assert all(field.data_source_id == data_source.id for field in fields)
    assert fields[0].name == "Field 1"  # Should be ordered by index
    assert fields[1].name == "Field 2"

    # Get fields for the second data source
    fields = await get_fields_by_data_source(db_session, other_data_source.id)
    assert len(fields) == 1
    assert fields[0].data_source_id == other_data_source.id
    assert fields[0].name == "Other Field"


@pytest.mark.asyncio
async def test_get_fields_by_data_source_nonexistent(db_session: AsyncSession):
    """Test getting fields for a non-existent data source returns empty list."""
    # Get fields for non-existent data source
    fields = await get_fields_by_data_source(db_session, 999)
    assert len(fields) == 0


@pytest.mark.asyncio
async def test_get_fields_by_ids(db_session: AsyncSession):
    """Test getting fields by IDs successfully."""
    # Create a data source
    data_source = await create_data_source(
        db_session=db_session,
        name="Test Data Source",
        type="csv",
    )

    # Create fields for the data source
    field1 = await create_field(
        db_session=db_session,
        name="Field 1",
        data_source_id=data_source.id,
        data_type=FieldType.nominal,
        unique_count=10,
        support=5,
    )
    field2 = await create_field(
        db_session=db_session,
        name="Field 2",
        data_source_id=data_source.id,
        data_type=FieldType.numeric,
        unique_count=10,
        support=5,
        min_value=1,
        max_value=10,
        avg_value=5.5,
    )

    # Get fields by IDs for the data source
    fields = await get_fields_by_ids(db_session, [field1.id, field2.id], data_source.id)
    assert len(fields) == 2
    assert all(field.data_source_id == data_source.id for field in fields)
    assert fields[0].id == field1.id  # Should be ordered by index
    assert fields[1].id == field2.id


@pytest.mark.asyncio
async def test_get_fields_by_ids_wrong_data_source(db_session: AsyncSession):
    """Test getting fields from wrong data source returns empty list."""
    # Create two data sources
    data_source = await create_data_source(
        db_session=db_session,
        name="Test Data Source",
        type="csv",
    )

    other_data_source = await create_data_source(
        db_session=db_session,
        name="Other Data Source",
        type="csv",
    )

    # Create a field for the other data source
    field3 = await create_field(
        db_session=db_session,
        name="Other Field",
        data_source_id=other_data_source.id,
        data_type=FieldType.numeric,
        unique_count=10,
        support=5,
        min_value=1,
        max_value=10,
        avg_value=5.5,
    )

    # Try to get fields from another data source with the first data source ID
    fields = await get_fields_by_ids(db_session, [field3.id], data_source.id)
    assert len(fields) == 0


@pytest.mark.asyncio
async def test_get_fields_by_ids_nonexistent(db_session: AsyncSession):
    """Test getting non-existent fields returns empty list."""
    # Create a data source
    data_source = await create_data_source(
        db_session=db_session,
        name="Test Data Source",
        type="csv",
    )

    # Try to get non-existent fields
    fields = await get_fields_by_ids(db_session, [999], data_source.id)
    assert len(fields) == 0


@pytest.mark.asyncio
async def test_update_field_stats(db_session: AsyncSession):
    """Test updating field statistics successfully."""
    # Create a data source
    data_source = await create_data_source(
        db_session=db_session,
        name="Test Data Source",
        type="csv",
    )

    # Create a field with initial stats
    field = await create_field(
        db_session=db_session,
        name="Test Field",
        data_source_id=data_source.id,
        data_type=FieldType.numeric,
        unique_count=10,
        support=5,
        min_value=1,
        max_value=10,
        avg_value=5.5,
    )

    # Update the field stats
    updated_field = await update_field_stats(db_session, field.id, 0, 20, 10.0)
    assert updated_field is not None
    assert updated_field.id == field.id
    assert updated_field.min_value == 0
    assert updated_field.max_value == 20
    assert updated_field.avg_value == 10.0

    # Check that the stats were updated in the database
    result = await db_session.execute(
        select(FieldNumericDetail).where(FieldNumericDetail.id == field.id)
    )
    db_field = result.scalar_one()
    assert db_field.min_value == 0
    assert db_field.max_value == 20
    assert db_field.avg_value == 10.0


@pytest.mark.asyncio
async def test_update_field_stats_nonexistent(db_session: AsyncSession):
    """Test updating statistics for a non-existent field returns None."""
    # Test updating non-existent field
    updated_field = await update_field_stats(db_session, 999, 0, 100, 50.0)
    assert updated_field is None
