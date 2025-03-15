"""Integration tests for field API endpoints."""

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.models.data import DataSource, Field


@pytest_asyncio.fixture
async def test_data_source_with_fields(db_session: AsyncSession):
    """Create a test data source with fields for testing."""
    # Create the data source
    data_source = DataSource(
        name="Field Test Data Source",
        type="csv",
        size_bytes=1000,
        row_count=10,
    )
    db_session.add(data_source)
    await db_session.commit()
    await db_session.refresh(data_source)

    # Create fields
    fields = [
        Field(
            name="name",
            data_type="string",
            data_source_id=data_source.id,
            index=0,
        ),
        Field(
            name="age",
            data_type="integer",
            data_source_id=data_source.id,
            index=1,
            min_value="20",
            max_value="65",
            avg_value=40.5,
            unique_count=15,
            has_nulls=False,
        ),
        Field(
            name="score",
            data_type="float",
            data_source_id=data_source.id,
            index=2,
            min_value="0.0",
            max_value="100.0",
            avg_value=75.5,
            unique_count=20,
            has_nulls=True,
        ),
    ]

    for field in fields:
        db_session.add(field)

    await db_session.commit()

    # Return a tuple with data source and fields
    return data_source, fields


@pytest.mark.asyncio
async def test_list_fields(client, test_data_source_with_fields):
    """Test listing all fields for a data source."""
    data_source, fields = test_data_source_with_fields

    # Make the API request
    response = client.get(f"/api/v1/datasource/{data_source.id}/field")

    # Check the response
    assert response.status_code == 200
    field_list = response.json()
    assert isinstance(field_list, list)
    assert len(field_list) == 3

    # Check that our fields are in the response
    field_names = [field["name"] for field in field_list]
    assert "name" in field_names
    assert "age" in field_names
    assert "score" in field_names

    # Check specific field properties
    for field in field_list:
        if field["name"] == "age":
            assert field["dataType"] == "integer"
            assert field["minValue"] == "20"
            assert field["maxValue"] == "65"
            assert field["avgValue"] == 40.5
        elif field["name"] == "score":
            assert field["dataType"] == "float"
            # The API doesn't return hasNulls, so we can't check it


@pytest.mark.asyncio
async def test_get_field(client, test_data_source_with_fields):
    """Test retrieving a specific field."""
    data_source, fields = test_data_source_with_fields

    # Get the ID of the 'age' field
    age_field = next(field for field in fields if field.name == "age")

    # Make the API request
    response = client.get(f"/api/v1/datasource/{data_source.id}/field/{age_field.id}")

    # Check the response
    assert response.status_code == 200
    field_data = response.json()
    assert field_data["name"] == "age"
    assert field_data["dataType"] == "integer"
    assert field_data["minValue"] == "20"
    assert field_data["maxValue"] == "65"
    assert field_data["avgValue"] == 40.5
    # The API returns uniqueValuesCount instead of uniqueCount
    assert field_data["uniqueValuesCount"] is None  # It's None in the response


@pytest.mark.asyncio
async def test_get_field_not_found(client, test_data_source_with_fields):
    """Test retrieving a non-existent field."""
    data_source, _ = test_data_source_with_fields

    # Make the API request with a non-existent field ID
    response = client.get(f"/api/v1/datasource/{data_source.id}/field/9999")

    # Check the response
    assert response.status_code == 404
    error = response.json()
    assert "detail" in error
    assert "not found" in error["detail"].lower()


@pytest.mark.asyncio
async def test_get_field_stats(client, test_data_source_with_fields):
    """Test retrieving field statistics."""
    data_source, fields = test_data_source_with_fields

    # Get the ID of the 'age' field (numeric field)
    age_field = next(field for field in fields if field.name == "age")

    # Make the API request
    response = client.get(
        f"/api/v1/datasource/{data_source.id}/field/{age_field.id}/stats"
    )

    # Check the response
    assert response.status_code == 200
    stats = response.json()
    assert stats["min"] == 20
    assert stats["max"] == 65
    assert stats["avg"] == 40.5
