"""Integration tests for data preview API endpoints."""

import pytest
from fastapi.testclient import TestClient

from easyminer.models.data import DataSource, Field


@pytest.mark.asyncio
async def test_preview_data_source(
    client: TestClient, test_data_source_with_chunks: tuple[DataSource, list[Field]]
):
    """Test retrieving preview data from a data source."""
    data_source, _ = test_data_source_with_chunks

    # Make the API request
    response = client.get(f"/api/v1/datasource/{data_source.id}/preview?limit=3")

    # Check the response
    assert response.status_code == 200
    preview_data = response.json()

    # Check the structure
    assert "fieldNames" in preview_data
    assert "rows" in preview_data

    # Check the content
    assert len(preview_data["fieldNames"]) == 3
    assert "name" in preview_data["fieldNames"]
    assert "age" in preview_data["fieldNames"]
    assert "score" in preview_data["fieldNames"]

    # Check the rows
    rows = preview_data["rows"]
    assert len(rows) <= 3  # Should respect the limit

    # Check a specific row
    assert len(rows) > 0
    first_row = rows[0]
    assert "name" in first_row
    assert "age" in first_row
    assert "score" in first_row


@pytest.mark.asyncio
async def test_get_instances(client, test_data_source_with_chunks: DataSource):
    """Test retrieving data instances from a data source."""
    data_source, _ = test_data_source_with_chunks

    # Make the API request with pagination
    response = client.get(
        f"/api/v1/datasource/{data_source.id}/instances?offset=1&limit=2"
    )

    # Check the response
    assert response.status_code == 200
    response_data = response.json()

    # Check that we have an instances list in the response
    instances = response_data
    assert isinstance(instances, list)
    assert len(instances) == 2

    # Each instance should have a values property
    for instance in instances:
        # Check for expected fields in the values
        assert "name" in instance
        assert "age" in instance
        assert "score" in instance


@pytest.mark.asyncio
async def test_get_instances_with_field_filter(client, test_data_source_with_chunks):
    """Test retrieving data instances with field filtering."""
    data_source, fields = test_data_source_with_chunks

    # Get IDs for name and age fields
    name_field_id = next(field.id for field in fields if field.name == "name")
    age_field_id = next(field.id for field in fields if field.name == "age")

    # Make the API request with field filtering
    response = client.get(
        f"/api/v1/datasource/{data_source.id}/instances?limit=3&field_ids={name_field_id}&field_ids={age_field_id}"
    )

    # Check the response
    assert response.status_code == 200
    response_data = response.json()

    # Check that we have an instances list in the response
    instances = response_data

    # Check that only requested fields are included
    for instance in instances:
        assert "name" in instance
        assert "age" in instance
        assert "score" not in instance  # Score field was not requested
