"""Integration tests for data retrieval API endpoints."""

from easyminer.models.data import DataSource
from fastapi.testclient import TestClient
import pytest


@pytest.mark.asyncio
async def test_get_field_stats(client, test_data_source_with_data):
    """Test retrieving field statistics."""
    ds_id = test_data_source_with_data.id

    # Get the fields
    response = client.get(f"/api/v1/datasource/{ds_id}/field")
    assert response.status_code == 200
    fields = response.json()

    # Find numeric fields
    for field in fields:
        if field["dataType"] in ["integer", "float", "numeric"]:
            # Get stats for this field
            response = client.get(
                f"/api/v1/datasource/{ds_id}/field/{field['id']}/stats"
            )
            assert response.status_code == 200
            stats = response.json()

            # Check the stats structure
            assert "min" in stats
            assert "max" in stats
            assert "avg" in stats

            # Check that the values are numeric
            assert isinstance(stats["min"], int | float)
            assert isinstance(stats["max"], int | float)
            assert isinstance(stats["avg"], int | float)

            # Check that min <= avg <= max
            assert stats["min"] <= stats["avg"] <= stats["max"]
            break


@pytest.mark.asyncio
async def test_get_field_values(
    client: TestClient, test_data_source_with_data: DataSource
):
    """Test retrieving field values."""
    ds_id = test_data_source_with_data.id

    # Get the fields
    response = client.get(f"/api/v1/datasource/{ds_id}/field")
    assert response.status_code == 200
    fields = response.json()

    # Test for each field
    for field in fields:
        # Get values for this field
        response = client.get(f"/api/v1/datasource/{ds_id}/field/{field['id']}/values")

        # Check the response
        assert response.status_code == 200
        values = response.json()

        # Should be a list of value objects
        assert isinstance(values, list)

        # If we have values, check their structure
        if values:
            assert "id" in values[0]
            assert "value" in values[0]
            assert "frequency" in values[0]


@pytest.mark.asyncio
async def test_get_instances(client, test_data_source_with_data):
    """Test retrieving data instances."""
    ds_id = test_data_source_with_data.id

    # Get instances with pagination
    response = client.get(f"/api/v1/datasource/{ds_id}/instances?offset=0&limit=2")

    # Check the response
    assert response.status_code == 200
    response_data = response.json()

    # Check that we have an instances list in the response
    instances = response_data
    assert isinstance(instances, list)

    # Should have two instances
    assert len(instances) == 2

    # Each instance should have a values property
    for instance in instances:
        assert "name" in instance
        assert "age" in instance
        assert "score" in instance

    # Test with field filtering
    # First, get field IDs
    response = client.get(f"/api/v1/datasource/{ds_id}/field")
    assert response.status_code == 200
    fields = response.json()

    # Find the 'name' field ID
    name_field_id = None
    for field in fields:
        if field["name"] == "name":
            name_field_id = field["id"]
            break

    if name_field_id is not None:
        # Get instances with just the name field
        response = client.get(
            f"/api/v1/datasource/{ds_id}/instances?field_ids={name_field_id}"
        )

        # Check the response
        assert response.status_code == 200
        response_data = response.json()

        # Check that we have an instances list in the response
        filtered_instances = response_data

        # Check that each instance has only the name field
        for instance in filtered_instances:
            assert "name" in instance
            assert "age" not in instance
            assert "score" not in instance
