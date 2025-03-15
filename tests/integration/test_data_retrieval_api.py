"""Integration tests for data retrieval API endpoints."""

import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.models.data import DataSource, Field
from easyminer.storage import DiskStorage


@pytest_asyncio.fixture
async def test_data_source_with_data(db_session: AsyncSession):
    """Create a test data source with data files for testing."""
    # Create a temporary directory for storage
    temp_dir = tempfile.mkdtemp()

    # Create a patched DiskStorage that uses the temp directory
    storage = DiskStorage(Path(temp_dir))

    # Use patch to override the DiskStorage constructor
    with patch("easyminer.storage.DiskStorage", return_value=storage):
        try:
            # Create the data source
            data_source = DataSource(
                name="Retrieval Test Data Source",
                type="csv",
                size_bytes=1000,
                row_count=5,
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
                    min_value="25",
                    max_value="40",
                    avg_value=32.5,
                ),
                Field(
                    name="score",
                    data_type="integer",
                    data_source_id=data_source.id,
                    index=2,
                    min_value="70",
                    max_value="95",
                    avg_value=85.0,
                ),
            ]

            for field in fields:
                db_session.add(field)

            await db_session.commit()

            # Create test CSV data
            csv_data = "name,age,score\nAlice,30,85\nBob,25,92\nCharlie,35,78\nDavid,40,90\nEve,33,80"

            # Create storage directory and save the chunk
            chunks_dir = Path(f"{data_source.id}/chunks")
            storage_dir = Path(temp_dir) / chunks_dir
            storage_dir.mkdir(parents=True, exist_ok=True)

            # Save the chunk file
            chunk_file = storage_dir / "testdata.chunk"
            chunk_file.write_text(csv_data)

            yield data_source

        finally:
            # Clean up the temporary directory
            shutil.rmtree(temp_dir, ignore_errors=True)


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
            assert isinstance(stats["min"], (int, float))
            assert isinstance(stats["max"], (int, float))
            assert isinstance(stats["avg"], (int, float))

            # Check that min <= avg <= max
            assert stats["min"] <= stats["avg"] <= stats["max"]
            break


@pytest.mark.asyncio
async def test_get_field_values(client, test_data_source_with_data):
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
    assert "instances" in response_data
    instances = response_data["instances"]
    assert isinstance(instances, list)

    # Should have two instances
    assert len(instances) == 2

    # Each instance should have a values property
    for instance in instances:
        assert "values" in instance
        assert "name" in instance["values"]
        assert "age" in instance["values"]
        assert "score" in instance["values"]

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
        assert "instances" in response_data
        filtered_instances = response_data["instances"]

        # Check that each instance has only the name field
        for instance in filtered_instances:
            assert "values" in instance
            values = instance["values"]
            assert "name" in values
            assert "age" not in values
            assert "score" not in values
