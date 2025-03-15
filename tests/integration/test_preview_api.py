"""Integration tests for data preview API endpoints."""

import csv
import io
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
async def test_data_source_with_chunks(db_session: AsyncSession):
    """Create a test data source with actual chunks for testing."""
    # Create a temporary directory for storage
    temp_dir = tempfile.mkdtemp()

    # Create a patched DiskStorage that uses the temp directory
    storage = DiskStorage(Path(temp_dir))

    # Use patch to override the DiskStorage constructor
    with patch("easyminer.storage.DiskStorage", return_value=storage):
        try:
            # Create the data source
            data_source = DataSource(
                name="Preview Test Data Source",
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
                    data_type="float",
                    data_source_id=data_source.id,
                    index=2,
                    min_value="70.0",
                    max_value="95.0",
                    avg_value=85.0,
                ),
            ]

            for field in fields:
                db_session.add(field)

            await db_session.commit()

            # Create the chunks directory for the data source
            chunk_dir = Path(f"{data_source.id}/chunks")
            storage_dir = Path(temp_dir) / chunk_dir
            storage_dir.mkdir(parents=True, exist_ok=True)

            # Write a CSV file
            csv_data = [
                ["name", "age", "score"],
                ["Alice", "30", "85.5"],
                ["Bob", "25", "92.0"],
                ["Charlie", "35", "78.5"],
                ["Dave", "40", "90.0"],
                ["Eve", "32", "88.5"],
            ]

            csv_output = io.StringIO()
            writer = csv.writer(csv_output)
            writer.writerows(csv_data)

            # Save the chunk file
            chunk_file = storage_dir / "test_chunk.chunk"
            chunk_file.write_text(csv_output.getvalue())

            # Yield the data source and fields
            yield data_source, fields

        finally:
            # Clean up the temporary directory
            shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_preview_data_source(client, test_data_source_with_chunks):
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
async def test_get_instances(client, test_data_source_with_chunks):
    """Test retrieving data instances from a data source."""
    data_source, fields = test_data_source_with_chunks

    # Make the API request with pagination
    response = client.get(
        f"/api/v1/datasource/{data_source.id}/instances?offset=1&limit=2"
    )

    # Check the response
    assert response.status_code == 200
    response_data = response.json()

    # Check that we have an instances list in the response
    assert "instances" in response_data
    instances = response_data["instances"]
    assert isinstance(instances, list)
    assert len(instances) == 2

    # Each instance should have a values property
    for instance in instances:
        assert "values" in instance
        # Check for expected fields in the values
        assert "name" in instance["values"]
        assert "age" in instance["values"]
        assert "score" in instance["values"]


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
    assert "instances" in response_data
    instances = response_data["instances"]

    # Check that only requested fields are included
    for instance in instances:
        assert "values" in instance
        values = instance["values"]
        assert "name" in values
        assert "age" in values
        assert "score" not in values  # Score field was not requested
