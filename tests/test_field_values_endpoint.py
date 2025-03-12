"""Test for the field values endpoint."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from easyminer.models import DataSource, Field, Upload

from .conftest import TEST_USER, client


@pytest.mark.asyncio
async def test_get_field_values_endpoint(override_dependencies, test_db):
    """End-to-end test for the get_field_values endpoint.

    This test verifies the full functionality by making a real HTTP request to the endpoint.
    """
    # Step 1: Create a test data source
    test_data_source = DataSource(
        name="Field Values Test Data Source",
        type="csv",
        user_id=TEST_USER.id,
        size_bytes=1000,
        row_count=10,
    )
    test_db.add(test_data_source)
    await test_db.commit()
    await test_db.refresh(test_data_source)

    data_source_id = test_data_source.id

    # Step 2: Create a test field
    test_field = Field(
        name="test_field",
        data_type="string",
        data_source_id=data_source_id,
        index=0,
    )
    test_db.add(test_field)
    await test_db.commit()
    await test_db.refresh(test_field)

    field_id = test_field.id

    # Step 3: Create an upload for the data source
    test_upload = Upload(
        uuid="test-field-values-uuid",
        name="test_field_values.csv",
        media_type="csv",
        db_type="limited",
        separator=",",
        quotes_char='"',
        encoding="utf-8",
        escape_char="\\",
        locale="en_US",
        compression="none",
        format="csv",
    )
    test_db.add(test_upload)
    await test_db.commit()
    await test_db.refresh(test_upload)

    # Link the upload to the data source
    test_data_source.upload_id = test_upload.id
    await test_db.commit()

    # Step 4: Prepare test CSV data
    csv_data = "TestField,OtherField\nvalue1,other1\nvalue2,other2\nvalue1,other3\nvalue3,other4"

    # Create a mock DiskStorage instance
    mock_storage = MagicMock()
    mock_storage._root = Path("/mock/path")

    # Step 5: Mock the storage operations
    with (
        patch("easyminer.storage.storage.DiskStorage", return_value=mock_storage),
        patch("easyminer.api.data.Path.glob", return_value=["chunk1.chunk"]),
        patch("easyminer.api.data.Path.exists", return_value=True),
        patch(
            "easyminer.api.data.Path.read_bytes", return_value=csv_data.encode("utf-8")
        ),
    ):
        # Step 6: Make the HTTP request to the endpoint
        response = client.get(
            f"/api/v1/datasource/{data_source_id}/field/{field_id}/values",
            headers={"Authorization": "Bearer test_token"},
        )

        # Step 7: Verify the response
        assert response.status_code == 200
        values = response.json()

        # Check the number of unique values
        assert len(values) == 3

        # Convert to dictionary for easier checking
        value_map = {item["value"]: item["frequency"] for item in values}
        assert "value1" in value_map
        assert value_map["value1"] == 2  # value1 appears twice
        assert value_map["value2"] == 1
        assert value_map["value3"] == 1

        # Check ordering (sorted by frequency descending)
        assert values[0]["value"] == "value1"  # Most frequent should be first
