import pytest
from sqlalchemy import text

from easyminer.models import DataSource, Field, Upload

from .conftest import TEST_USER, client


# Test listing data sources
@pytest.mark.asyncio
async def test_list_data_sources(override_dependencies, test_db):
    # Create a test data source directly
    data_source = DataSource(
        name="Test Data Source",
        type="csv",
        user_id=TEST_USER.id,
        size_bytes=1000,
        row_count=10,
    )

    test_db.add(data_source)
    await test_db.commit()
    await test_db.refresh(data_source)

    # Add the data source to the user's data_sources
    TEST_USER.data_sources = [data_source]

    # Make request to list data sources
    response = client.get(
        "/api/v1/sources", headers={"Authorization": "Bearer test_token"}
    )

    # Check response
    assert response.status_code == 200

    # Verify the response contains the test data source
    data_sources = response.json()
    assert len(data_sources) == 1
    assert data_sources[0]["name"] == "Test Data Source"
    assert data_sources[0]["id"] == data_source.id


# Test creating a data source
@pytest.mark.asyncio
async def test_create_data_source(override_dependencies, test_db):
    # Data source data
    data = {
        "name": "New Data Source",
        "type": "csv",
        "size_bytes": 2000,
        "row_count": 20,
    }

    # Make request to create data source
    response = client.post(
        "/api/v1/sources", json=data, headers={"Authorization": "Bearer test_token"}
    )

    # Check response
    assert response.status_code == 200

    # Verify the response contains the new data source
    data_source = response.json()
    assert data_source["name"] == "New Data Source"
    assert data_source["type"] == "csv"
    assert data_source["size_bytes"] == 2000
    assert data_source["row_count"] == 20

    # Verify the data source was created in the database
    result = await test_db.execute(
        text(f"SELECT * FROM data_source WHERE id = {data_source['id']}")
    )
    db_data_source = result.fetchone()
    assert db_data_source is not None
    assert db_data_source.name == "New Data Source"


# Test getting a specific data source
@pytest.mark.asyncio
async def test_get_data_source(override_dependencies, test_db):
    # Create a test data source directly
    data_source = DataSource(
        name="Test Data Source",
        type="csv",
        user_id=TEST_USER.id,
        size_bytes=1000,
        row_count=10,
    )

    test_db.add(data_source)
    await test_db.commit()
    await test_db.refresh(data_source)

    # Make request to get data source
    response = client.get(
        f"/api/v1/sources/{data_source.id}",
        headers={"Authorization": "Bearer test_token"},
    )

    # Check response
    assert response.status_code == 200

    # Verify the response contains the test data source
    data_source_response = response.json()
    assert data_source_response["name"] == "Test Data Source"
    assert data_source_response["id"] == data_source.id


# Test for list_fields endpoint
@pytest.mark.asyncio
async def test_list_fields(override_dependencies, test_db):
    # Create a test data source directly
    test_data_source = DataSource(
        name="Field Test Data Source",
        type="csv",
        user_id=TEST_USER.id,
        size_bytes=1000,
        row_count=10,
    )
    test_db.add(test_data_source)
    await test_db.commit()
    await test_db.refresh(test_data_source)

    # Get the ID of the created data source
    data_source_id = test_data_source.id

    # Manually add some test fields to the data source
    field1 = Field(
        name="test_field_1",
        data_type="string",
        data_source_id=data_source_id,
        index=0,
    )
    field2 = Field(
        name="test_field_2",
        data_type="integer",
        data_source_id=data_source_id,
        index=1,
    )

    test_db.add(field1)
    test_db.add(field2)
    await test_db.commit()

    # Get fields for the data source
    response = client.get(
        f"/api/v1/sources/{data_source_id}/fields",
        headers={"Authorization": "Bearer test_token"},
    )

    # Check response
    assert response.status_code == 200
    assert len(response.json()) == 2

    # Check that the field names are correct
    field_names = [field["name"] for field in response.json()]
    assert "test_field_1" in field_names
    assert "test_field_2" in field_names

    # Check that the field types are correct
    for field in response.json():
        if field["name"] == "test_field_1":
            assert field["data_type"] == "string"
        elif field["name"] == "test_field_2":
            assert field["data_type"] == "integer"


# Test for get_field endpoint
@pytest.mark.asyncio
async def test_get_field(override_dependencies, test_db):
    # Create a test data source directly
    test_data_source = DataSource(
        name="Field Detail Test Data Source",
        type="csv",
        user_id=TEST_USER.id,
        size_bytes=1000,
        row_count=10,
    )
    test_db.add(test_data_source)
    await test_db.commit()
    await test_db.refresh(test_data_source)

    data_source_id = test_data_source.id

    # Create a test field
    test_field = Field(
        name="test_numeric_field",
        data_type="float",
        data_source_id=data_source_id,
        index=0,
        min_value="0.5",
        max_value="100.5",
        avg_value=50.25,
        std_value=25.5,
        unique_values_count=20,
        missing_values_count=5,
    )

    test_db.add(test_field)
    await test_db.commit()
    await test_db.refresh(test_field)

    field_id = test_field.id

    # Get the field details
    response = client.get(
        f"/api/v1/sources/{data_source_id}/fields/{field_id}",
        headers={"Authorization": "Bearer test_token"},
    )

    # Check response
    assert response.status_code == 200
    field_data = response.json()

    # Verify field data
    assert field_data["name"] == "test_numeric_field"
    assert field_data["data_type"] == "float"
    assert field_data["id"] == field_id
    assert field_data["data_source_id"] == data_source_id
    assert field_data["index"] == 0
    assert field_data["min_value"] == "0.5"
    assert field_data["max_value"] == "100.5"
    assert field_data["avg_value"] == 50.25
    assert field_data["std_value"] == 25.5
    assert field_data["unique_values_count"] == 20
    assert field_data["missing_values_count"] == 5


# Test for preview_data_source endpoint
@pytest.mark.asyncio
async def test_preview_data_source(override_dependencies, test_db):
    # Create a test data source directly
    test_data_source = DataSource(
        name="Preview Test Data Source",
        type="csv",
        user_id=TEST_USER.id,
        size_bytes=1000,
        row_count=10,
    )
    test_db.add(test_data_source)
    await test_db.commit()
    await test_db.refresh(test_data_source)

    data_source_id = test_data_source.id

    # Add some fields to the data source
    fields = [
        Field(
            name="id",
            data_type="integer",
            data_source_id=data_source_id,
            index=0,
        ),
        Field(
            name="name",
            data_type="string",
            data_source_id=data_source_id,
            index=1,
        ),
        Field(
            name="value",
            data_type="float",
            data_source_id=data_source_id,
            index=2,
        ),
    ]

    for field in fields:
        test_db.add(field)

    await test_db.commit()

    # Get preview data
    response = client.get(
        f"/api/v1/sources/{data_source_id}/preview?limit=5",
        headers={"Authorization": "Bearer test_token"},
    )

    # Check response
    assert response.status_code == 200
    preview_data = response.json()

    # Check structure of preview data
    assert "field_names" in preview_data
    assert "rows" in preview_data

    # Check field names
    assert len(preview_data["field_names"]) == 3
    assert "id" in preview_data["field_names"]
    assert "name" in preview_data["field_names"]
    assert "value" in preview_data["field_names"]

    # Check rows
    assert len(preview_data["rows"]) <= 5  # Should respect the limit parameter

    # Check that each row has values for all fields
    for row in preview_data["rows"]:
        assert "id" in row
        assert "name" in row
        assert "value" in row


# Test for export_data_source endpoint
@pytest.mark.asyncio
async def test_export_data_source(override_dependencies, test_db):
    # Create a test data source directly
    test_data_source = DataSource(
        name="Export Test Data Source",
        type="csv",
        user_id=TEST_USER.id,
        size_bytes=1000,
        row_count=10,
    )
    test_db.add(test_data_source)
    await test_db.commit()
    await test_db.refresh(test_data_source)

    data_source_id = test_data_source.id

    # Request export
    response = client.get(
        f"/api/v1/sources/{data_source_id}/export?format=csv",
        headers={"Authorization": "Bearer test_token"},
    )

    # Check response
    assert response.status_code == 200
    export_data = response.json()

    # Check that the task was started
    assert "task_id" in export_data
    assert "task_name" in export_data
    assert "status_message" in export_data
    assert "status_location" in export_data

    # Check the task details
    assert export_data["task_name"] == "export_data"
    assert export_data["status_message"] == "Export task started"
    assert export_data["status_location"].startswith("/api/v1/tasks/")

    # Verify invalid format is rejected
    response = client.get(
        f"/api/v1/sources/{data_source_id}/export?format=invalid",
        headers={"Authorization": "Bearer test_token"},
    )

    # Should return a 400 Bad Request
    assert response.status_code == 400
    assert "not supported" in response.json()["detail"]


# Test renaming a data source
@pytest.mark.asyncio
async def test_rename_data_source(override_dependencies, test_db):
    # Create a test data source directly
    data_source = DataSource(
        name="Test Data Source",
        type="csv",
        user_id=TEST_USER.id,
        size_bytes=1000,
        row_count=10,
    )

    test_db.add(data_source)
    await test_db.commit()
    await test_db.refresh(data_source)

    # New name
    new_name = "Renamed Data Source"

    # Make request to rename data source
    response = client.put(
        f"/api/v1/sources/{data_source.id}/name",
        json=new_name,
        headers={"Authorization": "Bearer test_token"},
    )

    # Check response
    assert response.status_code == 200

    # Verify the response contains the renamed data source
    data_source_response = response.json()
    assert data_source_response["name"] == "Renamed Data Source"

    # Verify the data source was renamed in the database
    result = await test_db.execute(
        text(f"SELECT * FROM data_source WHERE id = {data_source.id}")
    )
    db_data_source = result.fetchone()
    assert db_data_source is not None
    assert db_data_source.name == "Renamed Data Source"


# Test deleting a data source
@pytest.mark.asyncio
async def test_delete_data_source(override_dependencies, test_db):
    # Create a test data source directly
    data_source = DataSource(
        name="Test Data Source",
        type="csv",
        user_id=TEST_USER.id,
        size_bytes=1000,
        row_count=10,
    )

    test_db.add(data_source)
    await test_db.commit()
    await test_db.refresh(data_source)

    # Make request to delete data source
    response = client.delete(
        f"/api/v1/sources/{data_source.id}",
        headers={"Authorization": "Bearer test_token"},
    )

    # Check response
    assert response.status_code == 204

    # Verify the data source was deleted from the database
    result = await test_db.execute(
        text(f"SELECT * FROM data_source WHERE id = {data_source.id}")
    )
    db_data_source = result.fetchone()
    assert db_data_source is None


# Test for get_instances endpoint
@pytest.mark.asyncio
async def test_get_instances(override_dependencies, test_db):
    """Test the get_instances endpoint following the same pattern as other CRUD tests."""
    # Create a test data source
    data_source = DataSource(
        name="Instances Test Source",
        type="csv",
        user_id=TEST_USER.id,
        size_bytes=1000,
        row_count=10,
    )
    test_db.add(data_source)
    await test_db.commit()
    await test_db.refresh(data_source)

    # Store the ID to avoid SQLAlchemy loading the object again
    data_source_id = data_source.id

    # Create an upload for the data source
    upload = Upload(
        uuid="test-instances-uuid",
        name="test_upload.csv",
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
    test_db.add(upload)
    await test_db.commit()
    await test_db.refresh(upload)  # Refresh the upload to ensure we have its ID

    # Store the upload ID
    upload_id = upload.id

    # Link the upload to the data source
    data_source.upload_id = upload_id
    await test_db.commit()
    await test_db.refresh(data_source)

    # Create fields for the data source
    fields = [
        Field(
            name="field1",
            data_source_id=data_source_id,
            data_type="string",
            index=0,
        ),
        Field(
            name="field2",
            data_source_id=data_source_id,
            data_type="numeric",
            index=1,
        ),
    ]
    for field in fields:
        test_db.add(field)
    await test_db.commit()

    # Create some sample data
    # In a real test, you would need to create actual file data
    # For now, we'll just test that the endpoint responds correctly

    # Test the get_instances endpoint
    response = client.get(
        f"/api/v1/sources/{data_source_id}/instances",
        headers={"Authorization": "Bearer test_token"},
    )

    # Check the response
    assert response.status_code == 200
    # Additional assertions can be added to check the structure of the response
    # For now, we'll just check that we get a response code of 200
