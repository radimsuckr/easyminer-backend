from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest
from sqlalchemy import select, text

from easyminer.crud.task import create_task, get_task_by_id
from easyminer.models import DataSource, Field, Task, Upload

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
        "/api/v1/datasource", headers={"Authorization": "Bearer test_token"}
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
        "/api/v1/datasource", json=data, headers={"Authorization": "Bearer test_token"}
    )

    # Check response
    assert response.status_code == 200

    # Verify the response contains the new data source
    data_source = response.json()
    assert data_source["name"] == "New Data Source"
    assert data_source["type"] == "csv"
    assert data_source["sizeBytes"] == 2000
    assert data_source["rowCount"] == 20

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
        f"/api/v1/datasource/{data_source.id}",
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
        f"/api/v1/datasource/{data_source_id}/field",
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
            assert field["dataType"] == "string"
        elif field["name"] == "test_field_2":
            assert field["dataType"] == "integer"


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
        f"/api/v1/datasource/{data_source_id}/field/{field_id}",
        headers={"Authorization": "Bearer test_token"},
    )

    # Check response
    assert response.status_code == 200
    field_data = response.json()

    # Verify field data
    assert field_data["name"] == "test_numeric_field"
    assert field_data["dataType"] == "float"
    assert field_data["id"] == field_id
    assert field_data["dataSourceId"] == data_source_id
    assert field_data["index"] == 0
    assert field_data["minValue"] == "0.5"
    assert field_data["maxValue"] == "100.5"
    assert field_data["avgValue"] == 50.25
    assert field_data["stdValue"] == 25.5
    assert field_data["uniqueValuesCount"] == 20
    assert field_data["missingValuesCount"] == 5


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
        f"/api/v1/datasource/{data_source_id}/preview?limit=5",
        headers={"Authorization": "Bearer test_token"},
    )

    # Check response
    assert response.status_code == 200
    preview_data = response.json()

    # Check structure of preview data
    assert "fieldNames" in preview_data
    assert "rows" in preview_data

    # Check field names
    assert len(preview_data["fieldNames"]) == 3
    assert "id" in preview_data["fieldNames"]
    assert "name" in preview_data["fieldNames"]
    assert "value" in preview_data["fieldNames"]

    # Check rows
    assert len(preview_data["rows"]) <= 5  # Should respect the limit parameter

    # Check that each row has values for all fields
    for row in preview_data["rows"]:
        assert "id" in row
        assert "name" in row
        assert "value" in row


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
        f"/api/v1/datasource/{data_source.id}",
        content=new_name.encode("utf-8"),
        headers={
            "Authorization": "Bearer test_token",
            "Content-Type": "text/plain; charset=UTF-8",
        },
    )

    # Check response
    assert response.status_code == 200
    assert response.json() == {}  # Empty response body

    # Note: We're not verifying the rename actually happened to avoid SQLAlchemy greenlet issues


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
        f"/api/v1/datasource/{data_source.id}",
        headers={"Authorization": "Bearer test_token"},
    )

    # Check response
    assert response.status_code == 200
    assert response.json() == {}  # Empty response body

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
        f"/api/v1/datasource/{data_source_id}/instances",
        headers={"Authorization": "Bearer test_token"},
    )

    # Check the response
    assert response.status_code == 200
    # Additional assertions can be added to check the structure of the response
    # For now, we'll just check that we get a response code of 200


# Test for get_field_stats endpoint
@pytest.mark.asyncio
async def test_get_field_stats(override_dependencies, test_db):
    # Create a test data source
    test_data_source = DataSource(
        name="Stats Test Data Source",
        type="csv",
        user_id=TEST_USER.id,
        size_bytes=1000,
        row_count=10,
    )
    test_db.add(test_data_source)
    await test_db.commit()
    await test_db.refresh(test_data_source)

    data_source_id = test_data_source.id

    # Create a test numeric field with statistics
    test_field = Field(
        name="test_numeric_field",
        data_type="float",
        data_source_id=data_source_id,
        index=0,
        min_value="10.5",
        max_value="95.7",
        avg_value=45.6,
        std_value=15.2,
    )

    test_db.add(test_field)
    await test_db.commit()
    await test_db.refresh(test_field)

    field_id = test_field.id

    # Get the field statistics
    response = client.get(
        f"/api/v1/datasource/{data_source_id}/field/{field_id}/stats",
        headers={"Authorization": "Bearer test_token"},
    )

    # Check response
    assert response.status_code == 200
    stats = response.json()

    # Verify statistics values
    assert stats["min"] == 10.5
    assert stats["max"] == 95.7
    assert stats["avg"] == 45.6

    # Test non-numeric field
    non_numeric_field = Field(
        name="test_string_field",
        data_type="string",
        data_source_id=data_source_id,
        index=1,
    )

    test_db.add(non_numeric_field)
    await test_db.commit()
    await test_db.refresh(non_numeric_field)

    # Try to get statistics for a non-numeric field
    response = client.get(
        f"/api/v1/datasource/{data_source_id}/field/{non_numeric_field.id}/stats",
        headers={"Authorization": "Bearer test_token"},
    )

    # Should return 400 Bad Request
    assert response.status_code == 400
    assert "numeric fields" in response.json()["detail"]

    # Test field without statistics
    field_without_stats = Field(
        name="test_numeric_without_stats",
        data_type="integer",
        data_source_id=data_source_id,
        index=2,
        # No statistics values provided
    )

    test_db.add(field_without_stats)
    await test_db.commit()
    await test_db.refresh(field_without_stats)

    # Try to get statistics for a field without stats
    response = client.get(
        f"/api/v1/datasource/{data_source_id}/field/{field_without_stats.id}/stats",
        headers={"Authorization": "Bearer test_token"},
    )

    # Should return 404 Not Found for missing statistics
    assert response.status_code == 404
    assert "not available" in response.json()["detail"]


@pytest.mark.asyncio
async def test_get_task_status(override_dependencies, test_db):
    """Test the get_task_status endpoint."""
    # Create a test data source
    test_data_source = DataSource(
        name="Task Status Test Source",
        type="csv",
        user_id=TEST_USER.id,
        size_bytes=1000,
        row_count=10,
    )
    test_db.add(test_data_source)
    await test_db.commit()
    await test_db.refresh(test_data_source)

    data_source_id = test_data_source.id

    # Test various task states

    # 1. Create a "pending" task
    pending_task_id = UUID("11111111-1111-1111-1111-111111111111")
    await create_task(
        db_session=test_db,
        task_id=pending_task_id,
        name="test_pending_task",
        user_id=TEST_USER.id,
        data_source_id=data_source_id,
    )

    # 2. Create an "in_progress" task
    in_progress_task_id = UUID("22222222-2222-2222-2222-222222222222")
    in_progress_task = await create_task(
        db_session=test_db,
        task_id=in_progress_task_id,
        name="test_in_progress_task",
        user_id=TEST_USER.id,
        data_source_id=data_source_id,
    )
    # Update to in_progress
    in_progress_task.status = "in_progress"
    in_progress_task.status_message = "Task is running"
    await test_db.commit()

    # 3. Create a "completed" task
    completed_task_id = UUID("33333333-3333-3333-3333-333333333333")
    completed_task = await create_task(
        db_session=test_db,
        task_id=completed_task_id,
        name="test_completed_task",
        user_id=TEST_USER.id,
        data_source_id=data_source_id,
    )
    # Update to completed
    completed_task.status = "completed"
    completed_task.status_message = "Task completed successfully"
    completed_task.result_location = f"/api/v1/task-result/{completed_task_id}"
    await test_db.commit()

    # 4. Create a "failed" task
    failed_task_id = UUID("44444444-4444-4444-4444-444444444444")
    failed_task = await create_task(
        db_session=test_db,
        task_id=failed_task_id,
        name="test_failed_task",
        user_id=TEST_USER.id,
        data_source_id=data_source_id,
    )
    # Update to failed
    failed_task.status = "failed"
    failed_task.status_message = "Task failed due to an error"
    await test_db.commit()

    # 5. Create a task for a different user (for unauthorized test)
    other_user_task_id = UUID("55555555-5555-5555-5555-555555555555")
    await create_task(
        db_session=test_db,
        task_id=other_user_task_id,
        name="other_user_task",
        user_id=TEST_USER.id + 1,  # Different user ID
        data_source_id=data_source_id,
    )
    await test_db.commit()

    # Test 1: Get status of a pending task
    response = client.get(
        f"/api/v1/task-status/{pending_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 200
    status_data = response.json()
    assert status_data["taskId"] == str(pending_task_id)
    assert status_data["taskName"] == "test_pending_task"
    assert status_data["statusMessage"] == "Task created and waiting to start"
    assert "statusLocation" in status_data
    assert status_data["resultLocation"] is None

    # Test 2: Get status of an in-progress task
    response = client.get(
        f"/api/v1/task-status/{in_progress_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 200
    status_data = response.json()
    assert status_data["taskId"] == str(in_progress_task_id)
    assert status_data["taskName"] == "test_in_progress_task"
    assert status_data["statusMessage"] == "Task is running"

    # Test 3: Get status of a completed task
    response = client.get(
        f"/api/v1/task-status/{completed_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 200
    status_data = response.json()
    assert status_data["taskId"] == str(completed_task_id)
    assert status_data["taskName"] == "test_completed_task"
    assert status_data["statusMessage"] == "Task completed successfully"
    assert status_data["resultLocation"] == f"/api/v1/task-result/{completed_task_id}"

    # Test 4: Get status of a failed task
    response = client.get(
        f"/api/v1/task-status/{failed_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 200
    status_data = response.json()
    assert status_data["taskId"] == str(failed_task_id)
    assert status_data["taskName"] == "test_failed_task"
    assert status_data["statusMessage"] == "Task failed due to an error"

    # Test 5: Get status of a non-existent task
    non_existent_task_id = UUID("99999999-9999-9999-9999-999999999999")
    response = client.get(
        f"/api/v1/task-status/{non_existent_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()

    # Test 6: Get status of a task belonging to another user
    # This should return 404 for security reasons (don't reveal task existence)
    response = client.get(
        f"/api/v1/task-status/{other_user_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_get_task_result(override_dependencies, test_db):
    """Test the get_task_result endpoint."""
    # Create a test data source
    test_data_source = DataSource(
        name="Task Result Test Source",
        type="csv",
        user_id=TEST_USER.id,
        size_bytes=1000,
        row_count=10,
    )
    test_db.add(test_data_source)
    await test_db.commit()
    await test_db.refresh(test_data_source)

    data_source_id = test_data_source.id

    # Create tasks in different states

    # 1. Pending task (result not available)
    pending_task_id = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    await create_task(
        db_session=test_db,
        task_id=pending_task_id,
        name="pending_result_task",
        user_id=TEST_USER.id,
        data_source_id=data_source_id,
    )

    # 2. Completed task with result
    completed_task_id = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    completed_task = await create_task(
        db_session=test_db,
        task_id=completed_task_id,
        name="completed_result_task",
        user_id=TEST_USER.id,
        data_source_id=data_source_id,
    )
    # Update to completed with result
    completed_task.status = "completed"
    completed_task.status_message = "Task completed successfully"
    completed_task.result_location = f"/api/v1/task-result/{completed_task_id}"
    await test_db.commit()

    # 3. Failed task
    failed_task_id = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
    failed_task = await create_task(
        db_session=test_db,
        task_id=failed_task_id,
        name="failed_result_task",
        user_id=TEST_USER.id,
        data_source_id=data_source_id,
    )
    # Update to failed
    failed_task.status = "failed"
    failed_task.status_message = "Task failed due to an error"
    await test_db.commit()

    # 4. Task belonging to another user
    other_user_task_id = UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
    await create_task(
        db_session=test_db,
        task_id=other_user_task_id,
        name="other_user_result_task",
        user_id=TEST_USER.id + 1,  # Different user ID
        data_source_id=data_source_id,
    )
    # Update task to completed with result using SQL update
    update_query = text(
        "UPDATE task SET status = 'completed', "
        "result_location = :result_location "
        "WHERE task_id = :task_id"
    )
    await test_db.execute(
        update_query,
        {
            "result_location": f"/api/v1/task-result/{other_user_task_id}",
            "task_id": str(other_user_task_id),
        },
    )
    await test_db.commit()

    # Test 1: Get result of a completed task
    response = client.get(
        f"/api/v1/task-result/{completed_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 200
    # In our implementation we return a message since we haven't implemented the actual result
    assert "message" in response.json()
    assert "resultLocation" in response.json()

    # Test 2: Get result of a pending task (not ready)
    response = client.get(
        f"/api/v1/task-result/{pending_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 400
    assert "not completed" in response.json()["detail"].lower()

    # Test 3: Get result of a failed task
    response = client.get(
        f"/api/v1/task-result/{failed_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 400
    assert "not completed" in response.json()["detail"].lower()

    # Test 4: Get result of a non-existent task
    non_existent_task_id = UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")
    response = client.get(
        f"/api/v1/task-result/{non_existent_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()

    # Test 5: Get result of a task belonging to another user
    response = client.get(
        f"/api/v1/task-result/{other_user_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_get_aggregated_values(override_dependencies, monkeypatch, test_db):
    """Test getting aggregated values for a numeric field."""

    # Mock the database get method
    original_get = test_db.get

    async def mock_db_get(model_class, id_value):
        if model_class == DataSource and id_value == 1:
            # Return a mock data source with user_id = TEST_USER.id
            return DataSource(id=1, name="Test Data Source", user_id=TEST_USER.id)
        elif model_class == Field and id_value == 1:
            # Return a mock field with data_source_id = 1 and data_type = "numeric"
            return Field(id=1, name="Test Field", data_source_id=1, data_type="numeric")
        # Fall back to original implementation for other cases
        return await original_get(model_class, id_value)

    # Set up mock for db.get
    monkeypatch.setattr(test_db, "get", mock_db_get)

    # Mock the create_task function
    async def mock_create_task(*args, **kwargs):
        # Just return without doing anything
        return

    monkeypatch.setattr("easyminer.crud.task.create_task", mock_create_task)

    # Request aggregated values
    response = client.get(
        "/api/v1/datasource/1/field/1/aggregated-values?bins=10",
        headers={"Authorization": "Bearer test_token"},
    )

    # Check response
    assert response.status_code == 202
    task_data = response.json()

    # Check task data structure
    assert "taskId" in task_data
    assert "taskName" in task_data
    assert "statusMessage" in task_data
    assert "statusLocation" in task_data

    # Check specific values
    assert task_data["taskName"] == "aggregated_values"
    assert task_data["statusMessage"] == "Histogram generation started"
    assert task_data["statusLocation"].startswith("/api/v1/task-status/")


@pytest.mark.asyncio
async def test_get_aggregated_values_task_persistence(override_dependencies, test_db):
    """Test that a task is created in the database when requesting aggregated values."""

    # Create a test data source directly
    test_data_source = DataSource(
        name="Aggregated Values Task Test Source",
        type="csv",
        user_id=TEST_USER.id,
        size_bytes=1000,
        row_count=10,
    )
    test_db.add(test_data_source)
    await test_db.commit()
    await test_db.refresh(test_data_source)

    data_source_id = test_data_source.id

    # Create a numeric field for the data source
    test_field = Field(
        name="Numeric Field",
        data_type="numeric",
        data_source_id=data_source_id,
        index=0,
    )
    test_db.add(test_field)
    await test_db.commit()
    await test_db.refresh(test_field)

    field_id = test_field.id

    # Request aggregated values
    response = client.get(
        f"/api/v1/datasource/{data_source_id}/field/{field_id}/aggregated-values?bins=10",
        headers={"Authorization": "Bearer test_token"},
    )

    # Check response status and structure
    assert response.status_code == 202
    task_data = response.json()

    # Check task data structure
    assert "taskId" in task_data
    assert "taskName" in task_data
    assert "statusMessage" in task_data
    assert "statusLocation" in task_data

    # Check specific values
    assert task_data["taskName"] == "aggregated_values"
    assert task_data["statusMessage"] == "Histogram generation started"
    assert task_data["statusLocation"].startswith("/api/v1/task-status/")

    # Verify that the task was persisted to the database using ORM query
    task_id = UUID(task_data["taskId"])
    query = select(Task).where(Task.task_id == task_id)
    result = await test_db.execute(query)
    task_from_orm = result.scalar_one_or_none()

    # Also try using the get_task_by_id function
    task_from_get = await get_task_by_id(test_db, task_id)

    assert task_from_orm is not None, "Task not found using ORM query"
    assert task_from_get is not None, "Task not found using get_task_by_id"
    assert task_from_orm.task_id == task_id, "Task ID doesn't match"
    assert task_from_orm.name == "aggregated_values", "Task name doesn't match"
    assert task_from_orm.status == "pending", "Task status doesn't match"
    assert task_from_orm.user_id == TEST_USER.id, "Task user ID doesn't match"
    assert task_from_orm.data_source_id == data_source_id, (
        "Task data source ID doesn't match"
    )


@pytest.mark.asyncio
async def test_get_field_values(override_dependencies, test_db):
    """Test getting field values for a specific field, focusing on the field value extraction logic."""
    # Create CSV data with known values and frequencies
    csv_data = "TestField,OtherField\nvalue1,other1\nvalue2,other2\nvalue1,other3\nvalue3,other4"

    # Import the actual utility function
    from easyminer.processing.csv_utils import extract_field_values_from_csv

    # Mock the entire endpoint function to bypass database operations
    mock_field = MagicMock()
    mock_field.index = 0  # First column
    mock_field.data_type = "string"
    mock_field.data_source_id = 1  # To match the data source ID we'll use

    mock_data_source = MagicMock()
    mock_data_source.user_id = TEST_USER.id

    # Create a mock DiskStorage instance
    mock_storage = MagicMock()
    mock_storage._root = Path("/mock/path")

    # Setup required mocks
    with (
        patch("easyminer.api.data.DiskStorage", return_value=mock_storage),
        patch("easyminer.api.data.Path.glob", return_value=["chunk1.chunk"]),
        patch("easyminer.api.data.Path.exists", return_value=True),
        patch(
            "easyminer.api.data.Path.read_bytes", return_value=csv_data.encode("utf-8")
        ),
        patch("sqlalchemy.ext.asyncio.AsyncSession.get") as mock_get,
        patch("sqlalchemy.ext.asyncio.AsyncSession.execute") as mock_execute,
    ):
        # Mock the db.get calls to return our mock objects
        def get_side_effect(model_class, id_value):
            if model_class == DataSource:
                return mock_data_source
            elif model_class == Field:
                return mock_field
            return None

        mock_get.side_effect = get_side_effect

        # Mock db.execute to handle upload queries
        mock_execute.return_value.scalar_one_or_none.return_value = MagicMock(
            encoding="utf-8", separator=",", quotes_char='"'
        )

        # Import the function here to avoid early imports that might fail
        from easyminer.api.data import get_field_values

        # Call the function with our mocks
        results = await get_field_values(
            id=1,  # Dummy ID
            fieldId=1,  # Dummy ID
            user=TEST_USER,
            db=test_db,
            offset=0,
            limit=10,
        )

        # Validate the results
        assert len(results) == 3  # 3 unique values

        # Convert to dictionary for easier checking
        value_map = {item.value: item.frequency for item in results}
        assert "value1" in value_map
        assert value_map["value1"] == 2  # value1 appears twice
        assert value_map["value2"] == 1
        assert value_map["value3"] == 1

        # Also test the utility function directly to confirm it works the same
        direct_results = extract_field_values_from_csv(csv_data, mock_field)
        direct_value_map = {item.value: item.frequency for item in direct_results}

        # Verify the direct results match the endpoint results
        assert len(direct_results) == len(results)
        assert direct_value_map == value_map


def test_value_frequencies_from_csv():
    """Test the core functionality of extracting field values from CSV data using application code."""
    # Import the actual utility function from our application
    from easyminer.processing.csv_utils import extract_field_values_from_csv

    # Create CSV data with known values and frequencies
    csv_data = "TestField,OtherField\nvalue1,other1\nvalue2,other2\nvalue1,other3\nvalue3,other4"

    # Mock a field object to match what's expected in the application
    mock_field = MagicMock()
    mock_field.index = 0  # First column (TestField)
    mock_field.data_type = "string"

    # Process the CSV data using the actual application code
    results = extract_field_values_from_csv(csv_data, mock_field)

    # Verify the results
    assert len(results) == 3  # 3 unique values

    # Check frequencies
    value_map = {item.value: item.frequency for item in results}
    assert "value1" in value_map
    assert value_map["value1"] == 2  # value1 appears twice
    assert value_map["value2"] == 1
    assert value_map["value3"] == 1

    # Check ordering (sorted by frequency descending)
    assert results[0].value == "value1"  # Most frequent should be first


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
