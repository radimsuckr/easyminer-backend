from pathlib import Path
from unittest.mock import patch
from uuid import UUID

import pytest
from sqlalchemy import select, text

from easyminer.crud.task import create_task
from easyminer.models import DataSource, Field, Upload


# Test listing data sources
@pytest.mark.skip(reason="Integration test - to be rewritten")
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
@pytest.mark.skip(reason="Integration test - to be rewritten")
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


# Test getting a data source
@pytest.mark.skip(reason="Integration test - to be rewritten")
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


# Test listing fields for a data source
@pytest.mark.skip(reason="Integration test - to be rewritten")
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

    data_source_id = test_data_source.id

    # Create some test fields
    field1 = Field(
        name="field1",
        data_type="string",
        data_source_id=data_source_id,
        index=0,
    )
    field2 = Field(
        name="field2",
        data_type="integer",
        data_source_id=data_source_id,
        index=1,
    )

    test_db.add_all([field1, field2])
    await test_db.commit()

    # Get fields for the data source
    response = client.get(
        f"/api/v1/datasource/{data_source_id}/field",
        headers={"Authorization": "Bearer test_token"},
    )

    # Check response
    assert response.status_code == 200

    # Verify the response contains the fields
    fields = response.json()
    assert len(fields) == 2

    # Check field properties
    assert fields[0]["name"] == "field1"
    assert fields[0]["dataType"] == "string"
    assert fields[0]["dataSourceId"] == data_source_id

    assert fields[1]["name"] == "field2"
    assert fields[1]["dataType"] == "integer"
    assert fields[1]["dataSourceId"] == data_source_id


# Test getting field details
@pytest.mark.skip(reason="Integration test - to be rewritten")
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
        name="test_field",
        data_type="integer",
        data_source_id=data_source_id,
        index=0,
        min_value=1,
        max_value=100,
        avg_value=50.5,
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

    # Verify the response contains the field details
    field = response.json()
    assert field["name"] == "test_field"
    assert field["dataType"] == "integer"
    assert field["dataSourceId"] == data_source_id
    assert field["minValue"] == 1
    assert field["maxValue"] == 100
    assert field["avgValue"] == 50.5


# Test preview data source
@pytest.mark.skip(reason="Integration test - to be rewritten")
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

    # Create some test fields
    fields = [
        Field(
            name="string_field",
            data_type="string",
            data_source_id=data_source_id,
            index=0,
        ),
        Field(
            name="integer_field",
            data_type="integer",
            data_source_id=data_source_id,
            index=1,
        ),
        Field(
            name="float_field",
            data_type="float",
            data_source_id=data_source_id,
            index=2,
        ),
    ]

    test_db.add_all(fields)
    await test_db.commit()

    # Refresh fields to get their IDs
    for field in fields:
        await test_db.refresh(field)

    # Get preview data
    response = client.get(
        f"/api/v1/datasource/{data_source_id}/preview?limit=5",
        headers={"Authorization": "Bearer test_token"},
    )

    # Check response
    assert response.status_code == 200

    # Verify the response structure
    preview_data = response.json()
    assert "field_names" in preview_data
    assert "rows" in preview_data

    # Check field names
    field_names = preview_data["field_names"]
    assert len(field_names) == 3
    assert "string_field" in field_names
    assert "integer_field" in field_names
    assert "float_field" in field_names

    # Check rows
    rows = preview_data["rows"]
    assert len(rows) <= 5  # Should not exceed the requested limit


# Test rename data source
@pytest.mark.skip(reason="Integration test - to be rewritten")
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

    new_name = "Updated Data Source Name"

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

    # Verify the data source was renamed in the database
    await test_db.refresh(data_source)
    assert data_source.name == new_name


# Test delete data source
@pytest.mark.skip(reason="Integration test - to be rewritten")
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

    # Verify the data source was deleted from the database
    result = await test_db.execute(
        select(DataSource).where(DataSource.id == data_source.id)
    )
    deleted_data_source = result.scalars().first()
    assert deleted_data_source is None


@pytest.mark.skip(reason="Integration test - to be rewritten")
@pytest.mark.asyncio
async def test_get_instances(override_dependencies, test_db):
    """Test retrieving instances from a data source."""
    # Create a test data source with a mock upload
    test_data_source = DataSource(
        name="Instances Test Source",
        type="csv",
        user_id=TEST_USER.id,
        size_bytes=1000,
        row_count=10,
    )

    # Create a test upload and associate it with the data source
    test_upload = Upload(
        uuid="test-instances-uuid",
        name="Test Instances Upload",
        media_type="csv",
        db_type="limited",
        separator=",",
        encoding="utf-8",
        quotes_char='"',
        escape_char="\\",
        locale="en_US",
        compression="",
        format="csv",
    )

    test_db.add(test_upload)
    await test_db.commit()
    await test_db.refresh(test_upload)

    # Set the upload_id on the data source
    test_data_source.upload_id = test_upload.id
    test_db.add(test_data_source)
    await test_db.commit()
    await test_db.refresh(test_data_source)

    data_source_id = test_data_source.id

    # Create test fields
    field1 = Field(
        name="string_field",
        data_type="string",
        data_source_id=data_source_id,
        index=0,
    )
    field2 = Field(
        name="integer_field",
        data_type="integer",
        data_source_id=data_source_id,
        index=1,
    )

    test_db.add_all([field1, field2])
    await test_db.commit()
    await test_db.refresh(field1)
    await test_db.refresh(field2)

    # Create mock chunk files for the upload
    with (
        patch("pathlib.Path.glob") as mock_glob,
        patch("pathlib.Path.read_bytes") as mock_read_bytes,
        patch("pathlib.Path.exists") as mock_exists,
    ):
        # Mock the glob to return a list of files
        mock_glob.return_value = [Path("chunk1.csv")]
        mock_exists.return_value = True

        # Mock CSV content
        mock_csv_content = b'string_value,42\n"another string",84\n'
        mock_read_bytes.return_value = mock_csv_content

        # Test the get_instances endpoint
        response = client.get(
            f"/api/v1/datasource/{data_source_id}/instances",
            headers={"Authorization": "Bearer test_token"},
        )

        # Check response
        assert response.status_code == 200

        # Verify response includes instances with the expected data
        instances = response.json()
        assert len(instances) > 0


@pytest.mark.skip(reason="Integration test - to be rewritten")
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

    # Create a numeric field with statistics
    numeric_field = Field(
        name="numeric_field",
        data_type="numeric",
        data_source_id=data_source_id,
        index=0,
        min_value=10,
        max_value=100,
        avg_value=55.5,
    )

    test_db.add(numeric_field)
    await test_db.commit()
    await test_db.refresh(numeric_field)

    field_id = numeric_field.id

    # Get the field statistics
    response = client.get(
        f"/api/v1/datasource/{data_source_id}/field/{field_id}/stats",
        headers={"Authorization": "Bearer test_token"},
    )

    # Check response
    assert response.status_code == 200

    # Verify the response contains the field statistics
    stats = response.json()
    assert stats["min"] == 10.0
    assert stats["max"] == 100.0
    assert stats["avg"] == 55.5

    # Create a non-numeric field
    non_numeric_field = Field(
        name="string_field",
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

    # Should return an error
    assert response.status_code == 400

    # Create a field without statistics
    field_without_stats = Field(
        name="field_without_stats",
        data_type="numeric",
        data_source_id=data_source_id,
        index=2,
    )

    test_db.add(field_without_stats)
    await test_db.commit()
    await test_db.refresh(field_without_stats)

    # Try to get statistics for a field without stats
    response = client.get(
        f"/api/v1/datasource/{data_source_id}/field/{field_without_stats.id}/stats",
        headers={"Authorization": "Bearer test_token"},
    )

    # Should return an error
    assert response.status_code == 404


@pytest.mark.skip(reason="Integration test - to be rewritten")
@pytest.mark.asyncio
async def test_get_task_status(override_dependencies, test_db):
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

    # Generate task UUIDs
    pending_task_id = UUID("11111111-1111-1111-1111-111111111111")
    in_progress_task_id = UUID("22222222-2222-2222-2222-222222222222")
    completed_task_id = UUID("33333333-3333-3333-3333-333333333333")
    failed_task_id = UUID("44444444-4444-4444-4444-444444444444")
    other_user_task_id = UUID("55555555-5555-5555-5555-555555555555")

    # Create a pending task
    await create_task(
        test_db,
        task_id=pending_task_id,
        name="test_pending_task",
        user_id=TEST_USER.id,
        data_source_id=data_source_id,
    )

    # Create an in-progress task
    await create_task(
        test_db,
        task_id=in_progress_task_id,
        name="test_in_progress_task",
        user_id=TEST_USER.id,
        data_source_id=data_source_id,
    )
    # Update to in_progress
    await test_db.execute(
        text(
            "UPDATE task SET status = 'in_progress', status_message = 'Processing...' "
            "WHERE task_id = :task_id"
        ),
        {"task_id": str(in_progress_task_id)},
    )

    # Create a completed task
    await create_task(
        test_db,
        task_id=completed_task_id,
        name="test_completed_task",
        user_id=TEST_USER.id,
        data_source_id=data_source_id,
    )
    # Update to completed
    await test_db.execute(
        text(
            "UPDATE task SET status = 'completed', "
            "status_message = 'Task completed successfully', "
            "result_location = '/results/test' "
            "WHERE task_id = :task_id"
        ),
        {"task_id": str(completed_task_id)},
    )

    # Create a failed task
    await create_task(
        test_db,
        task_id=failed_task_id,
        name="test_failed_task",
        user_id=TEST_USER.id,
        data_source_id=data_source_id,
    )
    # Update to failed
    await test_db.execute(
        text(
            "UPDATE task SET status = 'failed', "
            "status_message = 'Task failed', "
            "error_message = 'An error occurred' "
            "WHERE task_id = :task_id"
        ),
        {"task_id": str(failed_task_id)},
    )

    # Create a task that belongs to another user
    await create_task(
        test_db,
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
    pending_status = response.json()
    assert pending_status["task_id"] == str(pending_task_id)
    assert pending_status["status"] == "pending"

    # Test 2: Get status of an in-progress task
    response = client.get(
        f"/api/v1/task-status/{in_progress_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 200
    in_progress_status = response.json()
    assert in_progress_status["status"] == "in_progress"
    assert in_progress_status["status_message"] == "Processing..."

    # Test 3: Get status of a completed task
    response = client.get(
        f"/api/v1/task-status/{completed_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 200
    completed_status = response.json()
    assert completed_status["status"] == "completed"
    assert completed_status["result_location"] == "/results/test"

    # Test 4: Get status of a failed task
    response = client.get(
        f"/api/v1/task-status/{failed_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 200
    failed_status = response.json()
    assert failed_status["status"] == "failed"
    assert failed_status["error_message"] == "An error occurred"

    # Test 5: Get status of a non-existent task
    non_existent_task_id = UUID("99999999-9999-9999-9999-999999999999")
    response = client.get(
        f"/api/v1/task-status/{non_existent_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 404

    # Test 6: Get status of a task belonging to another user
    # This should return 404 for security reasons (don't reveal task existence)
    response = client.get(
        f"/api/v1/task-status/{other_user_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 404


@pytest.mark.skip(reason="Integration test - to be rewritten")
@pytest.mark.asyncio
async def test_get_task_result(override_dependencies, test_db):
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

    # Generate task UUIDs
    pending_task_id = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    completed_task_id = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    failed_task_id = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
    other_user_task_id = UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")

    # Create a pending task (no result)
    await create_task(
        test_db,
        task_id=pending_task_id,
        name="pending_result_task",
        user_id=TEST_USER.id,
        data_source_id=data_source_id,
    )

    # Create a completed task with result
    await create_task(
        test_db,
        task_id=completed_task_id,
        name="completed_result_task",
        user_id=TEST_USER.id,
        data_source_id=data_source_id,
    )
    # Update to completed with result
    await test_db.execute(
        text(
            "UPDATE task SET status = 'completed', "
            "result_location = '/results/completed', "
            'result = \'{"data": [1, 2, 3], "success": true}\' '
            "WHERE task_id = :task_id"
        ),
        {"task_id": str(completed_task_id)},
    )

    # Create a failed task
    await create_task(
        test_db,
        task_id=failed_task_id,
        name="failed_result_task",
        user_id=TEST_USER.id,
        data_source_id=data_source_id,
    )
    # Update to failed
    await test_db.execute(
        text(
            "UPDATE task SET status = 'failed', "
            "error_message = 'Task failed during execution' "
            "WHERE task_id = :task_id"
        ),
        {"task_id": str(failed_task_id)},
    )

    # Create a task that belongs to another user
    await create_task(
        test_db,
        task_id=other_user_task_id,
        name="other_user_result_task",
        user_id=TEST_USER.id + 1,  # Different user ID
        data_source_id=data_source_id,
    )
    # Update task to completed with result using SQL update
    await test_db.execute(
        text(
            "UPDATE task SET status = 'completed', "
            "result_location = '/results/other_user', "
            'result = \'{"private": true, "data": "sensitive"}\' '
            "WHERE task_id = :task_id"
        ),
        {"task_id": str(other_user_task_id)},
    )

    await test_db.commit()

    # Test 1: Get result of a completed task
    response = client.get(
        f"/api/v1/task-result/{completed_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 200
    result = response.json()
    assert "message" in result

    # Test 2: Get result of a pending task (not ready)
    response = client.get(
        f"/api/v1/task-result/{pending_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 400  # Task not completed yet

    # Test 3: Get result of a failed task
    response = client.get(
        f"/api/v1/task-result/{failed_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 400  # Task failed

    # Test 4: Get result of a non-existent task
    non_existent_task_id = UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")
    response = client.get(
        f"/api/v1/task-result/{non_existent_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 404

    # Test 5: Get result of a task belonging to another user
    response = client.get(
        f"/api/v1/task-result/{other_user_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 404  # Shouldn't reveal existence
