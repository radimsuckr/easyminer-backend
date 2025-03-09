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
        f"/api/v1/datasource/{data_source_id}/field/{field_id}",
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
        f"/api/v1/datasource/{data_source_id}/preview?limit=5",
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
        f"/api/v1/datasource/{data_source_id}/export?format=csv",
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
    assert export_data["status_location"].startswith("/api/v1/task-status/")

    # Verify invalid format is rejected
    response = client.get(
        f"/api/v1/datasource/{data_source_id}/export?format=invalid",
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
        f"/api/v1/datasource/{data_source.id}",
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
        f"/api/v1/datasource/{data_source.id}",
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
async def test_export_data_source_task_persistence(override_dependencies, test_db):
    # Create a test data source directly
    test_data_source = DataSource(
        name="Export Task Test Source",
        type="csv",
        user_id=TEST_USER.id,
        size_bytes=1000,
        row_count=10,
    )
    test_db.add(test_data_source)
    await test_db.commit()
    await test_db.refresh(test_data_source)

    data_source_id = test_data_source.id

    # Test direct task creation first
    test_task_id = UUID("efc941c5-b99f-4752-9cdd-6c5a6984765c")
    try:
        await create_task(
            db_session=test_db,
            task_id=test_task_id,
            name="test_task",
            user_id=TEST_USER.id,
            data_source_id=data_source_id,
        )

        # Verify direct task creation worked using ORM
        query = select(Task).where(Task.task_id == test_task_id)
        result = await test_db.execute(query)
        task_from_orm = result.scalar_one_or_none()

        # Also try using the get_task_by_id function
        task_from_get = await get_task_by_id(test_db, test_task_id)

        assert task_from_orm is not None, "Task not found using ORM query"
        assert task_from_get is not None, "Task not found using get_task_by_id"
        assert str(task_from_orm.task_id) == str(test_task_id), "Task ID doesn't match"
    except Exception as e:
        assert False, f"Direct task creation failed with error: {str(e)}"

    # Request export
    response = client.get(
        f"/api/v1/datasource/{data_source_id}/export?format=csv",
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
    assert export_data["status_location"].startswith("/api/v1/task-status/")

    # Verify that the task was persisted to the database
    task_id = export_data["task_id"]

    # Use ORM query instead of raw SQL
    query = select(Task).where(Task.task_id == UUID(task_id))
    result = await test_db.execute(query)
    task_from_orm = result.scalar_one_or_none()

    # Also try using the get_task_by_id function
    task_from_get = await get_task_by_id(test_db, UUID(task_id))

    # Check either method worked
    assert task_from_orm is not None or task_from_get is not None, (
        "Export task not found in database"
    )

    if task_from_orm:
        found_task = task_from_orm
    else:
        found_task = task_from_get

    assert found_task.name == "export_data"
    assert found_task.status == "pending"
    assert found_task.user_id == TEST_USER.id

    # Test the get_task_status endpoint
    status_response = client.get(
        f"/api/v1/task-status/{task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert status_response.status_code == 200
    task_status = status_response.json()
    assert task_status["task_id"] == task_id
    assert task_status["task_name"] == "export_data"


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
    assert status_data["task_id"] == str(pending_task_id)
    assert status_data["task_name"] == "test_pending_task"
    assert status_data["status_message"] == "Task created and waiting to start"
    assert "status_location" in status_data
    assert status_data["result_location"] is None

    # Test 2: Get status of an in-progress task
    response = client.get(
        f"/api/v1/task-status/{in_progress_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 200
    status_data = response.json()
    assert status_data["task_id"] == str(in_progress_task_id)
    assert status_data["task_name"] == "test_in_progress_task"
    assert status_data["status_message"] == "Task is running"

    # Test 3: Get status of a completed task
    response = client.get(
        f"/api/v1/task-status/{completed_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 200
    status_data = response.json()
    assert status_data["task_id"] == str(completed_task_id)
    assert status_data["task_name"] == "test_completed_task"
    assert status_data["status_message"] == "Task completed successfully"
    assert status_data["result_location"] == f"/api/v1/task-result/{completed_task_id}"

    # Test 4: Get status of a failed task
    response = client.get(
        f"/api/v1/task-status/{failed_task_id}",
        headers={"Authorization": "Bearer test_token"},
    )
    assert response.status_code == 200
    status_data = response.json()
    assert status_data["task_id"] == str(failed_task_id)
    assert status_data["task_name"] == "test_failed_task"
    assert status_data["status_message"] == "Task failed due to an error"

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
    assert "result_location" in response.json()

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
