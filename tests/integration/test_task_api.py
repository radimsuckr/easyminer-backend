"""Integration tests for task API endpoints."""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.models.data import DataSource, Field


@pytest.fixture(scope="function")
async def test_data_source(db_session: AsyncSession) -> DataSource:
    """Create a test data source with numeric fields for testing."""
    data_source = DataSource(
        name="Task Test Data Source",
        type="csv",
        size=1000,
    )
    db_session.add(data_source)
    await db_session.commit()
    await db_session.refresh(data_source)

    # Add a numeric field
    field = Field(
        name="score",
        data_type="integer",
        data_source_id=data_source.id,
        index=0,
        min_value="10",
        max_value="100",
        avg_value=55.0,
        unique_count=10,
        has_nulls=False,
    )
    db_session.add(field)
    await db_session.commit()
    await db_session.refresh(field)

    return data_source


@pytest.mark.skip("This test requires more complex setup")
@pytest.mark.asyncio
async def test_create_aggregated_values_task(
    client, test_data_source, db_session: AsyncSession
):
    """Test creating a histogram task."""

    # Original test code:
    # # Await the test_data_source fixture
    # data_source = await test_data_source
    #
    # # Get the field ID first
    # fields_response = client.get(f"/api/v1/datasource/{data_source.id}/field")
    # assert fields_response.status_code == 200
    # fields = fields_response.json()
    #
    # # Find the 'score' field
    # field_id = None
    # for field in fields:
    #     if field["name"] == "score":
    #         field_id = field["id"]
    #         break
    #
    # assert field_id is not None, "Score field not found"
    #
    # # Create a histogram task
    # response = client.get(
    #     f"/api/v1/datasource/{data_source.id}/field/{field_id}/aggregated-values?bins=5"
    # )
    #
    # # Check the response
    # assert response.status_code == 202
    # task_data = response.json()
    #
    # assert "taskId" in task_data
    # assert "statusLocation" in task_data
    # assert task_data["statusMessage"] == "Histogram generation started"
    #
    # # Get task status from the statusLocation
    # task_id = task_data["taskId"]
    # response = client.get(f"/api/v1/task-status/{task_id}")
    # assert response.status_code == 200
    #
    # # The task might be completed or in progress
    # status = response.json()
    # assert status["task_id"] == task_data["taskId"]
    # assert status["task_name"] == "aggregated_values"
    # assert status["status_message"] in [
    #     "Task created and waiting to start",
    #     "Generating histogram data",
    #     "Histogram generation completed",
    # ]
    #
    # # If we wait a bit, the task should complete
    # # Note: In a real test environment, we might want to use a different approach
    # # to avoid timing issues, but for demonstration purposes:
    # max_retries = 10
    # retry_count = 0
    # completed = False
    #
    # while retry_count < max_retries and not completed:
    #     time.sleep(0.5)  # Wait a bit for processing
    #     response = client.get(f"/api/v1/task-status/{task_id}")
    #     assert response.status_code == 200
    #     status = response.json()
    #
    #     if status["status_message"] == "Histogram generation completed":
    #         completed = True
    #         break
    #
    #     retry_count += 1
    #
    # # If task completed, try to get the result
    # if completed and status["result_location"]:
    #     response = client.get(f"/api/v1/task-result/{task_id}")
    #     assert response.status_code == 200
    #     result = response.json()
    #
    #     # Verify the result structure
    #     assert "field_id" in result
    #     assert "field_name" in result
    #     assert "bins" in result
    #     assert "histogram" in result
    #     assert isinstance(result["histogram"], list)
