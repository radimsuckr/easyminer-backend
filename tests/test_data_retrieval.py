import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.models.data import DataSource, Field
from easyminer.processing.data_retrieval import (
    DataRetrieval,
    generate_histogram_for_field,
    get_data_preview,
    read_task_result,
)
from easyminer.storage import DiskStorage


@pytest.fixture
def mock_storage():
    """Create a mock storage for testing."""
    storage = MagicMock(spec=DiskStorage)
    storage.read.return_value = (
        b"header1,header2,header3\nvalue1,value2,10\nvalue4,value5,20"
    )
    storage.exists.return_value = True
    storage.list_files.return_value = [Path("user/1/chunks/file1.chunk")]
    return storage


@pytest.fixture
def csv_test_data():
    """Create a test CSV data string."""
    return "name,age,score\nAlice,30,85\nBob,25,92\nCharlie,35,78"


@pytest.fixture
def mock_data_source():
    """Create a mock DataSource for testing."""
    data_source = MagicMock(spec=DataSource)
    data_source.id = 1
    data_source.user_id = 1
    data_source.upload = MagicMock()
    data_source.upload.encoding = "utf-8"
    data_source.upload.separator = ","
    data_source.upload.quotes_char = '"'
    return data_source


@pytest.fixture
def mock_field():
    """Create a mock Field for testing."""
    field = MagicMock(spec=Field)
    field.id = 1
    field.name = "score"
    field.data_type = "integer"
    field.data_source_id = 1
    field.min_value = "10"
    field.max_value = "100"
    field.avg_value = 50.0
    return field


@pytest.mark.asyncio
async def test_data_retrieval_get_preview_data(mock_storage, csv_test_data):
    """Test retrieving preview data."""
    # Setup
    mock_storage.read.return_value = csv_test_data.encode("utf-8")
    retrieval = DataRetrieval(mock_storage, 1, 1)

    # Execute
    header, rows = await retrieval.get_preview_data(limit=2)

    # Assert
    assert header == ["name", "age", "score"]
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice"
    assert rows[0]["age"] == "30"
    assert rows[0]["score"] == "85"
    assert rows[1]["name"] == "Bob"
    assert rows[1]["age"] == "25"
    assert rows[1]["score"] == "92"
    mock_storage.list_files.assert_called_once_with(Path("1/1/chunks"), "*.chunk")


@pytest.mark.asyncio
async def test_data_retrieval_get_preview_data_empty_chunks(mock_storage):
    """Test retrieving preview data with empty chunks."""
    # Setup
    mock_storage.list_files.return_value = []

    retrieval = DataRetrieval(mock_storage, 1, 1)

    # Execute & Assert
    with pytest.raises(FileNotFoundError):
        await retrieval.get_preview_data()


@pytest.mark.asyncio
async def test_generate_histogram(mock_storage, mock_field):
    """Test generating a histogram."""
    # Setup
    test_csv = "name,age,score\nAlice,30,60\nBob,25,70\nCharlie,35,80\nDave,40,90"
    mock_storage.read.return_value = test_csv.encode("utf-8")
    mock_storage.save.return_value = 100  # bytes written

    retrieval = DataRetrieval(mock_storage, 1, 1)

    # Execute
    histogram_data, result_path = await retrieval.generate_histogram(
        field=mock_field, bins=2
    )

    # Assert
    assert len(histogram_data) == 2  # Two bins
    assert "interval_start" in histogram_data[0]
    assert "interval_end" in histogram_data[0]
    assert "count" in histogram_data[0]
    # Check that we saved the result
    mock_storage.save.assert_called_once()
    # The path should contain the field ID and bins
    assert "histogram_1_2.json" in result_path


@pytest.mark.asyncio
async def test_generate_histogram_invalid_field_type(mock_storage, mock_field):
    """Test generating a histogram with a non-numeric field."""
    # Setup
    mock_field.data_type = "string"
    retrieval = DataRetrieval(mock_storage, 1, 1)

    # Execute & Assert
    with pytest.raises(ValueError, match="Field score is not numeric"):
        await retrieval.generate_histogram(field=mock_field)


@pytest.mark.asyncio
async def test_read_file_result(mock_storage):
    """Test reading a result file."""
    # Setup
    mock_storage.read.return_value = json.dumps(
        {
            "field_id": 1,
            "histogram": [{"interval_start": 0, "interval_end": 10, "count": 5}],
        }
    ).encode("utf-8")
    retrieval = DataRetrieval(mock_storage, 1, 1)

    # Execute
    result = await retrieval.read_file_result("results/histogram.json")

    # Assert
    assert result["field_id"] == 1
    assert "histogram" in result
    assert len(result["histogram"]) == 1
    assert result["histogram"][0]["count"] == 5


@pytest.mark.asyncio
async def test_read_file_result_not_found(mock_storage):
    """Test reading a non-existent result file."""
    # Setup
    mock_storage.read.side_effect = FileNotFoundError("File not found")
    retrieval = DataRetrieval(mock_storage, 1, 1)

    # Execute & Assert
    with pytest.raises(FileNotFoundError):
        await retrieval.read_file_result("results/nonexistent.json")


@pytest.mark.asyncio
async def test_read_file_result_invalid_json(mock_storage):
    """Test reading an invalid JSON result file."""
    # Setup
    mock_storage.read.return_value = b"invalid json"
    retrieval = DataRetrieval(mock_storage, 1, 1)

    # Execute & Assert
    with pytest.raises(ValueError, match="Invalid JSON"):
        await retrieval.read_file_result("results/invalid.json")


@pytest.mark.asyncio
async def test_get_data_preview():
    """Test the get_data_preview helper function."""
    # Setup mocks
    mock_db = AsyncMock(spec=AsyncSession)
    mock_data_source = MagicMock(spec=DataSource)
    mock_data_source.id = 1
    mock_data_source.upload = None  # No upload, use defaults

    # Mock DataRetrieval.get_preview_data method
    header = ["name", "age", "score"]
    rows = [
        {"name": "Alice", "age": "30", "score": "85"},
        {"name": "Bob", "age": "25", "score": "92"},
    ]

    with patch(
        "easyminer.processing.data_retrieval.DataRetrieval"
    ) as MockDataRetrieval:
        # Configure the mock
        mock_retrieval_instance = MockDataRetrieval.return_value
        mock_retrieval_instance.get_preview_data = AsyncMock(
            return_value=(header, rows)
        )

        # Execute
        result_header, result_rows = await get_data_preview(
            db=mock_db, data_source=mock_data_source, user_id=1, limit=10
        )

        # Assert
        assert result_header == header
        assert result_rows == rows
        MockDataRetrieval.assert_called_once()
        mock_retrieval_instance.get_preview_data.assert_called_once_with(limit=10)


@pytest.mark.asyncio
async def test_generate_histogram_for_field():
    """Test the generate_histogram_for_field helper function."""
    # Setup mocks
    mock_db = AsyncMock(spec=AsyncSession)
    mock_data_source = MagicMock(spec=DataSource)
    mock_data_source.id = 1
    mock_data_source.upload = None  # No upload, use defaults

    mock_field = MagicMock(spec=Field)
    mock_field.id = 1
    mock_field.name = "score"
    mock_field.data_type = "integer"

    # Mock DataRetrieval.generate_histogram method
    histogram_data = [
        {"interval_start": 0, "interval_end": 50, "count": 3},
        {"interval_start": 50, "interval_end": 100, "count": 2},
    ]
    result_path = "1/1/results/histogram_1_2.json"

    with patch(
        "easyminer.processing.data_retrieval.DataRetrieval"
    ) as MockDataRetrieval:
        # Configure the mock
        mock_retrieval_instance = MockDataRetrieval.return_value
        mock_retrieval_instance.generate_histogram = AsyncMock(
            return_value=(histogram_data, result_path)
        )

        # Execute
        result_histogram, result_path_output = await generate_histogram_for_field(
            db=mock_db,
            field=mock_field,
            data_source=mock_data_source,
            user_id=1,
            bins=2,
        )

        # Assert
        assert result_histogram == histogram_data
        assert result_path_output == result_path
        MockDataRetrieval.assert_called_once()
        mock_retrieval_instance.generate_histogram.assert_called_once_with(
            field=mock_field,
            bins=2,
            min_value=None,
            max_value=None,
            min_inclusive=True,
            max_inclusive=True,
        )


@pytest.mark.asyncio
async def test_read_task_result():
    """Test the read_task_result helper function."""
    # Setup
    result_data = {
        "field_id": 1,
        "histogram": [{"interval_start": 0, "interval_end": 10, "count": 5}],
    }
    result_path = "1/1/results/histogram_1_2.json"

    with patch(
        "easyminer.processing.data_retrieval.DataRetrieval"
    ) as MockDataRetrieval:
        # Configure the mock
        mock_retrieval_instance = MockDataRetrieval.return_value
        mock_retrieval_instance.read_file_result = AsyncMock(return_value=result_data)

        # Execute
        result = await read_task_result(user_id=1, result_path=result_path)

        # Assert
        assert result == result_data
        MockDataRetrieval.assert_called_once()
        mock_retrieval_instance.read_file_result.assert_called_once_with(result_path)
