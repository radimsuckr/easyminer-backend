import tempfile
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from easyminer.database import Base
from easyminer.models import DataSource, Upload
from easyminer.processing import CsvProcessor

# Test configuration
TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


# Setup and teardown for test database
@pytest_asyncio.fixture(scope="function")
async def test_db():
    # Create test engine and session
    engine = create_async_engine(TEST_DB_URL)
    TestingSessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=engine, class_=AsyncSession
    )

    # Create all tables in the test database
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Create a test session
    async with TestingSessionLocal() as session:
        yield session

    # Drop all tables after the test
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.mark.asyncio
async def test_csv_processor(test_db):
    """Test the CSV processor can parse CSV data and extract fields."""
    # Create test upload
    upload = Upload(
        id=123,  # Manual ID assignment to avoid async fetch issues
        uuid="test-uuid",
        name="test_upload",
        media_type="csv",
        db_type="limited",
        separator=",",
        encoding="utf-8",
        quotes_char='"',
        escape_char="\\",
        locale="en_US",
        compression="none",
        format="csv",
    )
    test_db.add(upload)

    # Create test data source with manual ID
    data_source = DataSource(
        id=456,  # Manual ID assignment to avoid async fetch issues
        name="test_source",
        type="csv",
        user_id=789,  # Mock user ID
        upload_id=123,  # Reference to upload ID
        size_bytes=0,
        row_count=0,
    )
    test_db.add(data_source)
    await test_db.commit()

    # Create temporary directory with test chunks
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create test CSV data
        csv_data = b"name,age,score\nAlice,30,95.5\nBob,25,88.0\nCharlie,35,92.3"

        # Save test data as a chunk
        chunk_path = Path(temp_dir) / "test.chunk"
        with open(chunk_path, "wb") as f:
            f.write(csv_data)

        # Process the chunks
        processor = CsvProcessor(
            data_source,
            test_db,
            data_source_id=456,  # Use the hardcoded ID
            encoding="utf-8",
            separator=",",
            quote_char='"',
        )
        await processor.process_chunks(Path(temp_dir))

        # Verify that the data source was updated
        await test_db.refresh(data_source)
        assert data_source.row_count == 3  # 3 rows in the test data

        # Verify that fields were created
        result = await test_db.execute(
            text("SELECT * FROM field WHERE data_source_id = :id"), {"id": 456}
        )
        fields = result.fetchall()
        assert len(fields) == 3  # 3 columns in the test data

        # Verify field types
        field_types = {field.name: field.data_type for field in fields}
        assert field_types["name"] == "string"
        assert field_types["age"] == "integer"
        assert field_types["score"] == "float"
