import csv
import io
import shutil
import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path
from unittest.mock import patch

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from easyminer.app import app
from easyminer.crud.aio.data_source import create_data_source
from easyminer.crud.aio.field import create_field
from easyminer.crud.aio.upload import create_upload
from easyminer.database import Base, get_db_session

# Import all models to ensure they're registered with Base.metadata
from easyminer.models import *  # noqa: F401,F403
from easyminer.models.data import DataSource, FieldType
from easyminer.models.upload import Upload
from easyminer.schemas.data import UploadSettings
from easyminer.storage.storage import DiskStorage

# Global variables for test database
test_engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
TestSessionLocal = async_sessionmaker(
    bind=test_engine, class_=AsyncSession, expire_on_commit=False
)


@pytest_asyncio.fixture(scope="function")
async def setup_db():
    """Set up the database for testing."""
    # Create all tables
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield

    # Drop all tables after tests
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture(scope="function")
async def db_session(setup_db) -> AsyncGenerator[AsyncSession]:
    """Create a SQLAlchemy session for each test that gets rolled back after the test."""
    async with TestSessionLocal() as session:
        yield session
        # The session will be automatically rolled back and closed after the test


@pytest_asyncio.fixture
async def test_upload(db_session: AsyncSession) -> Upload:
    """Create a test upload for testing."""
    upload = await create_upload(
        db_session=db_session,
        settings=UploadSettings(
            name="Test Upload",
            media_type="csv",
            db_type="limited",
            separator=",",
            quotes_char='"',
            escape_char="\\",
            locale="en",
            compression=None,
            encoding="utf-8",
            format="csv",
        ),
    )
    return upload


@pytest_asyncio.fixture
async def test_data_source(db_session: AsyncSession) -> DataSource:
    """Create a test upload for testing."""
    data_source = await create_data_source(
        db_session=db_session,
        name="Test Data Source",
        type="csv",
        size_bytes=1000,
        row_count=20,
    )
    return data_source


# Override the database dependency
async def override_get_db_session():
    """Override the database session for testing."""
    async with TestSessionLocal() as session:
        yield session


# Override the dependency in the app
app.dependency_overrides[get_db_session] = override_get_db_session


@pytest.fixture
def client(setup_db):
    """Create a FastAPI TestClient for testing."""
    # Create test client
    test_client = TestClient(app)

    yield test_client


@pytest.fixture
def test_csv_data():
    """Create test CSV data for uploads."""
    return "name,age,score\nAlice,30,85\nBob,25,92\nCharlie,35,78"


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
                await create_field(
                    db_session=db_session,
                    name="name",
                    data_type=FieldType.nominal,
                    data_source_id=data_source.id,
                    unique_count=10,
                    support=5,
                ),
                await create_field(
                    db_session=db_session,
                    name="age",
                    data_type=FieldType.numeric,
                    data_source_id=data_source.id,
                    unique_count=10,
                    support=5,
                    min_value=25,
                    max_value=40,
                    avg_value=32.5,
                ),
                await create_field(
                    db_session=db_session,
                    name="score",
                    data_type=FieldType.numeric,
                    data_source_id=data_source.id,
                    unique_count=10,
                    support=5,
                    min_value=70,
                    max_value=95,
                    avg_value=85.0,
                ),
            ]

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
                await create_field(
                    db_session=db_session,
                    name="name",
                    data_type=FieldType.nominal,
                    data_source_id=data_source.id,
                    unique_count=10,
                    support=5,
                ),
                await create_field(
                    db_session=db_session,
                    name="age",
                    data_type=FieldType.numeric,
                    data_source_id=data_source.id,
                    unique_count=10,
                    support=5,
                    min_value=25,
                    max_value=40,
                    avg_value=32.5,
                ),
                await create_field(
                    db_session=db_session,
                    name="score",
                    data_type=FieldType.numeric,
                    data_source_id=data_source.id,
                    unique_count=10,
                    support=5,
                    min_value=70.0,
                    max_value=95.0,
                    avg_value=85.0,
                ),
            ]

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
            _ = chunk_file.write_text(csv_output.getvalue())

            # Yield the data source and fields
            yield data_source, fields

        finally:
            # Clean up the temporary directory
            shutil.rmtree(temp_dir, ignore_errors=True)
