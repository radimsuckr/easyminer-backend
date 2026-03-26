import re
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from easyminer.storage.disk import DiskStorage
from easyminer.storage.s3 import S3Storage

TEST_BUCKET = "test-bucket"


@pytest.fixture
def disk_storage(tmp_path: Path) -> DiskStorage:
    return DiskStorage(root=tmp_path)


@pytest.fixture
def s3_storage():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=TEST_BUCKET)
        yield S3Storage(bucket=TEST_BUCKET, region="us-east-1")


@pytest.fixture(params=["disk", "s3"])
def storage(request, disk_storage, s3_storage):
    if request.param == "disk":
        return disk_storage
    return s3_storage


class TestStorageContract:
    """Tests that run against both DiskStorage and S3Storage."""

    def test_save_and_read_roundtrip(self, storage):
        content = b"hello world"
        key = storage.save("test/file.txt", content)
        assert key == "test/file.txt"
        assert storage.read(key) == content

    def test_save_overwrites_existing(self, storage):
        storage.save("test/file.txt", b"first")
        storage.save("test/file.txt", b"second")
        assert storage.read("test/file.txt") == b"second"

    def test_read_nonexistent_raises(self, storage):
        with pytest.raises(FileNotFoundError):
            storage.read("nonexistent/file.txt")

    def test_exists_true(self, storage):
        storage.save("test/file.txt", b"data")
        assert storage.exists("test/file.txt") is True

    def test_exists_false(self, storage):
        assert storage.exists("nonexistent/file.txt") is False

    def test_list_files(self, storage):
        storage.save("dir/a.txt", b"a")
        storage.save("dir/b.csv", b"b")
        storage.save("dir/c.txt", b"c")

        files = storage.list_files("dir")
        assert sorted(files) == ["dir/a.txt", "dir/b.csv", "dir/c.txt"]

    def test_list_files_with_pattern(self, storage):
        storage.save("dir/a.txt", b"a")
        storage.save("dir/b.csv", b"b")
        storage.save("dir/c.txt", b"c")

        files = storage.list_files("dir", pattern=re.compile(r"\.txt$"))
        assert sorted(files) == ["dir/a.txt", "dir/c.txt"]

    def test_save_and_read_binary(self, storage):
        content = bytes(range(256))
        storage.save("binary.bin", content)
        assert storage.read("binary.bin") == content

    def test_save_nested_path(self, storage):
        key = "a/b/c/d/file.txt"
        storage.save(key, b"nested")
        assert storage.read(key) == b"nested"


class TestDiskStorageSpecific:
    def test_creates_root_directory(self, tmp_path: Path):
        root = tmp_path / "new" / "storage"
        DiskStorage(root=root)
        assert root.exists()

    def test_list_files_nonexistent_dir_raises(self, disk_storage):
        with pytest.raises(FileNotFoundError):
            disk_storage.list_files("nonexistent")


class TestS3StorageSpecific:
    def test_bucket_required(self):
        with pytest.raises(ValueError, match="bucket name is required"):
            S3Storage(bucket=None)

    def test_prefix_applied(self):
        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket=TEST_BUCKET)

            storage = S3Storage(bucket=TEST_BUCKET, region="us-east-1", prefix="myprefix")
            storage.save("file.txt", b"data")

            # Verify the actual S3 key includes the prefix
            objects = client.list_objects_v2(Bucket=TEST_BUCKET)
            keys = [obj["Key"] for obj in objects["Contents"]]
            assert keys == ["myprefix/file.txt"]

            # But the returned/readable key is without prefix
            assert storage.read("file.txt") == b"data"

    def test_prefix_with_trailing_slash(self):
        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket=TEST_BUCKET)

            storage = S3Storage(bucket=TEST_BUCKET, region="us-east-1", prefix="myprefix/")
            storage.save("file.txt", b"data")

            objects = client.list_objects_v2(Bucket=TEST_BUCKET)
            keys = [obj["Key"] for obj in objects["Contents"]]
            assert keys == ["myprefix/file.txt"]
