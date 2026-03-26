import re
from typing import override

import boto3
from botocore.exceptions import ClientError

from .storage import Storage


class S3Storage(Storage):
    def __init__(
        self,
        bucket: str | None,
        endpoint_url: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        region: str = "us-east-1",
        prefix: str = "",
    ) -> None:
        super().__init__()

        if not bucket:
            raise ValueError("S3 bucket name is required")

        self._bucket = bucket
        self._prefix = prefix.strip("/") + "/" if prefix.strip("/") else ""
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )

    def _full_key(self, key: str) -> str:
        return f"{self._prefix}{key}"

    @override
    def save(self, key: str, content: bytes) -> str:
        self._client.put_object(
            Bucket=self._bucket,
            Key=self._full_key(key),
            Body=content,
        )
        return key

    @override
    def read(self, key: str) -> bytes:
        try:
            response = self._client.get_object(
                Bucket=self._bucket,
                Key=self._full_key(key),
            )
            return response["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(f"File not found: {key}") from e
            raise

    @override
    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(
                Bucket=self._bucket,
                Key=self._full_key(key),
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    @override
    def list_files(self, prefix: str, pattern: re.Pattern[str] | None = None) -> list[str]:
        full_prefix = self._full_key(prefix)
        if not full_prefix.endswith("/"):
            full_prefix += "/"

        keys: list[str] = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=full_prefix):
            for obj in page.get("Contents", []):
                # Strip the storage prefix to return keys relative to it
                rel_key = obj["Key"]
                if self._prefix and rel_key.startswith(self._prefix):
                    rel_key = rel_key[len(self._prefix) :]
                keys.append(rel_key)

        if pattern is not None:
            keys = [k for k in keys if pattern.search(k)]

        return keys
