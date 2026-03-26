from .disk import DiskStorage
from .storage import Storage

__all__ = ["Storage", "DiskStorage", "get_storage"]

_storage: Storage | None = None


def get_storage() -> Storage:
    global _storage
    if _storage is None:
        from easyminer.config import settings

        if settings.storage_backend == "s3":
            from .s3 import S3Storage

            _storage = S3Storage(
                endpoint_url=settings.s3_endpoint_url,
                bucket=settings.s3_bucket,
                access_key=settings.s3_access_key,
                secret_key=settings.s3_secret_key,
                region=settings.s3_region,
                prefix=settings.s3_prefix,
            )
        else:
            _storage = DiskStorage()
    return _storage
