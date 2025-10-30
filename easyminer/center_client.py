import logging

import httpx
from cachetools import TTLCache, cached
from fastapi import HTTPException, status

from easyminer.config import settings
from easyminer.schemas.center import DatabaseConfig, UserInfo
from easyminer.schemas.data import DbType

logger = logging.getLogger(__name__)


class EasyMinerCenterClient:
    def __init__(self, base_url: str | None = None):
        self.base_url: str = (base_url or settings.easyminer_center_url).rstrip("/")
        self.client: httpx.AsyncClient = httpx.AsyncClient(base_url=self.base_url, timeout=10)

    async def close(self) -> None:
        await self.client.aclose()

    @cached(cache=TTLCache(maxsize=8, ttl=300))
    async def get_user_info(self, api_key: str) -> UserInfo:
        try:
            response = await self.client.get(
                "/api/auth",
                headers={"Authorization": f"Bearer {api_key}"},
            )
            _ = response.raise_for_status()
            return UserInfo.model_validate(response.json())
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid API key",
                )
            logger.error(f"HTTP error from EasyMiner Center: {e}")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Failed to authenticate with EasyMiner Center",
            )
        except httpx.RequestError as e:
            logger.error(f"Request error to EasyMiner Center: {e}")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Failed to connect to EasyMiner Center",
            )

    @cached(cache=TTLCache(maxsize=8, ttl=300))
    async def get_database_config(self, api_key: str, db_type: DbType) -> DatabaseConfig:
        if db_type.value != DbType.limited:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only 'limited' database type is supported",
            )

        try:
            response = await self.client.get(
                f"/api/databases/{db_type.value}",
                headers={"Authorization": f"Bearer {api_key}"},
            )
            _ = response.raise_for_status()
            return DatabaseConfig.model_validate(response.json())
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid API key",
                )
            logger.error(f"HTTP error from EasyMiner Center: {e}")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Failed to get database configuration from EasyMiner Center",
            )
        except httpx.RequestError as e:
            logger.error(f"Request error to EasyMiner Center: {e}")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Failed to connect to EasyMiner Center",
            )


_center_client: EasyMinerCenterClient | None = None


def get_center_client() -> EasyMinerCenterClient:
    global _center_client
    if _center_client is None:
        _center_client = EasyMinerCenterClient()
    return _center_client
