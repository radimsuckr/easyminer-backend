import logging
from collections.abc import AsyncGenerator
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.api.security import get_api_key, get_current_user
from easyminer.center_client import get_center_client
from easyminer.database import get_user_db_session
from easyminer.schemas.center import DatabaseConfig, UserInfo
from easyminer.schemas.data import DbType

logger = logging.getLogger(__name__)


async def get_database_config(api_key: str, db_type: DbType = DbType.limited) -> DatabaseConfig:
    logger.debug("Getting database config from EM Center")
    client = get_center_client()
    config = await client.get_database_config(api_key, db_type)
    logger.debug(f"Got database config: {config.server}:{config.port}/{config.database}")
    return config


async def get_authenticated_db_session(
    api_key: Annotated[str, Depends(get_api_key)],
) -> AsyncGenerator[AsyncSession]:
    logger.debug("get_authenticated_db_session called")
    async for session in get_user_db_session(api_key):
        logger.debug("Yielding database session to endpoint")
        yield session
        logger.debug("Endpoint finished using database session")


AuthenticatedSession = Annotated[AsyncSession, Depends(get_authenticated_db_session)]
CurrentUser = Annotated[UserInfo, Depends(get_current_user)]
ApiKey = Annotated[str, Depends(get_api_key)]
