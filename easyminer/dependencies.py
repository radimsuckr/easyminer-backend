from collections.abc import AsyncGenerator
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.api.security import get_api_key, get_current_user
from easyminer.database import get_user_db_session
from easyminer.schemas.center import UserInfo
from easyminer.schemas.data import DbType


async def get_authenticated_db_session(
    api_key: Annotated[str, Depends(get_api_key)],
    db_type: DbType,
) -> AsyncGenerator[AsyncSession]:
    async for session in get_user_db_session(api_key, db_type.value):
        yield session


AuthenticatedSession = Annotated[AsyncSession, Depends(get_authenticated_db_session)]
CurrentUser = Annotated[UserInfo, Depends(get_current_user)]
ApiKey = Annotated[str, Depends(get_api_key)]
