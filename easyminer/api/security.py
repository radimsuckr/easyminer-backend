from typing import Annotated, override

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from easyminer.center_client import get_center_client
from easyminer.schemas.center import UserInfo


class EasyminerHTTPBearer(HTTPBearer):
    @override
    async def __call__(self, request: Request) -> HTTPAuthorizationCredentials | None:
        creds = await super().__call__(request)
        if not creds:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing authorization header",
            )
        return creds


http_bearer_token = EasyminerHTTPBearer()


async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(http_bearer_token)],
) -> UserInfo:
    client = get_center_client()
    user_info = await client.get_user_info(credentials.credentials)
    return user_info


def get_api_key(credentials: Annotated[HTTPAuthorizationCredentials, Depends(http_bearer_token)]) -> str:
    return credentials.credentials
