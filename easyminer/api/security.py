from typing import override

from fastapi import HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

STATIC_PASSWORD = "pwd"


class EasyminerHTTPBearer(HTTPBearer):
    @override
    async def __call__(self, request: Request) -> HTTPAuthorizationCredentials | None:
        creds = await super().__call__(request)
        if creds:
            if creds.credentials != STATIC_PASSWORD:
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid credentials.")
            return creds
        else:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Invalid authorization code.",
            )


http_bearer_token = EasyminerHTTPBearer()
