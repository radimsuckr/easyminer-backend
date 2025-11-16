from typing import Annotated

from fastapi import Depends, status
from fastapi.security import APIKeyHeader, APIKeyQuery

from easyminer.center_client import get_center_client
from easyminer.schemas.center import UserInfo
from easyminer.schemas.error import StructuredHTTPException

api_key_header = APIKeyHeader(
    name="Authorization",
    auto_error=False,
    description="API key in Authorization header with 'ApiKey ' prefix (e.g., 'ApiKey your-key-here')",
)
api_key_query = APIKeyQuery(
    name="apiKey", auto_error=False, description="API key as query parameter (e.g., ?apiKey=your-key-here)"
)


def get_api_key(
    api_key_h: Annotated[str | None, Depends(api_key_header)], api_key_q: Annotated[str | None, Depends(api_key_query)]
) -> str:
    """
    Extract API key from either Authorization header or apiKey query parameter.
    Prioritizes header over query parameter for security.

    Strictly compatible with original Scala implementation:
    - Header: Authorization: ApiKey <api_key> (MUST have "ApiKey " prefix)
    - Query: ?apiKey=<api_key>

    Scala reference: UserEndpoint.scala line 82
    """
    # Extract from header with mandatory "ApiKey " prefix
    if api_key_h:
        if not api_key_h.startswith("ApiKey "):
            raise StructuredHTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                error="InvalidAuthorizationHeader",
                message="Authorization header must use 'ApiKey ' prefix",
                details={"example": "Authorization: ApiKey your-key-here"},
            )
        api_key = api_key_h[7:]  # Remove "ApiKey " prefix, len("ApiKey ") == 7
    # Or use query parameter directly (no prefix needed)
    elif api_key_q:
        api_key = api_key_q
    else:
        raise StructuredHTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            error="MissingApiKey",
            message="Missing API key. Provide it in Authorization header or apiKey query parameter",
            details={"headerExample": "Authorization: ApiKey your-key-here", "queryExample": "?apiKey=your-key-here"},
        )

    return api_key


async def get_current_user(api_key: Annotated[str, Depends(get_api_key)]) -> UserInfo:
    client = get_center_client()
    user_info = await client.get_user_info(api_key)
    return user_info
