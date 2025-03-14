"""Authentication dependencies for FastAPI routes."""

from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends
from fastapi.security import APIKeyHeader

# Simple API key header
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


@dataclass
class UserInfo:
    """Simple user info class that replaces the User model."""

    id: int = 1  # Default to user ID 1 for simplicity
    is_admin: bool = True
    username: str = "admin"
    email: str = "admin@example.com"


async def get_current_user(
    api_key: Annotated[str, Depends(api_key_header)] = None,
) -> UserInfo:
    """Get current user based on API key.

    This is a simplified version that always returns the same user.
    In a real application, you would validate the API key.
    """
    if not api_key:
        # For development purposes, allow access without an API key
        return UserInfo()

    # In a real app, you'd validate the API key here
    # For now, always return a default user
    return UserInfo()
