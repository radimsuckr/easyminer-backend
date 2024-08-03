from typing import Annotated

from fastapi import APIRouter, Depends
from fastapi.security import HTTPAuthorizationCredentials

from easyminer.api import security
from easyminer.api.dependencies.core import DBSessionDep
from easyminer.crud.user import get_user
from easyminer.schemas.user import User

router = APIRouter(
    prefix="/api/users",
    tags=["users"],
    responses={404: {"description": "Not found"}},
)


@router.get(
    "/{user_id}",
    response_model=User,
    dependencies=[Depends(security.http_bearer_token)],
)
async def user_details(
    user_id: int,
    db_session: DBSessionDep,
    credentials: Annotated[
        HTTPAuthorizationCredentials, Depends(security.http_bearer_token)
    ],
):
    """
    Get any user details
    """
    return await get_user(db_session, user_id)
