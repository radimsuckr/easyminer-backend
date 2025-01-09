from typing import Annotated, Sequence

from fastapi import APIRouter, Depends
from fastapi.security import HTTPAuthorizationCredentials

from easyminer.api import security
from easyminer.api.dependencies.core import DBSessionDep
from easyminer.crud.user import get_user, get_users
from easyminer.schemas.user import User
from easyminer.models.user import User as DBUser

router = APIRouter(
    prefix="/api/users",
    tags=["users"],
)


@router.get(
    "/{user_id}",
    response_model=User,
    dependencies=[Depends(security.http_bearer_token)],
    responses={404: {"description": "Not found"}},
)
async def user_details(
    user_id: int,
    db_session: DBSessionDep,
    _: Annotated[HTTPAuthorizationCredentials, Depends(security.http_bearer_token)],
):
    """
    Get any user details
    """
    return await get_user(db_session, user_id)


@router.get(
    "",
    response_model=Sequence[User],
    dependencies=[Depends(security.http_bearer_token)],
)
async def users(
    db_session: DBSessionDep,
    _: Annotated[HTTPAuthorizationCredentials, Depends(security.http_bearer_token)],
) -> Sequence[DBUser]:
    """
    Get all users
    """
    return await get_users(db_session)
