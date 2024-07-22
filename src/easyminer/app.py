from http import HTTPStatus
from typing import Annotated, override

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from .model import Tournament
from .settings import init_db

app = FastAPI()
init_db(app)


class EasyminerHTTPBearer(HTTPBearer):
    @override
    async def __call__(self, request: Request) -> HTTPAuthorizationCredentials | None:
        creds = await super().__call__(request)
        if creds:
            if creds.credentials != "pwd":
                raise HTTPException(
                    status_code=HTTPStatus.FORBIDDEN, detail="Invalid credentials."
                )
            return creds
        else:
            raise HTTPException(
                status_code=HTTPStatus.FORBIDDEN, detail="Invalid authorization code."
            )


security = EasyminerHTTPBearer()


class UserAPIModel(BaseModel):
    id: int
    name: str


@app.get("/", tags=["root"])
async def root():
    t = await Tournament.create(name="test1")
    return t


@app.get("/user/{id}", tags=["user"])
async def get_user(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)], id: int
) -> UserAPIModel:
    return UserAPIModel(id=id, name=f"Pepik {id}")
