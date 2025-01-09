from pydantic import BaseModel


class User(BaseModel):
    class Config:
        orm_mode = True

    id: int
    username: str
    slug: str
    email: str
    first_name: str
    last_name: str
    is_superuser: bool = False


class UserPrivate(User):
    hashed_password: str
