from pydantic import BaseModel


class UserInfo(BaseModel):
    id: int
    name: str
    email: str
    role: list[str]


class DatabaseConfig(BaseModel):
    server: str
    port: int
    username: str
    password: str
    database: str

    def get_async_url(self) -> str:
        return f"mysql+aiomysql://{self.username}:{self.password}@{self.server}:{self.port}/{self.database}"

    def get_sync_url(self) -> str:
        return f"mysql+pymysql://{self.username}:{self.password}@{self.server}:{self.port}/{self.database}"
