import os

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str
    echo_sql: bool = True
    test: bool = False
    project_name: str = "My FastAPI project"
    oauth_token_secret: str = "my_dev_secret"
    debug_logs: bool = False
    version: str = "0.1.0"


database_url = os.getenv("DATABASE_URL")
if not database_url:
    raise ValueError('Set "DATABASE_URL" environment variable')
settings = Settings(database_url=database_url)
