import os

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()


API_V1_PREFIX = "/api/v1"


class Settings(BaseSettings):
    database_url: str
    echo_sql: bool = True
    test: bool = False
    project_name: str = "EasyMiner Backend"
    debug_logs: bool = False
    version: str = "0.1.0"


database_url = os.getenv("DATABASE_URL")
if not database_url:
    raise ValueError('Set "DATABASE_URL" environment variable')
settings = Settings(database_url=database_url)
