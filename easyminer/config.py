import os

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

_ = load_dotenv()


API_V1_PREFIX = "/api/v1"


class Settings(BaseSettings):
    database_url: str
    database_url_sync: str
    echo_sql: bool = True
    test: bool = False
    project_name: str = "EasyMiner Backend"
    debug_logs: bool = False
    version: str = "0.1.0"


database_url = os.getenv("DATABASE_URL")
if not database_url:
    raise ValueError('Set "DATABASE_URL" environment variable')
database_url_sync = os.getenv("DATABASE_URL_SYNC")
if not database_url_sync:
    raise ValueError('Set "DATABASE_URL_SYNC" environment variable')

celery_broker = os.getenv("CELERY_BROKER")
if not celery_broker:
    raise ValueError('Set "CELERY_BROKER" environment variable')
celery_backend = os.getenv("CELERY_BACKEND")
if not celery_backend:
    raise ValueError('Set "CELERY_BACKEND" environment variable')

settings = Settings(
    database_url=database_url,
    database_url_sync=database_url_sync,
)
