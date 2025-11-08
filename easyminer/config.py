import os
from enum import Enum
from pathlib import Path

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

_ = load_dotenv()


API_V1_PREFIX = "/api/v1"
ROOT_DIR: Path = Path(os.path.dirname(os.path.abspath(__file__)) + "/..").resolve()


class Settings(BaseSettings):
    echo_sql: bool = False
    test: bool = False
    project_name: str = "EasyMiner Backend"
    debug_logs: bool = False
    version: str = "0.1.0"
    easyminer_center_url: str = "http://localhost:8001"  # URL of the EasyMiner Center server
    user_session_ttl: int = 300
    skip_migrations: bool = False


celery_broker = os.getenv("CELERY_BROKER")
if not celery_broker:
    raise ValueError('Set "CELERY_BROKER" environment variable')
celery_backend = os.getenv("CELERY_BACKEND")
if not celery_backend:
    raise ValueError('Set "CELERY_BACKEND" environment variable')


class EasyMinerModules(str, Enum):
    data = "data"
    preprocessing = "preprocessing"
    miner = "miner"


_allowed_modules = {module.value for module in EasyMinerModules}
easyminer_modules = set(os.environ.get("EASYMINER_MODULES", "data,preprocessing,miner").split(","))
if len(easyminer_modules) == 0:
    raise ValueError('"EASYMINER_MODULES" cannot be empty')
if not easyminer_modules.issubset(_allowed_modules):
    raise ValueError(f'Invalid module in "EASYMINER_MODULES". Choose from {_allowed_modules}')


settings = Settings()

logging_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"}},
    "handlers": {
        "default": {
            "level": "DEBUG" if settings.debug_logs else "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Use standard output
        },
        "sqlalchemy": {"class": "logging.NullHandler"},
    },
    "loggers": {
        "": {
            "handlers": ["default"],
            "level": "DEBUG" if settings.debug_logs else "INFO",
            "propagate": True,
        },
        "sqlalchemy": {
            "handlers": ["sqlalchemy"],
            "level": "WARNING",
            "propagate": False,
        },
        "sqlalchemy.engine": {
            "handlers": ["sqlalchemy"],
            "level": "WARNING",
            "propagate": False,
        },
        "sqlalchemy.pool": {
            "handlers": ["sqlalchemy"],
            "level": "WARNING",
            "propagate": False,
        },
    },
}
