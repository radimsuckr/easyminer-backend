import logging
import logging.config
from contextlib import asynccontextmanager

import sqlalchemy.exc
import uvicorn
from fastapi import FastAPI, HTTPException, status

from easyminer.api.data import router as data_router
from easyminer.api.preprocessing import router as preprocessing_router
from easyminer.api.users import router as users_router
from easyminer.config import settings
from easyminer.database import sessionmanager

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
    },
    "handlers": {
        "default": {
            "level": "DEBUG" if settings.debug_logs else "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Use standard output
        },
    },
    "loggers": {
        "": {
            "handlers": ["default"],
            "level": "DEBUG" if settings.debug_logs else "INFO",
            "propagate": True,
        },
    },
}
logging.config.dictConfig(LOGGING_CONFIG)


@asynccontextmanager
async def lifespan(_: FastAPI):
    """
    Function that handles startup and shutdown events.
    To understand more, read https://fastapi.tiangolo.com/advanced/events/
    """
    yield
    if sessionmanager._engine is not None:
        # Close the DB connection
        await sessionmanager.close()


app = FastAPI(
    lifespan=lifespan,
    title=settings.project_name,
    docs_url="/docs",
    version=settings.version,
)
app.include_router(data_router)
app.include_router(preprocessing_router)
app.include_router(users_router)


@app.exception_handler(sqlalchemy.exc.OperationalError)
async def database_exception_handler(request, exc: sqlalchemy.exc.OperationalError):
    logger = logging.getLogger(__name__)
    logger.error(f"Database error: {exc}")
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Database error",
    )


if __name__ == "__main__":
    uvicorn.run("easyminer.app:app", host="0.0.0.0", reload=True, port=8000)
