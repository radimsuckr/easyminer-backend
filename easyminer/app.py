import logging
import logging.config
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from easyminer.api.data import router as data_router
from easyminer.api.router import router
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
    docs_url="/api/docs",
    version=settings.version,
)
app.include_router(data_router)
app.include_router(router)


@app.get("/")
async def root():
    return {"message": "Hello World"}


if __name__ == "__main__":
    uvicorn.run("easyminer.app:app", host="0.0.0.0", reload=True, port=8000)
