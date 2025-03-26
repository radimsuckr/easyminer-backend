import logging
import logging.config
from contextlib import asynccontextmanager
from typing import Any

import sqlalchemy.exc
import uvicorn
from fastapi import FastAPI, HTTPException, status

from easyminer.api.data import router as data_router
from easyminer.api.preprocessing import router as preprocessing_router
from easyminer.config import settings
from easyminer.database import sessionmanager
from easyminer.schemas import BaseSchema

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
        "sqlalchemy": {
            "handlers": ["default"],
            "propagate": False,
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
    description="API for the EasyMiner Backend modules Data, Preprocessing and Mining",
    summary="EasyMiner Backend API",
)
app.include_router(data_router)
app.include_router(preprocessing_router)


@app.get("/")
async def root() -> dict[str, str]:
    from .tasks import add

    task = add.delay(1, 2)
    return {"task_id": task.task_id}


class TaskStatus(BaseSchema):
    task_id: str
    task_status: str
    task_result: Any


@app.get("/task/{taskId}")
async def get_task(taskId: str) -> TaskStatus:
    from .tasks import add

    task = add.AsyncResult(taskId)
    result = None
    try:
        result = task.result
    except Exception:
        pass
    status = TaskStatus(
        task_id=task.id,
        task_status=task.status,
        task_result=result,
    )
    return status


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
