import logging
import logging.config
from contextlib import asynccontextmanager

import sqlalchemy.exc
from fastapi import FastAPI, HTTPException, Request, status

from easyminer.api.data import router as data_router
from easyminer.api.miner import router as miner_router
from easyminer.api.preprocessing import router as preprocessing_router
from easyminer.api.task import router as task_router
from easyminer.config import (
    EasyMinerModules,
    easyminer_modules,
    logging_config,
    settings,
)
from easyminer.database import sessionmanager

logging.config.dictConfig(logging_config)


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

app.include_router(task_router)
if EasyMinerModules.data in easyminer_modules:
    app.include_router(data_router)
if EasyMinerModules.preprocessing in easyminer_modules:
    app.include_router(preprocessing_router)
if EasyMinerModules.miner in easyminer_modules:
    app.include_router(miner_router)


@app.exception_handler(sqlalchemy.exc.OperationalError)
async def database_exception_handler(_: Request, exc: sqlalchemy.exc.OperationalError):
    logger = logging.getLogger(__name__)
    logger.error(f"Database error: {exc}")
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Database error",
    )
