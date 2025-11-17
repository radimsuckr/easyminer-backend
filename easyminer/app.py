import asyncio
import logging
import logging.config
from contextlib import asynccontextmanager
from http import HTTPStatus
from typing import Any

import sqlalchemy.exc
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

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
from easyminer.database import get_session_manager
from easyminer.schemas.error import StructuredHTTPException

logging.config.dictConfig(logging_config)


@asynccontextmanager
async def lifespan(_: FastAPI):
    """
    Function that handles startup and shutdown events.
    To understand more, read https://fastapi.tiangolo.com/advanced/events/
    """
    session_manager = get_session_manager()
    cleanup_task = asyncio.create_task(session_manager.cleanup_expired_sessions())

    yield

    # Shutdown: cancel cleanup task and close all sessions
    __ = cleanup_task.cancel()
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass
    await session_manager.close_all()


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


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(_: Request, exc: RequestValidationError):
    errors: list[Any] = []
    for error in exc.errors():
        error_detail = {
            "field": ".".join(str(loc) for loc in error["loc"]) if error["loc"] else "body",
            "message": error["msg"],
            "type": error["type"],
        }
        # Include input value if it's safe to show (not too large, not sensitive)
        if "input" in error and error["input"] is not None:
            input_str = str(error["input"])
            if len(input_str) <= 50:  # Only include if reasonably short
                error_detail["input"] = input_str
        errors.append(error_detail)

    content = {"error": "ValidationError", "message": "Request validation failed", "details": {"errors": errors}}
    return JSONResponse(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, content=content)


@app.exception_handler(HTTPException)
async def http_exception_handler(_: Request, exc: HTTPException):
    content: dict[str, str | dict[str, Any]]
    if isinstance(exc, StructuredHTTPException):
        content = {"error": exc.error, "message": exc.message}
        if exc.details:
            content["details"] = exc.details
    else:
        try:
            http_status = HTTPStatus(exc.status_code)
            error_type = "".join(word.capitalize() for word in http_status.name.split("_"))
        except ValueError:
            error_type = "Unknown Error"
        content = {"error": error_type, "message": str(exc.detail)}
    return JSONResponse(status_code=exc.status_code, content=content, headers=exc.headers)


@app.exception_handler(sqlalchemy.exc.DatabaseError)
async def database_error_handler(_: Request, exc: sqlalchemy.exc.DatabaseError):
    logger = logging.getLogger(__name__)
    logger.error(f"Database error: {exc}", exc_info=True)
    error_detail = str(exc.orig) if hasattr(exc, "orig") else str(exc)
    content = {
        "error": "DatabaseError",
        "message": "A database error occurred",
        "details": {"database_message": error_detail},
    }
    return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content=content)
