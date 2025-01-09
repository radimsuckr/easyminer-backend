import logging
import sys
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from easyminer.api.router import router
from easyminer.config import settings
from easyminer.data import api as data_api
from easyminer.database import sessionmanager

logging.basicConfig(
    stream=sys.stdout, level=logging.DEBUG if settings.debug_logs else logging.INFO
)


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
app.include_router(data_api.router)
app.include_router(router)


@app.get("/")
async def root():
    return {"message": "Hello World"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", reload=True, port=8000)
