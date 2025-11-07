import logging
from datetime import datetime
from typing import Annotated
from uuid import UUID

import lxml.etree
from fastapi import APIRouter, Body, HTTPException, Request, Response, status

from easyminer.database import get_database_config
from easyminer.dependencies import ApiKey
from easyminer.parsers.pmml.miner import SimplePmmlParser
from easyminer.schemas.data import DbType
from easyminer.schemas.miner import Miner, MineStartResponse
from easyminer.tasks.mine import mine as mine_task

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["Miner"])


@router.get(
    "/status",
    description="As of now this exists only for backwards compatibility and acts as an application server healthcheck. (Returns only 200)",
    responses={status.HTTP_200_OK: {}, status.HTTP_503_SERVICE_UNAVAILABLE: {}},
)
async def get_status():
    # TODO: Check additional healthcheck options.
    return Response(status_code=status.HTTP_200_OK)


@router.post("/mine")
async def mine(
    request: Request,
    api_key: ApiKey,
    body: Annotated[str, Body(media_type="application/xml")],
) -> MineStartResponse:
    parser = SimplePmmlParser(body)
    try:
        pmml = parser.parse()

        # Get database config to pass to Celery task
        db_config = await get_database_config(api_key, DbType.limited)
        db_url = db_config.get_sync_url()

        task = mine_task.delay(pmml, db_url)
        return MineStartResponse(
            code="200",
            miner=Miner(
                state=task.state,
                task_id=UUID(task.id),
                started=datetime.now(),
                result_url=str(request.url_for("get_task_result", task_id=task.id)),
            ),
        )
    except lxml.etree.LxmlError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Invalid PMML: {e}",
        ) from e


@router.get(
    "/partial-result/{task_id}",
    responses={
        status.HTTP_204_NO_CONTENT: {},
        status.HTTP_206_PARTIAL_CONTENT: {},
        status.HTTP_303_SEE_OTHER: {},
        status.HTTP_404_NOT_FOUND: {},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {},
    },
)
async def get_partial_result(task_id: UUID):
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)


@router.get(
    "/complete-result/{task_id}",
    responses={
        status.HTTP_200_OK: {},
        status.HTTP_404_NOT_FOUND: {},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {},
    },
)
async def get_complete_result(task_id: UUID):
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)
