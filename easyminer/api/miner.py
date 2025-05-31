import logging
from typing import Annotated
from uuid import UUID

import lxml.etree
from fastapi import APIRouter, Body, HTTPException, Response, status

from easyminer.parsers.pmml.miner import PMML, SimplePmmlParser

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["Miner"])


@router.get(
    "/status",
    description="As of now this exists only for backwards compatibility and acts as an application server healthcheck. (Returns only 200)",
    responses={
        status.HTTP_200_OK: {},
        status.HTTP_503_SERVICE_UNAVAILABLE: {},
    },
)
async def get_status():
    # TODO: Check additional healthcheck options.
    return Response(status_code=status.HTTP_200_OK)


@router.post("/mine")
async def mine(body: Annotated[str, Body(media_type="application/xml")]) -> PMML:
    parser = SimplePmmlParser(body)
    try:
        pmml = parser.parse()
        logger.info(pmml)
        return pmml
    except lxml.etree.XMLSyntaxError:
        raise HTTPException(status_code=status.HTTP_418_IM_A_TEAPOT)


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
    responses={status.HTTP_200_OK: {}, status.HTTP_404_NOT_FOUND: {}, status.HTTP_500_INTERNAL_SERVER_ERROR: {}},
)
async def get_complete_result(task_id: UUID):
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)
