import logging
from datetime import datetime
from typing import Annotated
from uuid import UUID

import lxml.etree
from celery.result import AsyncResult
from fastapi import APIRouter, Body, HTTPException, Request, Response, status
from sqlalchemy import select

from easyminer.dependencies import ApiKey, AuthenticatedSession, get_database_config
from easyminer.models.task import Task
from easyminer.parsers.pmml.miner import SimplePmmlParser
from easyminer.redis_client import get_partial_result_tracker
from easyminer.schemas.data import DbType
from easyminer.schemas.error import StructuredHTTPException
from easyminer.schemas.miner import Miner, MineStartResponse
from easyminer.tasks.mine import mine as mine_task
from easyminer.worker import app as celery_app

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


@router.post("/mine", status_code=status.HTTP_202_ACCEPTED)
async def mine(
    request: Request, api_key: ApiKey, body: Annotated[str, Body(media_type="application/xml")]
) -> MineStartResponse:
    parser = SimplePmmlParser(body)
    try:
        pmml = parser.parse()

        db_config = await get_database_config(api_key, DbType.limited)
        db_url = db_config.get_sync_url()

        task = mine_task.apply_async(args=(pmml,), headers={"db_url": db_url})
        return MineStartResponse(
            code="202",
            miner=Miner(
                state=task.state,
                task_id=UUID(task.id),
                started=datetime.now(),
                result_url=str(request.url_for("get_partial_result", task_id=task.id)),
            ),
        )
    except lxml.etree.LxmlError as e:
        raise StructuredHTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, error="InvalidPMML", message=f"Invalid PMML: {str(e)}"
        ) from e


@router.get(
    "/partial-result/{task_id}",
    responses={
        status.HTTP_204_NO_CONTENT: {"description": "Mining is in progress, no partial results available yet"},
        status.HTTP_206_PARTIAL_CONTENT: {
            "description": "Partial results available (mining still in progress)",
            "content": {"application/xml": {}},
        },
        status.HTTP_303_SEE_OTHER: {
            "description": "Mining completed, redirects to complete-result endpoint",
            "content": {"application/xml": {}},
        },
        status.HTTP_404_NOT_FOUND: {"description": "Task doesn't exist"},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Error occurred during mining"},
    },
)
async def get_partial_result(request: Request, db: AuthenticatedSession, task_id: UUID):
    task_record = await db.scalar(select(Task).where(Task.task_id == task_id))

    if not task_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")

    async_result: AsyncResult[str] = AsyncResult(str(task_id), app=celery_app)

    if async_result.failed():
        logger.error(f"Task {task_id} failed: {async_result.info}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Mining task failed: {str(async_result.info)}"
        )

    if async_result.successful():
        complete_result_url = str(request.url_for("get_complete_result", task_id=str(task_id)))
        response_body = f"""<?xml version="1.0" encoding="UTF-8"?>
<status>
	<code>303 See Other</code>
	<miner>
		<state>Done</state>
		<task-id>{task_id}</task-id>
		<complete-result-url>{complete_result_url}</complete-result-url>
	</miner>
</status>"""
        return Response(
            content=response_body,
            media_type="application/xml; charset=utf-8",
            status_code=status.HTTP_303_SEE_OTHER,
            headers={"Location": complete_result_url},
        )

    if async_result.ready():
        logger.info(f"Task {task_id} has result ready (state: {async_result.state})")

        tracker = get_partial_result_tracker()
        is_first_delivery = tracker.try_mark_partial_result_as_shown(str(task_id))

        if is_first_delivery:
            try:
                result = async_result.get()
                if not result:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Task completed but result is empty."
                    )

                logger.info(f"Returning partial result for task {task_id} (first delivery)")
                return Response(
                    content=result,
                    media_type="application/xml; charset=utf-8",
                    status_code=status.HTTP_206_PARTIAL_CONTENT,
                )
            except Exception as e:
                logger.error(f"Error retrieving result for task {task_id}: {e}", exc_info=True)
                raise StructuredHTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    error="ErrorRetrievingMinerResult",
                    message="Error retrieving task result.",
                    details={"exception": str(e)},
                )
        else:
            logger.info(f"Partial result already shown for task {task_id}, redirecting to complete result")
            complete_result_url = str(request.url_for("get_complete_result", task_id=str(task_id)))
            response_body = f"""<?xml version="1.0" encoding="UTF-8"?>
<status>
	<code>303 See Other</code>
	<miner>
		<state>InProgress</state>
		<task-id>{task_id}</task-id>
		<complete-result-url>{complete_result_url}</complete-result-url>
	</miner>
</status>"""
            return Response(
                content=response_body,
                media_type="application/xml; charset=utf-8",
                status_code=status.HTTP_303_SEE_OTHER,
                headers={"Location": complete_result_url},
            )

    logger.info(f"Task {task_id} is still in progress, no result available yet (state: {async_result.state})")
    return Response(status_code=status.HTTP_204_NO_CONTENT, media_type="application/xml; charset=utf-8")


@router.get(
    "/complete-result/{task_id}",
    responses={
        status.HTTP_200_OK: {
            "description": "Complete PMML result with all association rules",
            "content": {"application/xml": {}},
        },
        status.HTTP_404_NOT_FOUND: {"description": "Task doesn't exist or is still in progress"},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Error occurred during mining"},
    },
)
async def get_complete_result(db: AuthenticatedSession, task_id: UUID):
    task_record = await db.scalar(select(Task).where(Task.task_id == task_id))

    if not task_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")

    async_result: AsyncResult[str] = AsyncResult(str(task_id), app=celery_app)

    if async_result.failed():
        logger.error(f"Task {task_id} failed: {async_result.info}")
        raise StructuredHTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error="MiningTaskFailed",
            message="Mining task failed.",
            details={"info": async_result.info},
        )

    if not async_result.ready():
        logger.info(f"Task {task_id} is not ready yet (state: {async_result.state})")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Task is still in progress or has been picked up."
        )

    try:
        result = async_result.get()
        if not result:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Task completed but result is empty."
            )

        return Response(content=result, media_type="application/xml; charset=utf-8", status_code=status.HTTP_200_OK)
    except Exception as e:
        logger.error(f"Error retrieving result for task {task_id}: {e}", exc_info=True)
        raise StructuredHTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error="TaskResultRetrievalError",
            message="Error retrieving task result.",
            details={"exception": str(e)},
        )
