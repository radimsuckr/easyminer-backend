from uuid import UUID

from fastapi import APIRouter, HTTPException, status

router = APIRouter(prefix="/api/v1", tags=["Miner"])


@router.get("/status")
async def get_status():
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)


@router.post("/mine")
async def mine():
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)


@router.get("/partial-result/{task_id}")
async def get_partial_result(task_id: UUID):
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)


@router.get("/complete-result/{task_id}")
async def get_complete_result(task_id: UUID):
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)
