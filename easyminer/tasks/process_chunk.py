import logging

import pydantic

from easyminer.database import get_sync_db_session
from easyminer.models import Chunk
from easyminer.worker import app

logger = logging.getLogger(__name__)


class ProcessChunkResult(pydantic.BaseModel):
    chunk_id: int
    status: str


@app.task(pydantic=True)
def process_chunk(chunk_id: int) -> ProcessChunkResult:
    with get_sync_db_session() as db:
        chunk = db.get(Chunk, chunk_id)
        if not chunk:
            raise ValueError(f"Chunk with ID {chunk_id} not found")

        logger.info(f"Processing chunk {chunk_id} with path {chunk.path}")

    return ProcessChunkResult(
        chunk_id=chunk_id,
        status="processed",
    )
