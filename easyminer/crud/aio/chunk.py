from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.models import Chunk


async def create_chunk(db: AsyncSession, upload_id: int, uploaded_at: datetime, path: str) -> int:
    chunk = Chunk(
        upload_id=upload_id,
        uploaded_at=uploaded_at,
        path=path,
    )
    db.add(chunk)
    await db.flush()
    return chunk.id
