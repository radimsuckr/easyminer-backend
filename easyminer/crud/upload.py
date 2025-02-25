from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.models.upload import Upload


async def create_upload(db_session: AsyncSession, user_id: int, name: str) -> Upload:
    t = db_session.begin()
    upload = Upload(user_id=user_id, name=name)
    db_session.add(upload)
    await t.commit()
    return upload
