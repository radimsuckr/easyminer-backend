from collections.abc import Sequence

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.models import Field


async def get_fields_by_data_source(
    db_session: AsyncSession, data_source_id: int
) -> Sequence[Field]:
    """Get all fields for a data source."""
    result = await db_session.execute(
        select(Field)
        .where(Field.data_source_id == data_source_id)
        .order_by(Field.index)
    )
    return result.scalars().all()


async def get_field_by_id(
    db_session: AsyncSession, field_id: int, data_source_id: int
) -> Field | None:
    """Get a field by ID if it belongs to the data source."""
    field = await db_session.get(Field, field_id)
    if not field or field.data_source_id != data_source_id:
        return None
    return field


async def get_fields_by_ids(
    db_session: AsyncSession, field_ids: list[int], data_source_id: int
) -> Sequence[Field]:
    """Get fields by IDs if they belong to the data source."""
    result = await db_session.execute(
        select(Field)
        .where(and_(Field.data_source_id == data_source_id, Field.id.in_(field_ids)))
        .order_by(Field.index)
    )
    return result.scalars().all()


async def create_field(
    db_session: AsyncSession,
    name: str,
    data_source_id: int,
    data_type: str,
    index: int,
    min_value: str | None = None,
    max_value: str | None = None,
    avg_value: float | None = None,
) -> Field:
    """Create a new field."""
    field = Field(
        name=name,
        data_source_id=data_source_id,
        data_type=data_type,
        index=index,
        min_value=min_value,
        max_value=max_value,
        avg_value=avg_value,
    )
    db_session.add(field)
    await db_session.commit()
    await db_session.refresh(field)
    return field


async def update_field_stats(
    db_session: AsyncSession,
    field_id: int,
    min_value: str,
    max_value: str,
    avg_value: float,
) -> Field | None:
    """Update statistical information for a field."""
    field = await db_session.get(Field, field_id)
    if not field:
        return None

    field.min_value = min_value
    field.max_value = max_value
    field.avg_value = avg_value
    await db_session.commit()
    await db_session.refresh(field)
    return field
