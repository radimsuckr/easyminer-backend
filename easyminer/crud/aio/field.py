from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.models import Field, FieldType, FieldNumericDetail


async def get_fields_by_data_source(
    db_session: AsyncSession, data_source_id: int
) -> Sequence[Field]:
    """Get all fields for a data source."""
    result = await db_session.execute(
        select(Field).where(Field.data_source_id == data_source_id)
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


async def create_field(
    db_session: AsyncSession,
    name: str,
    data_source_id: int,
    data_type: FieldType,
    unique_count: int,
    support: int,
    min_value: float | None = None,
    max_value: float | None = None,
    avg_value: float | None = None,
) -> Field:
    """Create a new field."""
    field = Field(
        name=name,
        data_source_id=data_source_id,
        data_type=data_type,
        unique_count=unique_count,
        support=support,
    )
    db_session.add(field)
    if data_type == FieldType.numeric:
        if min_value is None or max_value is None or avg_value is None:
            raise ValueError("Numeric fields require statistical information.")
        field_numeric_details = FieldNumericDetail(
            id=field.id, min_value=min_value, max_value=max_value, avg_value=avg_value
        )
        db_session.add(field_numeric_details)
    await db_session.commit()
    await db_session.refresh(field)
    return field


async def update_field_stats(
    db_session: AsyncSession,
    field_id: int,
    min_value: float,
    max_value: float,
    avg_value: float,
) -> FieldNumericDetail | None:
    """Update statistical information for a field."""
    field_numeric_details = await db_session.get(FieldNumericDetail, field_id)
    if not field_numeric_details:
        return None

    field_numeric_details.min_value = min_value
    field_numeric_details.max_value = max_value
    field_numeric_details.avg_value = avg_value
    await db_session.commit()
    await db_session.refresh(field_numeric_details)
    return field_numeric_details


async def get_field_stats(
    db_session: AsyncSession, field_id: int
) -> FieldNumericDetail | None:
    """Get statistical information for a field."""
    return await db_session.get(FieldNumericDetail, field_id)
