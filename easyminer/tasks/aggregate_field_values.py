import logging
from decimal import Decimal

from sqlalchemy import func, select

from easyminer.database import get_sync_db_session
from easyminer.models.data import DataSourceInstance, Field
from easyminer.worker import app

logger = logging.getLogger(__name__)


@app.task
def aggregate_field_values(
    data_source_id: int,
    field_id: int,
    bins: int,
    min: Decimal,
    max: Decimal,
    min_inclusive: bool,
    max_inclusive: bool,
    db_url: str,
) -> list[dict[str, Decimal | bool | int]]:
    logger.info(
        f"Aggregating field values for data_source_id={data_source_id}, field_id={field_id}, bins={bins}, min={min}, max={max}, min_inclusive={min_inclusive}, max_inclusive={max_inclusive}"
    )

    with get_sync_db_session(db_url) as db:
        field = db.get(Field, field_id)
        if not field or field.data_source_id != data_source_id:
            raise ValueError(f"Field with ID {field_id} not found in data source {data_source_id}")

        histograms: list[dict[str, Decimal | bool | int]] = []
        bin_size = (max - min) / bins
        for i in range(bins):
            from_ = min + i * bin_size
            to = min + (i + 1) * bin_size
            from_inclusive = min_inclusive if i == 0 else True
            to_inclusive = max_inclusive if i == bins - 1 else False

            stmt = (
                select(func.count())
                .select_from(DataSourceInstance)
                .where(
                    DataSourceInstance.data_source_id == data_source_id,
                    DataSourceInstance.field_id == field.id,
                    DataSourceInstance.value_numeric != None,  # noqa: E711
                    DataSourceInstance.value_numeric >= from_
                    if from_inclusive
                    else DataSourceInstance.value_numeric > from_,
                    DataSourceInstance.value_numeric <= to if to_inclusive else DataSourceInstance.value_numeric < to,
                )
            )
            frequency = db.execute(stmt).scalar_one()

            histograms.append(
                {
                    "from": from_,
                    "to": to,
                    "from_inclusive": from_inclusive,
                    "to_inclusive": to_inclusive,
                    "frequency": frequency,
                }
            )

    return histograms
