import logging

from sqlalchemy import func, select

from easyminer.database import get_sync_db_session
from easyminer.models.data import DataSourceInstance, Field, FieldNumericDetail
from easyminer.schemas.data import FieldType
from easyminer.worker import app

logger = logging.getLogger(__name__)


@app.task
def calculate_field_numeric_detail(field_id: int, db_url: str):
    with get_sync_db_session(db_url) as db:
        field = db.get(Field, field_id)
        if not field:
            raise ValueError(f"Field with ID {field_id} not found")

        logger.info(f"Calculating statistics for field {field.name} (type: {field.data_type})")

        if field.data_type == FieldType.numeric:
            stats = db.execute(
                select(
                    func.count(func.distinct(DataSourceInstance.value_numeric)).label("unique_count"),
                    func.count(DataSourceInstance.value_numeric).label("support"),
                    func.min(DataSourceInstance.value_numeric).label("min_value"),
                    func.max(DataSourceInstance.value_numeric).label("max_value"),
                    func.avg(DataSourceInstance.value_numeric).label("avg_value"),
                ).where(
                    DataSourceInstance.field_id == field.id,
                    DataSourceInstance.value_numeric.is_not(None),
                )
            ).one()

            field.unique_values_size_numeric = stats.unique_count
            field.support_numeric = stats.support

            logger.info(
                f"Field {field.name} numeric stats: unique={stats.unique_count}, "
                + f"support={stats.support}, min={stats.min_value}, max={stats.max_value}, avg={stats.avg_value}"
            )

            if stats.support > 0:
                field_numeric_detail = db.get(FieldNumericDetail, field.id)
                if not field_numeric_detail:
                    field_numeric_detail = FieldNumericDetail(
                        id=field.id, min_value=stats.min_value, max_value=stats.max_value, avg_value=stats.avg_value
                    )
                    db.add(field_numeric_detail)
                else:
                    field_numeric_detail.min_value = stats.min_value
                    field_numeric_detail.max_value = stats.max_value
                    field_numeric_detail.avg_value = stats.avg_value
            else:
                logger.warning(f"Field {field.name} (id={field.id}) has no numeric values")
        else:
            stats = db.execute(
                select(
                    func.count(func.distinct(DataSourceInstance.value_nominal)).label("unique_count"),
                    func.count(DataSourceInstance.value_nominal).label("support"),
                ).where(
                    DataSourceInstance.field_id == field.id,
                    DataSourceInstance.value_nominal.is_not(None),
                )
            ).one()

            field.unique_values_size_nominal = stats.unique_count
            field.support_nominal = stats.support

            logger.info(f"Field {field.name} nominal stats: unique={stats.unique_count}, support={stats.support}")

        db.commit()
        logger.info(f"Statistics calculated successfully for field {field.name}")
