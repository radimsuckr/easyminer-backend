import logging

from sqlalchemy import func, select

from easyminer.database import get_sync_db_session
from easyminer.models.data import DataSourceInstance, Field, FieldNumericDetail
from easyminer.worker import app

logger = logging.getLogger(__name__)


@app.task
def calculate_field_numeric_detail(field_id: int, db_url: str) -> None:
    with get_sync_db_session(db_url) as db:
        field = db.get(Field, field_id)
        if not field:
            raise ValueError(f"Field with ID {field_id} not found")

        logger.info(f"Calculating numeric detail for field {field.name} (type: {field.data_type})")

        stats = db.execute(
            select(
                func.min(DataSourceInstance.value_numeric).label("min_value"),
                func.max(DataSourceInstance.value_numeric).label("max_value"),
                func.avg(DataSourceInstance.value_numeric).label("avg_value"),
                func.count(DataSourceInstance.value_numeric).label("count"),
            ).where(
                DataSourceInstance.field_id == field.id,
                DataSourceInstance.value_numeric.is_not(None),
            )
        ).one()

        if stats.count > 0:
            logger.info(
                f"Field {field.name} numeric detail: "
                + f"min={stats.min_value}, max={stats.max_value}, avg={stats.avg_value}, count={stats.count}"
            )

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
            logger.info(f"Field {field.name} has no numeric values, skipping numeric detail")

        db.commit()
        logger.info(f"Numeric detail calculated successfully for field {field.name}")
