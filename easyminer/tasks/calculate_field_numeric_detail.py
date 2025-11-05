import logging

from sqlalchemy import func, select

from easyminer.database import get_sync_db_session
from easyminer.models.data import DataSourceInstance, Field, FieldNumericDetail
from easyminer.schemas.data import FieldType
from easyminer.worker import app

logger = logging.getLogger(__name__)


@app.task
def calculate_field_numeric_detail(field_id: int, db_url: str | None = None):
    with get_sync_db_session(db_url) as db:
        field = db.get(Field, field_id)
        if not field:
            raise ValueError(f"Field with ID {field_id} not found")
        if field.data_type != FieldType.numeric:
            raise ValueError(f"Field {field.name} is not numeric, skipping detail calculation")

        logger.info(f"Calculating numeric field detail for field {field.name}")

        # Check if there are any numeric values
        numeric_count = db.execute(
            select(func.count())
            .select_from(DataSourceInstance)
            .where(
                DataSourceInstance.field_id == field.id,
                DataSourceInstance.value_numeric.is_not(None),
            )
        ).scalar_one()

        if numeric_count == 0:
            logger.warning(f"Field {field.name} (id={field.id}) has no numeric values, skipping detail calculation")
            return

        stats = (
            db.execute(
                select(
                    func.min(DataSourceInstance.value_numeric).label("min_value"),
                    func.max(DataSourceInstance.value_numeric).label("max_value"),
                    func.avg(DataSourceInstance.value_numeric).label("avg_value"),
                ).where(
                    DataSourceInstance.field_id == field.id,
                    DataSourceInstance.value_numeric.is_not(None),
                )
            )
            .tuples()
            .one()
        )

        field_numeric_detail = db.get(FieldNumericDetail, field.id)
        if not field_numeric_detail:
            field_numeric_detail = FieldNumericDetail(
                id=field.id, min_value=stats[0], max_value=stats[1], avg_value=stats[2]
            )
            db.add(field_numeric_detail)
        else:
            field_numeric_detail.min_value = stats[0]
            field_numeric_detail.max_value = stats[1]
            field_numeric_detail.avg_value = stats[2]
        db.commit()
