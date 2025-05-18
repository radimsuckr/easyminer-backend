import logging

from sqlalchemy import func, select

from easyminer.database import get_sync_db_session
from easyminer.models import Field, FieldNumericDetail, Instance
from easyminer.worker import app

logger = logging.getLogger(__name__)


@app.task
def calculate_field_numeric_detail(field_id: int):
    with get_sync_db_session() as db:
        field = db.get(Field, field_id)
        if not field:
            raise ValueError(f"Field with ID {field_id} not found")

        logger.info(f"Calculating numeric field detail for field {field.name}")
        stats = (
            db.execute(
                select(
                    func.min(Instance.value_numeric).label("min_value"),
                    func.max(Instance.value_numeric).label("max_value"),
                    func.avg(Instance.value_numeric).label("avg_value"),
                ).where(Instance.field_id == field.id, Instance.value_numeric.is_not(None))
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
