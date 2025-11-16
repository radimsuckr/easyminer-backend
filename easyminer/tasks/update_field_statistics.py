import logging

from sqlalchemy import func, select

from easyminer.database import get_sync_db_session
from easyminer.models.data import DataSourceInstance, Field
from easyminer.worker import app

logger = logging.getLogger(__name__)


@app.task
def update_field_statistics(field_id: int, db_url: str) -> None:
    """Calculate field statistics after upload completes.

    Calculates:
    - support_nominal: Count of rows containing this field (any value)
    - support_numeric: Count of rows with numeric values
    - unique_values_size_nominal: Distinct count of all string values
    - unique_values_size_numeric: Distinct count of numeric values
    """
    with get_sync_db_session(db_url) as db:
        field = db.get(Field, field_id)
        if not field:
            raise ValueError(f"Field with ID {field_id} not found")

        logger.info(f"Calculating statistics for field {field.name} (type: {field.data_type})")

        field.support_nominal = db.execute(
            select(func.count(func.distinct(DataSourceInstance.row_id))).where(DataSourceInstance.field_id == field.id)
        ).scalar_one()

        field.unique_values_size_nominal = db.execute(
            select(func.count(func.distinct(DataSourceInstance.value_nominal))).where(
                DataSourceInstance.field_id == field.id
            )
        ).scalar_one()

        field.support_numeric = db.execute(
            select(func.count(func.distinct(DataSourceInstance.row_id))).where(
                DataSourceInstance.field_id == field.id,
                DataSourceInstance.value_numeric.isnot(None),
            )
        ).scalar_one()

        field.unique_values_size_numeric = db.execute(
            select(func.count(func.distinct(DataSourceInstance.value_numeric))).where(
                DataSourceInstance.field_id == field.id,
                DataSourceInstance.value_numeric.isnot(None),
            )
        ).scalar_one()

        db.commit()

        logger.info(
            f"Field {field.name} statistics: "
            + f"support_nominal={field.support_nominal}, "
            + f"support_numeric={field.support_numeric}, "
            + f"unique_nominal={field.unique_values_size_nominal}, "
            + f"unique_numeric={field.unique_values_size_numeric}"
        )
