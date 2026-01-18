import logging

from sqlalchemy import func, select

from easyminer.database import get_sync_db_session
from easyminer.models.data import Field
from easyminer.models.dynamic_tables import get_data_source_table
from easyminer.worker import app

logger = logging.getLogger(__name__)


@app.task
def update_field_statistics(field_id: int, data_source_id: int, db_url: str) -> None:
    """Calculate field statistics after upload completes.

    Calculates:
    - support_nominal: Count of rows containing this field (any value)
    - support_numeric: Count of rows with numeric values
    - unique_values_size_nominal: Distinct count of all string values
    - unique_values_size_numeric: Distinct count of numeric values
    """
    with get_sync_db_session(db_url) as db:
        # Query field using composite PK (id, data_source)
        field = db.execute(
            select(Field).where(Field.id == field_id, Field.data_source == data_source_id)
        ).scalar_one_or_none()
        if not field:
            raise ValueError(f"Field with ID {field_id} in data source {data_source_id} not found")

        logger.info(f"Calculating statistics for field {field.name} (type: {field.data_type})")

        # Get the dynamic table for this data source
        instance_table = get_data_source_table(field.data_source)

        # Use dynamic table column names: id (was row_id), field (was field_id)
        field.support_nominal = db.execute(
            select(func.count(func.distinct(instance_table.c.id))).where(instance_table.c.field == field.id)
        ).scalar_one()

        field.unique_values_size_nominal = db.execute(
            select(func.count(func.distinct(instance_table.c.value_nominal))).where(instance_table.c.field == field.id)
        ).scalar_one()

        field.support_numeric = db.execute(
            select(func.count(func.distinct(instance_table.c.id))).where(
                instance_table.c.field == field.id,
                instance_table.c.value_numeric.isnot(None),
            )
        ).scalar_one()

        field.unique_values_size_numeric = db.execute(
            select(func.count(func.distinct(instance_table.c.value_numeric))).where(
                instance_table.c.field == field.id,
                instance_table.c.value_numeric.isnot(None),
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
