import logging

from sqlalchemy import func, select
from sqlalchemy.dialects.mysql import insert

from easyminer.database import get_sync_db_session
from easyminer.models.data import DataSourceInstance, DataSourceValue, Field
from easyminer.worker import app

logger = logging.getLogger(__name__)


@app.task
def populate_field_values(field_id: int, db_url: str) -> None:
    with get_sync_db_session(db_url) as db:
        field = db.get(Field, field_id)
        if not field:
            raise ValueError(f"Field with ID {field_id} not found")

        logger.info(f"Populating field values for field {field.name} (ID: {field_id})")

        # select the first row_id as a stable identifier for each unique value
        aggregation_query = (
            select(
                func.min(DataSourceInstance.row_id).label("value_id"),
                DataSourceInstance.value_nominal,
                DataSourceInstance.value_numeric,
                func.count(DataSourceInstance.id).label("frequency"),
            )
            .where(DataSourceInstance.field_id == field_id)
            .group_by(DataSourceInstance.value_nominal, DataSourceInstance.value_numeric)
        )

        results = db.execute(aggregation_query).all()
        logger.info(f"Found {len(results)} unique values for field {field.name}")

        total_rows_query = select(func.count(func.distinct(DataSourceInstance.row_id))).where(
            DataSourceInstance.data_source_id == field.data_source_id
        )
        total_rows = db.execute(total_rows_query).scalar_one()

        present_rows_query = select(func.count(func.distinct(DataSourceInstance.row_id))).where(
            DataSourceInstance.field_id == field_id
        )
        present_rows = db.execute(present_rows_query).scalar_one()

        null_frequency = total_rows - present_rows

        values_to_insert = [
            {
                "data_source_id": field.data_source_id,
                "field_id": field_id,
                "value_nominal": row.value_nominal,
                "value_numeric": row.value_numeric,
                "frequency": row.frequency,
            }
            for row in results
        ]

        if null_frequency > 0:
            values_to_insert.append(
                {
                    "data_source_id": field.data_source_id,
                    "field_id": field_id,
                    "value_nominal": None,
                    "value_numeric": None,
                    "frequency": null_frequency,
                }
            )
            logger.info(f"Adding NULL value entry with frequency {null_frequency}")

        if not values_to_insert:
            logger.warning(f"No values found for field {field.name}")
            db.commit()
            return

        stmt = insert(DataSourceValue).values(values_to_insert)
        _ = db.execute(stmt)
        db.commit()

        logger.info(f"Successfully populated {len(values_to_insert)} unique values for field {field.name}")
