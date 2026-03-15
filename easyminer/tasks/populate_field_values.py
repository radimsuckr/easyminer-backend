import logging

from sqlalchemy import func, insert, select
from sqlalchemy.sql.expression import distinct

from easyminer.database import get_sync_db_session
from easyminer.models.data import Field
from easyminer.models.dynamic_tables import get_data_source_table, get_data_source_value_table
from easyminer.worker import app

logger = logging.getLogger(__name__)


@app.task
def populate_field_values(field_id: int, data_source_id: int, db_url: str) -> None:
    with get_sync_db_session(db_url) as db:
        # Query field using composite PK (id, data_source)
        field = db.execute(
            select(Field).where(Field.id == field_id, Field.data_source == data_source_id)
        ).scalar_one_or_none()
        if not field:
            raise ValueError(f"Field with ID {field_id} not found in data source {data_source_id}")

        logger.info(f"Populating field values for field {field.name} (ID: {field_id})")

        # Get dynamic tables
        source_table = get_data_source_table(data_source_id)
        value_table = get_data_source_value_table(data_source_id)

        # Aggregate unique values with frequencies from dynamic data_source_{ID} table
        aggregation_query = (
            select(
                func.min(source_table.c.id).label("value_id"),
                source_table.c.value_nominal,
                source_table.c.value_numeric,
                func.count(source_table.c.pid).label("frequency"),
            )
            .where(source_table.c.field == field_id)
            .group_by(source_table.c.value_nominal, source_table.c.value_numeric)
        )

        results = db.execute(aggregation_query).all()
        logger.info(f"Found {len(results)} unique values for field {field.name}")

        # Calculate NULL frequency
        total_rows_query = select(func.count(distinct(source_table.c.id))).select_from(source_table)
        total_rows = db.execute(total_rows_query).scalar_one()

        present_rows_query = select(func.count(distinct(source_table.c.id))).where(source_table.c.field == field_id)
        present_rows = db.execute(present_rows_query).scalar_one()

        null_frequency = total_rows - present_rows

        # Build values to insert into dynamic value_{ID} table
        values_to_insert = [
            {
                "field": field_id,
                "value_nominal": row.value_nominal,
                "value_numeric": row.value_numeric,
                "frequency": row.frequency,
            }
            for row in results
        ]

        if null_frequency > 0:
            values_to_insert.append(
                {
                    "field": field_id,
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

        _ = db.execute(insert(value_table), values_to_insert)
        db.commit()

        logger.info(f"Successfully populated {len(values_to_insert)} unique values for field {field.name}")
