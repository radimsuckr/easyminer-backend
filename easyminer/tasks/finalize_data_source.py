import logging

from sqlalchemy import func, insert, select, text, update

from easyminer.database import get_sync_db_session
from easyminer.models.data import DataSource, Field, FieldNumericDetail
from easyminer.models.dynamic_tables import get_data_source_table, get_data_source_value_table
from easyminer.worker import app

logger = logging.getLogger(__name__)


@app.task
def finalize_data_source(data_source_id: int, db_url: str) -> None:
    with get_sync_db_session(db_url) as db:
        data_source = db.get(DataSource, data_source_id)
        if not data_source:
            raise ValueError(f"DataSource {data_source_id} not found")

        source_table = get_data_source_table(data_source_id)
        value_table = get_data_source_value_table(data_source_id)

        # Build covering index for the GROUP BY after all inserts are done
        table_name = source_table.name
        db.execute(
            text(
                f"ALTER TABLE `{table_name}` ADD INDEX `ix_{table_name}_field_values` (`field`, `value_numeric`, `value_nominal`)"
            )
        )

        # 1) Populate value table for ALL fields — same as Scala's MysqlValueBuilder.build()
        db.execute(
            insert(value_table).from_select(
                ["field", "value_nominal", "value_numeric", "frequency"],
                select(
                    source_table.c.field,
                    source_table.c.value_nominal,
                    source_table.c.value_numeric,
                    func.count().label("frequency"),
                ).group_by(
                    source_table.c.field,
                    source_table.c.value_numeric,
                    source_table.c.value_nominal,
                ),
            )
        )

        # 2) Update field unique value counts from the small value table
        for field in db.execute(select(Field.id).where(Field.data_source == data_source_id)):
            field_id = field.id
            db.execute(
                update(Field)
                .where(Field.id == field_id)
                .values(
                    support_nominal=data_source.size,
                    support_numeric=data_source.size,
                    unique_values_size_nominal=select(func.count())
                    .where(value_table.c.field == field_id)
                    .scalar_subquery(),
                    unique_values_size_numeric=select(func.count())
                    .where(value_table.c.field == field_id, value_table.c.value_numeric.isnot(None))
                    .scalar_subquery(),
                )
            )

        # 3) Insert numeric detail for ALL fields at once
        db.execute(
            insert(FieldNumericDetail).from_select(
                ["field_id", "min_value", "max_value", "avg_value"],
                select(
                    Field.id,
                    func.min(source_table.c.value_numeric),
                    func.max(source_table.c.value_numeric),
                    func.avg(source_table.c.value_numeric),
                )
                .select_from(Field.__table__.join(source_table, Field.id == source_table.c.field))
                .where(Field.data_source == data_source_id)
                .group_by(Field.id)
                .having(func.min(source_table.c.value_numeric).isnot(None)),
            )
        )

        db.commit()
        logger.info("Finalized data source %d", data_source_id)
