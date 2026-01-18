import csv
import logging
from decimal import Decimal, InvalidOperation

import pydantic
from sqlalchemy import func, insert, select, update

from easyminer.database import get_sync_db_session
from easyminer.models.data import (
    Chunk,
    DataSource,
    Field,
    Upload,
    UploadState,
)
from easyminer.models.dynamic_tables import (
    create_data_source_tables,
    get_data_source_table,
)
from easyminer.schemas.data import FieldType
from easyminer.worker import app

logger = logging.getLogger(__name__)


class ProcessChunkResult(pydantic.BaseModel):
    chunk_id: int
    status: str


@app.task(pydantic=True)
def process_chunk(
    chunk_id: int,
    original_state: UploadState,
    separator: str,
    quote_char: str,
    escape_char: str,
    encoding: str,
    null_values: list[str],
    data_types: list[FieldType | None],
    db_url: str,
) -> ProcessChunkResult:
    with get_sync_db_session(db_url) as db:
        chunk = db.get(Chunk, chunk_id)
        if not chunk:
            raise ValueError(f"Chunk with ID {chunk_id} not found")

        data_source = chunk.upload.data_source
        data_source_id = data_source.id

        logger.info(f"Processing chunk {chunk_id} with path {chunk.path}")
        logger.info(f"Using encoding: {encoding}, null_values: {null_values}, data_types: {data_types}")

        # Get sync engine for dynamic table operations
        sync_engine = db.get_bind()

        with open(chunk.path, encoding=encoding) as file:
            reader = csv.reader(file, delimiter=separator, quotechar=quote_char, escapechar=escape_char)
            if original_state == UploadState.initialized:
                # Create dynamic tables for this data source
                logger.info(f"Creating dynamic tables for data source {data_source_id}")
                create_data_source_tables(sync_engine, data_source_id)

                # Parse first row from CSV as header
                header = next(reader)
                logger.info(f"Header: {header}")
                # Process header - manually assign field IDs since composite PK doesn't support autoincrement
                field_id = 1
                for i, col in enumerate(header):
                    if data_types[i] is None:
                        logger.debug(f"Skipping column {i} ({col}) - null type")
                        continue

                    logger.info(f"Processing header column: {col}")
                    field = Field(
                        id=field_id,
                        name=col,
                        index=i,
                        data_type=data_types[i],
                        data_source=data_source_id,
                    )
                    db.add(field)
                    field_id += 1
                db.flush()

                # Mark data source as active after tables are created
                data_source.active = True
                db.flush()

            # Get the dynamic table for this data source
            instance_table = get_data_source_table(data_source_id)

            fields = db.query(Field).filter(Field.data_source == data_source_id).all()
            col_fields = {field.index: field for field in fields}

            # Count existing rows in dynamic table
            upload_size = db.execute(select(func.count()).select_from(instance_table)).scalar_one()
            # Only count non-skipped fields for row counter calculation
            non_skipped_fields = [f for f in fields if data_types[f.index] is not None]
            row_counter = int(upload_size / len(non_skipped_fields)) if non_skipped_fields else 0
            batch_size = 1000
            instance_values: list[dict[str, str | float | int | None]] = []
            for row in reader:
                logger.debug(f"Row: {row}")
                for i, col in enumerate(row):
                    if data_types[i] is None:
                        continue

                    if col in null_values:
                        continue

                    col_nominal = col
                    col_numeric: float | None = None

                    try:
                        col_numeric = float(Decimal(col))
                    except InvalidOperation:
                        pass

                    # Use dynamic table column names: id (was row_id), field (was field_id)
                    instance_values.append(
                        {
                            "id": row_counter,
                            "field": col_fields[i].id,
                            "value_nominal": col_nominal,
                            "value_numeric": col_numeric,
                        }
                    )
                row_counter += 1
                if len(instance_values) % batch_size == 0:
                    logger.debug(f"Processing batch of {len(instance_values)} instances")
                    _ = db.execute(insert(instance_table), instance_values)
                    instance_values.clear()
            if len(instance_values) > 0:
                logger.debug(f"Processing last batch of {len(instance_values)} instances")
                _ = db.execute(insert(instance_table), instance_values)
                instance_values.clear()

        # Unlock the upload
        logger.info("Unlocking the upload")
        _ = db.execute(update(Upload).values(state=UploadState.ready).where(Upload.id == chunk.upload_id))
        upload_size = db.execute(select(func.count()).select_from(instance_table)).scalar_one()
        _ = db.execute(update(DataSource).values(size=upload_size).where(DataSource.id == data_source_id))
        logger.info("Unlocked")

        db.commit()

    return ProcessChunkResult(chunk_id=chunk_id, status="processed")
