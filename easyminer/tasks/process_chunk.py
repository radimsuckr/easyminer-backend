import csv
import logging
import re
from decimal import Decimal, InvalidOperation

import pydantic
from sqlalchemy import insert, select, update

from easyminer.database import get_sync_db_session
from easyminer.models.data import (
    Chunk,
    DataSource,
    DataSourceInstance,
    Field,
    Upload,
    UploadState,
)
from easyminer.schemas.data import FieldType
from easyminer.worker import app

logger = logging.getLogger(__name__)

# Pre-compiled regex for numeric validation (avoids expensive exception handling)
# Matches standard numeric notations: optional sign, digits, optional decimal point
# Does NOT support scientific notation or other complex formats for simplicity
NUMERIC_PATTERN = re.compile(r"^-?\d+\.?\d*$")


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

        logger.info(f"Processing chunk {chunk_id} with path {chunk.path}")
        logger.info(f"Using encoding: {encoding}, null_values: {null_values}, data_types: {data_types}")

        with open(chunk.path, encoding=encoding, buffering=8192 * 16) as file:
            reader = csv.reader(file, delimiter=separator, quotechar=quote_char, escapechar=escape_char)
            if original_state == UploadState.initialized:
                # Parse first row from CSV as header
                header = next(reader)
                logger.info(f"Header: {header}")
                # Process header
                for i, col in enumerate(header):
                    if data_types[i] is None:
                        logger.debug(f"Skipping column {i} ({col}) - null type")
                        continue

                    logger.info(f"Processing header column: {col}")
                    field = Field(
                        name=col,
                        index=i,
                        data_type=data_types[i],
                        data_source_id=chunk.upload.data_source.id,
                    )
                    db.add(field)
                db.flush()

            # Optimized field loading: only select needed columns instead of full objects
            # This avoids loading unnecessary relationships and uses modern SQLAlchemy 2.0 syntax
            field_stmt = (
                select(Field.id, Field.index)
                .where(Field.data_source_id == chunk.upload.data_source.id)
                .order_by(Field.index)
            )
            field_results = db.execute(field_stmt).all()
            col_fields = {row.index: row.id for row in field_results}

            # Optimized row counter: use the data source size instead of expensive COUNT query
            # For subsequent chunks, the size is already tracked in the DataSource model
            non_skipped_fields = [idx for idx in col_fields.keys() if data_types[idx] is not None]
            row_counter = chunk.upload.data_source.size if non_skipped_fields else 0

            # Optimized batch size for better insert performance
            batch_size = 10000
            instance_values: list[dict[str, str | Decimal | int | None]] = []
            rows_processed = 0  # Track rows processed in this chunk
            for row in reader:
                logger.debug(f"Row: {row}")
                for i, col in enumerate(row):
                    if data_types[i] is None:
                        continue

                    if col in null_values:
                        continue

                    col_nominal = col
                    col_decimal: Decimal | None = None

                    # Optimized numeric conversion: pre-validate with regex to avoid expensive exception handling
                    # We check ALL values (not just numeric fields) because we store numeric representation
                    # for any value that can be parsed as a number, regardless of field type
                    if col and NUMERIC_PATTERN.match(col):
                        try:
                            col_decimal = Decimal(col)
                        except (InvalidOperation, ValueError):
                            # Rare edge case: overflow or other numeric error
                            pass

                    instance_values.append(
                        {
                            "row_id": row_counter,
                            "col_id": i,
                            "value_nominal": col_nominal,
                            "value_numeric": col_decimal,
                            "field_id": col_fields[i],  # Now directly contains field.id
                            "data_source_id": chunk.upload.data_source.id,
                        }
                    )
                row_counter += 1
                rows_processed += 1
                if len(instance_values) % batch_size == 0:
                    logger.debug(f"Processing batch of {len(instance_values)} instances")
                    _ = db.execute(insert(DataSourceInstance), instance_values)
                    instance_values.clear()
                    # Commit every batch to avoid accumulating huge transactions
                    db.commit()
            if len(instance_values) > 0:
                logger.debug(f"Processing last batch of {len(instance_values)} instances")
                _ = db.execute(insert(DataSourceInstance), instance_values)
                instance_values.clear()
                db.commit()

        # Unlock the upload and update data source size incrementally
        logger.info("Unlocking the upload")
        _ = db.execute(update(Upload).values(state=UploadState.ready).where(Upload.id == chunk.upload_id))

        # Update size incrementally instead of expensive COUNT query
        # Add the number of rows processed in this chunk to the existing size
        _ = db.execute(
            update(DataSource)
            .values(size=DataSource.size + rows_processed)
            .where(DataSource.id == chunk.upload.data_source.id)
        )
        logger.info(f"Unlocked - processed {rows_processed} rows")

        db.commit()

    return ProcessChunkResult(chunk_id=chunk_id, status="processed")
