import csv
import io
import logging
import re
from decimal import Decimal, InvalidOperation

import pydantic
from celery.exceptions import Retry
from sqlalchemy import insert, select, text

from easyminer.database import get_sync_db_session
from easyminer.models.data import (
    Chunk,
    ChunkStatus,
    Field,
)
from easyminer.models.dynamic_tables import (
    create_data_source_tables,
    get_data_source_table,
)
from easyminer.schemas.data import FieldType
from easyminer.worker import app

logger = logging.getLogger(__name__)

NUMERIC_PATTERN = re.compile(r"^-?\d+\.?\d*$")


class ProcessChunkResult(pydantic.BaseModel):
    chunk_id: int
    status: str


@app.task(bind=True, pydantic=True, max_retries=30)
def process_chunk(
    self,
    chunk_id: int,
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

        chunk.status = ChunkStatus.processing
        db.commit()

        data_source = chunk.upload.data_source
        data_source_id = data_source.id

        try:
            sync_engine = db.get_bind()

            if chunk.is_first:
                create_data_source_tables(sync_engine, data_source_id)

                with open(chunk.path, encoding=encoding, buffering=8192 * 16) as file:
                    reader = csv.reader(file, delimiter=separator, quotechar=quote_char, escapechar=escape_char)
                    header = next(reader)

                field_rows = []
                for i, col in enumerate(header):
                    if data_types[i] is None:
                        continue
                    field_rows.append(
                        {
                            "name": col,
                            "index": i,
                            "data_type": data_types[i],
                            "data_source": data_source_id,
                            "unique_values_size_nominal": 0,
                            "unique_values_size_numeric": 0,
                            "support_nominal": 0,
                            "support_numeric": 0,
                        }
                    )
                db.execute(insert(Field), field_rows)
                db.flush()

                data_source.active = True
                db.commit()
            else:
                # Wait for first chunk to create tables and fields
                if not data_source.active:
                    self.retry(countdown=2)

            # Count lines to atomically claim a row_id range
            with open(chunk.path, encoding=encoding, buffering=8192 * 16) as file:
                content = file.read()
            line_count = content.count("\n")
            if chunk.is_first:
                line_count -= 1  # exclude header
            if content and not content.endswith("\n"):
                line_count += 1
            if line_count <= 0:
                chunk.status = ChunkStatus.processed
                db.commit()
                return ProcessChunkResult(chunk_id=chunk_id, status="processed")

            # Atomically claim row_id range on a separate pooled connection
            # (avoids MariaDB error 1020 — stale row version on the ORM connection)
            engine = db.get_bind()
            with engine.connect() as conn:
                conn.execute(
                    text("UPDATE data_source SET size = LAST_INSERT_ID(size) + :count WHERE id = :id"),
                    {"count": line_count, "id": data_source_id},
                )
                start_row_id = conn.execute(text("SELECT LAST_INSERT_ID()")).scalar_one()
                conn.commit()

            # Parse and insert data
            instance_table = get_data_source_table(data_source_id)
            field_stmt = select(Field.id, Field.index).where(Field.data_source == data_source_id).order_by(Field.index)
            col_fields = {row.index: row.id for row in db.execute(field_stmt).all()}

            reader = csv.reader(
                io.StringIO(content),
                delimiter=separator,
                quotechar=quote_char,
                escapechar=escape_char,
            )
            if chunk.is_first:
                next(reader)  # skip header

            row_counter = start_row_id
            batch_size = 10000
            instance_values: list[dict[str, str | float | int | None]] = []

            for row in reader:
                for i, col in enumerate(row):
                    if data_types[i] is None:
                        continue
                    if col in null_values:
                        continue

                    col_numeric: float | None = None
                    if col and NUMERIC_PATTERN.match(col):
                        try:
                            col_numeric = float(Decimal(col))
                        except (InvalidOperation, ValueError):
                            pass

                    instance_values.append(
                        {
                            "id": row_counter,
                            "field": col_fields[i],
                            "value_nominal": col,
                            "value_numeric": col_numeric,
                        }
                    )

                row_counter += 1
                if len(instance_values) >= batch_size:
                    db.execute(insert(instance_table), instance_values)
                    instance_values.clear()
                    db.commit()

            if instance_values:
                db.execute(insert(instance_table), instance_values)
                db.commit()

            chunk.status = ChunkStatus.processed
            db.commit()
            logger.info("Chunk %d processed (%d rows)", chunk_id, line_count)

        except Retry:
            raise
        except Exception:
            chunk.status = ChunkStatus.failed
            db.commit()
            raise

    return ProcessChunkResult(chunk_id=chunk_id, status="processed")
