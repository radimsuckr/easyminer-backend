import csv
import logging

import pydantic
from sqlalchemy import insert, update

from easyminer.database import get_sync_db_session
from easyminer.models import Chunk, Field, Instance, Upload, UploadState
from easyminer.schemas.data import FieldType
from easyminer.worker import app

logger = logging.getLogger(__name__)


class ProcessChunkResult(pydantic.BaseModel):
    chunk_id: int
    status: str


@app.task(pydantic=True)
def process_chunk(
    chunk_id: int, original_state: UploadState, separator: str, quote_char: str, escape_char: str
) -> ProcessChunkResult:
    with get_sync_db_session() as db:
        chunk = db.get(Chunk, chunk_id)
        if not chunk:
            raise ValueError(f"Chunk with ID {chunk_id} not found")

        logger.info(f"Processing chunk {chunk_id} with path {chunk.path}")

        with open(chunk.path, encoding="utf-8") as file:
            reader = csv.reader(
                file,
                delimiter=separator,
                quotechar=quote_char,
                escapechar=escape_char,
            )
            if original_state == UploadState.initialized:
                # Parse first row from CSV as header
                header = next(reader)
                logger.info(f"Header: {header}")
                # Process header
                for i, col in enumerate(header):
                    logger.info(f"Processing header column: {col}")
                    field = Field(
                        name=col, index=i, data_type=FieldType.numeric, data_source_id=chunk.upload.data_source.id
                    )
                    db.add(field)
                db.flush()
            fields = db.query(Field).filter(Field.data_source_id == chunk.upload.data_source.id).all()
            col_fields = {field.index: field for field in fields}

            row_counter = 0
            batch_size = 1000
            instance_values: list[dict[str, str | float | None]] = []
            for row in reader:
                logger.debug(f"Row: {row}")
                for i, col in enumerate(row):
                    col_float: float | None = None
                    try:
                        col_float = float(col)
                    except ValueError:
                        pass
                    instance_values.append(
                        {
                            "row_id": row_counter,
                            "col_id": i,
                            "value_nominal": col,
                            "value_numeric": col_float,
                            "field_id": col_fields[i].id,
                            "data_source_id": chunk.upload.data_source.id,
                        }
                    )
                if len(instance_values) % batch_size == 0:
                    logger.debug(f"Processing batch of {len(instance_values)} instances")
                    _ = db.execute(insert(Instance), instance_values)
                    instance_values.clear()
            if len(instance_values) > 0:
                logger.debug(f"Processing last batch of {len(instance_values)} instances")
                _ = db.execute(insert(Instance), instance_values)
                instance_values.clear()

        # Unlock the upload
        logger.info("Unlocking the upload")
        _ = db.execute(update(Upload).values(state=UploadState.ready).where(Upload.id == chunk.upload_id))
        logger.info("Unlocked")

        db.commit()

    return ProcessChunkResult(
        chunk_id=chunk_id,
        status="processed",
    )
