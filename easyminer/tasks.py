import csv
import logging

from sqlalchemy import insert, select

from easyminer.database import get_sync_db_session
from easyminer.models import DataSource, Field, FieldType
from easyminer.schemas.data import MediaType, UploadResponseSchema
from easyminer.worker import app


@app.task
def process_csv(
    data_source_id: int,
    upload_media_type: MediaType,
    encoding: str,
    separator: str,
    quote_char: str,
) -> UploadResponseSchema:
    logger = logging.getLogger(__name__)
    if upload_media_type != MediaType.csv:
        raise ValueError("Only CSV data sources are supported")

    with get_sync_db_session() as session:
        data_source_record = session.execute(
            select(DataSource).filter(DataSource.id == data_source_id)
        ).scalar_one_or_none()
        if not data_source_record:
            raise ValueError(f"Data source with ID {data_source_id} not found")
        chunks = data_source_record.upload.chunks

    headers: list[str] = []
    str_chunks: list[str] = []
    for ix, chunk in enumerate(chunks):
        logger.info(f"Processing chunk {chunk.id} of data source {data_source_id}")
        with open(chunk.path, encoding=encoding) as f:
            reader = csv.reader(f, delimiter=separator, quotechar=quote_char)
            # Get headers row
            if ix == 0:
                headers = next(reader)
                str_chunks.append(str(headers))
            for row in reader:
                str_chunks.append(str(row))

    # Determine field types
    # TODO: For now, all fields are nominal

    # Create fields in database
    with get_sync_db_session() as session:
        for header in headers:
            logger.info(f"Header: {header}")
            # Check if field already exists
            existing_field = session.execute(
                select(Field).filter(
                    Field.name == header, Field.data_source_id == data_source_id
                )
            ).scalar_one_or_none()
            if existing_field:
                logger.warning(f"Field {header} already exists, skipping.")
                continue
            query = (
                insert(Field)
                .values(
                    name=header,
                    data_type=FieldType.nominal,
                    data_source_id=data_source_id,
                    unique_count=0,
                    support=0,
                )
                .returning(Field.id)
            )
            id = session.execute(query).scalar_one()
            session.commit()
            if not id:
                logger.error(f"Failed to insert field {header}")
                continue
            logger.info(f"Inserted field {header} with ID {id}")

    return UploadResponseSchema(
        id=data_source_record.id,
        name=data_source_record.name,
        type=data_source_record.type,
        size=data_source_record.row_count,
    )


@app.task
def aggregate_field_values(
    data_source_id: int,
    field_id: int,
    bins: int,
    min: float,
    max: float,
    min_inclusive: bool,
    max_inclusive: bool,
):
    pass
