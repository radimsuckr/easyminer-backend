import logging

from sqlalchemy import select

from easyminer.database import get_sync_db_session
from easyminer.models.data import DataSource
from easyminer.schemas.data import MediaType
from easyminer.worker import app


@app.task
def process_csv(
    data_source_id: int,
    upload_media_type: MediaType,
    encoding: str,
    separator: str,
    quote_char: str,
):
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

    for chunk in chunks:
        logger.info(f"Processing chunk {chunk.id} of data source {data_source_id}")
