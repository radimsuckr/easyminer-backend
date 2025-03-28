from sqlalchemy import select

from easyminer.database import get_sync_db_session
from easyminer.models.data import DataSource
from easyminer.worker import app


@app.task
def process_csv(
    data_source_id: int,
    upload_media_type: str,
    encoding: str,
    separator: str,
    quote_char: str,
):
    if upload_media_type != "csv":
        raise ValueError("Only CSV data sources are supported")

    with get_sync_db_session() as session:
        data_source_record = session.execute(
            select(DataSource).filter(DataSource.id == data_source_id)
        ).scalar_one_or_none()
        if not data_source_record:
            raise ValueError(f"Data source with ID {data_source_id} not found")

        chunks = data_source_record.upload.chunks
    tasks: list[str] = []
    for chunk in chunks:
        task = process_csv_chunk.delay(
            chunk_id=chunk.id,
            encoding=encoding,
            separator=separator,
            quote_char=quote_char,
        )
        tasks.append(task.id)


@app.task
def process_csv_chunk(
    chunk_id: int,
    encoding: str,
    separator: str,
    quote_char: str,
) -> int:
    # Process the CSV chunk
    print(chunk_id)
    return chunk_id
