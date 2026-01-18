from easyminer.database import get_sync_db_session
from easyminer.models.data import DataSource
from easyminer.models.dynamic_tables import create_dataset_tables
from easyminer.models.preprocessing import Dataset
from easyminer.worker import app


@app.task
def create_dataset(data_source_id: int, dataset_name: str, db_url: str):
    # This is done asynchronously to keep API compatibility with the Scala implementation.
    # It's easier than having to mock the flows. We could possibly return a fake task status,
    # store it somewhere and hack around it but it seems like way too much work than to
    # create the entity in Celery...
    with get_sync_db_session(db_url) as db:
        datasource = db.get(DataSource, data_source_id)
        if not datasource:
            raise ValueError(f"Data source with ID {data_source_id} not found")

        # Use renamed column: data_source instead of data_source_id
        dataset = Dataset(name=dataset_name, data_source=data_source_id)
        db.add(dataset)
        db.flush()  # Get the ID

        # Create dynamic tables for this dataset
        sync_engine = db.get_bind()
        create_dataset_tables(sync_engine, dataset.id)

        # Mark dataset as active after tables are created
        dataset.active = True

        db.commit()
