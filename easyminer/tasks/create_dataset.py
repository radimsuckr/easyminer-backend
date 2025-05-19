from easyminer.database import get_sync_db_session
from easyminer.models.data import DataSource
from easyminer.models.preprocessing import Dataset
from easyminer.worker import app


@app.task
def create_dataset(data_source_id: int, dataset_name: str):
    # This is done asynchronously to keep API compatibility with the Scala implementation. It's easier than having to mock the flows. We could possibly return a fake task status, store it somewhere and hack around it but it seems like way too much work than to create the entity in Celery...
    with get_sync_db_session() as db:
        datasource = db.get(DataSource, data_source_id)
        if not datasource:
            raise ValueError(f"Data source with ID {data_source_id} not found")

        dataset = Dataset(name=dataset_name, data_source_id=data_source_id)
        db.add(dataset)
        db.commit()
