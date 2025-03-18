from celery import Celery

from easyminer.config import celery_backend, celery_broker

app = Celery(
    "easyminer-backend-worker",
    broker=celery_broker,
    backend=celery_backend,
)
app.conf.update(
    broker_connection_retry_on_startup=True,
)
app.autodiscover_tasks(["easyminer"])
