import logging
import logging.config
from uuid import UUID

from celery import Celery
from celery.signals import (
    after_task_publish,
    before_task_publish,
    celeryd_init,
    task_postrun,
    task_prerun,
)
from kombu import Exchange
from sqlalchemy import insert, update

from easyminer.config import celery_backend, celery_broker, logging_config
from easyminer.database import get_sync_db_session
from easyminer.models.task import Task, TaskStatusEnum

logger = logging.getLogger(__name__)

app = Celery("easyminer-backend-worker", broker=celery_broker, backend=celery_backend)
app.conf.update(
    broker_connection_retry_on_startup=True,
    accept_content={"pickle"},
    result_accept_content={"pickle"},
    event_serializer="pickle",
    task_serializer="pickle",
    result_serializer="pickle",
)
app.autodiscover_tasks(["easyminer.tasks"])


@celeryd_init.connect()
def configure_logging(*args, **kwargs):
    logging.config.dictConfig(logging_config)


def get_db_url_from_headers(kwargs) -> str:
    db_url: str = kwargs.get("headers", {}).get("db_url")
    if not db_url:
        raise ValueError("db_url not found in task headers")
    return db_url


@before_task_publish.connect()
def before_task_publish_handler(body, exchange, routing_key, *args, **kwargs):
    logger = logging.getLogger(__name__)
    header_id: str = kwargs["headers"]["id"]
    try:
        db_url = get_db_url_from_headers(kwargs)
    except ValueError as e:
        logger.error(f"Failed to get db_url for task {header_id}: {e}")
        return

    logger.debug(f"Task {header_id} published")

    with get_sync_db_session(db_url) as session:
        query = insert(Task).values(task_id=UUID(header_id), name=header_id, status=TaskStatusEnum.pending)
        result = session.execute(query)
        session.commit()

    if result.rowcount == 0:
        logger.error(f"Task {header_id} not saved")


@after_task_publish.connect()
def after_task_publish_handler(body, exchange: str | Exchange, routing_key, *args, **kwargs):
    logger = logging.getLogger(__name__)
    header_id: str = kwargs["headers"]["id"]
    try:
        db_url = get_db_url_from_headers(kwargs)
    except ValueError as e:
        logger.error(f"Failed to get db_url for task {header_id}: {e}")
        return

    logger.debug(f"Task {header_id} published")

    with get_sync_db_session(db_url) as session:
        # TODO: we should probably also check Redis if the task exists there
        query = update(Task).where(Task.task_id == UUID(header_id)).values(status=TaskStatusEnum.scheduled)
        result = session.execute(query)
        session.commit()

    if result.rowcount == 0:
        logger.error(f"Task {header_id} not found")


@task_prerun.connect()
def task_prerun_handler(task_id: str, task, *args, **kwargs):
    logger = logging.getLogger(__name__)

    # Get db_url from task headers (consistent with publish handlers)
    db_url = task.request.headers.get("db_url") if hasattr(task.request, "headers") else None

    if not db_url:
        logger.error(f"Task {task_id}: db_url not found in task headers")
        return

    with get_sync_db_session(db_url) as session:
        update_query = update(Task).filter(Task.task_id == UUID(task_id)).values(status=TaskStatusEnum.started)
        result = session.execute(update_query)
        session.commit()

    if result.rowcount == 0:
        logger.error(f"Task with ID {task_id} not found")


@task_postrun.connect()
def task_postrun_handler(task_id: str, task, retval, state, *args, **kwargs):
    logger = logging.getLogger(__name__)

    # Get db_url from task headers (consistent with publish handlers)
    db_url = task.request.headers.get("db_url") if hasattr(task.request, "headers") else None

    if not db_url:
        logger.error(f"Task {task_id}: db_url not found in task headers")
        return

    with get_sync_db_session(db_url) as session:
        update_query = update(Task).filter(Task.task_id == UUID(task_id)).values(status=TaskStatusEnum.success)
        result = session.execute(update_query)
        session.commit()

    if result.rowcount == 0:
        logger.error(f"Task with ID {task_id} not found")
