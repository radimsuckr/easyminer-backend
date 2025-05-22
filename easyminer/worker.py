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

app = Celery(
    "easyminer-backend-worker",
    broker=celery_broker,
    backend=celery_backend,
)
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


@before_task_publish.connect()
def before_task_publish_handler(body, exchange, routing_key, *args, **kwargs):
    logger = logging.getLogger(__name__)
    header_id: str = kwargs["headers"]["id"]
    logger.debug(f"Task {header_id} published")

    with get_sync_db_session() as session:
        query = (
            insert(Task)
            .values(task_id=header_id, name=header_id, status=TaskStatusEnum.pending)
            .returning(Task.task_id)
        )
        task_id = session.execute(query).scalar_one_or_none()
        session.commit()

    if not task_id:
        logger.error(f"Task {task_id} not saved")


@after_task_publish.connect()
def after_task_publish_handler(body, exchange: str | Exchange, routing_key, *args, **kwargs):
    logger = logging.getLogger(__name__)
    header_id: UUID = kwargs["headers"]["id"]
    logger.debug(f"Task {header_id} published")

    with get_sync_db_session() as session:
        # TODO: we should probably also check Redis if the task exists there
        query = (
            update(Task)
            .where(Task.task_id == header_id)
            .values(status=TaskStatusEnum.scheduled)
            .returning(Task.task_id)
        )
        task_id = session.execute(query).scalar_one_or_none()
        session.commit()

    if not task_id:
        logger.error(f"Task {header_id} not found")


@task_prerun.connect()
def task_prerun_handler(task_id: UUID, task, *args, **kwargs):
    logger = logging.getLogger(__name__)
    with get_sync_db_session() as session:
        update_query = (
            update(Task).filter(Task.task_id == task_id).values(status=TaskStatusEnum.started).returning(Task.task_id)
        )
        t_id = session.execute(update_query)
        session.commit()

    if not t_id:
        logger.error(f"Task with ID {t_id} not found")


@task_postrun.connect()
def task_postrun_handler(task_id: UUID, task, retval, state, *args, **kwargs):
    logger = logging.getLogger(__name__)
    with get_sync_db_session() as session:
        update_query = (
            update(Task).filter(Task.task_id == task_id).values(status=TaskStatusEnum.success).returning(Task.task_id)
        )
        t_id = session.execute(update_query)
        session.commit()

    if not t_id:
        logger.error(f"Task with ID {t_id} not found")
