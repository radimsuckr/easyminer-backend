import logging

from sqlalchemy import func, select
from sqlalchemy.orm import joinedload

from easyminer.database import get_sync_db_session
from easyminer.models.data import Field as DataSourceField
from easyminer.models.data import Instance as DataSourceInstance
from easyminer.models.preprocessing import Dataset
from easyminer.parser import Attribute, PmmlTaskParser
from easyminer.schemas.data import FieldType
from easyminer.worker import app

logger = logging.getLogger(__name__)


def apply_attribute_transform(attribute: Attribute, instance: DataSourceInstance):
    value = instance.value_nominal if instance.field.data_type == FieldType.nominal else instance.value_numeric
    if not value:
        raise ValueError(f"There is no value for instance ID {instance.id}")
    transformed = attribute.transform(value)
    return transformed


@app.task
def process_field(field_id: int, attributes: list[Attribute]):
    logger.info("Process field: %d %s", field_id, attributes)
    with get_sync_db_session() as db:
        instance_count = db.execute(
            select(func.count()).select_from(DataSourceInstance).where(DataSourceInstance.field_id == field_id)
        ).scalar_one()
        logger.info("N.O. instances %d", instance_count)
        instances = db.scalars(select(DataSourceInstance).where(DataSourceInstance.field_id == field_id)).all()
        for attr in attributes:
            for i in instances:
                transformed = [apply_attribute_transform(attr, i)]
                logger.info("Transformed %s", transformed, extra={"transformed": transformed})


def _get_dataset(dataset_id: int) -> Dataset | None:
    with get_sync_db_session() as db:
        return db.get(Dataset, dataset_id, options=[joinedload(Dataset.data_source)])


@app.task
def create_attribute(dataset_id: int, pmml: str):
    if len(pmml) == 0:
        raise ValueError("PMML cannot be empty")

    dataset = _get_dataset(dataset_id)
    if not dataset:
        raise ValueError(f"Dataset with id {dataset_id} not found")

    parser = PmmlTaskParser(pmml)
    attributes = parser.parse()

    field_attributes: dict[int, list[Attribute]] = {}
    with get_sync_db_session() as db:
        for attr in attributes:
            field = db.get(DataSourceField, attr.field_id)
            if not field:
                logger.warning("Data source field with ID %d not found", attr.field_id)
                continue
            if attr.field_id not in field_attributes:
                field_attributes[attr.field_id] = []
            field_attributes[attr.field_id].append(attr)

    for field_id, attributes in field_attributes.items():
        _ = process_field.delay(field_id, attributes)
